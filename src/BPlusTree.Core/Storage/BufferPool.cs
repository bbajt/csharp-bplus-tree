using System;
using System.Buffers.Binary;
using BPlusTree.Core.Api;

namespace BPlusTree.Core.Storage;

/// <summary>
/// Fixed-capacity buffer pool using the clock-hand (second-chance) LRU algorithm.
/// All public methods are thread-safe via a single lock.
///
/// LOCKING MODEL (Phase 31):
/// Pin and Unpin are both fully locked — acquiring _lock on every call.
/// TryClaimForEviction is lock-free — uses Interlocked.CompareExchange on _isEvicting
/// as the authoritative claim point, with a mandatory post-CAS PinCount re-check.
///
/// COLD-PATH LOADING PROTOCOL (Phase 31):
/// FindVictim() claims the selected frame with TrySetEvicting() before returning.
/// Pin()/NewPage() cold paths re-claim (TrySetEvicting) after frame.Reset() clears
/// the flag, then release (ClearEvicting) after frame.Pin(). This closes the race
/// window where lock-free TryClaimForEviction could claim a frame currently being
/// loaded by Pin()'s cold path (which holds _lock throughout).
///
/// OccupancyFraction remains lock-free via volatile _occupiedCount, which is
/// maintained under _lock at every _pageIndex modification site and read with
/// volatile semantics for lock-free access by EvictionWorker and FetchPage.
/// </summary>
internal sealed class BufferPool
{
    // readonly: TryClaimForEviction reads _frames without _lock (Phase 31 lock-free scan).
    // If pool resizing is ever needed, all lock-free _frames accesses must be audited first.
    // See PHASE-31.MD Known Failure Point #3.
    private readonly Frame[]              _frames;
    private readonly Dictionary<uint,int> _pageIndex; // pageId → frame index
    private readonly StorageFile          _storage;
    private int          _clockHand;
    private volatile int _occupiedCount; // mirrors _pageIndex.Count; lock-free read
    private readonly object _lock = new();

    // Eviction signalling — used by EvictionWorker (Phase 26b).
    // _evictSignal: FetchPage wakes the worker when pool crosses HWM.
    // _evictDoneSignal: ReleaseEvictedFrame wakes waiting FetchPage calls.
    private readonly SemaphoreSlim _evictSignal     = new(0, 1);
    private readonly SemaphoreSlim _evictDoneSignal = new(0, 1);

    // CAS gate for SignalEviction() — Phase 32.
    // 0 = not pending; 1 = pending (semaphore already released).
    // Atomically set 0→1 in SignalEviction(); cleared by EvictionWorker via
    // ResetEvictPending() immediately before each _evictSignal.Wait().
    // volatile: read lock-free by SignalEviction(); both parties write via Interlocked.
    // Default int value (0) is the correct initial state — "not pending".
    private volatile int _evictPending;

    // Hit/miss counters — incremented inside Pin() under _lock; read via Interlocked for
    // thread-safe access outside the lock.
    private long _hitCount;
    private long _missCount;

    public int Capacity  => _frames.Length;
    public int PageSize  => _storage.PageSize;

    public BufferPool(StorageFile storage, int capacity)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _frames = new Frame[capacity];
        _pageIndex = new Dictionary<uint, int>();
        _clockHand = 0;

        for (int i = 0; i < capacity; i++)
        {
            _frames[i] = new Frame(_storage.PageSize);
        }
    }

    /// <summary>
    /// Fetch a page into the pool and pin it.
    ///
    /// All callers go through lock(_lock), serialising Pin with TryClaimForEviction.
    /// This eliminates the ABA window from Phase 28's lock-free hot path where
    /// EvictionWorker could claim a frame between TryGetValue and AtomicIncrementPin.
    ///
    /// Hot path (page already in pool):
    ///   lock(_lock) → TryGetValue → IsEvicting check → Pin() → ReferenceBit = true → return.
    ///   No ABA window: _pageIndex lookup and PinCount increment are atomic with respect
    ///   to any concurrent TryClaimForEviction (both hold _lock).
    ///
    /// Cold path (page not in pool or frame is evicting):
    ///   Find victim, load from disk. If no victim available (all EVICTING), spin-wait.
    /// </summary>
    public Frame Pin(uint pageId)
    {
        while (true)
        {
            lock (_lock)
            {
                if (_pageIndex.TryGetValue(pageId, out int frameIndex))
                {
                    var frame = _frames[frameIndex];
                    if (frame.IsEvicting)
                    {
                        // EvictionWorker holds this frame. Release lock and yield
                        // so the writer can call ReleaseEvictedFrame, then retry.
                        goto spin;
                    }
                    // Page is in pool and not being evicted — pin under lock.
                    //
                    // MUST use AtomicIncrementPin (Interlocked/LOCK XADD) rather than
                    // frame.Pin() (_pinCount++) here. The lock-free evictor reads _pinCount
                    // WITHOUT _lock. LOCK XADD flushes the store buffer immediately, making
                    // _pinCount=1 visible to all cores before the next instruction.
                    //
                    // x86 TSO ordering guarantee (Phase 31):
                    //   LOCK XADD commits _pinCount=1 (store A).
                    //   Any subsequent store (e.g. ClearEvicting) is committed after store A.
                    //   If the evictor reads _isEvicting=0 (the later store), x86 TSO guarantees
                    //   it also sees _pinCount=1 (the earlier store) — pre-check fails → no CAS.
                    //
                    // A plain _pinCount++ goes through the store buffer; another core may read 0
                    // from cache before the store commits, causing the evictor to pass the
                    // pre-check and return true from TryClaimForEviction while we still hold
                    // the frame — leading to ReleaseEvictedFrame corrupting the frame under us.
                    frame.AtomicIncrementPin(); // LOCK XADD — _pinCount=1 committed immediately

                    // Re-check IsEvicting AFTER AtomicIncrementPin (still under _lock).
                    //
                    // Race: the lock-free evictor can read _pinCount=0 (before LOCK XADD) AND
                    // succeed the CAS before LOCK XADD executes. In that case the post-CAS check
                    // (after LOCK CMPXCHG) sees _pinCount=0 (LOCK XADD not done yet) → returns
                    // true. This re-check detects the resulting IsEvicting=true and rolls back,
                    // so Pin() never returns a frame that the evictor believes it owns.
                    // After rollback (PinCount=0), the evictor safely evicts, and the next Pin()
                    // retry takes the cold path and reloads the page into a different frame.
                    if (frame.IsEvicting)
                    {
                        frame.Unpin(); // roll back: _pinCount-- (safe under _lock)
                        goto spin;
                    }

                    frame.ReferenceBit = true;
                    Interlocked.Increment(ref _hitCount);
                    return frame;
                }
                else
                {
                    // Page not in pool - find victim and load from disk
                    int victimIndex = FindVictim();
                    if (victimIndex < 0)
                        goto spin; // EVICTING frames are blocking; wait for ReleaseEvictedFrame

                    var frame = _frames[victimIndex];

                    // If victim was dirty, flush it to disk first
                    if (frame.IsDirty)
                    {
                        BinaryPrimitives.WriteUInt64BigEndian(
                            frame.Data.AsSpan(PageLayout.PageLsnOffset, sizeof(ulong)),
                            frame.PageLsn);
                        PageChecksum.Stamp(frame.Data);
                        _storage.WritePage(frame.PageId, frame.Data);
                    }

                    // Load new page into victim frame.
                    // frame.Reset() calls ClearEvicting(), clearing the TrySetEvicting claim
                    // that FindVictim() set. Re-claim immediately: PageId=NullPageId after Reset
                    // means TryClaimForEviction's Guard 1 returns false, so the re-claim CAS
                    // succeeds unconditionally. IsEvicting=true is then maintained through
                    // SetPageId and ReadPage (Guard 3 blocks any concurrent evictor attempt).
                    // ClearEvicting() is called after frame.Pin() — at that point PinCount=1,
                    // so TryClaimForEviction's Guard 2 (PinCount > 0) protects the frame.
                    frame.Reset();
                    frame.TrySetEvicting(); // re-claim: NullPageId → Guard 1 blocks evictor
                    frame.SetPageId(pageId);
                    _storage.ReadPage(pageId, frame.Data);
                    PageChecksum.Verify(frame.Data, pageId);
                    frame.PageLsn = BinaryPrimitives.ReadUInt64BigEndian(
                        frame.Data.AsSpan(PageLayout.PageLsnOffset, sizeof(ulong)));
                    frame.Pin();
                    frame.ClearEvicting(); // PinCount=1 now — Guard 2 protects from here
                    frame.ReferenceBit = true;

                    // Update page index
                    _pageIndex[pageId] = victimIndex;
                    _occupiedCount++;

                    Interlocked.Increment(ref _missCount);
                    return frame;
                }
            }
            spin:
            // Briefly yield — EvictionWorker will complete WritePage and call
            // ReleaseEvictedFrame, removing the page from _pageIndex.
            Thread.Sleep(0);
        }
    }

    /// <summary>
    /// Release one pin on pageId. Thread-safe.
    ///
    /// Fully locked: acquires _lock on every call, consistent with Pin.
    /// Since Pin, TryClaimForEviction, and FindVictim all hold _lock, no concurrent
    /// mutation of frame state is possible while Unpin holds the lock — the slow-path
    /// re-checks from the old semi-lock-free design are unnecessary.
    ///
    /// ReferenceBit is set before Unpin() per Fix 1 (PHASE-29.MD):
    ///   TryClaimForEviction checks (PinCount == 0 AND ReferenceBit == false).
    ///   Setting the bit while PinCount > 0 ensures TryClaimForEviction cannot
    ///   claim the frame in the moment PinCount transitions to 0.
    ///
    /// frame.Unpin() is non-atomic (plain _pinCount--) — safe under _lock.
    /// Throws InvalidOperationException on double-unpin (PinCount already 0).
    /// </summary>
    public void Unpin(uint pageId, bool isDirty = false)
    {
        lock (_lock)
        {
            if (!_pageIndex.TryGetValue(pageId, out int frameIndex))
                throw new InvalidOperationException(
                    $"Unpin({pageId}): page not in buffer pool. " +
                    "This is a double-unpin or unpin of a page that was never pinned.");

            var frame = _frames[frameIndex];

            // Fix 1 (PHASE-29.MD): set ReferenceBit before decrementing PinCount.
            frame.ReferenceBit = true;

            if (frame.PinCount <= 0)
                throw new InvalidOperationException(
                    $"Unpin({pageId}): PinCount is already {frame.PinCount} — double-unpin detected. " +
                    "Each FetchPage/AllocatePage must have exactly one matching Unpin.");

            frame.Unpin();  // plain _pinCount-- (non-atomic, safe under _lock)

            if (isDirty) frame.IsDirty = true;
        }
    }

    /// <summary>
    /// Returns the current pin count for <paramref name="pageId"/>, or 0 if the page is not
    /// in the buffer pool. Used by <see cref="PageManager.FreePage"/> to decide whether to
    /// defer reclamation. Thread-safe.
    /// </summary>
    internal int GetPinCount(uint pageId)
    {
        lock (_lock)
            return _pageIndex.TryGetValue(pageId, out int idx) ? _frames[idx].PinCount : 0;
    }

    /// <summary>
    /// Write a dirty frame to disk if its dirty flag is set. Clear dirty flag.
    /// Throws if pageId not in pool.
    /// </summary>
    public void FlushPage(uint pageId)
    {
        lock (_lock)
        {
            if (!_pageIndex.TryGetValue(pageId, out int frameIndex))
            {
                throw new ArgumentException("Page not in buffer pool", nameof(pageId));
            }

            var frame = _frames[frameIndex];
            if (frame.IsDirty)
            {
                BinaryPrimitives.WriteUInt64BigEndian(
                    frame.Data.AsSpan(PageLayout.PageLsnOffset, sizeof(ulong)),
                    frame.PageLsn);
                PageChecksum.Stamp(frame.Data);
                _storage.WritePage(pageId, frame.Data);
                frame.IsDirty = false;
            }
        }
    }

    /// <summary>
    /// Flush all dirty frames to disk. Does NOT fsync.
    /// </summary>
    public void FlushAllDirty()
    {
        lock (_lock)
        {
            for (int i = 0; i < _frames.Length; i++)
            {
                var frame = _frames[i];
                if (frame.IsDirty)
                {
                    BinaryPrimitives.WriteUInt64BigEndian(
                        frame.Data.AsSpan(PageLayout.PageLsnOffset, sizeof(ulong)),
                        frame.PageLsn);
                    PageChecksum.Stamp(frame.Data);
                    _storage.WritePage(frame.PageId, frame.Data);
                    frame.IsDirty = false;
                }
            }
        }
    }

    /// <summary>
    /// Install a brand-new (already written) page into the pool without loading from disk.
    /// Used by PageManager after allocating a new page.
    /// </summary>
    public Frame NewPage(uint pageId)
    {
        while (true)
        {
            lock (_lock)
            {
                int victimIndex = FindVictim();
                if (victimIndex < 0)
                    goto spin; // EVICTING frames are blocking; wait for ReleaseEvictedFrame

                var frame = _frames[victimIndex];

                // If victim was dirty, flush it to disk first
                if (frame.IsDirty)
                {
                    BinaryPrimitives.WriteUInt64BigEndian(
                        frame.Data.AsSpan(PageLayout.PageLsnOffset, sizeof(ulong)),
                        frame.PageLsn);
                    PageChecksum.Stamp(frame.Data);
                    _storage.WritePage(frame.PageId, frame.Data);
                }

                // Install new page into victim frame.
                // Same re-claim protocol as Pin() cold path (see comment there):
                // Reset clears the FindVictim TrySetEvicting claim; re-claim before SetPageId.
                frame.Reset();
                frame.TrySetEvicting(); // re-claim while PageId=NullPageId (Guard 1 blocks evictor)
                frame.SetPageId(pageId);
                frame.Pin();
                frame.ClearEvicting(); // PinCount=1 now — Guard 2 protects from here
                frame.IsDirty = true; // Newly allocated pages are dirty

                // Update page index
                _pageIndex[pageId] = victimIndex;
                _occupiedCount++;

                return frame;
            }
            spin:
            Thread.Sleep(0);
        }
    }

    /// <summary>
    /// Evicts all frames without flushing. All dirty pages must be flushed before calling this.
    /// Used after an atomic file rename to invalidate the entire stale buffer pool.
    /// </summary>
    internal void EvictAll()
    {
        lock (_lock)
        {
            _pageIndex.Clear();
            _occupiedCount = 0;
            for (int i = 0; i < _frames.Length; i++)
                _frames[i].Reset();
            _clockHand = 0;
        }
    }

    // ── Async eviction support (Phase 26a infrastructure) ─────────────────────

    /// <summary>
    /// Approximate fraction of frames currently holding a page (FREE frames excluded).
    /// Used by EvictionWorker to decide whether to continue evicting.
    /// </summary>
    public double OccupancyFraction
    {
        // _occupiedCount is volatile — lock-free read safe for EvictionWorker
        // and FetchPage (no lock held). Updated under _lock at every _pageIndex
        // modification site; volatile ensures visibility across threads.
        get => (double)_occupiedCount / _frames.Length;
    }

    /// <summary>Total buffer pool hits (page found in pool) since this instance was created.</summary>
    public long HitCount  => Interlocked.Read(ref _hitCount);

    /// <summary>Total buffer pool misses (page loaded from disk) since this instance was created.</summary>
    public long MissCount => Interlocked.Read(ref _missCount);

    /// <summary>
    /// Number of frames currently holding dirty (unflushed) pages.
    /// Approximate — scanned without a lock; suitable for diagnostics only.
    /// </summary>
    public int DirtyCount
    {
        get
        {
            int count = 0;
            foreach (ref readonly var frame in _frames.AsSpan())
                if (frame.IsDirty) count++;
            return count;
        }
    }

    /// <summary>
    /// Signal the eviction worker (non-blocking, idempotent, exception-free).
    ///
    /// Phase 30 used try/catch on SemaphoreFullException as a TOCTOU guard.
    /// That was correct for correctness but threw ~945K exceptions per compaction
    /// iteration (Phase 31 benchmark: ~2.8 s + ~945 MB allocation overhead).
    ///
    /// Phase 32 fix: CAS _evictPending 0→1.  Exactly one thread wins the CAS and
    /// calls Release(1).  All other concurrent callers see 1 already and return
    /// immediately — no semaphore call, no exception path.  The SemaphoreSlim(0,1)
    /// capacity is never exceeded because only the CAS winner calls Release().
    /// </summary>
    public void SignalEviction()
    {
        // Atomically transition _evictPending from 0 → 1.
        // If the previous value was already 1 (another thread already signalled),
        // the CAS fails — return immediately. No semaphore call, no exception.
        if (Interlocked.CompareExchange(ref _evictPending, 1, 0) != 0)
            return; // already pending — EvictionWorker will wake shortly

        // We won the CAS: _evictPending is now 1 and we are the sole caller of Release().
        // SemaphoreSlim(0,1) has capacity for exactly one release; since we are the only
        // caller (CAS ensures this), SemaphoreFullException cannot be thrown.
        _evictSignal.Release(1);
    }

    /// <summary>
    /// Clears _evictPending before each EvictionWorker wait.
    /// Must be called immediately before _evictSignal.Wait() — clearing after creates
    /// a window where a new signal is lost (see PHASE-32.MD Failure Point #1).
    /// Uses Interlocked.Exchange (unconditional atomic write with full barrier) rather
    /// than a plain write so the clear is immediately visible to all cores.
    /// </summary>
    internal void ResetEvictPending()
        => Interlocked.Exchange(ref _evictPending, 0);

    /// <summary>
    /// Direct access to the frame array for EvictionWorker's clock-hand sweep.
    /// </summary>
    internal Frame[] Frames => _frames;

    /// <summary>
    /// Returns the semaphore the EvictionWorker waits on.
    /// </summary>
    internal SemaphoreSlim EvictSignal     => _evictSignal;

    /// <summary>
    /// Returns the semaphore FetchPage waits on for a freed frame.
    /// </summary>
    internal SemaphoreSlim EvictDoneSignal => _evictDoneSignal;

    /// <summary>
    /// Test seam — current count of _evictSignal (Phase 32).
    /// Used by SignalEviction_ConcurrentCalls_ExactlyOneReleasePerSignal to verify
    /// the CAS gate prevents count > 1. Not part of the public API.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    internal int EvictSignalCount => _evictSignal.CurrentCount;

    /// <summary>
    /// Attempt to claim the frame at <paramref name="frameIndex"/> for eviction.
    /// Called exclusively by <see cref="EvictionWorker"/> during clock-hand sweep.
    ///
    /// Lock-free design (Phase 31):
    ///   Pre-checks (PageId, PinCount, IsEvicting, ReferenceBit) are OPTIMISTIC — they
    ///   may read stale values. A stale read causes a spurious false return (frame skipped
    ///   this sweep), which is acceptable: the clock hand will revisit on the next pass.
    ///
    ///   <see cref="Frame.TrySetEvicting"/> (Interlocked.CompareExchange) is the authoritative
    ///   claim point. Exactly one concurrent caller succeeds.
    ///
    ///   POST-CAS re-check on PinCount is MANDATORY — it detects the window where Pin()
    ///   incremented PinCount after our pre-check but before our CAS (see PHASE-31.MD
    ///   Known Failure Point #1). The CAS's full memory barrier ensures the post-CAS read
    ///   observes any PinCount write that happened before Pin() released _lock.
    ///
    /// Returns true if the frame was successfully claimed (IsEvicting=true, PinCount=0).
    /// Returns false in all other cases. Caller must call ReleaseEvictedFrame() after
    /// writing the page to disk.
    ///
    /// _frames is readonly — safe to access without _lock (see PHASE-31.MD Failure Point #3).
    /// ReferenceBit reads/writes are approximate — best-effort clock hint, no lock needed
    /// (see PHASE-31.MD for x64 memory model rationale).
    /// </summary>
    public bool TryClaimForEviction(int frameIndex)
    {
        // _frames is readonly — stable reference, safe to access without _lock.
        var frame = _frames[frameIndex];

        // ── Optimistic pre-checks (reads may be stale — see PHASE-31.MD Known Failure Point #1) ──

        // Guard: frame must hold a valid page (not FREE)
        if (frame.PageId == PageLayout.NullPageId) return false;

        // Guard: frame must not be pinned
        // (stale read: if Pin() incremented PinCount after this read, post-CAS check catches it)
        if (frame.PinCount > 0) return false;

        // Guard: frame must not already be claimed for eviction
        if (frame.IsEvicting) return false;

        // Second-chance clock — best-effort approximation (no lock, stale reads acceptable).
        // If ReferenceBit is true, frame was recently accessed; grant one more sweep.
        // Clear the bit so the NEXT sweep can evict if still unused.
        //
        // NOTE: ReferenceBit is read/written without _lock. On x64 (TSO), single-byte writes
        // are atomic; torn reads cannot occur. Approximation: a concurrent Unpin() may have set
        // ReferenceBit=true under _lock just before this read. If our stale read sees false, the
        // frame may be evicted one sweep earlier than ideal — an eviction-quality approximation,
        // not a correctness issue. The page is always written to disk before the frame is freed,
        // and WAL guarantees recoverability. Do NOT add Volatile.Read here — cost ≈ benefit
        // (see PHASE-31.MD Known Failure Point #4).
        if (frame.ReferenceBit)
        {
            frame.ReferenceBit = false; // plain write — no lock, no barrier needed for correctness
            return false;
        }

        // ── Atomic claim via CAS — the authoritative serialisation point ──────────────────────
        //
        // TrySetEvicting() calls Interlocked.CompareExchange(_isEvicting, 1, 0).
        // Exactly one concurrent caller will succeed. After CAS, no other TryClaimForEviction
        // can claim this frame, and Pin() will see IsEvicting=true and take the cold path.
        if (!frame.TrySetEvicting())
            return false; // another thread won the CAS race

        // ── Post-CAS correctness check — REQUIRED, not optional ──────────────────────────────
        //
        // Window: Pin() acquires _lock AFTER our PinCount pre-check but BEFORE our CAS.
        //   Pin():          _lock → IsEvicting=false → _pinCount++ → _lock released
        //   [our CAS here]: TrySetEvicting() succeeds — but frame._pinCount is now 1
        //
        // After the CAS (a full memory barrier on x64), the _pinCount write from Pin()'s
        // _lock release (Monitor.Exit release fence) is guaranteed visible here.
        // If PinCount > 0, Pin() won — roll back our claim.
        // ClearEvicting() is MANDATORY: omitting it leaks the frame in EVICTING state forever,
        // shrinking the effective pool until BufferPoolExhaustedException (see PHASE-31.MD
        // Known Failure Point #2).
        if (frame.PinCount > 0)
        {
            frame.ClearEvicting(); // roll back: _isEvicting ← false
            return false;
        }

        // Frame is ours: IsEvicting=true, PinCount=0, PageId is valid.
        // EvictionWorker will call WritePage() then ReleaseEvictedFrame().
        return true;
    }

    /// <summary>
    /// Release a frame that was in EVICTING state back to FREE.
    /// Removes pageId from the page index, resets the frame, and signals any FetchPage
    /// calls waiting for a free frame.
    /// Must be called only after the dirty-page write (if any) has completed.
    /// </summary>
    public void ReleaseEvictedFrame(int frameIndex)
    {
        lock (_lock)
        {
            var frame = _frames[frameIndex];
            if (_pageIndex.Remove(frame.PageId))
                _occupiedCount--;
            frame.Reset();    // clears PageId, IsDirty, IsEvicting, PinCount, Data
        }

        // Signal outside the lock — FetchPage waiters can proceed
        if (_evictDoneSignal.CurrentCount == 0)
            _evictDoneSignal.Release(1);
    }

    /// <summary>
    /// Returns the frame index for a given page, or -1 if not in pool.
    /// Internal — used by tests and EvictionWorker.
    /// </summary>
    internal int GetFrameIndex(uint pageId)
    {
        lock (_lock)
        {
            return _pageIndex.TryGetValue(pageId, out int idx) ? idx : -1;
        }
    }

    /// <summary>
    /// Returns the Frame for a given page, or null if not in pool.
    /// Test seam — not part of the public contract.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    internal Frame? GetFrameByPageId(uint pageId)
    {
        lock (_lock)
        {
            return _pageIndex.TryGetValue(pageId, out int idx) ? _frames[idx] : null;
        }
    }

    /// <summary>
    /// Returns the frame index for a given page, or -1 if not in pool.
    /// Test seam — not part of the public contract.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    internal int GetFrameIndexByPageId(uint pageId)
    {
        lock (_lock)
        {
            return _pageIndex.TryGetValue(pageId, out int idx) ? idx : -1;
        }
    }

    /// <summary>
    /// Returns a frame index to use as a victim, or -1 if no victim is available right now
    /// because some frames are in EVICTING state (caller should spin-wait and retry).
    /// Throws BufferPoolExhaustedException only when every frame is truly pinned with no
    /// EVICTING frames that could free up shortly.
    /// </summary>
    private int FindVictim()
    {
        int totalChecks = 0;
        int maxChecks = _frames.Length * 2; // Two full sweeps at most
        bool hasEvicting = false;

        while (totalChecks < maxChecks)
        {
            int candidateIndex = _clockHand;
            _clockHand = (_clockHand + 1) % _frames.Length;
            totalChecks++;

            var frame = _frames[candidateIndex];

            if (frame.IsEvicting)
            {
                hasEvicting = true;
                continue; // Being written by EvictionWorker — will be FREE soon
            }

            if (frame.IsPinned)
                continue; // In use by a reader/writer — cannot evict

            if (frame.ReferenceBit)
            {
                frame.ReferenceBit = false; // Second chance: clear and skip
                continue;
            }

            // Unpinned and reference bit clear — attempt to claim before returning.
            // TryClaimForEviction (lock-free) can race here: it reads _frames without _lock
            // and could CAS _isEvicting before we do. Whoever wins the CAS owns the frame.
            if (!frame.TrySetEvicting())
                continue; // evictor won this frame — try the next one

            // We own the frame. Remove old mapping from _pageIndex (if any).
            if (frame.PageId != PageLayout.NullPageId)
            {
                _pageIndex.Remove(frame.PageId);
                _occupiedCount--;
            }

            return candidateIndex;
        }

        // If EVICTING frames were seen, they will become FREE once the EvictionWorker
        // calls ReleaseEvictedFrame. Signal the caller to spin-wait rather than throw.
        if (hasEvicting)
            return -1;

        throw new BufferPoolExhaustedException(
            "Buffer pool exhausted: all frames are pinned and cannot be evicted.");
    }
}