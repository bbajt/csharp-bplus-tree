using System.Collections.Concurrent;
using System.Linq;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Storage;

namespace ByTech.BPlusTree.Core.Engine;

/// <summary>
/// Allocates monotonically increasing transaction IDs and manages the page-level
/// write lock table for concurrent transaction conflict detection. Thread-safe.
///
/// TxId 0 is reserved for auto-commit (single-op) writes and is never allocated.
///
/// Locking protocol: no-wait. If a page is already locked by a different transaction,
/// <see cref="AcquirePageWriteLock"/> throws <see cref="TransactionConflictException"/>
/// immediately. There is no waiting or deadlock detection — callers must abort and retry.
/// </summary>
internal sealed class TransactionCoordinator : IDisposable
{
    private uint _nextTxId = 1;

    // Page-level write lock table: pageId → txId that holds the write lock.
    // Protected by _lockGuard. Acquired in CaptureBeforeImage; released on Commit/rollback.
    private readonly Dictionary<uint, uint> _pageLocks = new();
    private readonly object                 _lockGuard  = new();

    // ── LOCK ORDERING ─────────────────────────────────────────────────────────
    // When both locks must be held simultaneously, ALWAYS acquire in this order
    // to prevent deadlock:
    //   1. _checkpointLock  (ReaderWriterLockSlim — read or write)
    //   2. _commitMutex     (SemaphoreSlim — always exclusive)
    //
    // CheckpointManager.TakeCheckpointCore() acquires _checkpointLock (write),
    // then operations inside the critical section may call EnterWriterLock().
    // Transaction.Commit() acquires _commitMutex via EnterWriterLock(); it never
    // directly acquires _checkpointLock — it blocks externally via EnterTransactionLock().
    // ─────────────────────────────────────────────────────────────────────────

    // Checkpoint/transaction mutual exclusion gate.
    // Transactions enter with a read lock (shared); checkpoints enter with a write lock (exclusive).
    // This guarantees that TruncateWal is never called while a transaction is in-flight.
    // SupportsRecursion: same thread may hold multiple read locks (multiple concurrent transactions).
    // A write-lock holder (checkpoint) can never hold a read lock on the same thread, so
    // the upgrade restriction (read→write) is irrelevant here.
    private readonly ReaderWriterLockSlim _checkpointLock = new(LockRecursionPolicy.SupportsRecursion);

    // Commit-critical-section mutex (Phase 109a).
    // Serialises shadow root installation (ApplyCoWTxCommit) and WAL Commit record
    // append across concurrent transactions. Held only during the commit critical
    // section (~microseconds); NOT held for the full transaction lifetime.
    // Compactor also acquires via EnterWriterLock for Phase A setup and Phase B swap.
    //
    // _txWriterDepth tracks per-thread depth for reentrancy: a thread committing a
    // nested transaction does not block if it already holds this mutex (depth > 0).
    private readonly SemaphoreSlim    _commitMutex    = new(1, 1);
    private readonly ThreadLocal<int> _txWriterDepth  = new(() => 0);

    // ── Compaction barrier (Phase 109a deadlock fix) ──────────────────────────
    // Set by Compactor at the start of Phase B to quiesce in-flight transactions
    // before Phase B acquires the commit mutex (preventing the checkpoint-lock /
    // commit-mutex circular deadlock introduced by Phase 109a).
    //
    // _activeTxCount counts transactions that have called EnterTransactionLock
    // but not yet called ExitTransactionLock (i.e. are between constructor and
    // Commit/Dispose). Interlocked: no contention with the commit mutex itself.
    private int _compactionBarrier;  // 0 = open, 1 = draining; accessed via Volatile.Read/Write
    private int _activeTxCount;      // Interlocked

    /// <summary>
    /// Raise the compaction barrier. Subsequent <see cref="CheckCompactionBarrier"/>
    /// calls from <c>BeginTransaction</c> will throw <see cref="InvalidOperationException"/>
    /// until <see cref="ClearCompactionBarrier"/> is called.
    /// Called by <see cref="Compactor{TKey,TValue}"/> at the start of Phase B.
    /// </summary>
    public void SetCompactionBarrier()   => Volatile.Write(ref _compactionBarrier, 1);

    /// <summary>
    /// Lower the compaction barrier. Called from the Compactor finally block.
    /// </summary>
    public void ClearCompactionBarrier() => Volatile.Write(ref _compactionBarrier, 0);

    /// <summary>
    /// Throw <see cref="InvalidOperationException"/> if the compaction barrier is raised.
    /// Called by <c>BeginTransaction</c> before allocating a txId, so the Transaction
    /// constructor never runs while Phase B is in progress.
    /// </summary>
    public void CheckCompactionBarrier()
    {
        if (Volatile.Read(ref _compactionBarrier) != 0)
            throw new CompactionInProgressException();
    }

    /// <summary>
    /// Increment the count of transactions currently between constructor and Commit/Dispose.
    /// Called from the Transaction constructor immediately after <see cref="EnterTransactionLock"/>.
    /// </summary>
    public void IncrementActiveTx() => Interlocked.Increment(ref _activeTxCount);

    /// <summary>
    /// Decrement the active-transaction count.
    /// Called from <c>Transaction.Commit()</c> and <c>Transaction.Dispose()</c>
    /// (rollback path) immediately before <see cref="ExitTransactionLock"/>.
    /// </summary>
    public void DecrementActiveTx() => Interlocked.Decrement(ref _activeTxCount);

    /// <summary>Number of transactions currently open (began but not yet committed or rolled back).</summary>
    public int ActiveTransactionCount => Volatile.Read(ref _activeTxCount);

    /// <summary>Number of snapshots currently open on this tree.</summary>
    public int ActiveSnapshotCount => Volatile.Read(ref _snapshotCount);

    /// <summary>
    /// Returns <c>true</c> if the calling thread currently holds the commit writer lock.
    /// Used by <c>Debug.Assert</c> in callers that must run under the commit mutex.
    /// </summary>
    internal bool IsWriterLockHeld => _txWriterDepth.Value > 0;

    /// <summary>
    /// Spin until <see cref="_activeTxCount"/> drops to zero, meaning all transactions
    /// that were in-flight when <see cref="SetCompactionBarrier"/> was called have
    /// completed (committed or rolled back). Because <see cref="CheckCompactionBarrier"/>
    /// prevents new transactions from starting, the count can only decrease once the
    /// barrier is raised.
    /// Called by <see cref="Compactor{TKey,TValue}"/> Phase B before acquiring the
    /// commit mutex, so that <see cref="EnterWriterLock"/> is guaranteed contention-free.
    /// </summary>
    public void WaitForTxQuiescence()
    {
        var spinner = new SpinWait();
        while (Volatile.Read(ref _activeTxCount) > 0)
            spinner.SpinOnce();
    }

    /// <summary>
    /// Returns the next available transaction ID (≥ 1). Never returns 0.
    /// </summary>
    public uint Allocate() => Interlocked.Increment(ref _nextTxId) - 1;

    /// <summary>
    /// Attempt to acquire an exclusive write lock on <paramref name="pageId"/> for
    /// <paramref name="txId"/>. No-wait protocol: throws immediately if the page is
    /// already locked by a different transaction.
    ///
    /// Idempotent: if <paramref name="txId"/> already holds the lock, returns silently.
    /// </summary>
    /// <exception cref="TransactionConflictException">
    /// The page is currently locked by a different transaction.
    /// </exception>
    public void AcquirePageWriteLock(uint pageId, uint txId)
    {
        lock (_lockGuard)
        {
            if (!_pageLocks.TryGetValue(pageId, out uint owner))
            {
                _pageLocks[pageId] = txId;
                return; // lock granted
            }
            if (owner == txId)
                return; // re-entrant: same transaction, no-op

            throw new TransactionConflictException(txId, owner, pageId);
        }
    }

    /// <summary>
    /// Release all write locks held by <paramref name="txId"/>.
    /// Called by <see cref="Transaction{TKey,TValue}.Commit"/> and
    /// <see cref="Transaction{TKey,TValue}.Dispose"/> (rollback path).
    /// Idempotent: safe to call even if no locks are held.
    /// </summary>
    public void ReleaseAllLocks(uint txId)
    {
        lock (_lockGuard)
        {
            var toRemove = _pageLocks
                .Where(kv => kv.Value == txId)
                .Select(kv => kv.Key)
                .ToList();
            foreach (uint pageId in toRemove)
                _pageLocks.Remove(pageId);
        }
    }

    /// <summary>
    /// Returns true when at least one transaction holds a page write lock.
    /// The auto-checkpoint thread uses this to defer checkpoints while a
    /// transaction is modifying pages — truncating the WAL during an active
    /// write would discard the transaction's before-image records, making
    /// crash recovery of that transaction impossible.
    /// Thread-safe; acquires _lockGuard internally.
    /// </summary>
    public bool HasActiveLocks
    {
        get { lock (_lockGuard) return _pageLocks.Count > 0; }
    }

    // ── Checkpoint / transaction mutual exclusion ────────────────────────────

    /// <summary>Enter the shared (transaction) side of the checkpoint gate.</summary>
    public void EnterTransactionLock()   => _checkpointLock.EnterReadLock();

    /// <summary>Exit the shared (transaction) side of the checkpoint gate.</summary>
    public void ExitTransactionLock()
    {
        _checkpointLock.ExitReadLock();
        SweepRetiredPages();
        PruneSsiLog();   // SSI Phase 89: prune stale retire-log entries
    }

    /// <summary>Enter the exclusive (checkpoint) side of the checkpoint gate.</summary>
    public void EnterCheckpointLock()    => _checkpointLock.EnterWriteLock();

    /// <summary>Exit the exclusive (checkpoint) side of the checkpoint gate.</summary>
    public void ExitCheckpointLock()     => _checkpointLock.ExitWriteLock();

    /// <summary>
    /// Acquire exclusive single-writer access for a CoW write path.
    /// Reentrant on the same thread: if the calling thread already holds the lock
    /// (depth > 0), increments the depth counter and returns immediately without
    /// blocking. Cross-thread callers block on the semaphore until depth returns to 0.
    /// </summary>
    public void EnterWriterLock()
    {
        if (_txWriterDepth.Value > 0) { _txWriterDepth.Value++; return; }
        _commitMutex.Wait();
        _txWriterDepth.Value = 1;
    }

    /// <summary>
    /// Release the single-writer CoW lock.
    /// Decrements the reentrant depth counter; only releases the underlying semaphore
    /// when the count reaches zero (i.e. the outermost holder exits).
    /// </summary>
    public void ExitWriterLock()
    {
        _txWriterDepth.Value--;
        if (_txWriterDepth.Value == 0)
            _commitMutex.Release();
    }

    // ── Lightweight reader count (allocation-free) ────────────────────────────────
    // Used by TryGet/Scan to prevent CoW page retirement mid-traversal without any
    // heap allocation.  Interlocked operations keep it lock-free.
    private int _activeReaderCount;

    /// <summary>
    /// Register a lightweight (non-epoch) reader traversal.
    /// Must be paired with <see cref="ExitReader"/>.
    /// Allocation-free; safe to call from hot TryGet / Scan paths.
    /// </summary>
    public void EnterReader() => Interlocked.Increment(ref _activeReaderCount);

    /// <summary>
    /// Deregister a lightweight reader traversal.
    /// Triggers <see cref="SweepRetiredPages"/> once the count reaches zero.
    /// </summary>
    public void ExitReader()
    {
        if (Interlocked.Decrement(ref _activeReaderCount) == 0)
            SweepRetiredPages();
    }

    // ── Epoch-based reader registry ──────────────────────────────────────────────

    // _epochCounter is incremented on every EnterReadEpoch() call — each reader
    // receives a unique, monotonically increasing epoch number.
    // _activeEpochs tracks all epochs whose readers have not yet called ExitReadEpoch.
    // OldestActiveEpoch is the safe reclamation boundary for the epoch-gated freelist (M+2).
    private ulong                     _epochCounter;
    private int                       _snapshotCount;
    private readonly SortedSet<ulong> _activeEpochs = new();
    private readonly object           _epochLock    = new();

    // ── Epoch-gated retired-page queue ────────────────────────────────────────────
    // Pages retired via RetirePage are queued with the epoch at retirement time.
    // SweepRetiredPages releases them to FreePage once no active reader's epoch
    // is ≤ their retire-epoch. The queue is ordered by retire-epoch (non-decreasing)
    // so a linear scan from the front suffices.
    //
    // _retiredPageCount is a volatile counter that mirrors _retiredPages.Count.
    // SweepRetiredPages reads it as a fast exit before acquiring _epochLock, eliminating
    // the monitor overhead (~25 ns) on every TryGet when the queue is empty (common case).
    private PageManager? _pageManager;
    private readonly Queue<(uint pageId, ulong retireEpoch)> _retiredPages = new();
    private volatile int _retiredPageCount;

    // ── SSI retire log (Phase 89) ─────────────────────────────────────────────
    // Maps pageId → retire epoch for all pages retired since the oldest active snapshot.
    // Populated alongside _retiredPages; pruned lazily when transaction epochs advance.
    // Used by Transaction.Commit() to detect read-write conflicts via FindConflictingPage.
    private readonly ConcurrentDictionary<uint, ulong> _ssiRetireLog = new();

    /// <summary>
    /// Register a new reader snapshot. Returns a monotonically increasing epoch
    /// number that the caller must pass to <see cref="ExitReadEpoch"/> when the
    /// snapshot is no longer needed.
    /// Thread-safe. May be called concurrently from multiple reader threads.
    /// </summary>
    public ulong EnterReadEpoch()
    {
        lock (_epochLock)
        {
            ulong epoch = ++_epochCounter;
            _activeEpochs.Add(epoch);
            _snapshotCount++;
            return epoch;
        }
    }

    /// <summary>
    /// Deregister a reader snapshot. Must be called exactly once for each epoch
    /// returned by <see cref="EnterReadEpoch"/>, after the caller has finished
    /// reading all pages pinned under that snapshot.
    /// Thread-safe. Idempotent: if the epoch is already absent, no-op.
    /// </summary>
    public void ExitReadEpoch(ulong epoch)
    {
        lock (_epochLock)
        {
            _activeEpochs.Remove(epoch);
            _snapshotCount--;
        }
        SweepRetiredPages();
    }

    /// <summary>
    /// The epoch of the oldest currently active reader, or <c>null</c> when no
    /// reader snapshot is open.
    ///
    /// The epoch-gated freelist (M+2) uses this as the reclamation boundary:
    /// a retired page with retire-epoch E may be freed when
    /// <c>OldestActiveEpoch == null || OldestActiveEpoch &gt; E</c>.
    /// Thread-safe.
    /// </summary>
    public ulong? OldestActiveEpoch
    {
        get { lock (_epochLock) return _activeEpochs.Count > 0 ? _activeEpochs.Min : (ulong?)null; }
    }

    /// <summary>
    /// Returns true when at least one <see cref="ISnapshot{TKey,TValue}"/> is currently open.
    /// Lock-free (~5 ns via Volatile.Read); safe to call from the hot insert/delete path.
    /// Auto-commit write paths use this to choose between the in-place fast path (no snapshot)
    /// and the CoW shadow path (snapshot open — frozen view must be preserved).
    /// </summary>
    public bool HasActiveSnapshots => Volatile.Read(ref _snapshotCount) > 0;

    /// <summary>
    /// Bind the PageManager used by <see cref="SweepRetiredPages"/> to release
    /// reclaimed pages. Called once from the TreeEngine constructor after both
    /// objects are constructed.
    /// </summary>
    public void SetPageManager(PageManager pageManager)
        => _pageManager = pageManager;

    /// <summary>
    /// Queue <paramref name="pageId"/> for deferred reclamation.
    /// The page will be passed to <see cref="PageManager.FreePage"/> once all
    /// active readers whose epoch is ≤ the current retire-epoch have exited.
    /// If no readers are currently active, the page is freed immediately.
    /// Thread-safe.
    /// </summary>
    public void RetirePage(uint pageId)
    {
        lock (_epochLock)
        {
            _retiredPages.Enqueue((pageId, _epochCounter));
            _ssiRetireLog[pageId] = _epochCounter;   // SSI Phase 89: record retire epoch
        }
        Interlocked.Increment(ref _retiredPageCount);
        SweepRetiredPages();
    }

    /// <summary>
    /// Queue a batch of pages for deferred reclamation and sweep once.
    /// Preferable to calling <see cref="RetirePage"/> in a loop because it
    /// acquires <c>_epochLock</c> once and triggers at most one sweep,
    /// keeping per-write heap allocation near zero.
    /// </summary>
    public void RetirePages(ReadOnlySpan<uint> pageIds, int count)
    {
        lock (_epochLock)
        {
            ulong epoch = _epochCounter;
            for (int i = 0; i < count; i++)
            {
                _retiredPages.Enqueue((pageIds[i], epoch));
                _ssiRetireLog[pageIds[i]] = epoch;   // SSI Phase 89: record retire epoch
            }
        }
        Interlocked.Add(ref _retiredPageCount, count);
        SweepRetiredPages();
    }

    /// <summary>
    /// Reclaim all retired pages whose retire-epoch is strictly less than the
    /// oldest active reader's epoch (or all pages if no readers are active).
    /// FreePage only performs in-memory buffer-pool operations (no disk I/O), so
    /// it is safe to call while _epochLock is held, eliminating the List allocation.
    /// </summary>
    private void SweepRetiredPages()
    {
        if (_pageManager == null) return;
        // Defer all reclamation while lightweight readers (TryGet/Scan) are active.
        if (Volatile.Read(ref _activeReaderCount) > 0) return;
        // Fast exit: skip the monitor acquisition when the queue is provably empty.
        // _retiredPageCount is incremented before SweepRetiredPages is called from
        // RetirePage/RetirePages, so a zero read here is never a false negative.
        if (_retiredPageCount == 0) return;

        lock (_epochLock)
        {
            if (_retiredPages.Count == 0) return;
            ulong? oldest = _activeEpochs.Count > 0 ? _activeEpochs.Min : (ulong?)null;
            while (_retiredPages.TryPeek(out var entry))
            {
                // Safe to free when no readers are active, or the oldest active reader
                // entered after this page was retired (they can never have seen it).
                if (oldest.HasValue && oldest.Value <= entry.retireEpoch) break;
                _retiredPages.Dequeue();
                Interlocked.Decrement(ref _retiredPageCount);
                _pageManager.FreePage(entry.pageId);
            }
        }
    }

    // ── SSI Phase 89: read-write conflict detection ───────────────────────────

    /// <summary>
    /// Scan <paramref name="readSet"/> against the SSI retire log.
    /// Returns the first page whose retire epoch is strictly greater than
    /// <paramref name="snapshotEpoch"/> (i.e. the page was retired by a concurrent
    /// writer after this transaction started), or <c>0</c> if no conflict exists.
    ///
    /// Called from <see cref="Transaction{TKey,TValue}.Commit"/> /
    /// <see cref="Transaction{TKey,TValue}.CommitAsync"/> before any side effects
    /// are installed, so throwing here leaves the live tree completely untouched.
    /// </summary>
    internal uint FindConflictingPage(HashSet<uint>? readSet, ulong snapshotEpoch)
    {
        if (readSet == null || readSet.Count == 0) return 0u;
        foreach (uint pageId in readSet)
            if (_ssiRetireLog.TryGetValue(pageId, out ulong retireEpoch) && retireEpoch > snapshotEpoch)
                return pageId;
        return 0u;
    }

    /// <summary>
    /// Remove retire-log entries that can no longer cause a conflict.
    /// An entry (P, R) is prunable when all active snapshot epochs are &gt; R —
    /// no current or future transaction could have snapshotEpoch ≤ R.
    /// Called from <see cref="ExitTransactionLock"/> after the checkpoint gate is
    /// released, piggybacking on the existing commit-time cleanup cadence.
    /// </summary>
    private void PruneSsiLog()
    {
        ulong threshold;
        lock (_epochLock)
            threshold = _activeEpochs.Count > 0 ? _activeEpochs.Min : ulong.MaxValue;

        foreach (var kvp in _ssiRetireLog)
            if (kvp.Value < threshold)
                _ssiRetireLog.TryRemove(kvp.Key, out _);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _commitMutex.Dispose();
        _txWriterDepth.Dispose();
        _checkpointLock.Dispose();
    }
}
