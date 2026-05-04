using System.Buffers.Binary;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;

namespace ByTech.BPlusTree.Core.Engine;

/// <summary>
/// Background thread that writes dirty pages from the buffer pool to the data file.
/// Runs at BelowNormal priority; wakes when the pool crosses HighWatermark or every 100 ms.
///
/// WAL-before-page invariant: a dirty page is never written to the data file until
/// WalWriter.FlushedLsn >= frame.PageLsn. Enforced in TryEvictBatch via FlushUpTo().
///
/// Thread safety: all public methods are thread-safe. BufferPool._lock is never held
/// while calling StorageFile.WritePage or WalWriter.FlushUpTo (deadlock prevention —
/// see PHASE-26.MD Known Failure Points #2).
/// </summary>
internal sealed class EvictionWorker : IDisposable
{
    private readonly BufferPool        _bufferPool;
    private readonly StorageFile       _storage;
    private readonly WalWriter         _walWriter;
    private readonly BPlusTreeOptions  _options;
    private readonly CancellationTokenSource _cts = new();
    private readonly Thread            _thread;
    private int  _clockHand;    // single-threaded — no Interlocked needed
    private bool _started;      // tracks whether Start() was called (for safe Dispose)

    // Pre-allocated batch buffers — safe because EvictionWorker is single-threaded.
    // Allocated once at construction; reused across every TryEvictBatch call.
    // Size = EvictionBatchSize (fixed for the worker's lifetime).
    // (Phase 33 — eliminates per-batch List<T> and LINQ ToList allocations)
    private readonly int                           _evictionBatchSize;
    private readonly (int frameIndex, bool isDirty)[] _candidateBuffer;
    private readonly Frame[]                           _dirtyBuffer;

    public EvictionWorker(
        BufferPool       bufferPool,
        StorageFile      storage,
        WalWriter        wal,
        BPlusTreeOptions options)
    {
        _bufferPool        = bufferPool;
        _storage           = storage;
        _walWriter               = wal;
        _options           = options;
        _evictionBatchSize = options.EvictionBatchSize * options.CoWWriteAmplification;
        _candidateBuffer   = new (int, bool)[_evictionBatchSize];
        _dirtyBuffer       = new Frame[_evictionBatchSize];
        _thread = new Thread(EvictLoop)
        {
            Name         = "BPlusTree.EvictionWorker",
            IsBackground = true,
            Priority     = ThreadPriority.BelowNormal,
        };
    }

    /// <summary>Start the background eviction thread.</summary>
    public void Start() { _started = true; _thread.Start(); }

    /// <summary>
    /// Main eviction loop. Waits for the HWM signal or a 100 ms periodic wake-up,
    /// then evicts until occupancy drops to LWM. Calls FlushAll() before exiting.
    /// </summary>
    private void EvictLoop()
    {
        var token = _cts.Token;
        try
        {
            while (!token.IsCancellationRequested)
            {
                // Clear _evictPending BEFORE waiting — ensures any signal arriving after
                // the clear calls Release(1) and unblocks Wait, so no signal is lost.
                // Clearing after Wait() creates a lost-signal window (PHASE-32.MD Failure #1).
                // The 100 ms timeout is a safety net: even if a signal is somehow missed,
                // the loop wakes within 100 ms to catch stragglers. (Phase 32)
                _bufferPool.ResetEvictPending();
                _bufferPool.EvictSignal.Wait(100, token);

                if (token.IsCancellationRequested)
                    break;

                // Evict frames until occupancy falls below LWM.
                while (_bufferPool.OccupancyFraction > _options.EvictionLowWatermark)
                {
                    int evicted = TryEvictBatch(_evictionBatchSize);
                    if (evicted == 0)
                        break;  // full sweep found nothing evictable — stop
                }
            }
        }
        catch (OperationCanceledException) { }

        // Graceful shutdown: drain all remaining dirty frames before the thread exits.
        // Called whether the loop exited cleanly or via cancellation.
        FlushAll();
    }

    /// <summary>
    /// Attempt to evict up to <paramref name="batchSize"/> frames using a clock-hand sweep.
    /// Returns the number of frames successfully evicted (0 = full sweep found nothing).
    ///
    /// Ordering contract (non-negotiable — PHASE-26.MD Known Failure Points #3):
    ///   1. TryClaimForEviction  (atomic claim)
    ///   2. FlushUpTo(maxDirtyLsn) for dirty candidates
    ///   3. Stamp PageLsn + checksum into frame.Data, then WritePage / WritePageBatch
    ///   4. ReleaseEvictedFrame  (return frame to free pool)
    ///
    /// Uses pre-allocated _candidateBuffer and _dirtyBuffer (Phase 33 — zero per-call heap allocation).
    /// _dirtyBuffer is always sliced to [0, dirtyCount) via ArraySegment before passing to WritePageBatch
    /// to prevent stale entries from a prior batch being written (Failure Point #3, PHASE-33.MD).
    /// </summary>
    private int TryEvictBatch(int batchSize)
    {
        var frames        = _bufferPool.Frames;
        int candidateCount = 0;
        int fullSweep      = 0;

        // Clock-hand sweep: write directly into pre-allocated buffer (no heap allocation).
        while (candidateCount < batchSize && fullSweep < frames.Length)
        {
            _clockHand = (_clockHand + 1) % frames.Length;
            var frame  = frames[_clockHand];
            fullSweep++;

            // Skip FREE, EVICTING, or PINNED frames — cannot evict them.
            if (frame.PageId == PageLayout.NullPageId) continue;
            if (frame.IsPinned || frame.IsEvicting)   continue;

            // Second-chance: clear reference bit on first encounter, evict on second.
            if (frame.ReferenceBit)
            {
                frame.ReferenceBit = false;
                continue;
            }

            // Atomically claim the frame for eviction.
            if (_bufferPool.TryClaimForEviction(_clockHand))
                _candidateBuffer[candidateCount++] = (_clockHand, frames[_clockHand].IsDirty);
        }

        if (candidateCount == 0)
            return 0;

        // ── WAL-before-page enforcement ──────────────────────────────────────
        // Find the maximum PageLsn among all dirty candidates; flush WAL once.
        // Single FlushUpTo(max) is equivalent to per-frame calls and avoids extra syscalls.
        ulong maxDirtyLsn = 0;
        for (int i = 0; i < candidateCount; i++)
        {
            if (_candidateBuffer[i].isDirty)
                maxDirtyLsn = Math.Max(maxDirtyLsn, frames[_candidateBuffer[i].frameIndex].PageLsn);
        }

        if (maxDirtyLsn > _walWriter.FlushedLsn)
            _walWriter.FlushUpTo(maxDirtyLsn);

        // ── Write dirty pages to storage ─────────────────────────────────────
        // Build slice of pre-allocated _dirtyBuffer (zero allocation).
        // Always pass _dirtyBuffer.AsSpan(0, dirtyCount) — never the full array —
        // so stale entries from a prior batch cannot be written (Failure Point #3).
        int dirtyCount = 0;
        for (int i = 0; i < candidateCount; i++)
        {
            if (_candidateBuffer[i].isDirty)
                _dirtyBuffer[dirtyCount++] = frames[_candidateBuffer[i].frameIndex];
        }

        if (dirtyCount == 1)
        {
            BinaryPrimitives.WriteUInt64BigEndian(
                _dirtyBuffer[0].Data.AsSpan(PageLayout.PageLsnOffset, sizeof(ulong)),
                _dirtyBuffer[0].PageLsn);
            PageChecksum.Stamp(_dirtyBuffer[0].Data);
            _storage.WritePage(_dirtyBuffer[0].PageId, _dirtyBuffer[0].Data);
        }
        else if (dirtyCount > 1)
        {
            for (int i = 0; i < dirtyCount; i++)
            {
                BinaryPrimitives.WriteUInt64BigEndian(
                    _dirtyBuffer[i].Data.AsSpan(PageLayout.PageLsnOffset, sizeof(ulong)),
                    _dirtyBuffer[i].PageLsn);
                PageChecksum.Stamp(_dirtyBuffer[i].Data);
            }
            _storage.WritePageBatch(new ArraySegment<Frame>(_dirtyBuffer, 0, dirtyCount));
        }

        // ── Release all claimed frames back to FREE ───────────────────────────
        for (int i = 0; i < candidateCount; i++)
            _bufferPool.ReleaseEvictedFrame(_candidateBuffer[i].frameIndex);

        return candidateCount;
    }

    /// <summary>Test seam: invoke one batch directly without the background thread.</summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    internal int InvokeTryEvictBatch() => TryEvictBatch(_evictionBatchSize);

    /// <summary>
    /// Drain all remaining dirty frames regardless of watermarks.
    /// Called during graceful shutdown (after cancellation is requested).
    /// </summary>
    private void FlushAll()
    {
        while (true)
        {
            int evicted = TryEvictBatch(_evictionBatchSize);
            if (evicted == 0)
                break;  // nothing left to evict (all FREE or PINNED)
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _cts.Cancel();
        _bufferPool.SignalEviction();   // wake thread so it sees cancellation immediately
        // Best-effort join — never throw from Dispose, otherwise downstream cleanup
        // (engine.Close, pageManager.Dispose) would be skipped, risking data loss.
        if (_started)
            _thread.Join(TimeSpan.FromSeconds(5));
        _cts.Dispose();
    }
}
