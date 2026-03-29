using System.Diagnostics;
using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;

namespace BPlusTree.Core.Engine;

/// <summary>
/// Full tree rewrite compaction. Produces a minimal, tightly-packed copy of the tree.
///
/// Algorithm (Phase 73 — epoch-protected leaf walk):
///   1a. Acquire writer lock (blocks new transactions and auto-commit writes).
///       Capture FirstLeafPageId while frozen under the writer lock.
///   1b. Register read epoch — prevents page retirement during the leaf walk.
///   2.  Determine compact file path: _options.DataFilePath + ".compact"
///   3.  Create and initialize a brand-new PageManager over the compact file.
///   4.  Walk the leaf chain of the SOURCE tree in key order (captured firstLeafId → NextLeafPageId).
///       Epoch-protected: pages cannot be retired while the epoch is active.
///   5.  For each leaf page: bulk-insert all live key-value pairs into the COMPACT tree.
///   6.  After all leaves: flush and fsync the compact file.
///   7.  Acquire checkpoint lock (guaranteed immediate — writer lock is held; no transaction
///       can hold the checkpoint read lock). Short critical section:
///   8.  Write a WalRecordType.CompactionComplete record to the WAL.
///   9.  Flush + fsync the WAL.
///  10.  Atomic rename: File.Move(compactPath, _options.DataFilePath, overwrite: true)
///  11.  Reload _pageManager from the renamed file (reinitialize buffer pool).
///  12.  Reset WAL: TruncateWal() — safe because writer lock prevents any new transactions.
///  13.  Release checkpoint lock, read epoch, writer lock.
///
/// Crash safety:
///   - Crash before step 10: .compact file may exist; original file is untouched.
///     On reopen: detect .compact file; delete it (it is incomplete or superseded).
///   - Crash after step 10: renamed file is complete; normal WAL recovery applies.
///
/// The Compact() call is synchronous and blocking. Writes are blocked by the writer mutex
/// for the full duration. TryGet (reads) remain unblocked — they do not acquire either lock.
/// </summary>
public sealed class Compactor<TKey, TValue>
    where TKey : IComparable<TKey>
{
    private readonly PageManager                  _pageManager;
    private readonly NodeSerializer<TKey, TValue> _nodeSerializer;
    private readonly TreeMetadata                 _metadata;
    private readonly BPlusTreeOptions             _options;
    private readonly WalWriter                    _walWriter;
    private readonly TransactionCoordinator?      _coordinator;
    private readonly Action?                      _enableDelta;
    private readonly Func<Dictionary<TKey, (DeltaOp Op, TValue? Value)>>? _disableDelta;
    private readonly Action?                      _takeCheckpoint;

    internal Compactor(
        PageManager                  pageManager,
        NodeSerializer<TKey, TValue> nodeSerializer,
        TreeMetadata                 metadata,
        BPlusTreeOptions             options,
        WalWriter                    walWriter,
        TransactionCoordinator?      coordinator  = null,
        Action?                      enableDelta  = null,
        Func<Dictionary<TKey, (DeltaOp Op, TValue? Value)>>? disableDelta = null,
        Action?                      takeCheckpoint = null)
    {
        _pageManager    = pageManager;
        _nodeSerializer = nodeSerializer;
        _metadata       = metadata;
        _options        = options;
        _walWriter      = walWriter;
        _coordinator    = coordinator;
        _enableDelta    = enableDelta;
        _disableDelta   = disableDelta;
        _takeCheckpoint = takeCheckpoint;
    }

    /// <summary>
    /// Online compaction. Phase A uses a barrier+quiescence protocol instead of the writer lock,
    /// so concurrent transactions can run and commit during the leaf walk. Phase B holds the
    /// writer lock for the short atomic swap.
    ///
    /// Protocol (Phase 109a — updated):
    ///   Phase A (background — no writer lock):
    ///     [1-6]   Barrier, quiescence, capture firstLeafId, epoch, enable delta, lower barrier.
    ///     [7-10]  Open compact engine, walk source leaf chain, insert all KV pairs.
    ///   Phase B (swap — barrier + quiescence + writer lock):
    ///     [11-28] Barrier, quiescence, acquire writer lock, disable delta, apply delta,
    ///             live-tree checkpoint, atomic swap, TruncateWal, release.
    ///
    /// Lock ordering: barrier → quiescence → commit mutex → checkpoint write lock.
    /// Barrier+quiescence guarantee no checkpoint read locks are held when Phase B
    /// acquires the commit mutex, eliminating the circular deadlock that existed
    /// when the commit mutex was held while waiting for the checkpoint write lock.
    ///
    /// On success: _pageManager is reloaded from the new compact file.
    /// On exception: original file is untouched; .compact file may need cleanup.
    /// </summary>
    public CompactionResult Compact()
    {
        string compactPath    = _options.DataFilePath + ".compact";
        string compactWalPath = _options.WalFilePath  + ".compact";

        long sizeBefore = new FileInfo(_options.DataFilePath).Length;
        var  sw         = Stopwatch.StartNew();
        long sizeAfter  = sizeBefore;   // default: unchanged (set after successful rename)

        // ── Phase A setup (Phase 109a: no writer lock needed for setup) ─────────
        // Writer lock is NOT held during Phase A setup. Instead:
        //   1. Barrier prevents new transactions from starting.
        //   2. Quiescence ensures all in-flight transactions finish.
        //   3. firstLeafId is captured AFTER quiescence so it reflects the live tree
        //      (including any committed transactions that ran before Phase A).
        //   4. Read epoch is registered to protect the leaf chain during the walk.
        //   5. Delta is enabled so that transactions starting after step 6 record their changes.
        // Phase B still acquires the writer lock for the short atomic swap (steps [11-12]).

        bool writerLockHeld = false;

        // [1] Raise compaction barrier — new BeginTransaction() calls throw.
        _coordinator?.SetCompactionBarrier();

        // [2] Drain all in-flight transactions. Only after quiescence is firstLeafId stable.
        _coordinator?.WaitForTxQuiescence();

        // [3] Capture firstLeafId from the live, quiescent tree. All prior commits are visible.
        uint firstLeafId = _metadata.FirstLeafPageId;

        // [4] Register read epoch — prevents retirement of leaf pages during the walk.
        ulong epoch      = _coordinator != null ? _coordinator.EnterReadEpoch() : 0;
        bool  epochExited = false;

        // [5] Enable delta tracking. Safe: barrier set, quiescent, no concurrent writes.
        _enableDelta?.Invoke();

        // [6] Lower barrier — new transactions can now start and will see DeltaEnabled = true,
        //     guaranteeing their commits call RecordDelta and contribute to the delta.
        _coordinator?.ClearCompactionBarrier();

        TreeEngine<TKey, TValue>? compactEngine = null;
        PageManager?              compactMgr    = null;

        try
        {
        // [7] Delete leftover compact files from a previous aborted run.
        if (File.Exists(compactPath))    File.Delete(compactPath);
        if (File.Exists(compactWalPath)) File.Delete(compactWalPath);

        // [8] Open compact PageManager + TreeEngine (separate WAL).
        var compactOptions = new BPlusTreeOptions
        {
            DataFilePath        = compactPath,
            WalFilePath         = compactWalPath,
            PageSize            = _options.PageSize,
            BufferPoolCapacity  = _options.BufferPoolCapacity,
            CheckpointThreshold = _options.CheckpointThreshold,
        };
        var compactWal = WalWriter.Open(compactWalPath, bufferSize: _options.WalBufferSize);
        compactMgr     = PageManager.Open(compactOptions, compactWal);
        var compactMeta = new TreeMetadata(compactMgr);
        compactMeta.Load();
        compactEngine = new TreeEngine<TKey, TValue>(compactMgr, _nodeSerializer, compactMeta);

        // [9-10] Walk source leaf chain from firstLeafId under epoch protection.
        // Writes can proceed concurrently; delta map records all changes.
        uint leafId = firstLeafId;
        while (leafId != PageLayout.NullPageId)
        {
            var frame = _pageManager.FetchPage(leafId);
            var leaf  = _nodeSerializer.AsLeaf(frame);
            int count = leaf.Count;

            for (int i = 0; i < count; i++)
            {
                TKey   key   = leaf.GetKey(i);
                TValue value = leaf.GetValue(i);
                compactEngine.Insert(key, value);
            }

            uint next = leaf.NextLeafPageId;
            _pageManager.Unpin(leafId);
            leafId = next;
        }

        // ── Phase B (barrier + drain + writer lock) ───────────────────────────
        // [11] Raise compaction barrier — BeginTransaction() now throws until Phase B
        //      completes. Then drain all in-flight transactions (wait for _activeTxCount
        //      to reach zero). Only THEN acquire the commit mutex. This eliminates the
        //      lock-ordering deadlock: previously, Phase B held the commit mutex and then
        //      tried to acquire the checkpoint write lock, while in-flight transactions
        //      held the checkpoint read lock and were waiting for the commit mutex.
        _coordinator?.SetCompactionBarrier();
        _coordinator?.WaitForTxQuiescence();
        _coordinator?.EnterWriterLock();
        writerLockHeld = true;

        // [12] Disable delta tracking and snapshot the accumulated map.
        var delta = _disableDelta != null
            ? _disableDelta()
            : new Dictionary<TKey, (DeltaOp, TValue?)>();

        // Exit epoch here — BEFORE any rename — so SweepRetiredPages frees pages from
        // the current source file (not from the compact file after rename).
        // Writer lock is held: no new writes can race with the sweep.
        if (_coordinator != null)
        {
            _coordinator.ExitReadEpoch(epoch);
            epochExited = true;
        }

        // [13] Apply delta to compact tree (last-write-wins per key).
        foreach (var (key, (op, value)) in delta)
        {
            if (op == DeltaOp.Insert) compactEngine.Insert(key, value!);
            else                       compactEngine.Delete(key);
        }

        // [14-15] Checkpoint and close compact engine (fsync compact file; discard compact WAL).
        compactEngine.Close();
        compactEngine = null;
        compactMgr.Dispose();
        compactMgr = null;

        // [16] Take a live-tree checkpoint before the atomic swap.
        // This ensures the live WAL is clean (all dirty pages durable, WAL truncated)
        // before CompactionComplete is written. Prevents stale UpdatePage records from
        // being replayed against the compact file after a crash-between-rename-and-TruncateWal.
        _takeCheckpoint?.Invoke();

        // [17] Acquire checkpoint lock (guaranteed immediate — writer lock held).
        _coordinator?.EnterCheckpointLock();
        try
        {
            // [18-19] Write CompactionComplete to live WAL and flush+fsync.
            _walWriter.Append(
                WalRecordType.CompactionComplete, 0, 0,
                LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
            _walWriter.Flush();

            // [20-22] Close source storage handle, rename, reopen.
            _pageManager.PrepareForRename();
            File.Move(compactPath, _options.DataFilePath, overwrite: true);
            _pageManager.AfterRename();
            sizeAfter = new FileInfo(_options.DataFilePath).Length;

            // [23] Reload source metadata from the compact meta page.
            _metadata.Load();

            // [24] Delete compact WAL (no longer needed after rename).
            if (File.Exists(compactWalPath)) File.Delete(compactWalPath);

            // [25] Truncate live WAL — removes CompactionComplete and all pre-swap records.
            _walWriter.TruncateWal();
        }
        finally
        {
            // [26]
            _coordinator?.ExitCheckpointLock();
        }

        } // end outer try
        finally
        {
            // Cleanup compact resources if an exception aborted Phase A or B.
            try { compactEngine?.Close(); } catch { /* best-effort */ }
            try { compactMgr?.Dispose();  } catch { /* best-effort */ }

            // Exit epoch if not already done (normal path exits it before rename;
            // exception path exits it here so retired pages are processed safely).
            if (!epochExited && _coordinator != null)
                _coordinator.ExitReadEpoch(epoch);

            // Only exit writer lock if currently held (prevents double-release if an
            // exception occurred between [6] ExitWriterLock and [11] EnterWriterLock).
            if (writerLockHeld)
                _coordinator?.ExitWriterLock();

            // Always lower the compaction barrier so BeginTransaction() is unblocked.
            // Safe to call even if SetCompactionBarrier was never reached (no-op: 0→0).
            _coordinator?.ClearCompactionBarrier();
        }

        sw.Stop();
        long saved = Math.Max(0L, sizeBefore - sizeAfter);
        return new CompactionResult
        {
            BytesSaved = saved,
            PagesFreed = _options.PageSize > 0 ? (int)(saved / _options.PageSize) : 0,
            Duration   = sw.Elapsed,
        };
    }

    /// <summary>
    /// Called during Open() to clean up a leftover .compact file from a previous aborted compaction.
    /// If .compact file exists: delete it (it is incomplete). Original file is authoritative.
    /// </summary>
    public static void CleanupAbortedCompaction(string dataFilePath)
    {
        var compactPath = dataFilePath + ".compact";
        if (File.Exists(compactPath))
            File.Delete(compactPath);
    }
}
