using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;

namespace ByTech.BPlusTree.Core.Engine;

/// <summary>
/// Checkpoint protocol (Phase 20a) + ARIES redo recovery (Phase 20b).
///
/// Checkpoint (7 steps):
///   1. Write WalRecordType.CheckpointBegin record.
///   2. Call PageManager.CheckpointFlush() — writes all dirty pages to StorageFile.
///   3. Call StorageFile.Flush() — fsync data file.
///   4. Write WalRecordType.CheckpointEnd record.
///   5. Call WalWriter.Flush() — fsync WAL.
///   6. Update Metadata.LastCheckpointLsn = CheckpointEnd.Lsn. Flush meta page.
///   7. Truncate WAL: discard all pre-checkpoint records via WalWriter.TruncateWal().
///
/// GracefulClose = TakeCheckpoint() + StorageFile.Flush().
/// RecoverFromWal() = ARIES redo-only forward scan from last CheckpointEnd.
/// </summary>
// ── Recovery architecture note ─────────────────────────────────────────────
// This class implements redo-only recovery (ARIES redo pass, no undo pass).
// Every committed single-operation write survives a crash. The WAL record layout
// includes TransactionId (offset 13) and PrevLsn (offset 21) in every record,
// reserving the wire format for a future undo phase when multi-operation
// transaction support is added. The undo pass would live in a separate class
// that builds the Active Transaction Table from WAL records during the Analysis
// Pass and chains records via PrevLsn for per-transaction undo.
// See: WalRecordLayout.TransactionIdOffset, WalRecordLayout.PrevLsnOffset.
internal sealed class CheckpointManager
{
    private readonly PageManager              _pageManager;
    private readonly WalWriter                _walWriter;
    private readonly TreeMetadata             _metadata;
    private readonly string                   _walPath;
    private readonly TransactionCoordinator?  _coordinator;

    private volatile bool _closed;

    // ── Auto-checkpoint state ─────────────────────────────────────────────────
    private long                      _autoThreshold;   // 0 = disabled
    private Func<bool>?               _hasActiveLocks;
    private CancellationTokenSource?  _autoCts;
    private Thread?                   _autoThread;

    /// <summary>
    /// Optional callback invoked at the end of <see cref="RecoverFromWal"/> when WAL
    /// records were replayed. Set by <see cref="TreeEngine{TKey,TValue}"/> in DEBUG
    /// builds to run <see cref="TreeValidator{TKey,TValue}"/> after recovery completes.
    /// Null in Release builds — no allocation, no overhead.
    /// </summary>
    internal Action? PostRecoveryValidation { get; set; }

    internal CheckpointManager(
        PageManager             pageManager,
        WalWriter               walWriter,
        TreeMetadata            metadata,
        string                  walPath,
        TransactionCoordinator? coordinator = null)
    {
        _pageManager = pageManager;
        _walWriter   = walWriter;
        _metadata    = metadata;
        _walPath     = walPath;
        _coordinator = coordinator;
    }

    /// <summary>Implements the 7-step checkpoint protocol described above.</summary>
    public void TakeCheckpoint() => TakeCheckpointCore(skipTruncation: false);

    /// <summary>
    /// Runs checkpoint steps 1-5 only (skips WAL truncation — step 6).
    /// Used in tests to simulate a crash between CheckpointEnd fsync and WAL truncation.
    /// </summary>
    internal void TakeCheckpointSkipTruncation() => TakeCheckpointCore(skipTruncation: true);

    private void TakeCheckpointCore(bool skipTruncation)
    {
        if (_closed) return;   // safe no-op after GracefulClose
        // Acquire the exclusive checkpoint gate — blocks until all active transactions
        // have exited their shared (EnterTransactionLock) read locks.
        // This guarantees TruncateWal is never called while a transaction is in-flight.
        _coordinator?.EnterCheckpointLock();
        try
        {

        // Step 1: Write CheckpointBegin record.
        _walWriter.Append(WalRecordType.CheckpointBegin, 0, 0, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);

        // Step 2: Flush all dirty pages to StorageFile.
        _pageManager.CheckpointFlush();

        // Step 3: fsync data file.
        _pageManager.Storage.Flush();

        // Step 4: Write CheckpointEnd record.
        var endLsn = _walWriter.Append(WalRecordType.CheckpointEnd, 0, 0, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);

        // Step 5: fsync WAL.
        _walWriter.Flush();

        // Step 6: Update LastCheckpointLsn in metadata and flush meta page.
        // Runs before TruncateWal so the UpdateMeta WAL record is discarded by truncation.
        // After truncation the WAL contains only CheckpointEnd — clean opens stay O(1).
        _metadata.SetLastCheckpointLsn(endLsn.Value);
        _metadata.Flush();

        if (!skipTruncation)
        {
            // Step 7: Truncate WAL — discard all pre-checkpoint records.
            _walWriter.TruncateWal();
        }

        } // end try
        finally
        {
            _coordinator?.ExitCheckpointLock();
        }
    }

    /// <summary>
    /// Start a background WAL-size auto-checkpoint thread.
    /// No-op when <paramref name="walSizeThresholdBytes"/> is 0.
    /// Must be called at most once. Thread is stopped by <see cref="GracefulClose"/>.
    /// </summary>
    internal void StartAutoCheckpoint(
        long walSizeThresholdBytes,
        Func<bool> hasActiveLocks,
        int pollIntervalMs = 250)
    {
        if (walSizeThresholdBytes <= 0) return;

        _autoThreshold  = walSizeThresholdBytes;
        _hasActiveLocks = hasActiveLocks;
        _autoCts        = new CancellationTokenSource();
        _autoThread     = new Thread(() => AutoCheckpointLoop(pollIntervalMs))
                          { IsBackground = true, Name = "WalAutoCheckpoint" };
        _autoThread.Start();
    }

    private void AutoCheckpointLoop(int pollIntervalMs)
    {
        var token = _autoCts!.Token;
        try
        {
            while (!token.IsCancellationRequested)
            {
                token.WaitHandle.WaitOne(pollIntervalMs);
                if (token.IsCancellationRequested) break;

                // WAL size = current byte offset; resets to 0 after TruncateWal().
                if ((long)_walWriter.CurrentLsn.Value < _autoThreshold) continue;

                // Defer while any transaction holds a page write lock — truncating
                // the WAL now would lose that transaction's before-image records.
                if (_hasActiveLocks!()) continue;

                TakeCheckpoint();
            }
        }
        catch (OperationCanceledException) { }
    }

    /// <summary>
    /// TakeCheckpoint() + StorageFile.Flush().
    /// Idempotent: safe to call multiple times without exception.
    /// </summary>
    public void GracefulClose()
    {
        if (_closed) return;
        _closed = true;

        // Stop the auto-checkpoint background thread before taking the final
        // checkpoint to avoid a concurrent TakeCheckpoint() race.
        if (_autoCts != null)
        {
            _autoCts.Cancel();
            _autoThread!.Join(millisecondsTimeout: 5000);
            _autoCts.Dispose();
        }

        TakeCheckpoint();
        _pageManager.Storage.Flush();
    }

    /// <summary>
    /// ARIES redo-only recovery. Replays WAL after-images forward from the last complete
    /// checkpoint. Pages whose on-disk LSN is already ≥ the WAL record's LSN are skipped
    /// (idempotent). Stops gracefully on CRC error or truncated tail — never throws.
    ///
    /// Algorithm:
    ///   1. Open WalReader from _walPath.
    ///   2. Scan entire WAL to find the last CheckpointEnd LSN.
    ///      If none: replayFrom = LogSequenceNumber.None (replay everything).
    ///      If found: replayFrom = CheckpointEnd.Lsn.
    ///   3. Scan forward from replayFrom.
    ///   4. UpdatePage / UpdateMeta: extend data file if needed, apply after-image if stale.
    ///   5. AllocPage: extend data file to materialise the allocation.
    ///   6. Other record types: ignored (redo-only strategy).
    ///   7. Flush all recovered dirty pages and fsync the data file.
    /// </summary>
    public void RecoverFromWal()
    {
        if (!File.Exists(_walPath)) return;

        var reader         = new WalReader(_walPath);
        var lastCkptEnd    = reader.FindLastCheckpointEnd();
        var lastCompaction = reader.FindLastCompactionComplete();
        // Defence-in-depth: if a CompactionComplete record exists AFTER the last CheckpointEnd
        // (i.e., a crash occurred between rename and TruncateWal before step [16] ran),
        // start replay from the CompactionComplete boundary to avoid replaying stale
        // UpdatePage records against the newly renamed compact file.
        LogSequenceNumber? effectiveBoundary = lastCkptEnd;
        if (lastCompaction.HasValue)
        {
            if (!effectiveBoundary.HasValue || lastCompaction.Value.Value > effectiveBoundary.Value.Value)
                effectiveBoundary = lastCompaction;
        }
        var replayFrom = effectiveBoundary ?? LogSequenceNumber.None;

        bool anyReplayed    = false; // true when at least one after-image was applied (redo)
        bool anyDataRecords = false; // true when at least one UpdatePage/UpdateMeta record was scanned

        // Gap 2: collect FreeOverflowChain page IDs during the Redo scan.
        // FreePage cannot be called here because ReloadFreeList() runs after the scan and
        // resets _headPageId from the meta page, discarding any frees made during the scan.
        // These are processed after ReloadFreeList() below.
        var pendingFreeChains = new System.Collections.Generic.List<uint>();

        foreach (var record in reader.ReadForward(replayFrom))
        {
            switch (record.Type)
            {
                case WalRecordType.UpdatePage:
                case WalRecordType.UpdateMeta:
                {
                    if (record.Data.Length == 0) break;

                    anyDataRecords = true; // a data record exists — Analysis Pass must run

                    // Ensure the page physically exists in the file (covers wiped-data-file scenario).
                    _pageManager.EnsurePageExists(record.PageId);

                    var frame = _pageManager.FetchPage(record.PageId);
                    if (frame.PageLsn < record.Lsn.Value)
                    {
                        // Auto-commit record: DataLength == PageSize  → data IS the after-image.
                        // Transactional record: DataLength == 2×PageSize → data is [before][after].
                        // Always apply the LAST PageSize bytes (the after-image).
                        int pageSize = _pageManager.PageSize;
                        System.ReadOnlyMemory<byte> afterImage =
                            record.Data.Length == pageSize * 2
                                ? record.Data.Slice(pageSize, pageSize)
                                : record.Data;
                        afterImage.CopyTo(frame.Data);
                        frame.PageLsn = record.Lsn.Value;
                        _pageManager.MarkDirtyAndUnpin(record.PageId, bypassWal: true);
                        anyReplayed = true; // inside guard — set only when after-image is actually applied
                    }
                    else
                    {
                        _pageManager.Unpin(record.PageId);
                    }
                    break;
                }

                case WalRecordType.AllocPage:
                    // Materialise the page in the file if it was truncated away.
                    _pageManager.EnsurePageExists(record.PageId);
                    break;

                case WalRecordType.FreeOverflowChain:
                {
                    // Gap 2 closure: collect page IDs for post-ReloadFreeList processing.
                    // FreePage cannot run here — ReloadFreeList() later resets _headPageId
                    // from the meta page, discarding any in-memory frees made during this scan.
                    var span = record.Data.Span;
                    for (int i = 0; i + 3 < span.Length; i += 4)
                        pendingFreeChains.Add(System.Buffers.Binary.BinaryPrimitives
                            .ReadUInt32BigEndian(span.Slice(i, 4)));
                    break;
                }

                default:
                    // CheckpointBegin, CheckpointEnd, Begin, Commit, Abort, FreePage — skip.
                    break;
            }
        }

        _pageManager.CheckpointFlush();
        _pageManager.Storage.Flush();

        // Reload metadata and free-list from the recovered meta page.
        // This repairs any stale values that PageManager.Open() read from a blank
        // or wiped data file before recovery ran.
        _metadata.Load();
        _pageManager.ReloadFreeList();

        // Gap 2 + Gap 3 closure: apply FreeOverflowChain Redo now that the free list is correct.
        // Idempotency: skip pages already reachable from the free-list head (already freed).
        // NOTE: FreeList.Deallocate zeroes PageLayout.PageTypeOffset (offset 4), so the
        // old PageType == Overflow check fails for pages freed via epoch retirement before
        // the crash (PageType is 0 on disk but not yet in the persistent free list).
        // Walking the free list is the correct idempotency check: any page reachable from
        // FreeListHead was already freed and must not be freed again.
        if (pendingFreeChains.Count > 0)
        {
            // Build the set of pages already in the free list (O(free-list-size)).
            // After crash recovery ReloadFreeList() loads from the meta page, which may be
            // stale (FreeListHeadPageId in TreeMetadata is not auto-synced from FreeList).
            // In practice this set is empty after most recoveries, making the walk O(1).
            var alreadyFree = new System.Collections.Generic.HashSet<uint>();
            uint scanId = _pageManager.FreeListHead;
            while (scanId != PageLayout.NullPageId && alreadyFree.Add(scanId))
            {
                var scanFrame = _pageManager.FetchPage(scanId);
                uint nextId = System.Buffers.Binary.BinaryPrimitives
                    .ReadUInt32LittleEndian(scanFrame.Data.AsSpan(0, 4));
                _pageManager.Unpin(scanId);
                scanId = nextId;
            }

            foreach (var pid in pendingFreeChains)
            {
                if (alreadyFree.Contains(pid)) continue;
                _pageManager.EnsurePageExists(pid);
                _pageManager.FreePage(pid);
            }
            _pageManager.CheckpointFlush();
            _pageManager.Storage.Flush();
        }

        // Run Analysis + Undo Pass whenever the WAL contains data records.
        // This covers two scenarios:
        //   (a) Redo was needed (pages were stale) — anyReplayed = true.
        //   (b) All pages were already current (flushed before crash with stamped LSN)
        //       but a transaction was in-flight — anyReplayed = false, anyDataRecords = true.
        // Skipped only on a clean open after GracefulClose (WAL truncated to CheckpointEnd,
        // so anyDataRecords = false and this block is never entered).
        if (anyDataRecords)
        {
            // ── Analysis Pass: build Active Transaction Table (ATT) ───────────
            // Scan WAL forward from the same checkpoint start to find all transactions
            // that have a Begin record but no matching Commit or Abort record.
            // These are transactions that were in-flight when the process crashed.
            var att                = new Dictionary<uint, LogSequenceNumber>(); // txId → last seen LSN
            var crashedAllocations = new Dictionary<uint, List<uint>>();        // txId → orphaned pageIds
            List<uint>? pendingAutoShadows = null; // Gap 1: last uncommitted auto-commit CoW shadows

            foreach (var record in reader.ReadForward(replayFrom))
            {
                // ── Gap 1: auto-commit CoW shadow tracking ────────────────────────────
                // AllocShadowChain(txId=0) and UpdateMeta(txId=0) are handled here before
                // the blanket continue for auto-commit records.
                if (record.TransactionId == 0)
                {
                    if (record.Type == WalRecordType.AllocShadowChain)
                    {
                        // A new auto-commit CoW write started. Track its shadow page IDs.
                        // (Single writer — any previous pendingAutoShadows had a following UpdateMeta.)
                        pendingAutoShadows = new List<uint>();
                        var span = record.Data.Span;
                        for (int i = 0; i + 3 < span.Length; i += 4)
                            pendingAutoShadows.Add(System.Buffers.Binary.BinaryPrimitives
                                .ReadUInt32BigEndian(span.Slice(i, 4)));
                    }
                    else if (record.Type == WalRecordType.UpdateMeta && pendingAutoShadows != null)
                    {
                        // The auto-commit CoW write completed. Shadow pages are live.
                        pendingAutoShadows = null;
                    }
                    continue; // all other txId=0 records are always committed
                }

                switch (record.Type)
                {
                    case WalRecordType.Begin:
                        att[record.TransactionId] = record.Lsn;
                        break;
                    case WalRecordType.Commit:
                    case WalRecordType.Abort:
                        att.Remove(record.TransactionId); // committed or already aborted
                        crashedAllocations.Remove(record.TransactionId); // not crashed — discard
                        break;
                    case WalRecordType.UpdatePage:
                    case WalRecordType.UpdateMeta:
                        if (att.ContainsKey(record.TransactionId))
                            att[record.TransactionId] = record.Lsn; // update last LSN
                        break;
                    case WalRecordType.AllocPage:
                        if (att.ContainsKey(record.TransactionId))
                        {
                            if (!crashedAllocations.TryGetValue(record.TransactionId, out var list))
                                crashedAllocations[record.TransactionId] = list = new List<uint>();
                            list.Add(record.PageId);
                        }
                        break;
                    case WalRecordType.AllocOverflowChain:
                    case WalRecordType.AllocShadowChain:
                        if (att.ContainsKey(record.TransactionId))
                        {
                            if (!crashedAllocations.TryGetValue(record.TransactionId, out var chainList))
                                crashedAllocations[record.TransactionId] = chainList = new List<uint>();
                            var span = record.Data.Span;
                            for (int i = 0; i + 3 < span.Length; i += 4)
                                chainList.Add(System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(span.Slice(i, 4)));
                        }
                        break;
                }
            }

            // ── Undo Pass: for each crashed transaction follow PrevLsn backward ─
            // Apply before-images (first PageSize bytes of transactional records)
            // to reverse the uncommitted writes.
            bool anyUndone = att.Count > 0 || pendingAutoShadows != null;
            if (anyUndone)
            {
                int pageSize = _pageManager.PageSize;
                foreach (var (_, lastLsn) in att)
                {
                    var lsn = lastLsn;
                    while (lsn.Value != 0)
                    {
                        var rec = reader.ReadAt(lsn);
                        if (rec == null) break;

                        if (rec.Value.Type is WalRecordType.UpdatePage or WalRecordType.UpdateMeta)
                        {
                            // Only transactional records carry a before-image.
                            if (rec.Value.Data.Length == pageSize * 2)
                            {
                                ReadOnlySpan<byte> beforeImage =
                                    rec.Value.Data.Span.Slice(0, pageSize);
                                var frame = _pageManager.FetchPage(rec.Value.PageId);
                                beforeImage.CopyTo(frame.Data);
                                _pageManager.MarkDirtyAndUnpin(rec.Value.PageId, bypassWal: true);
                            }
                        }

                        lsn = rec.Value.PrevLsn;
                    }
                }

                // Free pages allocated by crashed transactions.
                // Before-images have been fully restored above — shadow pages are now unreachable.
                foreach (var (_, pageIds) in crashedAllocations)
                    foreach (var pageId in pageIds)
                        _pageManager.FreePage(pageId);

                // Gap 1: free orphaned auto-commit CoW shadow pages.
                // pendingAutoShadows is non-null when AllocShadowChain(txId=0) was seen in the
                // WAL without a following UpdateMeta — the write was in-flight at crash time.
                if (pendingAutoShadows != null)
                    foreach (var pageId in pendingAutoShadows)
                        _pageManager.FreePage(pageId);

                // Make undo durable before returning.
                _pageManager.CheckpointFlush();
                _pageManager.Storage.Flush();
            }

            // Recompute TotalRecordCount and run structural validation whenever the tree
            // was actually modified (redo applied after-images OR undo applied before-images).
            if (anyReplayed || anyUndone)
            {
                RecomputeRecordCount();
                PostRecoveryValidation?.Invoke();   // DEBUG-only structural validation
            }
        }
    }

    /// <summary>
    /// Walks the leaf chain and recomputes <see cref="TreeMetadata.TotalRecordCount"/>
    /// from actual leaf slot counts. Called after WAL replay when a truncated meta-page
    /// record may have left TotalRecordCount inconsistent with the leaf chain.
    /// Uses <see cref="NodePage"/> directly (non-generic) — no TKey/TValue needed.
    /// </summary>
    private void RecomputeRecordCount()
    {
        long total  = 0;
        uint pageId = _metadata.FirstLeafPageId;

        while (pageId != PageLayout.NullPageId)
        {
            var frame = _pageManager.FetchPage(pageId);
            var page  = new NodePage(frame.Data.AsSpan());
            total  += page.SlotCount;
            uint next = page.NextLeafPageId;
            _pageManager.Unpin(pageId);
            pageId = next;
        }

        _metadata.SetTotalRecordCount((ulong)total);
        _metadata.Flush();
        _pageManager.Storage.Flush();   // Make corrected TotalRecordCount immediately durable.
                                        // Without this, a second crash before the next eviction
                                        // would re-run the leaf-chain recompute on next open.
    }
}
