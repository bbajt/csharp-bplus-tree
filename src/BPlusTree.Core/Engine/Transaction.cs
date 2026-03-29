using System.Collections;
using BPlusTree.Core.Api;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;

namespace BPlusTree.Core.Engine;

/// <summary>
/// Implements a multi-operation atomic transaction using the CoW shadow write path.
///
/// WAL record chain: Begin → UpdatePage* (from Splitter/Merger) → Commit (or Abort on rollback).
///
/// CoW model:
///   - Each write operation creates shadow copies of the write path via
///     CopyWritePathAndAllocShadows; the live tree is never modified mid-transaction.
///   - <see cref="TxRootId"/> tracks the current shadow root; <see cref="UpdateTxRoot"/>
///     advances it after each write.
///   - Shadow pages are tracked in <see cref="_allocatedPages"/> (freed on rollback).
///   - Obsolete old-path pages accumulate in <see cref="_obsoletePages"/> (retired on commit).
///   - Deferred frees (Merger's merged siblings) accumulate in <see cref="_deferredFrees"/>
///     and are epoch-retired on commit.
///
/// On rollback, before-images of ORIGINAL pages modified by Merger (e.g. the right
/// neighbor's PrevLeafPageId fix) are selectively restored by skipping pages that are
/// in <see cref="_allocatedPages"/> (shadow pages that are freed anyway).
///
/// The writer lock is acquired per-operation (not per-transaction) to allow multiple
/// concurrent transactions on the same thread (Test 5 compatibility) while still
/// serialising CoW write paths.
/// </summary>
internal sealed class Transaction<TKey, TValue> : ITransaction<TKey, TValue>, ITransactionContext
    where TKey : IComparable<TKey>
{
    private readonly TreeEngine<TKey, TValue> _engine;
    private readonly WalWriter                _walWriter;
    private readonly PageManager              _pageManager;
    private readonly uint                     _txId;
    private          LogSequenceNumber        _lastLsn;
    private          bool                     _committed;
    private          bool                     _disposed;
    private readonly TransactionCoordinator   _coordinator;

    // Before-image store: pageId → full page content captured before first write.
    // Used by Splitter/Merger (GetBeforeImage for WAL records) and for selective
    // restore of original pages on rollback.
    private readonly Dictionary<uint, byte[]> _beforeImages   = new();
    // Insertion-order tracking for LIFO selective restore on rollback.
    private readonly List<uint>               _writeOrder     = new();
    // Shadow pages (new allocations during this tx): freed on rollback.
    private readonly List<uint>               _allocatedPages = new();
    // Fast-lookup set kept in sync with _allocatedPages (path-copy pages only).
    // Exposed as OwnedShadowPages so TreeEngine can skip CoW re-copy for pages
    // already shadow-owned by this transaction.
    private readonly HashSet<uint>            _ownedShadowSet = new();
    // Deferred frees (Merger's merged siblings): epoch-retired on commit, cleared on rollback.
    private readonly List<uint>               _deferredFrees  = new();

    // ── CoW transaction state ─────────────────────────────────────────────────

    // Current working shadow root; updated by UpdateTxRoot after each CoW write.
    private uint   _txRootId;
    private uint   _txTreeHeight;
    // Snapshot values captured at construction (before any write).
    private readonly uint  _snapshotRootId;
    private readonly ulong _snapshotRecordCount;
    // Epoch registered at construction to prevent snapshot root retirement.
    private readonly ulong _snapshotEpoch;
    // Accumulated old-path pages: epoch-retired on commit.
    private readonly List<uint> _obsoletePages = new();
    // Overflow chain pages epoch-retired on commit; FreeOverflowChain WAL written at Commit.
    private readonly List<uint> _obsoleteOverflowPageIds = new();

    // ── Online compaction delta tracking (Phase 106) ─────────────────────────
    // Non-null only when delta tracking was active at transaction start.
    // Populated as each write operation is applied; handed off to engine at Commit().
    private readonly List<(TKey Key, TValue? Value, DeltaOp Op)>? _deltaKeys;

    // ── SSI read-set (Phase 88) ──────────────────────────────────────────────
    // Records every leaf page ID visited by transactional reads (TryGet, Scan,
    // CountRange). Used by SSI Phase 2 to detect read-write conflicts at commit.
    // Null until first read — write-only transactions pay zero allocation overhead.
    private static readonly IReadOnlySet<uint> _emptyReadSet = new HashSet<uint>(0);
    private HashSet<uint>? _readSet;

    /// <summary>Record a leaf page visited by a transactional read operation.</summary>
    internal void TrackLeafRead(uint leafPageId)
        => (_readSet ??= new HashSet<uint>()).Add(leafPageId);

    /// <summary>
    /// The set of leaf page IDs visited by transactional reads since this transaction began.
    /// Null (exposed as empty) for write-only transactions.
    /// </summary>
    internal IReadOnlySet<uint> ReadSet => _readSet ?? _emptyReadSet;

    // First-leaf change tracking (deferred to ApplyCoWTxCommit at commit).
    private bool _firstLeafChanged;
    private uint _newFirstLeafId;

    internal Transaction(
        TreeEngine<TKey, TValue> engine,
        WalWriter                walWriter,
        PageManager              pageManager,
        uint                     txId,
        TransactionCoordinator   coordinator)
    {
        _engine      = engine;
        _walWriter   = walWriter;
        _pageManager = pageManager;
        _txId        = txId;
        _coordinator = coordinator;

        // Writer lock NOT held for full transaction lifetime (Phase 109a).
        // Shadow page creation, WAL Begin, and all writes run concurrently.
        // The commit mutex is acquired in Commit() / CommitAsync() only for the
        // short critical section: root version check + root install + WAL append.
        _snapshotRootId      = engine.CurrentRootId;
        _txRootId            = _snapshotRootId;
        _txTreeHeight        = engine.CurrentTreeHeight;
        _snapshotRecordCount = engine.CurrentRecordCount;
        _snapshotEpoch       = _coordinator.EnterReadEpoch();
        // Allocate delta tracking list only when online compaction is in Phase A.
        if (engine.DeltaEnabled) _deltaKeys = new();

        // Hold the shared checkpoint gate before writing the Begin record.
        // This prevents TruncateWal from running while the transaction is active.
        _coordinator.EnterTransactionLock();
        // Register as an active transaction so the compaction barrier can drain us.
        _coordinator.IncrementActiveTx();
        // Write Begin record; _lastLsn starts the PrevLsn chain for this transaction.
        _lastLsn = walWriter.Append(
            WalRecordType.Begin, txId, pageId: 0,
            prevLsn: LogSequenceNumber.None,
            data: ReadOnlySpan<byte>.Empty);
    }

    public uint TransactionId => _txId;

    /// <summary>LSN of the most recently appended record for this transaction.</summary>
    public LogSequenceNumber LastLsn => _lastLsn;

    /// <summary>Update _lastLsn after a transactional MarkDirtyAndUnpin returns an LSN.</summary>
    public void UpdateLastLsn(LogSequenceNumber lsn) => _lastLsn = lsn;

    // ── CoW accessors (used by InTransaction methods in TreeEngine) ────────────

    /// <summary>Current working shadow root for this transaction.</summary>
    internal uint TxRootId => _txRootId;

    /// <summary>Current shadow tree height for this transaction.</summary>
    internal uint TxTreeHeight => _txTreeHeight;

    /// <summary>Update the working shadow root after a CoW write.</summary>
    internal void UpdateTxRoot(uint id, uint height) { _txRootId = id; _txTreeHeight = height; }

    /// <summary>Track a newly allocated shadow page. Freed on rollback.</summary>
    internal void TrackObsoletePage(uint id) => _obsoletePages.Add(id);

    /// <summary>
    /// Track an overflow chain page being CoW-retired.
    /// Adds to _obsoletePages (epoch-retired at Commit) and to _obsoleteOverflowPageIds
    /// (FreeOverflowChain WAL record written at Commit — Gap 3 closure).
    /// </summary>
    internal void TrackObsoleteOverflowPage(uint id)
    {
        _obsoletePages.Add(id);
        _obsoleteOverflowPageIds.Add(id);
    }

    /// <summary>
    /// Returns the effective first-leaf page ID within this transaction's shadow tree.
    /// If any write has already shadowed the first leaf, returns the latest shadow ID;
    /// otherwise falls back to the live tree's first leaf.
    /// Used by InTransaction methods to correctly detect and forward the first-leaf pointer.
    /// </summary>
    internal uint TxFirstLeafId(uint liveFirstLeaf)
        => _firstLeafChanged ? _newFirstLeafId : liveFirstLeaf;

    /// <summary>Track an old path page to be epoch-retired at commit.</summary>
    internal void TrackFirstLeafChange(uint newFirstLeafId)
    {
        _firstLeafChanged = true;
        _newFirstLeafId   = newFirstLeafId;
    }

    // ── ITransactionContext (used by Splitter and Merger) ─────────────────────

    /// <summary>
    /// Capture the full content of <paramref name="pageId"/> as the before-image
    /// for this transaction. Only the FIRST call per page is recorded; subsequent
    /// calls are no-ops to preserve the true pre-modification state.
    /// Also acquires an exclusive page write lock for conflict detection.
    /// </summary>
    public void CaptureBeforeImage(uint pageId, ReadOnlySpan<byte> pageData)
    {
        _coordinator.AcquirePageWriteLock(pageId, _txId);
        if (!_beforeImages.ContainsKey(pageId))
        {
            var copy = new byte[pageData.Length];
            pageData.CopyTo(copy);
            _beforeImages[pageId] = copy;
            _writeOrder.Add(pageId);
        }
    }

    /// <summary>Returns the captured before-image bytes for <paramref name="pageId"/>.</summary>
    public byte[] GetBeforeImage(uint pageId) => _beforeImages[pageId];

    /// <summary>
    /// Records a newly allocated page (from Splitter/Merger). On rollback, this page is freed.
    /// Also writes an AllocPage WAL record so crash recovery can identify and free orphaned
    /// shadow pages that were never installed in the live tree (Phase 67).
    /// </summary>
    public void TrackAllocatedPage(uint pageId)
    {
        ThrowIfDisposedOrCommitted();
        _allocatedPages.Add(pageId);
        _ownedShadowSet.Add(pageId);
        var lsn = _walWriter.Append(WalRecordType.AllocPage, _txId, pageId, _lastLsn,
                                    ReadOnlySpan<byte>.Empty);
        _lastLsn = lsn;
    }

    /// <summary>
    /// Pages already owned by this transaction as shadow copies (path-copy pages tracked
    /// via <see cref="TrackAllocatedPage"/>). TreeEngine checks this set before CoW-copying
    /// a page that was already shadow-allocated by an earlier write in the same transaction,
    /// allowing in-place reuse and avoiding O(N × H) page amplification for batch writes.
    /// </summary>
    internal IReadOnlySet<uint> OwnedShadowPages => _ownedShadowSet;

    /// <summary>
    /// Registers all pages in an overflow chain for freeing on rollback.
    /// AllocOverflowChain WAL record is already written by AllocateOverflowChain;
    /// no per-page AllocPage WAL record is written here to avoid WAL bloat.
    /// </summary>
    internal void TrackAllocatedOverflowChain(uint[] chainPageIds)
        => _allocatedPages.AddRange(chainPageIds);

    /// <summary>
    /// Defers a FreePage call until Commit(). On rollback, the page is NOT freed —
    /// it remains reachable from the live tree via the untouched original path.
    /// At commit, the page is epoch-retired via RetirePage (reader-safe).
    /// </summary>
    public void DeferFreePage(uint pageId)
    {
        ThrowIfDisposedOrCommitted();
        _deferredFrees.Add(pageId);
    }

    // ── ITransaction<TKey,TValue> ────────────────────────────────────────────

    public void Insert(TKey key, TValue value)
    {
        ThrowIfDisposedOrCommitted();
        _engine.InsertInTransaction(key, value, this);
        _deltaKeys?.Add((key, value, DeltaOp.Insert));
    }

    public bool TryUpdate(TKey key, TValue newValue)
    {
        ThrowIfDisposedOrCommitted();
        bool result = _engine.TryUpdateInTransaction(key, newValue, this);
        if (result) _deltaKeys?.Add((key, newValue, DeltaOp.Insert));
        return result;
    }

    public bool TryUpdate(TKey key, Func<TValue, TValue> updateFactory)
    {
        ThrowIfDisposedOrCommitted();
        bool result = _engine.TryUpdateWithFactoryInTransaction(key, updateFactory, this);
        if (result && _deltaKeys != null)
        {
            // Re-read the new value from the shadow tree (factory result is unknown to caller).
            _engine.TryGetInTransaction(key, this, out TValue updated);
            _deltaKeys.Add((key, updated, DeltaOp.Insert));
        }
        return result;
    }

    public bool TryDelete(TKey key)
    {
        ThrowIfDisposedOrCommitted();
        bool result = _engine.TryDeleteInTransaction(key, this);
        if (result) _deltaKeys?.Add((key, default, DeltaOp.Delete));
        return result;
    }

    public bool TryInsert(TKey key, TValue value)
    {
        ThrowIfDisposedOrCommitted();
        bool result = _engine.TryInsertInTransaction(key, value, this);
        if (result) _deltaKeys?.Add((key, value, DeltaOp.Insert));
        return result;
    }

    public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
    {
        ThrowIfDisposedOrCommitted();
        TValue result = _engine.AddOrUpdateInTransaction(key, addValue, updateValueFactory, this);
        _deltaKeys?.Add((key, result, DeltaOp.Insert));
        return result;
    }

    public TValue GetOrAdd(TKey key, TValue addValue)
    {
        ThrowIfDisposedOrCommitted();
        TValue result = _engine.GetOrAddInTransaction(key, addValue, this);
        _deltaKeys?.Add((key, result, DeltaOp.Insert));
        return result;
    }

    public bool TryGetAndDelete(TKey key, out TValue value)
    {
        ThrowIfDisposedOrCommitted();
        bool result = _engine.TryGetAndDeleteInTransaction(key, this, out value);
        if (result) _deltaKeys?.Add((key, default, DeltaOp.Delete));
        return result;
    }

    public bool TryCompareAndSwap(TKey key, TValue expected, TValue newValue, IEqualityComparer<TValue>? comparer = null)
    {
        ThrowIfDisposedOrCommitted();
        bool result = _engine.TryCompareAndSwapInTransaction(key, expected, newValue, comparer, this);
        if (result) _deltaKeys?.Add((key, newValue, DeltaOp.Insert));
        return result;
    }

    public long Count
    {
        get
        {
            ThrowIfDisposedOrCommitted();
            return _engine.GetRecordCount();
        }
    }

    public bool TryGet(TKey key, out TValue value)
    {
        ThrowIfDisposedOrCommitted();
        return _engine.TryGetInTransaction(key, this, out value);
    }

    public bool ContainsKey(TKey key)
    {
        ThrowIfDisposedOrCommitted();
        return _engine.TryGetInTransaction(key, this, out _);
    }

    public bool TryGetFirst(out TKey key, out TValue value)
    {
        ThrowIfDisposedOrCommitted();
        return _engine.TryGetFirstInTransaction(this, out key, out value);
    }

    public bool TryGetLast(out TKey key, out TValue value)
    {
        ThrowIfDisposedOrCommitted();
        return _engine.TryGetLastInTransaction(this, out key, out value);
    }

    public bool TryGetNext(TKey key, out TKey nextKey, out TValue value)
    {
        ThrowIfDisposedOrCommitted();
        return _engine.TryGetNextInTransaction(key, this, out nextKey, out value);
    }

    public bool TryGetPrev(TKey key, out TKey prevKey, out TValue value)
    {
        ThrowIfDisposedOrCommitted();
        return _engine.TryGetPrevInTransaction(key, this, out prevKey, out value);
    }

    public IEnumerable<(TKey Key, TValue Value)> Scan(
        TKey? startKey = default,
        TKey? endKey   = default)
    {
        ThrowIfDisposedOrCommitted();
        return _engine.ScanInTransaction(startKey, endKey, this);
    }

    public IEnumerable<(TKey Key, TValue Value)> ScanReverse(
        TKey? endKey   = default,
        TKey? startKey = default)
    {
        ThrowIfDisposedOrCommitted();
        return _engine.ScanReverseInTransaction(endKey, startKey, this);
    }

    public int DeleteRange(TKey startKey, TKey endKey)
    {
        ThrowIfDisposedOrCommitted();
        if (_deltaKeys != null)
        {
            // Collect keys in range before deleting (delta tracking requires knowing which keys).
            var rangeKeys = _engine.ScanInTransaction(startKey, endKey, this).Select(kv => kv.Key).ToList();
            int deleted = _engine.DeleteRangeInTransaction(startKey, endKey, this);
            foreach (var k in rangeKeys) _deltaKeys.Add((k, default, DeltaOp.Delete));
            return deleted;
        }
        return _engine.DeleteRangeInTransaction(startKey, endKey, this);
    }

    public long CountRange(TKey startKey, TKey endKey)
    {
        ThrowIfDisposedOrCommitted();
        return _engine.CountRangeInTransaction(startKey, endKey, this);
    }

    public void Commit()
    {
        ThrowIfDisposedOrCommitted();

        // SSI Phase 89: detect read-write conflicts before the commit critical section.
        // Cheap early-exit for the common stale-read case; avoids mutex contention.
        uint conflictPage = _coordinator.FindConflictingPage(_readSet, _snapshotEpoch);
        if (conflictPage != 0u)
            throw new TransactionConflictException(_txId, 0u, conflictPage);

        // ── Commit critical section (commit mutex held ~microseconds) ──────────
        _coordinator.EnterWriterLock();
        try
        {
            // Root version check: only applies when this transaction made writes.
            // If _txRootId == _snapshotRootId the transaction is effectively read-only
            // (ApplyCoWTxCommit would be a no-op) and there is no lost-update risk.
            // When the transaction DID write, abort if another transaction committed since
            // our snapshot — installing our shadow tree would silently drop their changes.
            // This catches lost updates that SSI misses for write-only transactions (Phase 109a).
            if (_txRootId != _snapshotRootId)
            {
                if (_engine.CurrentRootId != _snapshotRootId)
                    throw new TransactionConflictException(_txId, 0u, 0u);
                _engine.ApplyCoWTxCommit(_txRootId, _txTreeHeight, _firstLeafChanged, _newFirstLeafId);
            }

            // Write Commit WAL record.
            // Gap 3 closure: FreeOverflowChain AFTER Commit, flushed atomically below.
            _walWriter.Append(WalRecordType.Commit, _txId, pageId: 0,
                              _lastLsn, data: ReadOnlySpan<byte>.Empty);
            if (_obsoleteOverflowPageIds.Count > 0)
                _walWriter.AppendFreeOverflowChain(_txId, [.. _obsoleteOverflowPageIds]);

            // Phase 106: hand off delta keys inside commit mutex (_deltaMap is not thread-safe).
            if (_deltaKeys != null)
                foreach (var (k, v, op) in _deltaKeys)
                    _engine.RecordDelta(k, v, op);
        }
        finally
        {
            _coordinator.ExitWriterLock();
        }
        // ── End critical section ─────────────────────────────────────────────

        // WAL flush OUTSIDE commit mutex — concurrent flushes coalesce (group commit).
        _walWriter.Flush();

        // Epoch-retire old-path pages (no longer reachable from the new live root).
        foreach (uint pageId in _obsoletePages)
            _coordinator.RetirePage(pageId);

        // Epoch-retire deferred frees (Merger's merged siblings).
        foreach (uint pageId in _deferredFrees)
            _coordinator.RetirePage(pageId);

        _committed = true;
        _coordinator.ExitReadEpoch(_snapshotEpoch);
        _coordinator.ReleaseAllLocks(_txId);
        _coordinator.DecrementActiveTx();
        _coordinator.ExitTransactionLock();
    }

    public Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposedOrCommitted();

        // SSI Phase 89: same read-write conflict check as Commit().
        uint conflictPage = _coordinator.FindConflictingPage(_readSet, _snapshotEpoch);
        if (conflictPage != 0u)
            throw new TransactionConflictException(_txId, 0u, conflictPage);

        // ── Commit critical section (commit mutex held ~microseconds) ──────────
        _coordinator.EnterWriterLock();
        try
        {
            // Root version check (same as Commit() — only when this tx made writes).
            if (_txRootId != _snapshotRootId)
            {
                if (_engine.CurrentRootId != _snapshotRootId)
                    throw new TransactionConflictException(_txId, 0u, 0u);
                _engine.ApplyCoWTxCommit(_txRootId, _txTreeHeight, _firstLeafChanged, _newFirstLeafId);
            }

            _walWriter.Append(WalRecordType.Commit, _txId, pageId: 0,
                              _lastLsn, data: ReadOnlySpan<byte>.Empty);
            if (_obsoleteOverflowPageIds.Count > 0)
                _walWriter.AppendFreeOverflowChain(_txId, [.. _obsoleteOverflowPageIds]);

            // Phase 106: delta hand-off inside commit mutex (_deltaMap is not thread-safe).
            if (_deltaKeys != null)
                foreach (var (k, v, op) in _deltaKeys)
                    _engine.RecordDelta(k, v, op);
        }
        finally
        {
            _coordinator.ExitWriterLock();
        }
        // ── End critical section ─────────────────────────────────────────────

        // WAL flush: async fsync task returned to caller.
        Task fsyncTask = _walWriter.FlushAsync(cancellationToken);

        // Release all engine locks on the calling thread BEFORE returning the async Task.
        // ReaderWriterLockSlim.ExitReadLock() is thread-affine; must not cross thread boundary.
        foreach (uint pageId in _obsoletePages)
            _coordinator.RetirePage(pageId);
        foreach (uint pageId in _deferredFrees)
            _coordinator.RetirePage(pageId);

        _committed = true;
        _coordinator.ExitReadEpoch(_snapshotEpoch);
        _coordinator.ReleaseAllLocks(_txId);
        _coordinator.DecrementActiveTx();
        _coordinator.ExitTransactionLock();

        return fsyncTask;
    }

    public void InsertRange(IEnumerable<(TKey Key, TValue Value)> items)
    {
        ThrowIfDisposedOrCommitted();
        foreach (var (key, value) in items)
            Insert(key, value);
    }

    public void InsertRange(IEnumerable<KeyValuePair<TKey, TValue>> items)
    {
        ThrowIfDisposedOrCommitted();
        foreach (var kvp in items)
            Insert(kvp.Key, kvp.Value);
    }

    public IEnumerator<(TKey Key, TValue Value)> GetEnumerator()
        => Scan().GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (!_committed)
        {
            try
            {
                // CoW rollback: the live tree (from _snapshotRootId) is completely untouched.
                // Only restore before-images of ORIGINAL pages modified by Merger/Splitter.
                // Shadow pages (in _allocatedPages) are skipped — they are freed below.
                var allocSet = new HashSet<uint>(_allocatedPages);
                for (int i = _writeOrder.Count - 1; i >= 0; i--)
                {
                    uint   pageId = _writeOrder[i];
                    if (allocSet.Contains(pageId)) continue; // shadow page — skip, being freed
                    byte[] before = _beforeImages[pageId];
                    var    frame  = _pageManager.FetchPage(pageId);
                    before.CopyTo(frame.Data, 0);
                    // txId=0: rollback writes are auto-commit — never re-undone by recovery.
                    _pageManager.MarkDirtyAndUnpin(pageId);
                }

                // Free shadow pages (they were never installed in the live tree).
                foreach (uint pageId in _allocatedPages)
                    _pageManager.FreePage(pageId);

                // Restore record count (undoes all IncrementRecordCount/DecrementRecordCount calls).
                _engine.RestoreSnapshotRecordCount(_snapshotRecordCount);

                // Discard deferred frees — merged siblings remain reachable from the live tree.
                _deferredFrees.Clear();

                // Write Abort WAL record.
                _walWriter.Append(WalRecordType.Abort, _txId, pageId: 0,
                                  _lastLsn, data: ReadOnlySpan<byte>.Empty);
                _walWriter.Flush();
            }
            finally
            {
                // Lock release MUST run even if rollback throws (e.g. FetchPage on an
                // exhausted pool). A leaked writer lock deadlocks every subsequent caller.
                // Note: commit mutex NOT held on rollback — never acquired (Phase 109a).
                _coordinator.ExitReadEpoch(_snapshotEpoch);
                _coordinator.ReleaseAllLocks(_txId);
                _coordinator.DecrementActiveTx();
                _coordinator.ExitTransactionLock();
            }
        }
        // Committed: ExitReadEpoch + ReleaseAllLocks + ExitTransactionLock already called in Commit().

        _beforeImages.Clear();
        _writeOrder.Clear();
        _allocatedPages.Clear();
        _ownedShadowSet.Clear();
        _obsoletePages.Clear();
        _obsoleteOverflowPageIds.Clear();
    }

    private void ThrowIfDisposedOrCommitted()
    {
        if (_disposed)  throw new ObjectDisposedException(nameof(Transaction<TKey, TValue>));
        if (_committed) throw new InvalidOperationException("Transaction has already been committed.");
    }
}
