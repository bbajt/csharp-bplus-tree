using System.Collections;
using System.ComponentModel;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;

namespace BPlusTree.Core.Api;

/// <summary>
/// Public entry point. Thread-safe.
/// All operations delegate to internal components.
/// </summary>
public sealed class BPlusTree<TKey, TValue>
    : IReadableBPlusTree<TKey, TValue>,
      IReadOnlyDictionary<TKey, TValue>,
      IBPlusTreeMaintenance,
      IDisposable
    where TKey : IComparable<TKey>
{
    private readonly PageManager                   _pageManager;
    private readonly WalWriter                     _walWriter;
    private readonly TreeEngine<TKey, TValue>      _engine;
    private readonly CheckpointManager             _checkpointManager;
    private readonly EvictionWorker                _evictionWorker;
    private bool _disposed;

    public WalSyncMode SyncMode { get; }

    /// <summary>
    /// Raised when <see cref="Open"/> detects a condition that may degrade durability
    /// or performance. The string argument is a human-readable description.
    /// Subscribe before calling <see cref="Open"/> to receive warnings.
    /// </summary>
    public static event Action<string>? TreeWarning;

    /// <summary>
    /// Raised on the calling thread immediately before a compaction begins.
    /// The argument is the data file path being compacted.
    /// Static: shared across all <see cref="BPlusTree{TKey,TValue}"/> instances.
    /// </summary>
    public static event Action<string>? CompactionStarted;

    /// <summary>
    /// Raised on the calling thread immediately after a compaction completes successfully.
    /// The first argument is the data file path; the second is the compaction outcome.
    /// Static: shared across all <see cref="BPlusTree{TKey,TValue}"/> instances.
    /// </summary>
    public static event Action<string, CompactionResult>? CompactionCompleted;

    private void ThrowIfDisposed()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(BPlusTree<TKey, TValue>));
    }

    /// <summary>
    /// Retry <paramref name="operation"/> until it succeeds, spinning on
    /// <see cref="TransactionConflictException"/> (Phase 109b: auto-commit backs off
    /// when a concurrent transaction holds page write locks).
    /// </summary>
    private static T RetryOnConflict<T>(Func<T> operation, CancellationToken cancellationToken = default)
    {
        SpinWait spin = default;
        while (true)
        {
            try { return operation(); }
            catch (TransactionConflictException)
            {
                cancellationToken.ThrowIfCancellationRequested();
                spin.SpinOnce();
            }
        }
    }

    private BPlusTree(
        PageManager pageManager,
        WalWriter walWriter,
        TreeEngine<TKey, TValue> engine,
        CheckpointManager checkpointManager,
        EvictionWorker evictionWorker,
        WalSyncMode syncMode)
    {
        _pageManager        = pageManager;
        _walWriter          = walWriter;
        _engine             = engine;
        _checkpointManager  = checkpointManager;
        _evictionWorker     = evictionWorker;
        SyncMode            = syncMode;
    }

    /// <summary>
    /// Open or create a B+ tree.
    /// Validates options, runs WAL recovery if the data file exists, returns a ready instance.
    /// </summary>
    public static BPlusTree<TKey, TValue> Open(
        BPlusTreeOptions options,
        IKeySerializer<TKey> keySerializer,
        IValueSerializer<TValue> valueSerializer)
    {
        ArgumentNullException.ThrowIfNull(keySerializer);
        ArgumentNullException.ThrowIfNull(valueSerializer);
        options.Validate();

        if (options.IsCheckpointThresholdOversized)
            TreeWarning?.Invoke(
                $"Performance warning: CheckpointThreshold ({options.CheckpointThreshold}) " +
                $"≥ BufferPoolCapacity ({options.BufferPoolCapacity}). " +
                $"Auto-checkpoint may not fire until the buffer pool is full. " +
                $"Consider setting CheckpointThreshold < {options.BufferPoolCapacity}.");

        if (options.WillOverflowWalBuffer)
        {
            long walPerCycle = (long)options.CheckpointThreshold * options.PageSize;
            TreeWarning?.Invoke(
                $"Performance warning: WAL buffer will overflow during checkpoints. " +
                $"CheckpointThreshold ({options.CheckpointThreshold}) × PageSize ({options.PageSize}) " +
                $"= {walPerCycle / 1024}KB per cycle, exceeds WalBufferSize ({options.WalBufferSize / 1024}KB). " +
                $"Each overflow adds a synchronous fsync. " +
                $"Max safe CheckpointThreshold: {options.MaxSafeCheckpointThreshold} pages. " +
                $"To fix: set CheckpointThreshold ≤ {options.MaxSafeCheckpointThreshold}, " +
                $"or set WalBufferSize ≥ {walPerCycle / 1024}KB.");
        }

        var walWriter   = WalWriter.Open(
            options.WalFilePath,
            bufferSize:      options.WalBufferSize,
            syncMode:        options.SyncMode,
            flushIntervalMs: options.FlushIntervalMs,
            flushBatchSize:  options.FlushBatchSize);
        var pageManager = PageManager.Open(options, walWriter);
        var ns          = new NodeSerializer<TKey, TValue>(keySerializer, valueSerializer);
        var metadata    = new TreeMetadata(pageManager);
        metadata.Load();
        var engine      = new TreeEngine<TKey, TValue>(pageManager, ns, metadata);
        var ckptMgr     = engine.CheckpointManager
            ?? throw new InvalidOperationException("CheckpointManager requires a WAL.");

        // Start the async eviction worker. Must be started after PageManager and WalWriter
        // are fully initialised, and must be stopped before engine.Close() in Dispose.
        var evictionWorker = new EvictionWorker(
            pageManager.BufferPool,
            pageManager.Storage,
            walWriter,
            options);
        evictionWorker.Start();

        // Start WAL size-based auto-checkpoint if configured.
        if (options.WalAutoCheckpointThresholdBytes > 0)
            engine.StartAutoCheckpoint(options.WalAutoCheckpointThresholdBytes);

        return new BPlusTree<TKey, TValue>(pageManager, walWriter, engine, ckptMgr, evictionWorker, options.SyncMode);
    }

    /// <summary>
    /// Begin a new multi-operation atomic transaction.
    /// All operations performed on the returned <see cref="ITransaction{TKey,TValue}"/>
    /// are atomic: either all commit or all roll back on <see cref="ITransaction{TKey,TValue}.Dispose"/>.
    /// </summary>
    public ITransaction<TKey, TValue> BeginTransaction()
    {
        ThrowIfDisposed();
        return _engine.BeginTransaction();
    }

    /// <summary>
    /// Open a point-in-time read-only snapshot of the tree.
    /// The snapshot sees the committed state at the moment this call returns.
    /// Writes committed after the snapshot is opened are never visible.
    /// Unlike <see cref="BeginTransaction"/>, a snapshot does not hold the
    /// checkpoint gate lock and therefore does not block WAL truncation or
    /// auto-checkpoints.
    /// Dispose the snapshot when done to release the epoch token.
    /// </summary>
    public ISnapshot<TKey, TValue> BeginSnapshot()
    {
        ThrowIfDisposed();
        return _engine.BeginSnapshot();
    }

    /// <summary>
    /// Begin a transaction scope. Call <see cref="BPlusTreeScope{TKey,TValue}.Complete"/> before
    /// the scope exits to commit all operations; omitting it (or exiting via exception) rolls
    /// back all operations automatically on <see cref="BPlusTreeScope{TKey,TValue}.Dispose"/>.
    /// </summary>
    public BPlusTreeScope<TKey, TValue> BeginScope()
    {
        ThrowIfDisposed();
        return new BPlusTreeScope<TKey, TValue>(_engine.BeginTransaction());
    }

    /// <summary>The number of key-value pairs currently in the tree.</summary>
    public long Count { get { ThrowIfDisposed(); return _engine.GetRecordCount(); } }

    public bool TryGet(TKey key, out TValue value)  { ThrowIfDisposed(); return _engine.TryGet(key, out value); }
    public bool Put(TKey key, TValue value, CancellationToken ct = default)    { ThrowIfDisposed(); return RetryOnConflict(() => _engine.Insert(key, value), ct); }
    public bool Delete(TKey key, CancellationToken ct = default)               { ThrowIfDisposed(); return RetryOnConflict(() => _engine.Delete(key), ct); }
    public bool ContainsKey(TKey key)               { ThrowIfDisposed(); return _engine.TryGet(key, out _); }

    /// <summary>Insert a key/value pair only if the key does not already exist.
    /// Returns true if inserted; false if the key already existed (value unchanged).</summary>
    public bool TryInsert(TKey key, TValue value, CancellationToken ct = default)
    { ThrowIfDisposed(); return RetryOnConflict(() => _engine.TryInsert(key, value), ct); }

    /// <summary>Update value for an existing key. Returns false if key not found.</summary>
    public bool TryUpdate(TKey key, TValue newValue, CancellationToken ct = default)
    { ThrowIfDisposed(); return RetryOnConflict(() => _engine.Update(key, _ => newValue), ct); }

    /// <summary>Compute and update the value for an existing key.
    /// If key is absent, the factory is never called and false is returned.</summary>
    public bool TryUpdate(TKey key, Func<TValue, TValue> updateFactory, CancellationToken ct = default)
    { ThrowIfDisposed(); return RetryOnConflict(() => _engine.Update(key, updateFactory), ct); }

    /// <summary>
    /// Atomically add or update a key/value pair.
    /// If <paramref name="key"/> is absent: inserts <paramref name="addValue"/>; returns <paramref name="addValue"/>.
    /// If <paramref name="key"/> is present: calls <paramref name="updateValueFactory"/>(<paramref name="key"/>, existingValue),
    /// overwrites with the result, and returns the result.
    /// </summary>
    public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory, CancellationToken ct = default)
    { ThrowIfDisposed(); return RetryOnConflict(() => _engine.AddOrUpdate(key, addValue, updateValueFactory), ct); }

    /// <summary>
    /// Fetch or insert a key/value pair.
    /// If <paramref name="key"/> is present: returns the existing value; tree is unchanged.
    /// If <paramref name="key"/> is absent: inserts <paramref name="addValue"/> and returns it.
    /// </summary>
    public TValue GetOrAdd(TKey key, TValue addValue, CancellationToken ct = default)
    { ThrowIfDisposed(); return RetryOnConflict(() => _engine.GetOrAdd(key, addValue), ct); }

    /// <summary>
    /// Atomically read and delete a key/value pair.
    /// If <paramref name="key"/> is absent: sets <paramref name="value"/> to default and returns false.
    /// If <paramref name="key"/> is present: captures the value, deletes the key, and returns true.
    /// </summary>
    public bool TryGetAndDelete(TKey key, out TValue value, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        TValue captured = default!;
        bool result = RetryOnConflict(() => { bool r = _engine.TryGetAndDelete(key, out captured); return r; }, ct);
        value = captured;
        return result;
    }

    /// <summary>
    /// Atomically update the value for <paramref name="key"/> only if the stored value equals
    /// <paramref name="expected"/>. Returns true and updates to <paramref name="newValue"/> on match;
    /// returns false and leaves tree unchanged if key absent or value mismatches.
    /// <paramref name="comparer"/> defaults to <see cref="EqualityComparer{TValue}.Default"/> when null.
    /// </summary>
    public bool TryCompareAndSwap(TKey key, TValue expected, TValue newValue, IEqualityComparer<TValue>? comparer = null, CancellationToken ct = default)
    { ThrowIfDisposed(); return RetryOnConflict(() => _engine.TryCompareAndSwap(key, expected, newValue, comparer), ct); }

    public IEnumerator<(TKey Key, TValue Value)> GetEnumerator()
        => Scan().GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    // ── IReadOnlyDictionary<TKey,TValue> ─────────────────────────────────────

    TValue IReadOnlyDictionary<TKey, TValue>.this[TKey key]
        => TryGet(key, out var v)
            ? v
            : throw new KeyNotFoundException($"The key '{key}' was not found in the tree.");

    IEnumerable<TKey>   IReadOnlyDictionary<TKey, TValue>.Keys
        => Scan().Select(p => p.Key);

    IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values
        => Scan().Select(p => p.Value);

    bool IReadOnlyDictionary<TKey, TValue>.ContainsKey(TKey key)
        => ContainsKey(key);

    bool IReadOnlyDictionary<TKey, TValue>.TryGetValue(TKey key, out TValue value)
        => TryGet(key, out value);

    int IReadOnlyCollection<KeyValuePair<TKey, TValue>>.Count
        => checked((int)Count);

    IEnumerator<KeyValuePair<TKey, TValue>>
        IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        => Scan()
            .Select(p => new KeyValuePair<TKey, TValue>(p.Key, p.Value))
            .GetEnumerator();

    /// <summary>Return the smallest key-value pair in the tree. Returns false if empty.</summary>
    public bool TryGetFirst(out TKey key, out TValue value)
    { ThrowIfDisposed(); return _engine.TryGetFirst(out key, out value); }

    /// <summary>Return the largest key-value pair in the tree. Returns false if empty.</summary>
    public bool TryGetLast(out TKey key, out TValue value)
    { ThrowIfDisposed(); return _engine.TryGetLast(out key, out value); }

    /// <summary>Return the smallest key strictly greater than <paramref name="key"/>. Returns false if none exists.</summary>
    public bool TryGetNext(TKey key, out TKey nextKey, out TValue value)
    { ThrowIfDisposed(); return _engine.TryGetNext(key, out nextKey, out value); }

    /// <summary>Return the largest key strictly less than <paramref name="key"/>. Returns false if none exists.</summary>
    public bool TryGetPrev(TKey key, out TKey prevKey, out TValue value)
    { ThrowIfDisposed(); return _engine.TryGetPrev(key, out prevKey, out value); }

    public IEnumerable<(TKey Key, TValue Value)> Scan(
        TKey? startKey = default, TKey? endKey = default)
    {
        ThrowIfDisposed();
        return _engine.Scan(startKey, endKey);
    }

    /// <summary>
    /// Enumerate all key-value pairs in [startKey, endKey] in descending key order.
    /// Null endKey = from the largest key. Null startKey = to the smallest key.
    /// </summary>
    public IEnumerable<(TKey Key, TValue Value)> ScanReverse(
        TKey? endKey = default, TKey? startKey = default)
    {
        ThrowIfDisposed();
        return _engine.ScanReverse(endKey, startKey);
    }

    /// <summary>
    /// Atomically delete all keys in the closed interval [startKey, endKey].
    /// Returns the number of keys deleted.
    /// </summary>
    public int DeleteRange(TKey startKey, TKey endKey)
    {
        ThrowIfDisposed();
        return _engine.DeleteRange(startKey, endKey);
    }

    /// <summary>Count keys in the closed interval [startKey, endKey].</summary>
    public long CountRange(TKey startKey, TKey endKey)
    { ThrowIfDisposed(); return _engine.CountRange(startKey, endKey); }

    /// <summary>
    /// Inserts all <paramref name="items"/> atomically in a single WAL flush.
    /// If a key already exists its value is overwritten (upsert semantics, same as <see cref="Put"/>).
    /// If an exception is thrown during enumeration or commit, all changes are rolled back —
    /// either all items are visible after the call or none are.
    /// </summary>
    public void PutRange(IEnumerable<(TKey Key, TValue Value)> items)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(items);
        using var tx = BeginTransaction();
        tx.InsertRange(items);
        tx.Commit();
    }

    /// <summary>
    /// Overload accepting <see cref="KeyValuePair{TKey,TValue}"/> for interoperability
    /// with dictionary-based sources.
    /// </summary>
    public void PutRange(IEnumerable<KeyValuePair<TKey, TValue>> items)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(items);
        PutRange(items.Select(kvp => (kvp.Key, kvp.Value)));
    }

    public void Checkpoint() { ThrowIfDisposed(); _checkpointManager.TakeCheckpoint(); }
    public CompactionResult Compact()
    {
        ThrowIfDisposed();
        CompactionStarted?.Invoke(_pageManager.DataFilePath);
        var result = _engine.Compact();
        CompactionCompleted?.Invoke(_pageManager.DataFilePath, result);
        return result;
    }

    /// <summary>
    /// Blocks until all writes that have returned before this call are durable on disk.
    ///
    /// In <see cref="WalSyncMode.Synchronous"/> mode this is a near-instant no-op —
    /// every write is already fsynced before its originating call returns.
    ///
    /// In <see cref="WalSyncMode.GroupCommit"/> mode this blocks until the WAL has been
    /// fsynced at least up to the position current at call time. Use this to obtain an
    /// explicit durability guarantee after a batch of <see cref="Put"/> or
    /// <see cref="Delete"/> calls, without switching the entire tree to Synchronous mode.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The tree has been disposed.</exception>
    public void Flush()
    {
        ThrowIfDisposed();
        _walWriter.FlushUpTo(_walWriter.CurrentLsn.Value);
    }

    public TreeStatistics GetStatistics()
    {
        ThrowIfDisposed();
        return new TreeStatistics
        {
            TotalPages                  = (int)_pageManager.TotalPageCount,
            TotalRecords                = _engine.Metadata.TotalRecordCount,
            TreeHeight                  = _engine.Metadata.TreeHeight,
            FreePages                   = _pageManager.FreeList.Count,
            BufferPoolHits              = _pageManager.BufferPool.HitCount,
            BufferPoolMisses            = _pageManager.BufferPool.MissCount,
            WalSizeBytes                = File.Exists(_pageManager.WalFilePath)
                                              ? new FileInfo(_pageManager.WalFilePath).Length : 0L,
            BufferPoolOccupancyFraction = _pageManager.BufferPool.OccupancyFraction,
            DirtyPageCount              = _pageManager.BufferPool.DirtyCount,
            ActiveTransactionCount      = _engine.Coordinator.ActiveTransactionCount,
            ActiveSnapshotCount         = _engine.Coordinator.ActiveSnapshotCount,
        };
    }

    public Engine.ValidationResult Validate()
    {
        ThrowIfDisposed();
        return _engine.Validate();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        // Order is non-negotiable — see PHASE-26.MD Known Failure Point #5.
        _evictionWorker.Dispose(); // 1. Stop worker (FlushAll + join) — references storage + wal
        _engine.Close();           // 2. GracefulClose (checkpoint + flush) + latches
        _pageManager.Dispose();    // 3. FlushAllDirty, close storage, dispose WAL
    }

    /// <summary>Alias for <see cref="Dispose"/>. Idempotent — safe to call multiple times.</summary>
    public void Close() => Dispose();

    /// <summary>Test-seam accessor for WalWriter. Not part of the public API.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    internal WalWriter GetWalWriterForTesting() => _walWriter;
}
