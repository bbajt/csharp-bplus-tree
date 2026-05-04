using ByTech.BPlusTree.Core.Engine;

namespace ByTech.BPlusTree.Core.Api;

/// <summary>
/// Ergonomic transaction scope: rollback-on-exception is the default.
///
/// Call <see cref="Complete"/> before the scope exits to commit all operations;
/// omitting it (or leaving the <c>using</c> block via exception or early return)
/// causes automatic rollback on <see cref="Dispose"/>.
///
/// <code>
/// using var scope = tree.BeginScope();
/// scope.Insert(1, "a");
/// scope.Insert(2, "b");
/// scope.Complete();   // absent → rollback on Dispose
/// </code>
///
/// If <see cref="Complete"/> is called and the underlying <see cref="Commit"/>
/// throws (e.g. WAL write failure), <see cref="Dispose"/> still releases all
/// page locks and transaction resources via the inner transaction's rollback path.
/// </summary>
public sealed class BPlusTreeScope<TKey, TValue> : IDisposable, IAsyncDisposable
    where TKey : notnull
{
    private readonly ITransaction<TKey, TValue> _tx;
    private bool _completed;
    private bool _disposed;

    internal BPlusTreeScope(ITransaction<TKey, TValue> tx) => _tx = tx;

    /// <summary>
    /// Mark this scope as successfully completed.
    /// <see cref="Dispose"/> will commit the underlying transaction.
    /// Calling <see cref="Complete"/> more than once is a no-op.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The scope has already been disposed.</exception>
    public void Complete()
    {
        ThrowIfDisposed();
        _completed = true;
    }

    /// <summary>Insert a key/value pair. If the key exists, its value is updated.</summary>
    public void Insert(TKey key, TValue value)
    { ThrowIfDisposed(); _tx.Insert(key, value); }

    /// <summary>Update value for an existing key. Returns false if key not found.</summary>
    public bool TryUpdate(TKey key, TValue newValue)
    { ThrowIfDisposed(); return _tx.TryUpdate(key, newValue); }

    /// <summary>Compute and update the value for an existing key within this scope (read-your-own-writes).
    /// Returns false if key not found; factory is never called in that case.</summary>
    public bool TryUpdate(TKey key, Func<TValue, TValue> updateFactory)
    { ThrowIfDisposed(); return _tx.TryUpdate(key, updateFactory); }

    /// <summary>Delete a key. Returns false if key not found.</summary>
    public bool TryDelete(TKey key)
    { ThrowIfDisposed(); return _tx.TryDelete(key); }

    /// <summary>Insert a key/value pair only if the key does not already exist.</summary>
    public bool TryInsert(TKey key, TValue value)
    { ThrowIfDisposed(); return _tx.TryInsert(key, value); }

    /// <summary>
    /// Inserts all <paramref name="items"/> into this scope's shadow tree.
    /// Equivalent to calling <see cref="Insert"/> for each item.
    /// All items are committed or rolled back together with the rest of the scope.
    /// </summary>
    public void InsertRange(IEnumerable<(TKey Key, TValue Value)> items)
    { ThrowIfDisposed(); _tx.InsertRange(items); }

    /// <summary>
    /// Overload accepting <see cref="KeyValuePair{TKey,TValue}"/> for interoperability
    /// with dictionary-based sources.
    /// </summary>
    public void InsertRange(IEnumerable<KeyValuePair<TKey, TValue>> items)
    { ThrowIfDisposed(); _tx.InsertRange(items); }

    /// <summary>Atomically add or update a key/value pair within this scope (read-your-own-writes).</summary>
    public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
    { ThrowIfDisposed(); return _tx.AddOrUpdate(key, addValue, updateValueFactory); }

    /// <summary>Fetch or insert a key/value pair within this scope (read-your-own-writes).</summary>
    public TValue GetOrAdd(TKey key, TValue addValue)
    { ThrowIfDisposed(); return _tx.GetOrAdd(key, addValue); }

    /// <summary>Atomically read and delete a key/value pair within this scope (read-your-own-writes).</summary>
    public bool TryGetAndDelete(TKey key, out TValue value)
    { ThrowIfDisposed(); return _tx.TryGetAndDelete(key, out value); }

    /// <summary>Atomically update the value for <paramref name="key"/> only if the stored value equals
    /// <paramref name="expected"/>. Returns true and updates to <paramref name="newValue"/> on match;
    /// returns false and leaves tree unchanged if key absent or value mismatches (read-your-own-writes).
    /// <paramref name="comparer"/> defaults to <see cref="EqualityComparer{TValue}.Default"/> when null.</summary>
    public bool TryCompareAndSwap(TKey key, TValue expected, TValue newValue, IEqualityComparer<TValue>? comparer = null)
    { ThrowIfDisposed(); return _tx.TryCompareAndSwap(key, expected, newValue, comparer); }

    /// <summary>
    /// The number of key-value pairs visible to this scope (read-your-own-writes).
    /// </summary>
    public long Count
    {
        get { ThrowIfDisposed(); return _tx.Count; }
    }

    /// <summary>
    /// Search for the given key within this scope's snapshot.
    /// Reads include all writes made within this scope (read-your-own-writes).
    /// </summary>
    public bool TryGet(TKey key, out TValue value)
    { ThrowIfDisposed(); return _tx.TryGet(key, out value); }

    /// <summary>Return the smallest key-value pair visible to this scope (read-your-own-writes).</summary>
    public bool TryGetFirst(out TKey key, out TValue value)
    { ThrowIfDisposed(); return _tx.TryGetFirst(out key, out value); }

    /// <summary>Return the largest key-value pair visible to this scope (read-your-own-writes).</summary>
    public bool TryGetLast(out TKey key, out TValue value)
    { ThrowIfDisposed(); return _tx.TryGetLast(out key, out value); }

    /// <summary>Return the smallest key strictly greater than <paramref name="key"/> (read-your-own-writes).</summary>
    public bool TryGetNext(TKey key, out TKey nextKey, out TValue value)
    { ThrowIfDisposed(); return _tx.TryGetNext(key, out nextKey, out value); }

    /// <summary>Return the largest key strictly less than <paramref name="key"/> (read-your-own-writes).</summary>
    public bool TryGetPrev(TKey key, out TKey prevKey, out TValue value)
    { ThrowIfDisposed(); return _tx.TryGetPrev(key, out prevKey, out value); }

    /// <summary>
    /// Enumerate all key-value pairs in [startKey, endKey] as seen by this scope's snapshot.
    /// In-scope inserts and deletes are included.
    /// </summary>
    public IEnumerable<(TKey Key, TValue Value)> Scan(
        TKey? startKey = default, TKey? endKey = default)
    { ThrowIfDisposed(); return _tx.Scan(startKey, endKey); }

    /// <summary>
    /// Enumerate all key-value pairs in [startKey, endKey] in descending key order
    /// as seen by this scope's snapshot. In-scope inserts and deletes are included.
    /// </summary>
    public IEnumerable<(TKey Key, TValue Value)> ScanReverse(
        TKey? endKey = default, TKey? startKey = default)
    { ThrowIfDisposed(); return _tx.ScanReverse(endKey, startKey); }

    /// <summary>
    /// Delete all keys in the closed interval [startKey, endKey].
    /// Returns the number of keys deleted.
    /// </summary>
    public int DeleteRange(TKey startKey, TKey endKey)
    { ThrowIfDisposed(); return _tx.DeleteRange(startKey, endKey); }

    /// <summary>Count keys in the closed interval [startKey, endKey] (read-your-own-writes).</summary>
    public long CountRange(TKey startKey, TKey endKey)
    { ThrowIfDisposed(); return _tx.CountRange(startKey, endKey); }

    /// <summary>
    /// Commits the underlying transaction if <see cref="Complete"/> was called;
    /// otherwise rolls back. Idempotent — subsequent calls are no-ops.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        try
        {
            if (_completed)
                _tx.Commit();
        }
        finally
        {
            _tx.Dispose();
        }
    }

    /// <summary>
    /// Async variant of <see cref="Dispose"/>. If <see cref="Complete"/> was called,
    /// commits via <see cref="ITransaction{TKey,TValue}.CommitAsync"/> (non-blocking fsync);
    /// otherwise rolls back synchronously. Idempotent — subsequent calls are no-ops.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        try
        {
            if (_completed)
                await _tx.CommitAsync();
        }
        finally
        {
            _tx.Dispose();
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(BPlusTreeScope<TKey, TValue>));
    }
}
