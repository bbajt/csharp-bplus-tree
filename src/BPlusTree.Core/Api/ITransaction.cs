namespace ByTech.BPlusTree.Core.Api;

/// <summary>
/// Represents an in-progress multi-operation transaction.
/// Call <see cref="Commit"/> to make all operations durable and visible.
/// Disposing without calling <see cref="Commit"/> rolls back all operations.
/// </summary>
public interface ITransaction<TKey, TValue> : IReadableBPlusTree<TKey, TValue>, IDisposable
    where TKey : notnull
{
    /// <summary>Stable identifier for this transaction, written into WAL records.</summary>
    uint TransactionId { get; }

    /// <summary>Insert a key/value pair. If the key exists, its value is updated.</summary>
    void Insert(TKey key, TValue value);

    /// <summary>Update value for an existing key. Returns false if key not found.</summary>
    bool TryUpdate(TKey key, TValue newValue);

    /// <summary>
    /// Compute and update the value for an existing key within this transaction.
    /// If <paramref name="key"/> is absent, <paramref name="updateFactory"/> is never called
    /// and the method returns false. If present, overwrites with the factory result and returns true.
    /// Reads are from the transaction's shadow tree (read-your-own-writes).
    /// </summary>
    bool TryUpdate(TKey key, Func<TValue, TValue> updateFactory);

    /// <summary>Delete a key. Returns false if key not found.</summary>
    bool TryDelete(TKey key);

    /// <summary>
    /// Insert a key/value pair only if the key does not already exist.
    /// Returns true if inserted; false if the key already existed (value unchanged).
    /// </summary>
    bool TryInsert(TKey key, TValue value);

    /// <summary>
    /// Atomically add or update a key/value pair within this transaction.
    /// If <paramref name="key"/> is absent: inserts <paramref name="addValue"/>; returns <paramref name="addValue"/>.
    /// If <paramref name="key"/> is present: calls <paramref name="updateValueFactory"/>(<paramref name="key"/>, existingValue),
    /// overwrites with the result, and returns the result.
    /// Reads are from the transaction's shadow tree (read-your-own-writes).
    /// </summary>
    TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory);

    /// <summary>
    /// Fetch or insert a key/value pair within this transaction.
    /// If <paramref name="key"/> is present: returns the existing value; shadow tree unchanged.
    /// If <paramref name="key"/> is absent: inserts <paramref name="addValue"/> and returns it.
    /// Reads are from the transaction's shadow tree (read-your-own-writes).
    /// </summary>
    TValue GetOrAdd(TKey key, TValue addValue);

    /// <summary>
    /// Atomically read and delete a key/value pair within this transaction.
    /// If <paramref name="key"/> is absent: sets <paramref name="value"/> to default and returns false.
    /// If <paramref name="key"/> is present: captures the value, deletes the key, and returns true.
    /// Reads and deletes are within the transaction's shadow tree (read-your-own-writes).
    /// </summary>
    bool TryGetAndDelete(TKey key, out TValue value);

    /// <summary>
    /// Atomically update the value for <paramref name="key"/> only if the current stored
    /// value equals <paramref name="expected"/>.
    /// If <paramref name="key"/> is absent: returns false; tree is unchanged.
    /// If <paramref name="key"/> is present but stored value ≠ expected: returns false; tree is unchanged.
    /// If <paramref name="key"/> is present and stored value == expected: updates to
    /// <paramref name="newValue"/> and returns true.
    /// Reads and writes are within the transaction's shadow tree (read-your-own-writes).
    /// <paramref name="comparer"/> defaults to <see cref="EqualityComparer{TValue}.Default"/> when null.
    /// </summary>
    bool TryCompareAndSwap(
        TKey key,
        TValue expected,
        TValue newValue,
        IEqualityComparer<TValue>? comparer = null);

    /// <summary>
    /// Delete all keys in the closed interval [startKey, endKey].
    /// Returns the number of keys deleted.
    /// Keys not present in the tree are silently skipped.
    /// </summary>
    int DeleteRange(TKey startKey, TKey endKey);

    /// <summary>
    /// Inserts all <paramref name="items"/> into this transaction's shadow tree.
    /// Equivalent to calling <see cref="Insert"/> for each item.
    /// All items are part of this transaction — they are committed or rolled back
    /// together with the rest of the transaction.
    /// </summary>
    void InsertRange(IEnumerable<(TKey Key, TValue Value)> items);

    /// <summary>
    /// Overload accepting <see cref="KeyValuePair{TKey,TValue}"/> for interoperability
    /// with dictionary-based sources.
    /// </summary>
    void InsertRange(IEnumerable<KeyValuePair<TKey, TValue>> items);

    /// <summary>
    /// Flush all operations to WAL and write a Commit record.
    /// After this returns, all operations are durable and crash-safe.
    /// </summary>
    void Commit();

    /// <summary>
    /// Async variant of <see cref="Commit"/>. Flushes all operations to WAL with a
    /// non-blocking fsync. After the returned <see cref="Task"/> completes, all
    /// operations are durable and crash-safe.
    /// </summary>
    Task CommitAsync(CancellationToken cancellationToken = default);
}
