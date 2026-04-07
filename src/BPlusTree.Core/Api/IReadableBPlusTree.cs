using System.Collections.Generic;

namespace BPlusTree.Core.Api;

/// <summary>
/// Read-only view of a B+ tree. Implemented by <see cref="BPlusTree{TKey,TValue}"/>,
/// <see cref="ISnapshot{TKey,TValue}"/>, and
/// <see cref="ITransaction{TKey,TValue}"/>.
///
/// Callers that only need to read from the tree should depend on this interface
/// rather than the concrete type.
/// </summary>
/// <remarks>
/// <para>
/// <b>foreach and epoch safety:</b> iterating any implementation with <c>foreach</c>
/// is epoch-safe — the compiler-generated <c>try/finally</c> ensures the underlying
/// enumerator (and its epoch token) is disposed on every exit path, including
/// <c>break</c>, early <c>return</c>, and exception.
/// </para>
/// <para>
/// <b>Consistency under concurrent writes:</b> <c>foreach</c> on a live
/// <see cref="BPlusTree{TKey,TValue}"/> is epoch-safe but not linearisable — a
/// concurrent writer between two <c>MoveNext()</c> calls may produce a result set
/// that reflects neither the pre-write nor the post-write state. For a consistent
/// point-in-time view, use <see cref="BPlusTree{TKey,TValue}.BeginSnapshot"/> and
/// enumerate the snapshot. Snapshot and transaction iterators are always consistent
/// within their own scope.
/// </para>
/// </remarks>
public interface IReadableBPlusTree<TKey, TValue>
    : IEnumerable<(TKey Key, TValue Value)>
    where TKey : notnull
{
    /// <summary>
    /// Searches for <paramref name="key"/>. Returns <c>true</c> and sets
    /// <paramref name="value"/> if found; otherwise returns <c>false</c>.
    /// </summary>
    /// <param name="key">The key to look up. Must not be <c>null</c> for reference-type keys.</param>
    /// <param name="value">Set to the associated value on success; default otherwise.</param>
    bool TryGet(TKey key, out TValue value);

    /// <summary>
    /// Enumerates all key-value pairs in the range [<paramref name="startKey"/>,
    /// <paramref name="endKey"/>] in ascending key order.
    /// </summary>
    /// <param name="startKey">
    /// Inclusive lower bound. Pass <c>null</c> (or omit) to start from the first key in the tree.
    /// </param>
    /// <param name="endKey">
    /// Inclusive upper bound. Pass <c>null</c> (or omit) to scan to the last key in the tree.
    /// </param>
    IEnumerable<(TKey Key, TValue Value)> Scan(
        TKey? startKey = default,
        TKey? endKey   = default);

    /// <summary>The number of key-value pairs visible from this view.</summary>
    long Count { get; }

    /// <summary>Returns true if <paramref name="key"/> exists in this view.</summary>
    bool ContainsKey(TKey key);

    /// <summary>
    /// Return the smallest key-value pair in this view.
    /// Returns false if the view is empty.
    /// </summary>
    bool TryGetFirst(out TKey key, out TValue value);

    /// <summary>
    /// Return the largest key-value pair in this view.
    /// Returns false if the view is empty.
    /// </summary>
    bool TryGetLast(out TKey key, out TValue value);

    /// <summary>
    /// Return the smallest key strictly greater than <paramref name="key"/>.
    /// Returns false if no such key exists.
    /// </summary>
    bool TryGetNext(TKey key, out TKey nextKey, out TValue value);

    /// <summary>
    /// Return the largest key strictly less than <paramref name="key"/>.
    /// Returns false if no such key exists.
    /// </summary>
    bool TryGetPrev(TKey key, out TKey prevKey, out TValue value);

    /// <summary>
    /// Enumerate all key-value pairs in the closed interval [startKey, endKey]
    /// in descending key order.
    /// </summary>
    /// <param name="endKey">
    /// Inclusive upper bound for reverse iteration. Pass <c>null</c> (or omit) to start
    /// from the last key in the tree.
    /// </param>
    /// <param name="startKey">
    /// Inclusive lower bound. Pass <c>null</c> (or omit) to iterate to the first key in the tree.
    /// </param>
    IEnumerable<(TKey Key, TValue Value)> ScanReverse(
        TKey? endKey   = default,
        TKey? startKey = default);

    /// <summary>
    /// Count the number of key-value pairs in the closed interval [startKey, endKey].
    /// Returns 0 if startKey &gt; endKey or the range is empty.
    /// </summary>
    long CountRange(TKey startKey, TKey endKey);
}
