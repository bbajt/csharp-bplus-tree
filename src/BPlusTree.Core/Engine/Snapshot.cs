using System.Collections;
using System.Collections.Generic;
using System.Linq;
using ByTech.BPlusTree.Core.Api;

namespace ByTech.BPlusTree.Core.Engine;

/// <summary>
/// Point-in-time read-only snapshot. Created by
/// <see cref="TreeEngine{TKey,TValue}.BeginSnapshot"/>.
///
/// The snapshot root ID and first-leaf ID are captured while the writer lock
/// is held, then an epoch is registered atomically — this prevents CoW writers
/// from retiring snapshot pages between the state capture and epoch registration.
///
/// On Dispose, the epoch is released via ExitReadEpoch, allowing retired pages
/// that were visible to this snapshot to be reclaimed once no other reader holds
/// a ≤ retire-epoch token.
/// </summary>
internal sealed class Snapshot<TKey, TValue> : ISnapshot<TKey, TValue>
    where TKey : notnull
{
    private readonly TreeEngine<TKey, TValue> _engine;
    private readonly TransactionCoordinator   _coordinator;
    private readonly uint                     _snapshotRootId;
    private readonly uint                     _snapshotFirstLeafId;
    private readonly long                     _snapshotRecordCount;
    private readonly ulong                    _epoch;
    private          bool                     _disposed;

    /// <summary>
    /// Construct a snapshot from a pre-captured root, first-leaf, record count, and a
    /// pre-registered epoch (all captured by BeginSnapshot while holding
    /// the writer lock).
    /// </summary>
    internal Snapshot(
        TreeEngine<TKey, TValue> engine,
        TransactionCoordinator   coordinator,
        uint                     snapshotRootId,
        uint                     snapshotFirstLeafId,
        long                     snapshotRecordCount,
        ulong                    epoch)
    {
        _engine              = engine;
        _coordinator         = coordinator;
        _snapshotRootId      = snapshotRootId;
        _snapshotFirstLeafId = snapshotFirstLeafId;
        _snapshotRecordCount = snapshotRecordCount;
        _epoch               = epoch;
    }

    public long Count
    {
        get { ThrowIfDisposed(); return _snapshotRecordCount; }
    }

    /// <inheritdoc />
    public bool TryGet(TKey key, out TValue value)
    {
        ThrowIfDisposed();
        return _engine.TryGetFromSnapshot(key, _snapshotRootId, out value);
    }

    /// <inheritdoc />
    public bool ContainsKey(TKey key)
    {
        ThrowIfDisposed();
        return _engine.TryGetFromSnapshot(key, _snapshotRootId, out _);
    }

    /// <inheritdoc />
    public bool TryGetFirst(out TKey key, out TValue value)
    {
        ThrowIfDisposed();
        return _engine.TryGetFirstFromSnapshot(_snapshotFirstLeafId, out key, out value);
    }

    /// <inheritdoc />
    public bool TryGetLast(out TKey key, out TValue value)
    {
        ThrowIfDisposed();
        return _engine.TryGetLastFromSnapshot(_snapshotRootId, out key, out value);
    }

    /// <inheritdoc />
    public bool TryGetNext(TKey key, out TKey nextKey, out TValue value)
    {
        ThrowIfDisposed();
        return _engine.TryGetNextFromSnapshot(key, _snapshotRootId, out nextKey, out value);
    }

    /// <inheritdoc />
    public bool TryGetPrev(TKey key, out TKey prevKey, out TValue value)
    {
        ThrowIfDisposed();
        return _engine.TryGetPrevFromSnapshot(key, _snapshotRootId, out prevKey, out value);
    }

    public IEnumerable<(TKey Key, TValue Value)> Scan(
        TKey? startKey = default,
        TKey? endKey   = default)
    {
        ThrowIfDisposed();
        return _engine.ScanFromSnapshot(startKey, endKey, _snapshotRootId, _snapshotFirstLeafId);
    }

    public IEnumerable<(TKey Key, TValue Value)> ScanReverse(
        TKey? endKey   = default,
        TKey? startKey = default)
    {
        ThrowIfDisposed();
        return _engine.ScanReverseFromSnapshot(endKey, startKey, _snapshotRootId);
    }

    /// <inheritdoc />
    public long CountRange(TKey startKey, TKey endKey)
    {
        ThrowIfDisposed();
        return _engine.CountRangeFromSnapshot(startKey, endKey, _snapshotRootId);
    }

    public IEnumerator<(TKey Key, TValue Value)> GetEnumerator()
        => Scan().GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    // ── IReadOnlyDictionary<TKey,TValue> ─────────────────────────────────────

    TValue IReadOnlyDictionary<TKey, TValue>.this[TKey key]
        => TryGet(key, out var v)
            ? v
            : throw new KeyNotFoundException($"The key '{key}' was not found in the snapshot.");

    IEnumerable<TKey>   IReadOnlyDictionary<TKey, TValue>.Keys
        => Scan().Select(p => p.Key);

    IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values
        => Scan().Select(p => p.Value);

    bool IReadOnlyDictionary<TKey, TValue>.ContainsKey(TKey key)
        => TryGet(key, out _);

    bool IReadOnlyDictionary<TKey, TValue>.TryGetValue(TKey key, out TValue value)
        => TryGet(key, out value);

    int IReadOnlyCollection<KeyValuePair<TKey, TValue>>.Count
        => checked((int)Count);

    IEnumerator<KeyValuePair<TKey, TValue>>
        IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        => Scan()
            .Select(p => new KeyValuePair<TKey, TValue>(p.Key, p.Value))
            .GetEnumerator();

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _coordinator.ExitReadEpoch(_epoch);
    }

    private void ThrowIfDisposed()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(Snapshot<TKey, TValue>));
    }
}
