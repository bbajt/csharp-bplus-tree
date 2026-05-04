using System.Collections.Generic;

namespace ByTech.BPlusTree.Core.Api;

/// <summary>
/// A point-in-time read-only view of the tree, opened via
/// <see cref="ByTech.BPlusTree.Core.Api.BPlusTree{TKey,TValue}.BeginSnapshot"/>.
///
/// Guarantees:
///   - Reads are served from the committed state at the moment the snapshot was opened.
///   - Writes committed after the snapshot was opened are never visible.
///   - Multiple snapshots may be open simultaneously on any number of threads.
///
/// Lifecycle:
///   - The snapshot holds an epoch token that prevents CoW-retired pages from being
///     reclaimed. Dispose must be called to release the epoch and allow reclamation.
///   - Unlike <see cref="ITransaction{TKey,TValue}"/>, a snapshot holds no checkpoint
///     gate lock, so long-lived snapshots do not block WAL truncation or checkpoints.
/// </summary>
public interface ISnapshot<TKey, TValue>
    : IReadableBPlusTree<TKey, TValue>,
      IReadOnlyDictionary<TKey, TValue>,
      IDisposable
    where TKey : notnull
{
    /// <summary>
    /// The number of key-value pairs in this snapshot.
    /// This value is frozen at snapshot-open time; writes committed after the
    /// snapshot was opened are not reflected.
    /// </summary>
    new long Count { get; }

}
