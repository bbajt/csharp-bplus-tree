namespace BPlusTree.Core.Api;

/// <summary>
/// Maintenance operations on a B+ tree store.
/// Implemented by <see cref="BPlusTree{TKey,TValue}"/>.
/// </summary>
public interface IBPlusTreeMaintenance
{
    /// <summary>
    /// Blocks until all writes that have returned before this call are durable on disk.
    /// </summary>
    void Flush();

    /// <summary>
    /// Runs a full checkpoint: flushes all dirty pages, writes a WAL checkpoint record,
    /// and truncates the WAL. Blocks until complete.
    /// </summary>
    void Checkpoint();

    /// <summary>
    /// Rewrites the data file to reclaim space from deleted/overflow-retired pages.
    /// Returns metrics describing the compaction outcome.
    /// Thread-safe with concurrent reads and writes.
    /// </summary>
    CompactionResult Compact();

    /// <summary>
    /// Returns a point-in-time snapshot of internal engine statistics.
    /// </summary>
    TreeStatistics GetStatistics();

    /// <summary>
    /// Validates the structural integrity of the tree.
    /// Checks key sort order across the leaf chain, record count consistency,
    /// and separator alignment in internal nodes.
    /// Returns a <see cref="ValidationResult"/> describing
    /// any violations found; <see cref="ValidationResult.IsValid"/>
    /// is <c>true</c> when the tree is structurally sound.
    /// </summary>
    ValidationResult Validate();
}
