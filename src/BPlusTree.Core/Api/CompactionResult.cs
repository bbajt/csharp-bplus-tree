namespace BPlusTree.Core.Api;

/// <summary>
/// Outcome of a <see cref="BPlusTree{TKey,TValue}.Compact"/> operation.
/// </summary>
public readonly struct CompactionResult
{
    /// <summary>
    /// Number of data-file bytes reclaimed (old file size minus new file size).
    /// Zero if the file did not shrink (e.g. the tree was already compact).
    /// </summary>
    public long BytesSaved { get; init; }

    /// <summary>
    /// Number of logical pages freed during compaction (pages not copied to the
    /// new file). Includes deleted-record pages and retired CoW pages.
    /// </summary>
    public int PagesFreed { get; init; }

    /// <summary>Wall-clock duration of the compaction operation.</summary>
    public TimeSpan Duration { get; init; }
}
