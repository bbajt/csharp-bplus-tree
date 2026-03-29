namespace BPlusTree.Core.Api;

/// <summary>
/// Thrown when <see cref="BPlusTree{TKey,TValue}.BeginTransaction"/> is called
/// while compaction is in progress (swap phase).
/// Callers must retry after a brief delay.
/// </summary>
public sealed class CompactionInProgressException : BPlusTreeException
{
    public CompactionInProgressException()
        : base("Cannot begin a transaction while compaction is in progress. Retry after a brief delay.") { }
}
