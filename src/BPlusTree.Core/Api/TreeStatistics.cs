namespace ByTech.BPlusTree.Core.Api;

/// <summary>Read-only snapshot of tree metrics returned by GetStatistics().</summary>
public readonly struct TreeStatistics
{
    public int    TotalPages                  { get; init; }
    public ulong  TotalRecords                { get; init; }
    public uint   TreeHeight                  { get; init; }
    public int    FreePages                   { get; init; }
    public long   BufferPoolHits              { get; init; }
    public long   BufferPoolMisses            { get; init; }
    public long   WalSizeBytes                { get; init; }

    /// <summary>Fraction of buffer pool frames currently holding a page (0.0 – 1.0).</summary>
    public double BufferPoolOccupancyFraction { get; init; }

    /// <summary>Pages currently marked dirty (modified, not yet flushed to disk).</summary>
    public int    DirtyPageCount              { get; init; }

    /// <summary>Number of transactions currently open (BeginTransaction / BeginScope).</summary>
    public int    ActiveTransactionCount      { get; init; }

    /// <summary>Number of snapshots open at the time GetStatistics() was called.</summary>
    public int    ActiveSnapshotCount         { get; init; }
}
