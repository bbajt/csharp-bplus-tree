namespace BPlusTree.Core.Api;

/// <summary>
/// Default values for <see cref="BPlusTreeOptions"/> properties.
/// Reference these constants when constructing custom option objects so that
/// partial overrides stay readable.
/// </summary>
public static class BPlusTreeDefaults
{
    /// <summary>Default WAL flush interval in milliseconds. See <see cref="BPlusTreeOptions.FlushIntervalMs"/>.</summary>
    public const int FlushIntervalMs = 5;

    /// <summary>Default WAL group-commit flush batch size. See <see cref="BPlusTreeOptions.FlushBatchSize"/>.</summary>
    public const int FlushBatchSize = 256;

    /// <summary>Default buffer pool capacity in pages. See <see cref="BPlusTreeOptions.BufferPoolCapacity"/>.</summary>
    public const int BufferPoolCapacity = 2048;

    /// <summary>Default eviction high-water mark. See <see cref="BPlusTreeOptions.EvictionHighWatermark"/>.</summary>
    public const double EvictionHighWatermark = 0.85;

    /// <summary>Default eviction low-water mark. See <see cref="BPlusTreeOptions.EvictionLowWatermark"/>.</summary>
    public const double EvictionLowWatermark = 0.70;

    /// <summary>Default eviction batch size. See <see cref="BPlusTreeOptions.EvictionBatchSize"/>.</summary>
    public const int EvictionBatchSize = 32;

    /// <summary>Default CoW write-amplification factor. See <see cref="BPlusTreeOptions.CoWWriteAmplification"/>.</summary>
    public const int CoWWriteAmplification = 3;

    /// <summary>Default auto-checkpoint dirty-page threshold. See <see cref="BPlusTreeOptions.CheckpointThreshold"/>.</summary>
    public const int CheckpointThreshold = 256;

    /// <summary>Default B+ tree fill factor. See <see cref="BPlusTreeOptions.FillFactor"/>.</summary>
    public const double FillFactor = 0.70;

    /// <summary>
    /// Default milliseconds <c>FetchPage</c> waits for eviction before throwing
    /// <see cref="BufferPoolExhaustedException"/>. See
    /// <see cref="BPlusTreeOptions.EvictionWaitTimeoutMs"/>.
    /// </summary>
    public const int EvictionWaitTimeoutMs = 5_000;
}
