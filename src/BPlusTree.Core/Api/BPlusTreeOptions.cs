namespace BPlusTree.Core.Api;

/// <summary>Configuration options for opening a BPlusTree. Call Validate() before use.</summary>
public sealed class BPlusTreeOptions
{
    // ── WAL sync mode ─────────────────────────────────────────────────────────
    /// <summary>Sync mode for the WAL. GroupCommit defers fsync to a background thread.</summary>
    public WalSyncMode SyncMode        { get; set; } = WalSyncMode.Synchronous;

    /// <summary>Milliseconds between background fsyncs in GroupCommit mode (1–10000).</summary>
    public int         FlushIntervalMs { get; set; } = BPlusTreeDefaults.FlushIntervalMs;

    /// <summary>Record count threshold that triggers an early fsync in GroupCommit mode (1–65536).</summary>
    public int         FlushBatchSize  { get; set; } = BPlusTreeDefaults.FlushBatchSize;


    // ── File paths ────────────────────────────────────────────────────────────
    /// <summary>Full path to the data file, e.g. "mydb.db"</summary>
    public string DataFilePath { get; set; } = string.Empty;

    /// <summary>Full path to the WAL file, e.g. "mydb.wal"</summary>
    public string WalFilePath  { get; set; } = string.Empty;

    // ── Page settings ─────────────────────────────────────────────────────────
    /// <summary>Page size in bytes. Must be a power of 2 between 4096 and 65536.</summary>
    public int PageSize { get; set; } = 8192;

    // ── Buffer pool ───────────────────────────────────────────────────────
    /// <summary>
    /// Maximum number of pages held in memory. Must be ≥ 16.
    /// Raised from 1024 to 2048 in Phase 62 to absorb CoW shadow pages without
    /// premature eviction pressure on H≈3 trees.
    /// </summary>
    public int BufferPoolCapacity { get; set; } = BPlusTreeDefaults.BufferPoolCapacity;

    // ── Async eviction ────────────────────────────────────────────────────
    /// <summary>
    /// Fraction of pool frames in use at which the eviction thread wakes (0 &lt; LWM &lt; HWM ≤ 1.0).
    /// Default 0.85 — evict when 85 % of frames hold a page.
    /// </summary>
    public double EvictionHighWatermark { get; set; } = BPlusTreeDefaults.EvictionHighWatermark;

    /// <summary>
    /// Fraction of pool frames in use at which the eviction thread stops evicting.
    /// Default 0.70 — stop when pool drops to 70 % occupancy.
    /// </summary>
    public double EvictionLowWatermark { get; set; } = BPlusTreeDefaults.EvictionLowWatermark;

    /// <summary>Maximum pages the eviction thread writes per wake-up. Must be ≥ 1.</summary>
    public int EvictionBatchSize { get; set; } = BPlusTreeDefaults.EvictionBatchSize;

    /// <summary>
    /// Copy-on-Write write-amplification factor: the number of dirty buffer-pool frames
    /// produced per logical insert or delete on the CoW write path.
    /// For a tree of height H, each write creates H shadow pages, so set this to H.
    ///
    /// The EvictionWorker uses this to scale its effective eviction batch size:
    ///   effective batch = EvictionBatchSize × CoWWriteAmplification
    /// This normalises the number of WAL fsyncs per logical insert back to the
    /// pre-CoW baseline, preventing the H× eviction-frequency regression.
    ///
    /// Guidance:
    ///   H = 1 (small trees, &lt; ~300 keys at pageSize=8192)   → set to 1
    ///   H = 2 (~300 – 100K keys)                             → set to 2
    ///   H = 3 (~100K – 30M keys)   &lt;-- default              → set to 3
    ///   H = 4 (&gt; 30M keys)                                  → set to 4
    ///
    /// Default: 3 (appropriate for workloads up to ~30M keys at pageSize=8192).
    /// Set to 1 to restore pre-Phase-62 behaviour.
    /// </summary>
    public int CoWWriteAmplification { get; set; } = BPlusTreeDefaults.CoWWriteAmplification;

    /// <summary>
    /// Milliseconds FetchPage will wait for the eviction thread to free a frame before
    /// throwing <see cref="BufferPoolExhaustedException"/>. Default 5000. Use -1 for infinite.
    /// </summary>
    public int EvictionWaitTimeoutMs { get; set; } = BPlusTreeDefaults.EvictionWaitTimeoutMs;

    // ── Write-ahead log ───────────────────────────────────────────────────────
    /// <summary>Number of dirty pages that triggers an automatic checkpoint. Must be ≥ 4.</summary>
    public int CheckpointThreshold { get; set; } = BPlusTreeDefaults.CheckpointThreshold;

    /// <summary>
    /// In-memory WAL buffer size in bytes before an explicit flush is forced.
    /// Must be ≥ CheckpointThreshold × PageSize to avoid mid-checkpoint WAL buffer overflows.
    /// Raised from 4 MB to 8 MB in Phase 62; max safe CheckpointThreshold = 8MB/8KB = 1024 pages.
    /// </summary>
    public int WalBufferSize { get; set; } = 8 * 1024 * 1024; // 8 MB

    /// <summary>
    /// WAL file size in bytes that triggers an automatic background checkpoint.
    /// 0 = disabled (default; manual checkpoints only).
    /// When enabled, a background thread polls every 250 ms and calls
    /// TakeCheckpoint() when (a) WAL size ≥ this value and
    /// (b) no transaction currently holds a page write lock.
    /// Recommended for production: 64 * 1024 * 1024 (64 MB).
    /// </summary>
    public long WalAutoCheckpointThresholdBytes { get; set; } = 0;

    // ── WAL buffer / checkpoint interaction ───────────────────────────────────
    /// <summary>
    /// Returns true when CheckpointThreshold × PageSize exceeds WalBufferSize,
    /// causing WAL buffer overflows during checkpoint cycles.
    ///
    /// Each overflow is a synchronous fsync on the insert thread — the same bottleneck
    /// that Phase 24 fixed by raising WalBufferSize from 512KB to 4MB.
    ///
    /// Impact: each checkpoint cycle with overflow adds (overflow count) extra fsyncs.
    ///   overflow count = ceil((CheckpointThreshold × PageSize) / WalBufferSize) - 1
    ///
    /// Example: CheckpointThreshold=1024, PageSize=8192, WalBufferSize=4MB:
    ///   1024 × 8192 = 8MB per cycle; 8MB / 4MB = 2 overflows per cycle.
    ///   Observed: Phase 28 insert 25.46s → 63.41s (+149%) from this exact config.
    ///
    /// To fix: reduce CheckpointThreshold or increase WalBufferSize so that
    ///   CheckpointThreshold × PageSize ≤ WalBufferSize.
    ///   Safe maximum: WalBufferSize / PageSize pages (e.g. 4MB / 8KB = 512 pages).
    /// </summary>
    public bool WillOverflowWalBuffer =>
        CheckpointThreshold > 0
        && PageSize > 0
        && WalBufferSize > 0
        && (long)CheckpointThreshold * PageSize > WalBufferSize;

    /// <summary>
    /// Returns the maximum CheckpointThreshold that will not overflow the WAL buffer.
    /// Equal to WalBufferSize / PageSize. Returns 0 if PageSize is 0.
    /// </summary>
    public int MaxSafeCheckpointThreshold =>
        PageSize > 0 ? WalBufferSize / PageSize : 0;

    // ── Callbacks ──────────────────────────────────────────────────────────────
    /// <summary>
    /// Optional callback invoked when <see cref="BPlusTree{TKey,TValue}.Open"/> detects
    /// a condition that may degrade durability or performance.
    /// Set before calling <c>Open()</c>.
    /// </summary>
    public Action<string>? OnWarning { get; set; }

    /// <summary>
    /// Optional callback invoked immediately before a compaction begins.
    /// The argument is the data file path being compacted.
    /// </summary>
    public Action<string>? OnCompactionStarted { get; set; }

    /// <summary>
    /// Optional callback invoked immediately after a compaction completes.
    /// The first argument is the data file path; the second is the compaction outcome.
    /// </summary>
    public Action<string, CompactionResult>? OnCompactionCompleted { get; set; }

    // ── Tree behaviour ────────────────────────────────────────────────────────
    /// <summary>Target fill factor for new pages (0.50 – 0.95).</summary>
    public double FillFactor { get; set; } = BPlusTreeDefaults.FillFactor;

    /// <summary>
    /// Validates all options and throws <see cref="ArgumentException"/> on the first violation.
    /// Call before opening or creating a tree.
    /// </summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(DataFilePath))
            throw new ArgumentException("Data file path must not be null or empty.", nameof(DataFilePath));

        if (string.IsNullOrWhiteSpace(WalFilePath))
            throw new ArgumentException("WAL file path must not be null or empty.", nameof(WalFilePath));

        if (string.Equals(Path.GetFullPath(DataFilePath), Path.GetFullPath(WalFilePath), StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException(
                "DataFilePath and WalFilePath must not resolve to the same file.",
                nameof(WalFilePath));

        if (!Enum.IsDefined(SyncMode))
            throw new ArgumentException(
                $"SyncMode value {(int)SyncMode} is not a valid WalSyncMode.",
                nameof(SyncMode));

        if (!IsPowerOfTwo(PageSize) || PageSize < 4096 || PageSize > 65536)
            throw new ArgumentException("Page size must be a power of 2 between 4096 and 65536 bytes.", nameof(PageSize));

        if (BufferPoolCapacity < 16)
            throw new ArgumentException("Buffer pool capacity must be at least 16 pages.", nameof(BufferPoolCapacity));

        if (CheckpointThreshold < 4)
            throw new ArgumentException("Checkpoint threshold must be at least 4 dirty pages.", nameof(CheckpointThreshold));

        if (WalBufferSize < PageSize)
            throw new ArgumentException("WAL buffer size must be at least one page size.", nameof(WalBufferSize));

        if (WalAutoCheckpointThresholdBytes < 0)
            throw new ArgumentException(
                "WalAutoCheckpointThresholdBytes must be ≥ 0 (0 = disabled).",
                nameof(WalAutoCheckpointThresholdBytes));

        if (double.IsNaN(FillFactor) || FillFactor < 0.50 || FillFactor > 0.95)
            throw new ArgumentException("Fill factor must be between 0.50 and 0.95 inclusive.", nameof(FillFactor));

        if (FlushIntervalMs < 1 || FlushIntervalMs > 10_000)
            throw new ArgumentException("FlushIntervalMs must be between 1 and 10000.", nameof(FlushIntervalMs));

        if (FlushBatchSize < 1 || FlushBatchSize > 65_536)
            throw new ArgumentException("FlushBatchSize must be between 1 and 65536.", nameof(FlushBatchSize));

        if (double.IsNaN(EvictionHighWatermark) || EvictionHighWatermark <= 0 || EvictionHighWatermark > 1.0)
            throw new ArgumentException("EvictionHighWatermark must be in (0, 1.0].", nameof(EvictionHighWatermark));

        if (double.IsNaN(EvictionLowWatermark) || EvictionLowWatermark <= 0 || EvictionLowWatermark >= EvictionHighWatermark)
            throw new ArgumentException("EvictionLowWatermark must be in (0, EvictionHighWatermark).", nameof(EvictionLowWatermark));

        if (EvictionBatchSize < 1)
            throw new ArgumentException("EvictionBatchSize must be at least 1.", nameof(EvictionBatchSize));

        if (EvictionWaitTimeoutMs < -1)
            throw new ArgumentException(
                "EvictionWaitTimeoutMs must be -1 (infinite) or a non-negative millisecond count.",
                nameof(EvictionWaitTimeoutMs));

        if (CoWWriteAmplification < 1 || CoWWriteAmplification > 8)
            throw new ArgumentException(
                "CoWWriteAmplification must be between 1 and 8.",
                nameof(CoWWriteAmplification));
    }

    /// <summary>
    /// Returns true when CheckpointThreshold ≥ BufferPoolCapacity.
    /// This is a performance concern (auto-checkpoint may not fire until the pool is
    /// already full) but not a correctness error — the tree remains safe.
    /// <see cref="BPlusTree{TKey,TValue}.Open"/> emits a <see cref="BPlusTree{TKey,TValue}.TreeWarning"/>
    /// when this is true.
    /// </summary>
    public bool IsCheckpointThresholdOversized
        => CheckpointThreshold >= BufferPoolCapacity;

    // Helper: returns true if n is a power of two (n > 0)
    private static bool IsPowerOfTwo(int n) => n > 0 && (n & (n - 1)) == 0;
}