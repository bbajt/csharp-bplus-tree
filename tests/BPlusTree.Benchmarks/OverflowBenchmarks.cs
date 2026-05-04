using BenchmarkDotNet.Attributes;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;

namespace ByTech.BPlusTree.Core.Benchmarks;

// ── Phase 101a: Auto-commit Put ────────────────────────────────────────────────
// Measures tree.Put(key, largeValue) on the in-place fast path (no snapshot active).
//
// Pool state: COLD — each IterationSetup deletes and recreates the db file.
// The buffer pool is empty at the start of every measured iteration.
// Each Put allocates new overflow pages (never previously cached).
//
// Validates:
//   P1 — cold-pool cost: ~21.7 µs/op baseline + ~50 µs/overflow page
//         → predicted ~672 µs/op for 100 KB (K=13 overflow pages)
//   P5 — no CoW overhead: Allocated/op ≈ value bytes + small fixed overhead,
//         not (K+H) × PageSize as on the CoW path.

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class OverflowAutoCommitPutBenchmarks
{
    [Params(1_024, 8_192, 32_768, 102_400, 1_048_576)]
    public int ValueSize { get; set; }

    private BPlusTree<int, byte[]> _tree    = null!;
    private string                 _dbPath  = null!;
    private string                 _walPath = null!;
    private byte[]                 _value   = null!;
    private int                    _nextKey;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_ovput_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_ovput_{Guid.NewGuid():N}.wal");
        _value   = new byte[ValueSize];
        Random.Shared.NextBytes(_value);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        if (File.Exists(_dbPath))  File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);
        _tree    = OverflowTreeHelper.Open(_dbPath, _walPath);
        _nextKey = 0;
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    // 100 inserts per BDN invocation — avoids cold-start domination for small
    // ValueSizes while keeping per-invocation time reasonable at 1 MB (≈100 ms).
    [Benchmark(OperationsPerInvoke = 100)]
    public void Put()
    {
        for (int i = 0; i < 100; i++)
            _tree.Put(_nextKey++, _value);
    }
}

// ── Phase 101a: Auto-commit TryGet ─────────────────────────────────────────────
// Measures tree.TryGet(key) for a key whose value is stored in an overflow chain.
//
// Pool state: HOT — one overflow value is seeded in GlobalSetup; tree is reopened
// cold; BDN warmup iterations (3) heat the overflow chain pages into the pool.
// Measured iterations read from the hot pool — steady-state regime.
//
// Validates:
//   P2 — hot-pool read cost: ~87 µs/op predicted for 100 KB (K=13 overflow pages).
//
// Note: cold-pool TryGet cost (P1 read side) is not directly measured here —
// infer from the Put benchmark where each write path reads the pool cold.

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class OverflowAutoCommitGetBenchmarks
{
    [Params(1_024, 8_192, 32_768, 102_400, 1_048_576)]
    public int ValueSize { get; set; }

    private BPlusTree<int, byte[]> _tree    = null!;
    private string                 _dbPath  = null!;
    private string                 _walPath = null!;

    private const int SeededKey = 42;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_ovget_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_ovget_{Guid.NewGuid():N}.wal");

        // Seed one overflow value in a temporary tree, then close it.
        // This flushes dirty pages to disk and releases all pool frames.
        var seedValue = new byte[ValueSize];
        Random.Shared.NextBytes(seedValue);

        using (var seed = OverflowTreeHelper.Open(_dbPath, _walPath))
            seed.Put(SeededKey, seedValue);

        // Reopen with a cold pool. BDN warmup iterations will heat the chain pages
        // before the timed iterations begin. Each TryGet follows the full chain.
        _tree = OverflowTreeHelper.Open(_dbPath, _walPath);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _tree?.Dispose();
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    // 1,000 reads per BDN invocation — hot pool; each call follows the overflow chain.
    // At 100 KB (K=13 pages, hot): ~87 µs/op predicted → 1,000 ops ≈ 87 ms/invocation.
    [Benchmark(OperationsPerInvoke = 1_000)]
    public void TryGet()
    {
        for (int i = 0; i < 1_000; i++)
            _tree.TryGet(SeededKey, out _);
    }
}

// ── Phase 101a: Auto-commit Delete ─────────────────────────────────────────────
// Measures tree.Delete(key) for overflow values — chain-walk + free overhead.
//
// Pool state: WARM (not hot, not cold) — IterationSetup writes the key, then the
// benchmark deletes it. The overflow pages may be hot in the pool from the write.
// This reflects the realistic "write then immediately delete" pattern.
//
// No explicit design-doc prediction for delete cost. Results establish the chain-walk
// baseline: each delete performs K FetchPage calls to walk the chain before freeing.
//
// Scope: ValueSize excludes 1 MB — IterationSetup writes 1 MB per iteration;
// at 3 warmup + 5 measured × 2 jobs = 16 writes × 1 MB ≈ 16 MB setup I/O.
// The delete itself (walk + free) is measurable at ≤100 KB without excessive setup cost.

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class OverflowAutoCommitDeleteBenchmarks
{
    [Params(1_024, 8_192, 32_768, 102_400)]
    public int ValueSize { get; set; }

    private BPlusTree<int, byte[]> _tree    = null!;
    private string                 _dbPath  = null!;
    private string                 _walPath = null!;
    private byte[]                 _value   = null!;

    private const int DeleteKey = 1;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_ovdel_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_ovdel_{Guid.NewGuid():N}.wal");
        _value   = new byte[ValueSize];
        Random.Shared.NextBytes(_value);
    }

    // IterationSetup runs before EACH timed iteration (including warmup).
    // It seeds the key to be deleted, then the benchmark deletes it exactly once.
    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        if (File.Exists(_dbPath))  File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);
        _tree = OverflowTreeHelper.Open(_dbPath, _walPath);
        _tree.Put(DeleteKey, _value);
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    // Single delete per BDN invocation (OperationsPerInvoke = 1, default).
    // IterationSetup re-seeds the key before each timed invocation, so there is
    // always exactly one overflow key to delete when Delete() runs.
    [Benchmark]
    public void Delete()
    {
        _tree.Delete(DeleteKey);
    }
}

// ── Phase 101b: CoW Put (snapshot active) ─────────────────────────────────────
// Measures tree.Put(key, largeValue) while a snapshot is open, forcing HasActiveSnapshots=true.
// Every Put goes through the CoW shadow-write path:
//   - H ancestor shadow pages (H ≈ tree height, typically 3 for a seeded tree)
//   - K new overflow pages (K = ceil(valueSize / 8154))
// Total shadow allocation per Put ≈ (K + H) × PageSize bytes.
//
// Pool state: WARM — tree is pre-seeded in IterationSetup to build a multi-level
// structure (height ≥ 2). The snapshot is opened once per iteration and held open
// throughout all Put calls in the benchmark method.
//
// Validates:
//   P3 — CoW shadow-page count: Allocated/op ≈ (K + H) × 8192 bytes
//         At 100 KB (K=13, H=3): predicted ~128 KB = 16 × 8 KB per Put.

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class OverflowCoWPutBenchmarks
{
    // Skip 1 MB: (K=129) + H=3 = 132 shadow pages × 8 KB = ~1 MB per Put.
    // At OperationsPerInvoke=10, each invocation allocates ~10 MB — pool thrash.
    [Params(1_024, 8_192, 32_768, 102_400)]
    public int ValueSize { get; set; }

    private BPlusTree<int, byte[]> _tree     = null!;
    private ISnapshot<int, byte[]> _snapshot = null!;
    private string                 _dbPath   = null!;
    private string                 _walPath  = null!;
    private byte[]                 _value    = null!;
    private int                    _nextKey;

    // Pre-seed count: enough to build a 2-level tree (height ≥ 2).
    // At PageSize=8192 with int keys and byte[] values (small seed values, 10 bytes),
    // the leaf fills at ~(8192-48)/(6+4+10)=407 entries. 500 seeds ≥ 1 full leaf → split.
    private const int SeedCount = 500;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"bench_ovcow_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_ovcow_{Guid.NewGuid():N}.wal");
        _value  = new byte[ValueSize];
        Random.Shared.NextBytes(_value);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _snapshot?.Dispose(); _snapshot = null!;
        _tree?.Dispose();
        if (File.Exists(_dbPath))  File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);

        _tree = OverflowTreeHelper.Open(_dbPath, _walPath);

        // Pre-seed with small inline values to build multi-level tree structure.
        var smallValue = new byte[10];
        for (int i = 0; i < SeedCount; i++)
            _tree.Put(i, smallValue);

        // Open snapshot — held open for the full benchmark iteration.
        // HasActiveSnapshots = true → every subsequent Put uses the CoW path.
        _snapshot = _tree.BeginSnapshot();
        _nextKey  = SeedCount;   // inserts go above the seeded range (no key conflicts)
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _snapshot?.Dispose(); _snapshot = null!;
        _tree?.Dispose();     _tree     = null!;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    // 10 CoW overflow Puts per BDN invocation.
    // Each Put: shadow-copies H ancestor pages + allocates K new overflow pages.
    // Allocated/op should be ≈ (K + H) × 8192 bytes — primary P3 measurement.
    [Benchmark(OperationsPerInvoke = 10)]
    public void PutWithSnapshotActive()
    {
        for (int i = 0; i < 10; i++)
            _tree.Put(_nextKey++, _value);
    }
}

// ── Phase 101b: PutRange with large values ─────────────────────────────────────
// Compares PutRange (1 WAL fsync for N inserts) against N individual auto-commit
// Put calls for large overflow values. Identifies the practical N ceiling at 1 MB
// where CoW shadow-page Gen2 GC pressure becomes significant.
//
// PutRange uses BeginTransaction → N inserts → Commit, so every insert goes through
// the CoW shadow-write path (transaction always forces CoW). The fsync is amortised
// across all N inserts (1 WAL fsync total). The tradeoff: N × (K+H) shadow-page
// allocations are held simultaneously before the transaction commits.
//
// AutoCommit_Put baseline uses N individual Put calls (N WAL fsyncs); each Put
// uses the in-place fast path (no snapshot active, no split needed for sequential keys).
//
// Validates:
//   P4 — PutRange N ceiling at 1 MB: predicted ~10–20 inserts before Gen2 GC pressure.
//         Visible as Gen2 GC count > 0 in the Allocated/Gen2 columns at large N.

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class OverflowPutRangeLargeBenchmarks
{
    [Params(10, 50, 100)]
    public int N { get; set; }

    // Fixed at 1 MB to target P4's specific prediction. K=129 overflow pages.
    private const int ValueSize = 1_048_576;

    private BPlusTree<int, byte[]>    _tree      = null!;
    private string                    _dbPath    = null!;
    private string                    _walPath   = null!;
    private (int Key, byte[] Value)[] _items     = null!;
    private byte[]                    _sharedVal = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath    = Path.Combine(Path.GetTempPath(), $"bench_ovpr_{Guid.NewGuid():N}.db");
        _walPath   = Path.Combine(Path.GetTempPath(), $"bench_ovpr_{Guid.NewGuid():N}.wal");
        _sharedVal = new byte[ValueSize];
        Random.Shared.NextBytes(_sharedVal);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        if (File.Exists(_dbPath))  File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);
        _tree  = OverflowTreeHelper.Open(_dbPath, _walPath);

        // Pre-build item array: each item shares the same value buffer (no extra allocation
        // in the hot path). Keys are sequential starting at 0.
        _items = Enumerable.Range(0, N).Select(i => (i, _sharedVal)).ToArray();
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    // ── Baseline: N individual auto-commit Puts (N WAL fsyncs, in-place path) ──
    [Benchmark(Baseline = true)]
    public void AutoCommit_Put_N()
    {
        for (int i = 0; i < N; i++)
            _tree.Put(i, _sharedVal);
    }

    // ── PutRange: N inserts in one transaction (1 WAL fsync, CoW path) ──────────
    // N × (K+H) shadow pages held simultaneously until Commit.
    // Gen2 column reveals when allocation exceeds the large-object heap threshold.
    [Benchmark]
    public void PutRange_N()
    {
        _tree.PutRange(_items);
    }
}

// ── Shared factory ─────────────────────────────────────────────────────────────
// Separate from TreeHelper (int/int) to avoid confusion and to use larger
// WalBufferSize + BufferPoolCapacity needed for large overflow values.

internal static class OverflowTreeHelper
{
    /// <summary>
    /// Open a BPlusTree&lt;int, byte[]&gt; with configuration tuned for overflow benchmarks.
    ///
    /// WalBufferSize = 32 MB: a single 1 MB overflow Put writes ~1 MB to the WAL
    /// per operation; 100 ops × 1 MB = 100 MB WAL data per BDN invocation. The
    /// WalBufferSize does not need to cover the full invocation — it only needs to
    /// hold the per-checkpoint window. At CheckpointThreshold=256 pages × 8 KB = 2 MB,
    /// 32 MB gives 16× headroom.
    ///
    /// BufferPoolCapacity = 4096: a single 1 MB value occupies 129 overflow pages
    /// in the pool. At 100 hot reads, the steady-state pool must fit the chain
    /// (129 pages) plus the leaf path (3 pages) plus headroom for CoW shadows.
    /// 4096 frames × 8 KB = 32 MB pool — sufficient for any ValueSize tested.
    /// </summary>
    internal static BPlusTree<int, byte[]> Open(string dbPath, string walPath) =>
        BPlusTree<int, byte[]>.Open(
            new BPlusTreeOptions
            {
                DataFilePath          = dbPath,
                WalFilePath           = walPath,
                PageSize              = 8_192,
                BufferPoolCapacity    = 4_096,
                CheckpointThreshold   = 256,
                WalBufferSize         = 32 * 1_024 * 1_024,
                CoWWriteAmplification = 3,
            },
            Int32Serializer.Instance,
            ByteArraySerializer.Instance);
}
