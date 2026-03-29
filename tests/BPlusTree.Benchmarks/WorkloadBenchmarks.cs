using BenchmarkDotNet.Attributes;
using BPlusTree.Core.Api;

namespace BPlusTree.Benchmarks;

// ── Online compaction throughput benchmarks ────────────────────────────────────
// Quantifies the write-throughput cost of running concurrent compaction.
//
// Both methods start from an identical state (10K-record tree, fresh per iteration).
// At this scale, compaction (~1s) and 10K puts (~100ms) are the same order of
// magnitude, making the ratio meaningful: a blocking compaction would show
// Writes_DuringCompaction ≈ compaction + writes; online compaction shows
// Writes_DuringCompaction ≈ Max(compaction, writes) ≈ compaction.
//
// With Phase 106 online compaction:
//   - Writes proceed concurrently during the leaf walk (Phase A).
//   - Only the short atomic swap (Phase B) briefly blocks writers.
//   - Delta tracked correctly → no data loss.
//
// The ratio Writes_DuringCompaction / Writes_NoCompaction shows how much
// wall-clock time is added by running alongside compaction. A ratio close to
// compaction_time / write_time means writes are fully unblocked (ideal case).

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 5)]
public class OnlineCompactionBenchmarks
{
    private const int SeedCount  = 10_000;
    private const int WriteCount = 10_000;

    private BPlusTree<int, int> _tree       = null!;
    private string              _dbPath     = null!;
    private string              _walPath    = null!;
    private int[]               _writeKeys  = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath    = Path.Combine(Path.GetTempPath(), $"bench_ocp_{Guid.NewGuid():N}.db");
        _walPath   = Path.Combine(Path.GetTempPath(), $"bench_ocp_{Guid.NewGuid():N}.wal");

        // Write keys start above the seeded range — no duplicate key conflict.
        // Same array re-used each iteration (tree is recreated in IterationSetup).
        _writeKeys = Enumerable.Range(SeedCount, WriteCount).ToArray();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        File.Delete(_dbPath);
        File.Delete(_walPath);
        _tree = TreeHelper.Open(_dbPath, _walPath);
        for (int i = 0; i < SeedCount; i++) _tree.Put(i, i);
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    // ── Baseline: 10K puts, no concurrent compaction ───────────────────────────
    [Benchmark(Baseline = true)]
    public void Writes_NoCompaction()
    {
        for (int i = 0; i < WriteCount; i++) _tree.Put(_writeKeys[i], i);
    }

    // ── With online compaction: 10K puts + Compact() running concurrently ──────
    // Compact() runs on a background thread; puts run on the benchmark thread.
    // Task.Wait() ensures both complete before BDN records elapsed time.
    [Benchmark]
    public void Writes_DuringCompaction()
    {
        var compactTask = Task.Run(() => _tree.Compact());
        for (int i = 0; i < WriteCount; i++) _tree.Put(_writeKeys[i], i);
        compactTask.Wait();
    }
}

// ── OLTP workload benchmarks ───────────────────────────────────────────────────
// Single-threaded mixed read/write workload at three ratios.
// Models a realistic application access pattern rather than a pure-op microbenchmark.
//
// Pre-seeded with 100K keys (0–99999). Each benchmark invocation executes 10K ops.
// Operation sequences are pre-built in GlobalSetup so the hot path contains only
// the tree calls — no branching, no key computation, no array allocation.
//
// Read ops: random keys from 0–99999 (hot-pool, mostly hits).
// Write ops: keys starting at 100_000 (above seeded range; no conflicts; tree
//   is recreated each IterationSetup so same keys are valid every iteration).
//
// Ops are interleaved: for ReadHeavy, every 10 ops = 8 reads + 2 writes.
// For WriteHeavy, every 10 ops = 2 reads + 8 writes. For Balanced: 5+5.
// Interleaving models realistic application behavior (not reads-then-writes).

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 5)]
public class OltpWorkloadBenchmarks
{
    private const int SeedCount = 100_000;
    private const int OpsCount  =  10_000;

    private BPlusTree<int, int>    _tree       = null!;
    private string                 _dbPath     = null!;
    private string                 _walPath    = null!;

    // Pre-built operation sequences: (IsWrite=true → Put, IsWrite=false → TryGet).
    private (bool IsWrite, int Key)[] _ops80Read  = null!;  // 80% read / 20% write
    private (bool IsWrite, int Key)[] _ops80Write = null!;  // 20% read / 80% write
    private (bool IsWrite, int Key)[] _ops50      = null!;  // 50% read / 50% write

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_oltp_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_oltp_{Guid.NewGuid():N}.wal");

        var rng      = new Random(42);   // deterministic seed for reproducible key sequences
        var readKeys = Enumerable.Range(0, OpsCount).Select(_ => rng.Next(0, SeedCount)).ToArray();

        _ops80Read  = BuildOps(readKeys, writeStartKey: SeedCount,       readFraction: 0.8);
        _ops80Write = BuildOps(readKeys, writeStartKey: SeedCount + 2000, readFraction: 0.2);
        _ops50      = BuildOps(readKeys, writeStartKey: SeedCount + 4000, readFraction: 0.5);
    }

    private static (bool IsWrite, int Key)[] BuildOps(
        int[] readKeys, int writeStartKey, double readFraction)
    {
        // Interleave reads and writes in a regular pattern.
        // readFraction = 0.8 → pattern of length 10: RRRRRRRRWW (8 reads, 2 writes).
        // Write keys start at writeStartKey and increment — unique, above seeded range.
        int total      = OpsCount;
        int readsTotal = (int)(total * readFraction);
        int writesTotal = total - readsTotal;

        // Build the interleaved pattern: spread writes evenly across the sequence.
        var ops       = new (bool IsWrite, int Key)[total];
        int writeIdx  = 0;
        int readIdx   = 0;
        double writeEvery = readFraction > 0 ? (double)readsTotal / writesTotal : 0;

        for (int i = 0; i < total; i++)
        {
            // Place a write when the accumulated write budget is due.
            bool doWrite = writesTotal > 0
                && writeIdx < writesTotal
                && (readFraction == 0 || writeIdx * writeEvery <= readIdx);

            if (doWrite)
            {
                ops[i] = (true, writeStartKey + writeIdx);
                writeIdx++;
            }
            else
            {
                ops[i] = (false, readKeys[readIdx % readKeys.Length]);
                readIdx++;
            }
        }
        return ops;
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        File.Delete(_dbPath);
        File.Delete(_walPath);
        _tree = TreeHelper.Open(_dbPath, _walPath);
        for (int i = 0; i < SeedCount; i++) _tree.Put(i, i);
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    private void RunOps((bool IsWrite, int Key)[] ops)
    {
        foreach (var (isWrite, key) in ops)
        {
            if (isWrite) _tree.Put(key, key);
            else         _tree.TryGet(key, out _);
        }
    }

    [Benchmark]
    public void Oltp_ReadHeavy() => RunOps(_ops80Read);

    [Benchmark]
    public void Oltp_Balanced() => RunOps(_ops50);

    [Benchmark]
    public void Oltp_WriteHeavy() => RunOps(_ops80Write);
}
