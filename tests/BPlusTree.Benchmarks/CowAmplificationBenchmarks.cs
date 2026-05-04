using BenchmarkDotNet.Attributes;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;

namespace ByTech.BPlusTree.Core.Benchmarks;

// ── Class A: Pure memory-copy cost (lower bound) ───────────────────────────────
// Measures the raw Span<byte>.CopyTo cost of duplicating H pages of 8192 bytes.
// No allocation, no I/O, no buffer pool — CPU/memory-bandwidth only.
// This is the irreducible floor of shadow-paging overhead.

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class CowMemoryCopyBenchmarks
{
    [Params(1, 2, 3, 4)]
    public int Height { get; set; }

    private byte[]   _source = null!;
    private byte[][] _dests  = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _source = new byte[8_192];
        // Fill with non-zero data so the JIT cannot eliminate the copy as dead store.
        Random.Shared.NextBytes(_source);
        _dests = Enumerable.Range(0, Height).Select(_ => new byte[8_192]).ToArray();
    }

    /// <summary>
    /// Simulate the per-insert content copy chain for a shadow-paging write path
    /// of height H. Each iteration represents one logical insert.
    /// </summary>
    [Benchmark(OperationsPerInvoke = 1_000_000)]
    public void CopyH_Pages()
    {
        ReadOnlySpan<byte> src = _source;
        for (int i = 0; i < 1_000_000; i++)
        {
            for (int h = 0; h < Height; h++)
                src.CopyTo(_dests[h]);
        }
    }
}

// ── Class B: Realistic PageManager cost (upper bound) ─────────────────────────
// Measures AllocatePage + Span.CopyTo + MarkDirtyAndUnpin(bypassWal) × H under
// buffer-pool pressure. bypassWal=true isolates pool cost from WAL fsync cost.
//
// Pool pressure design:
//   BufferPoolCapacity = 512. With H=4 and 100K iterations the inner loop allocates
//   400K pages. The pool holds 512 frames; it enters the eviction-dominated regime
//   within the first ~128 simulated inserts at H=4. This is the steady-state regime
//   that matters for MVCC throughput.
//
// Old-version retention:
//   Allocated pages are NOT freed during the inner loop. This simulates epoch-gated
//   retention of old-version pages — worst-case pool pressure. In the real MVCC
//   implementation, old pages are retained until the oldest active reader's snapshot
//   epoch advances. IterationCleanup frees all pages after each timed iteration.

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class CowPageAllocBenchmarks
{
    [Params(1, 2, 3, 4)]
    public int Height { get; set; }

    private PageManager      _mgr      = null!;
    private WalWriter        _wal      = null!;
    private byte[]           _source   = null!;
    private string           _dbPath   = null!;
    private string           _walPath  = null!;

    // Pages allocated during the benchmark inner loop.
    // Retained (not freed) to simulate epoch-gated old-version retention.
    // Freed in IterationCleanup after the timed iteration completes.
    private readonly List<uint> _allocated = new(capacity: 100_000 * 4);

    [GlobalSetup]
    public void GlobalSetup()
    {
        _source = new byte[8_192];
        Random.Shared.NextBytes(_source);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_cow_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_cow_{Guid.NewGuid():N}.wal");
        _wal     = WalWriter.Open(_walPath, bufferSize: 4 * 1_024 * 1_024);
        _mgr     = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 8_192,
            BufferPoolCapacity  = 512,
            CheckpointThreshold = 256,
        }, _wal);
        _allocated.Clear();
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        foreach (uint pageId in _allocated)
            _mgr.FreePage(pageId);
        _allocated.Clear();
        _mgr.Dispose();
        _wal.Dispose();
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    /// <summary>
    /// Simulate the per-insert shadow-paging page-allocation chain for height H.
    /// Each iteration = one logical insert: allocate H new frames, copy source
    /// content into each, mark dirty and unpin (WAL bypassed to isolate pool cost).
    /// Allocated pages are retained for epoch-gated retention simulation.
    /// </summary>
    [Benchmark(OperationsPerInvoke = 100_000)]
    public void AllocCopyH_Pages()
    {
        ReadOnlySpan<byte> src = _source;
        for (int i = 0; i < 100_000; i++)
        {
            for (int h = 0; h < Height; h++)
            {
                var frame = _mgr.AllocatePage(PageType.Leaf);
                src.CopyTo(frame.Data);
                _mgr.MarkDirtyAndUnpin(frame.PageId, bypassWal: true);
                _allocated.Add(frame.PageId);
            }
        }
    }
}
