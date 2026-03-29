using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Running;
using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;

namespace BPlusTree.Benchmarks;

// ── Insert benchmark ───────────────────────────────────────────────────────────
// Fresh empty tree each iteration: every measurement reflects inserting 1 M
// records into a clean tree, not an already-populated one.

[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.CpuSampling)]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class InsertBenchmarks
{
    private BPlusTree<int, int> _tree = null!;
    private string _dbPath  = null!;
    private string _walPath = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_ins_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_ins_{Guid.NewGuid():N}.wal");
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        File.Delete(_dbPath);
        File.Delete(_walPath);
        _tree = TreeHelper.Open(_dbPath, _walPath);
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    [Benchmark]
    public void SequentialInsert_1M()
    {
        for (int i = 0; i < 1_000_000; i++) _tree.Put(i, i);
    }
}

// ── Read benchmarks ────────────────────────────────────────────────────────────
// Seed 1 M records once in GlobalSetup; all iterations read from the same tree.
// Neither PointLookup nor RangeScan mutates state, so results are stable.

[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.CpuSampling)]
[SimpleJob]
public class ReadBenchmarks
{
    private BPlusTree<int, int> _tree = null!;
    private string _dbPath  = null!;
    private string _walPath = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_read_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_read_{Guid.NewGuid():N}.wal");

        // ── Seed phase ─────────────────────────────────────────────────────────
        // Insert 1M records sequentially. After this, the buffer pool is occupied
        // by pages near key=999999 (tail of the sequential write). The PointLookup
        // benchmark reads keys 0–9999 (head of the tree) — a completely different
        // working set. If the tree is left open here, every PointLookup iteration
        // is a cold-disk read, not a cache-warm B+ tree read.
        //
        // HISTORICAL NOTE: Phase 24–27 benchmarks did NOT close the tree after
        // seeding. The 6.99ms Phase 27 PointLookup baseline was measuring the
        // warm-up behaviour of synchronous eviction (which self-healed the working
        // set on the first iteration) — not steady-state warm-cache reads. After
        // Phase 26 introduced async EvictionWorker, this self-healing stopped,
        // exposing the measurement artifact as a large apparent regression.
        // The cold-reopen below is the correct design: it decouples the seeding
        // working set from the read working set. (Validated in Phase 29b.)
        var seedTree = TreeHelper.Open(_dbPath, _walPath);
        for (int i = 0; i < 1_000_000; i++)
            seedTree.Put(i, i);
        seedTree.Close();   // flushes dirty pages + WAL checkpoint; releases all pool frames

        // ── Reopen with cold pool ───────────────────────────────────────────────
        // The reopened tree has only the root/meta page loaded. The pool is cold.
        // BenchmarkDotNet's warmup iterations will run PointLookup against this pool,
        // loading the 0–9999 working set before the measured iterations begin.
        _tree = TreeHelper.Open(_dbPath, _walPath);

        // ── Minimal pre-warmup ─────────────────────────────────────────────────
        // Touch the first 1000 keys to load root and first-level internal pages
        // before BenchmarkDotNet's own warmup begins.
        for (int i = 0; i < 1_000; i++)
            _tree.TryGet(i, out _);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _tree?.Dispose();
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    [Benchmark]
    public void PointLookup()
    {
        for (int i = 0; i < 10_000; i++) _tree.TryGet(i % 1_000_000, out _);
    }

    [Benchmark]
    public void RangeScan_1K() => _tree.Scan(0, 999).ToList();

    [Benchmark]
    public void ScanReverse_1K() => _tree.ScanReverse(999, 0).ToList();
}

// ── Random insert benchmark ────────────────────────────────────────────────────
// Same as InsertBenchmarks but keys are pre-shuffled once in GlobalSetup.
// Measures the cost of random-order insertions (more splits, less sequential I/O)
// vs the sequential baseline. Shuffle is done outside the timed path.

[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.CpuSampling)]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class RandomInsertBenchmarks
{
    private BPlusTree<int, int> _tree = null!;
    private string _dbPath  = null!;
    private string _walPath = null!;
    private int[]  _keys    = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"bench_rins_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_rins_{Guid.NewGuid():N}.wal");

        // Build and shuffle once — all iterations use the same key permutation.
        _keys = Enumerable.Range(0, 1_000_000).ToArray();
        Random.Shared.Shuffle(_keys);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        File.Delete(_dbPath);
        File.Delete(_walPath);
        _tree = TreeHelper.Open(_dbPath, _walPath);
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    [Benchmark]
    public void RandomInsert_1M()
    {
        for (int i = 0; i < 1_000_000; i++) _tree.Put(_keys[i], _keys[i]);
    }
}

// ── Compaction benchmark ───────────────────────────────────────────────────────
// Re-seed 1 M records before every iteration so each measurement compacts
// a freshly-written (partially-fragmented) tree rather than an already-compact one.

[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.CpuSampling)]
// iterationCount: 5 — minimum for CI 99.9% < 10% at σ ≈ 0.26s.
// Each iteration takes ~32s; N=3 gave 15% CI margin (too wide for regression detection).
// Do not lower below 5 without re-evaluating CI margin. (Phase 32)
[SimpleJob(warmupCount: 1, iterationCount: 5)]
public class CompactionBenchmarks
{
    private BPlusTree<int, int> _tree = null!;
    private string _dbPath  = null!;
    private string _walPath = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_cpt_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_cpt_{Guid.NewGuid():N}.wal");
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        File.Delete(_dbPath);
        File.Delete(_walPath);
        _tree = TreeHelper.Open(_dbPath, _walPath);
        for (int i = 0; i < 1_000_000; i++) _tree.Put(i, i);
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    [Benchmark]
    public void FullCompaction() => _tree.Compact();
}

// ── Delete benchmark ───────────────────────────────────────────────────────────
// Measures inline (non-overflow) delete cost: sequential and random key order.
// Seeds 100K keys per iteration to keep setup time ~0.5s (vs 32s for 1M).
// SequentialDelete is the baseline (ascending key order, best-case leaf locality).
// RandomDelete exercises the rebalance/merge path more aggressively.

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 5)]
public class DeleteBenchmarks
{
    private const int N = 100_000;

    private BPlusTree<int, int> _tree         = null!;
    private string              _dbPath       = null!;
    private string              _walPath      = null!;
    private int[]               _shuffledKeys = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_del_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_del_{Guid.NewGuid():N}.wal");

        // Build and shuffle once — same permutation for all iterations.
        _shuffledKeys = Enumerable.Range(0, N).ToArray();
        Random.Shared.Shuffle(_shuffledKeys);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        File.Delete(_dbPath);
        File.Delete(_walPath);
        _tree = TreeHelper.Open(_dbPath, _walPath);
        for (int i = 0; i < N; i++) _tree.Put(i, i);
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    [Benchmark(Baseline = true)]
    public void SequentialDelete_100K()
    {
        for (int i = 0; i < N; i++) _tree.Delete(i);
    }

    [Benchmark]
    public void RandomDelete_100K()
    {
        for (int i = 0; i < N; i++) _tree.Delete(_shuffledKeys[i]);
    }
}

// ── GroupCommit insert benchmark ───────────────────────────────────────────────
// Identical to InsertBenchmarks but opens the tree with WalSyncMode.GroupCommit
// (FlushIntervalMs=5, FlushBatchSize=256). Fsync is deferred to the background
// thread; the insert hot path only drains the in-memory buffer to the OS.

[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.CpuSampling)]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class GroupCommitInsertBenchmarks
{
    private BPlusTree<int, int> _tree = null!;
    private string _dbPath  = null!;
    private string _walPath = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_gc_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_gc_{Guid.NewGuid():N}.wal");
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        File.Delete(_dbPath);
        File.Delete(_walPath);
        _tree = TreeHelper.OpenGroupCommit(_dbPath, _walPath);
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    [Benchmark]
    public void SequentialInsert_1M()
    {
        for (int i = 0; i < 1_000_000; i++) _tree.Put(i, i);
    }
}

// ── Transaction throughput benchmarks ──────────────────────────────────────────
// Measures BeginTransaction/Commit throughput across four contention scenarios.
// All scenarios perform TxCount total transactions with InsertsPerTx inserts each.
// Fresh empty tree per iteration (no pre-seeding) — consistent with InsertBenchmarks.
//
// Results to watch:
//   Scenario 1 (baseline): overhead of BeginTransaction/Commit vs raw Put().
//   Scenario 2 (non-conflicting): _writerMutex contention cost at 4 threads.
//   Scenario 3 (conflicting): conflict rate + retry tax at page-level lock granularity.
//   Scenario 4 (mixed): whether snapshot-reader epoch churn degrades writer throughput.

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 5)]
public class TransactionBenchmarks
{
    private const int TxCount      = 1_000;   // total transactions per iteration
    private const int InsertsPerTx = 10;       // inserts per transaction
    private const int ThreadCount  = 4;        // concurrent threads for multi-threaded scenarios

    private BPlusTree<int, int> _tree = null!;
    private string _dbPath  = null!;
    private string _walPath = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_tx_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_tx_{Guid.NewGuid():N}.wal");
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        File.Delete(_dbPath);
        File.Delete(_walPath);
        _tree = TreeHelper.Open(_dbPath, _walPath);

        // Pre-seed the full key space used by all benchmark scenarios.
        // This ensures the tree has a mature, multi-page leaf structure before any
        // concurrent transactions begin. Without pre-seeding all threads' first inserts
        // land on the same single-leaf root in an empty tree, causing unexpected
        // TransactionConflictException in the "non-conflicting" scenario and a
        // PageNotFoundException (NullPageId root) in the conflicting scenario when
        // concurrent transactions race to create the initial leaf. Pre-seeding also
        // reflects realistic production usage — trees are never empty in production.
        // With 10,000 records at PageSize=8192 and 8-byte int+int entries, the tree
        // grows to ~14 leaf pages; each thread's 2,500-key range spans ~3 separate
        // leaf pages, eliminating false startup conflicts in non-conflicting scenarios.
        for (int i = 0; i < TxCount * InsertsPerTx; i++)
            _tree.Put(i, i);

        // Pre-seed each concurrent-thread slot so concurrent transaction benchmarks
        // perform Put-updates (no leaf splits) rather than pure inserts that can
        // trigger concurrent root splits and race on _metadata.SetRoot().
        // 10,000-key gaps between thread slots prevent leaf-boundary page contention.
        // ThreadCount=4 slots cover both the 4-writer NonConflicting scenario and
        // the 2-writer Mixed scenario.
        const int concBase = TxCount * InsertsPerTx;  // 10 000
        for (int t = 0; t < ThreadCount; t++)
            for (int i = 0; i < 10_000; i++)
                _tree.Put(concBase + t * 10_000 + i, i);
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    // ── Scenario 1: Single-thread baseline ─────────────────────────────────────
    // 1,000 transactions × 10 inserts each = 10,000 total inserts, single thread.
    // Establishes the overhead of BeginTransaction/Commit vs raw auto-commit Put().
    // Each transaction writes a unique key range — no conflicts possible.
    [Benchmark(Baseline = true)]
    public void Transaction_SingleThread_1K_Commits()
    {
        for (int t = 0; t < TxCount; t++)
        {
            using var tx = _tree.BeginTransaction();
            int baseKey = t * InsertsPerTx;
            for (int i = 0; i < InsertsPerTx; i++)
                tx.Insert(baseKey + i, i);
            tx.Commit();
        }
    }

    // ── Scenario 2: Concurrent non-conflicting writers ──────────────────────────
    // 4 threads × 250 transactions × 10 inserts = 10,000 total inserts.
    // Each thread owns a disjoint key range so there are no key-level conflicts.
    // However the commit-serialized model uses a whole-tree root version check:
    // if thread A commits while thread B is in-flight, B's snapshot root is stale
    // at commit time → TransactionConflictException, even with disjoint keys.
    // Threads therefore retry on conflict, just like Scenario 3.
    // This benchmark measures commit-mutex contention + retry cost for disjoint
    // key ranges, which is structurally lower than same-key conflicts (fewer
    // retries, shorter critical section per commit).
    [Benchmark]
    public void Transaction_Concurrent_NonConflicting_4T()
    {
        const int txPerThread = TxCount / ThreadCount;        // 250 successful commits
        // Each thread gets an isolated 10,000-key slot (pre-seeded in IterationSetup).
        // 10K gaps prevent leaf-boundary page contention between threads.
        // Put (upsert) is used so transactions update pre-seeded keys — no leaf splits,
        // no concurrent root-split race on _metadata.SetRoot().
        const int concBase = TxCount * InsertsPerTx;           // 10 000

        var tasks = Enumerable.Range(0, ThreadCount).Select(t => Task.Run(() =>
        {
            int rangeBase = concBase + t * 10_000;
            int completed = 0;
            int nextTx    = 0;   // tracks which tx slot to attempt next
            while (completed < txPerThread)
            {
                try
                {
                    using var tx = _tree.BeginTransaction();
                    int keyBase = rangeBase + nextTx * InsertsPerTx;
                    for (int k = 0; k < InsertsPerTx; k++)
                        tx.Insert(keyBase + k, k);
                    tx.Commit();
                    nextTx++;
                    completed++;
                }
                catch (TransactionConflictException)
                {
                    // Dispose() already rolled back. Retry the same key slot.
                }
            }
        })).ToArray();
        Task.WaitAll(tasks);
    }

    // ── Scenario 3: Concurrent conflicting writers ──────────────────────────────
    // 4 threads all targeting the same 10-key range (keys 0–9), which maps to a
    // single leaf page. Per-page write locks are held for the transaction lifetime,
    // so concurrent transactions on the same page will conflict and throw
    // TransactionConflictException. Each thread retries until it accumulates
    // TxCount/ThreadCount successful commits. Failed attempts are visible in the
    // elapsed time — they represent the conflict rate + retry overhead.
    //
    // Retry pattern: the using-block's finally calls Dispose() (rollback) before
    // the catch runs. The catch simply restarts the outer while loop — no manual
    // rollback needed.
    [Benchmark]
    public void Transaction_Concurrent_Conflicting_4T()
    {
        const int txPerThread = TxCount / ThreadCount;   // 250 successful commits per thread

        var tasks = Enumerable.Range(0, ThreadCount).Select(_ => Task.Run(() =>
        {
            int completed = 0;
            while (completed < txPerThread)
            {
                try
                {
                    using var tx = _tree.BeginTransaction();
                    for (int k = 0; k < InsertsPerTx; k++)
                        tx.Insert(k, k);        // all threads write keys 0–9 (same leaf page)
                    tx.Commit();
                    completed++;
                }
                catch (TransactionConflictException)
                {
                    // using-block Dispose() already rolled back the transaction.
                    // Retry immediately.
                }
            }
        })).ToArray();
        Task.WaitAll(tasks);
    }

    // ── Scenario 4: Mixed snapshot-readers + writers ────────────────────────────
    // 2 writer threads (disjoint key ranges) + 2 snapshot-reader threads running
    // concurrently. Readers open a snapshot, call TryGet, dispose — 500 times each.
    // HasActiveSnapshots stays true for most of the iteration because reader threads
    // continuously hold open epoch tokens, forcing all writer operations through the
    // CoW shadow path. Measures epoch-registry interaction cost and whether reader
    // snapshot churn degrades writer throughput.
    //
    // Writers retry on TransactionConflictException: the commit-serialized model
    // fires root-version conflicts when two writers' snapshots race, even with
    // disjoint key ranges. Retry cost is expected to be lower than Scenario 3
    // (writers don't contend on the same leaf page).
    [Benchmark]
    public void Transaction_Mixed_ReadersWriters_4T()
    {
        const int txPerWriter    = TxCount / 2;                  // 500 successful commits
        const int snapsPerReader = 500;
        // Each writer gets an isolated 10,000-key slot (pre-seeded in IterationSetup).
        // Put (upsert) avoids leaf splits during the concurrent phase.
        const int concBase = TxCount * InsertsPerTx;             // 10 000

        var writerTasks = Enumerable.Range(0, 2).Select(t => Task.Run(() =>
        {
            int rangeBase = concBase + t * 10_000;
            int completed = 0;
            int nextTx    = 0;
            while (completed < txPerWriter)
            {
                try
                {
                    using var tx = _tree.BeginTransaction();
                    int keyBase = rangeBase + nextTx * InsertsPerTx;
                    for (int k = 0; k < InsertsPerTx; k++)
                        tx.Insert(keyBase + k, k);
                    tx.Commit();
                    nextTx++;
                    completed++;
                }
                catch (TransactionConflictException)
                {
                    // Dispose() already rolled back. Retry the same key slot.
                }
            }
        }));

        var readerTasks = Enumerable.Range(0, 2).Select(t => Task.Run(() =>
        {
            for (int i = 0; i < snapsPerReader; i++)
            {
                using var snap = _tree.BeginSnapshot();
                snap.TryGet(i % 1_000, out _);
            }
        }));

        Task.WaitAll(writerTasks.Concat(readerTasks).ToArray());
    }
}

// ── SSI overhead benchmarks ────────────────────────────────────────────────────
// Measures the cost of SSI read-set tracking (Phase 88) and conflict detection
// at commit (Phase 89) relative to a write-only transaction baseline.
//
// Pre-seeded with SeedCount keys to guarantee a mature multi-leaf structure.
// New inserts use monotonically increasing _nextKey (above the seeded range) to
// avoid key collisions. Same-thread nested transactions (Scenario 4) update an
// existing seeded key to trigger CoW retirement of the first leaf.
//
// Results to watch:
//   Scenario 1 (write-only)  : zero SSI overhead — _readSet stays null.
//   Scenario 2 (read-write)  : TrackLeafRead ×1 + FindConflictingPage ×1 entry.
//   Scenario 3 (scan-write)  : TrackLeafRead ×leaf_count + FindConflictingPage ×leaf_count.
//   Scenario 4 (abort+retry) : SSI abort path — conflict detected + exception ctor
//                              + rollback Dispose; followed by a clean retry commit.

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 5)]
public class SsiBenchmarks
{
    // SeedCount guarantees a mature multi-leaf tree at PageSize=8192 (int-int entries:
    // ~509 slots/leaf → 5,000 keys spans ~10 leaves). All new inserts use keys ≥ SeedCount.
    private const int SeedCount    = 5_000;
    private const int TxCount      = 1_000;   // iterations for write-only and read-write scenarios
    private const int ScanTxCount  =   100;   // iterations for scan-write (scan is relatively expensive)
    private const int ConflictCount =  500;   // abort+retry pairs for SSI conflict scenario

    private BPlusTree<int, int> _tree    = null!;
    private string              _dbPath  = null!;
    private string              _walPath = null!;
    private int                 _nextKey;   // monotonically increasing; reset to SeedCount each iteration

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_ssi_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_ssi_{Guid.NewGuid():N}.wal");
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        File.Delete(_dbPath);
        File.Delete(_walPath);
        _tree = TreeHelper.Open(_dbPath, _walPath);
        for (int i = 0; i < SeedCount; i++)
            _tree.Put(i, i);
        _nextKey = SeedCount;
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    // ── Scenario 1: Write-only (SSI baseline) ─────────────────────────────────
    // 1,000 transactions × 1 insert each. No reads → _readSet = null for all txs.
    // FindConflictingPage returns 0u at the null-guard (first line of the method).
    // No HashSet allocation, no ConcurrentDictionary lookups. Pure transaction cost.
    [Benchmark(Baseline = true)]
    public void Ssi_WriteOnly_1K()
    {
        for (int t = 0; t < TxCount; t++)
        {
            using var tx = _tree.BeginTransaction();
            tx.Insert(_nextKey++, t);
            tx.Commit();
        }
    }

    // ── Scenario 2: Read-write, small ReadSet (1 leaf) ────────────────────────
    // 1,000 transactions × 1 TryGet + 1 Insert. TryGet(0) resolves to the first
    // leaf → ReadSet = {1 page ID}. FindConflictingPage scans 1 HashSet entry.
    // Overhead vs Scenario 1 = HashSet allocation (first call) + HashSet.Add ×1 +
    // ConcurrentDictionary.TryGetValue ×1 at commit.
    [Benchmark]
    public void Ssi_ReadWrite_SmallReadSet_1K()
    {
        for (int t = 0; t < TxCount; t++)
        {
            using var tx = _tree.BeginTransaction();
            tx.TryGet(0, out _);    // first leaf enters ReadSet
            tx.Insert(_nextKey++, t);
            tx.Commit();
        }
    }

    // ── Scenario 3: Scan-then-write, large ReadSet ────────────────────────────
    // 100 transactions × full Scan + 1 Insert. Scan visits all ~10 leaves in the
    // seeded tree; TrackLeafRead fires once per leaf transition. ReadSet grows to
    // ~10 page IDs. FindConflictingPage scans ~10 HashSet entries at commit.
    // Overhead vs Scenario 1 = TrackLeafRead ×(leaf_count) + TryGetValue ×(leaf_count).
    [Benchmark]
    public void Ssi_ScanWrite_LargeReadSet_100()
    {
        for (int t = 0; t < ScanTxCount; t++)
        {
            using var tx = _tree.BeginTransaction();
            foreach (var _ in tx.Scan()) { }   // TrackLeafRead fires at each leaf boundary
            tx.Insert(_nextKey++, t);
            tx.Commit();
        }
    }

    // ── Scenario 4: SSI conflict abort+retry ─────────────────────────────────
    // 500 iterations. Each iteration:
    //   1. Open outer tx; read key 0 (first leaf enters ReadSet).
    //   2. Open inner tx (same thread, _txWriterDepth++); update key 0 → CoW retires first leaf.
    //   3. Inner tx commits; retire epoch > outer tx's snapshot epoch.
    //   4. Outer tx.Commit() → FindConflictingPage returns first-leaf page ID → throws.
    //   5. using-block Dispose() rolls back outer tx (before-image restore, lock release).
    //   6. Retry: open a fresh tx; read updated key 0; insert a new key; commit cleanly.
    //
    // Measures: SSI abort path = FindConflictingPage (conflict) + TransactionConflictException
    // ctor + Dispose() rollback + clean retry overhead.
    [Benchmark]
    public void Ssi_ConflictAbortRetry_500()
    {
        for (int t = 0; t < ConflictCount; t++)
        {
            // Phase 1: provoke an SSI conflict on the outer transaction.
            try
            {
                using var outerTx = _tree.BeginTransaction();
                outerTx.TryGet(0, out _);   // first leaf page enters ReadSet

                using (var innerTx = _tree.BeginTransaction())   // same thread → depth++
                {
                    innerTx.Insert(0, t);    // CoW-retires first leaf; retire epoch > outerTx epoch
                    innerTx.Commit();
                }

                outerTx.Commit();   // FindConflictingPage: retireEpoch > snapshotEpoch → throws
            }
            catch (TransactionConflictException) { /* expected; outerTx already rolled back */ }

            // Phase 2: clean retry (inner tx already committed; no new retires in progress).
            using var retryTx = _tree.BeginTransaction();
            retryTx.TryGet(0, out _);
            retryTx.Insert(_nextKey++, t);
            retryTx.Commit();
        }
    }
}

// ── PutRange benchmark ─────────────────────────────────────────────────────────
// Measures the fsync amortization benefit of PutRange (1 WAL fsync for N inserts)
// vs N individual auto-commit Put calls (N WAL fsyncs).
//
// Phase 90 established the auto-commit floor: 0.394 ms/tx for a single-insert
// transaction (dominated by WAL fsync). This benchmark quantifies how much of
// that cost PutRange eliminates at N=1,000 and N=10,000.
//
// Item arrays are pre-built in IterationSetup to exclude tuple-allocation overhead
// from the measurement. The hot path is purely tree I/O + WAL.

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 5)]
public class PutRangeBenchmarks
{
    private const int N1K  = 1_000;
    private const int N10K = 10_000;

    private BPlusTree<int, int>    _tree     = null!;
    private string                 _dbPath   = null!;
    private string                 _walPath  = null!;
    private (int Key, int Value)[] _items1K  = null!;
    private (int Key, int Value)[] _items10K = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_pr_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_pr_{Guid.NewGuid():N}.wal");
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        File.Delete(_dbPath);
        File.Delete(_walPath);
        _tree = TreeHelper.Open(_dbPath, _walPath);

        // Pre-build item arrays so tuple allocation is not part of the hot path.
        _items1K  = Enumerable.Range(0, N1K) .Select(i => (i, i)).ToArray();
        _items10K = Enumerable.Range(0, N10K).Select(i => (i, i)).ToArray();
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    // ── Baseline: 1,000 individual auto-commit Put calls (1,000 WAL fsyncs) ────
    [Benchmark(Baseline = true)]
    public void AutoCommit_Put_1K()
    {
        for (int i = 0; i < N1K; i++) _tree.Put(i, i);
    }

    // ── PutRange: 1,000 inserts in one transaction (1 WAL fsync) ───────────────
    [Benchmark]
    public void PutRange_1K()
    {
        _tree.PutRange(_items1K);
    }

    // ── Scale-up baseline: 10,000 individual auto-commit Put calls ─────────────
    [Benchmark]
    public void AutoCommit_Put_10K()
    {
        for (int i = 0; i < N10K; i++) _tree.Put(i, i);
    }

    // ── Scale-up PutRange: 10,000 inserts in one transaction ───────────────────
    [Benchmark]
    public void PutRange_10K()
    {
        _tree.PutRange(_items10K);
    }
}

// ── DeleteRange benchmark ──────────────────────────────────────────────────────
// Compares one DeleteRange(0, 9999) call against 10,000 individual Delete calls.
// Mirrors PutRangeBenchmarks structure. Both methods operate on the same 10K keys
// seeded in IterationSetup. Measures the batch-delete overhead reduction.

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 5)]
public class DeleteRangeBenchmarks
{
    private const int N = 10_000;

    private BPlusTree<int, int> _tree    = null!;
    private string              _dbPath  = null!;
    private string              _walPath = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath  = Path.Combine(Path.GetTempPath(), $"bench_dr_{Guid.NewGuid():N}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"bench_dr_{Guid.NewGuid():N}.wal");
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tree?.Dispose();
        File.Delete(_dbPath);
        File.Delete(_walPath);
        _tree = TreeHelper.Open(_dbPath, _walPath);
        for (int i = 0; i < N; i++) _tree.Put(i, i);
    }

    [IterationCleanup]
    public void IterationCleanup() { _tree?.Dispose(); _tree = null!; }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }

    // ── Baseline: 10,000 individual auto-commit Delete calls ───────────────────
    [Benchmark(Baseline = true)]
    public void IndividualDeletes_10K()
    {
        for (int i = 0; i < N; i++) _tree.Delete(i);
    }

    // ── DeleteRange: single call covering all 10,000 keys ──────────────────────
    [Benchmark]
    public void DeleteRange_10K() => _tree.DeleteRange(0, N - 1);
}

// ── Shared factory ─────────────────────────────────────────────────────────────

internal static class TreeHelper
{
    /// <summary>
    /// Opens a BPlusTree with the standard benchmark configuration.
    ///
    /// CONFIGURATION NOTE — WAL buffer interaction (Phase 62):
    /// CheckpointThreshold × PageSize must be ≤ WalBufferSize to avoid
    /// WAL buffer overflow during checkpoint cycles. Each overflow adds a
    /// synchronous fsync on the insert thread, degrading throughput.
    ///
    /// With PageSize=8192 and WalBufferSize=8MB (raised in Phase 62):
    ///   max safe CheckpointThreshold = 8MB / 8KB = 1024 pages
    ///
    /// This config uses CheckpointThreshold=256 (quarter the safe max).
    ///
    /// Historical record:
    ///   Phase 28 silently used CheckpointThreshold=1024, causing the WAL
    ///   buffer to overflow twice per cycle and adding +149% to insert time.
    ///   This comment exists to prevent that from happening again.
    ///
    /// CoW write-amplification (Phase 62):
    /// CoWWriteAmplification=3 scales the effective eviction batch to 32×3=96
    /// pages, normalising WAL fsyncs per insert to pre-MVCC levels for H≈3 trees.
    /// BufferPoolCapacity=2048 (was 1024) provides headroom for CoW shadow pages.
    /// </summary>
    internal static BPlusTree<int, int> Open(string dbPath, string walPath,
        WalSyncMode syncMode = WalSyncMode.Synchronous) =>
        BPlusTree<int, int>.Open(
            new BPlusTreeOptions
            {
                DataFilePath          = dbPath,
                WalFilePath           = walPath,
                PageSize              = 8_192,
                BufferPoolCapacity    = 2_048,           // was 1024; 2× for CoW shadow-page headroom
                CheckpointThreshold   = 256,             // max safe = WalBufferSize/PageSize = 1024
                WalBufferSize         = 8 * 1_024 * 1_024, // was 4MB; raised for CoW workloads
                CoWWriteAmplification = 3,               // effective eviction batch = 32×3 = 96
                SyncMode              = syncMode,
                FlushIntervalMs       = 5,
                FlushBatchSize        = 256,
            },
            Int32Serializer.Instance,
            Int32Serializer.Instance);

    internal static BPlusTree<int, int> OpenGroupCommit(string dbPath, string walPath) =>
        Open(dbPath, walPath, WalSyncMode.GroupCommit);
}

// ── Entry point ────────────────────────────────────────────────────────────────

public static class Program
{
    public static void Main(string[] args) =>
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
}
