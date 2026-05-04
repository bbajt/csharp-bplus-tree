using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Integration;

/// <summary>
/// M140 P2 — multi-axis bisect of DEBT-BPT-M140. Each [Fact] flips ONE axis off
/// the baseline `Baseline_reproduces_DEBT_BPT_M140` (which mirrors the failing
/// `Oracle_invariant_holds_under_upsert_heavy_changing_size_workload` from M139
/// P5 with deterministic seed + op count). The first axis whose flip turns
/// FAIL → PASS partitions the bug class.
///
/// Verdict log lives at <c>.docs/milestones/M140-NARROWING.MD</c>.
/// </summary>
public sealed class M140NarrowingTests : IDisposable
{
    private readonly string _dbPath  = Path.Combine(Path.GetTempPath(), $"m140-{Guid.NewGuid():N}.db");
    private readonly string _walPath = Path.Combine(Path.GetTempPath(), $"m140-{Guid.NewGuid():N}.wal");

    public void Dispose()
    {
        try { File.Delete(_dbPath); }  catch { }
        try { File.Delete(_walPath); } catch { }
    }

    private const int FailingSeed = 623211317;     // M139 P5 — value mismatch (closed at TreeEngine:506)
    private const int FailingSeed2 = 2114412983;   // M140 P2 — key missing (count drop) — under investigation
    private const int OpCount     = 2000;

    private sealed record Config(
        int    Seed             = FailingSeed,
        int    OpCount          = OpCount,
        int    PageSize         = 8192,
        int    CheckpointThresh = 256,           // BPlusTreeDefaults.CheckpointThreshold
        int    KeyPoolSize      = 16,
        int    ValueMin         = 200,
        int    ValueMax         = 600,           // inline only on 8KB pages
        int    UpsertPct        = 95,            // rest is delete
        bool   ScanViaSnapshot  = true,          // false → foreach (tree)
        int    AssertEveryNOps  = 10);

    /// <summary>
    /// M140 P4 regression pin: post-fix the baseline workload that surfaced
    /// DEBT-BPT-M140 must remain green. Pre-fix verdict (TreeEngine.cs:506
    /// ignored TryInsert's bool return on the SplitPath re-traversal): FAILED
    /// at op#1510 with value mismatch.
    /// </summary>
    [Fact]
    public void Axis0_baseline_DEBT_BPT_M140_regression_pin()
    {
        var verdict = RunAndCheck(new Config());
        Console.Error.WriteLine($"[M140-NARROWING axis0 baseline] {verdict}");
        verdict.Failed.Should().BeFalse($"baseline must stay green post-M140 fix; verdict={verdict}");
    }

    [Fact]
    public void Axis1a_forced_inline_max_value_100B()
    {
        var verdict = RunAndCheck(new Config { ValueMin = 50, ValueMax = 100 });
        Console.Error.WriteLine($"[M140-NARROWING axis1a forced-inline-100B] {verdict}");
    }

    [Fact]
    public void Axis1b_forced_overflow_min_value_5000B()
    {
        // MaxEntrySize on 8KB page ≈ 4KB; 5000B forces every value through overflow chain.
        var verdict = RunAndCheck(new Config { ValueMin = 5000, ValueMax = 6000 });
        Console.Error.WriteLine($"[M140-NARROWING axis1b forced-overflow-5KB] {verdict}");
    }

    [Fact]
    public void Axis2_keypool_4()
    {
        var verdict = RunAndCheck(new Config { KeyPoolSize = 4 });
        Console.Error.WriteLine($"[M140-NARROWING axis2 keypool-4] {verdict}");
    }

    [Fact]
    public void Axis3_upsert_only_no_delete()
    {
        var verdict = RunAndCheck(new Config { UpsertPct = 100 });
        Console.Error.WriteLine($"[M140-NARROWING axis3 upsert-only] {verdict}");
    }

    [Fact]
    public void Axis5_scan_via_direct_iteration_not_snapshot()
    {
        var verdict = RunAndCheck(new Config { ScanViaSnapshot = false });
        Console.Error.WriteLine($"[M140-NARROWING axis5 direct-iteration] {verdict}");
    }

    /// <summary>
    /// Direct copy of ScanConsistencyStressTests.Oracle_invariant_holds_under_randomized_upsert_delete_workload
    /// with seed 2114412983 hardcoded — verifies my narrowing harness is compatible.
    /// </summary>
    /// <summary>
    /// M141 P3 regression pin: seed 1878241319 op#9245 DEL pre-fix dropped 7
    /// keys (oracle dropped 1). Root cause: Splitter.SplitInternalAndInsert
    /// chose midIndex by COUNT rather than BYTES, then ignored
    /// TryInsertSeparator's bool — silently dropping the pending separator AND
    /// its child pointer to a leaf added to the chain by the parent leaf-split.
    /// The leaf was orphaned from tree parent pointers while the leaf chain
    /// still included it. Fix: byte-aware split-point selection + rebuild both
    /// halves from scratch (Initialize + TryAppend with throwing assertions).
    /// </summary>
    [Fact]
    public void AxisCopy2_seed_1878241319_assert_every_op()
    {
        // Op-by-op assertion to find the EXACT op that loses keys.
        const int seed = 1878241319;
        var rng = new Random(seed);
        using var tree = BPlusTree<string, byte[]>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 2048,
                CheckpointThreshold = 256,
                SyncMode            = WalSyncMode.Synchronous,
            },
            new CodecShapedOrdinalSerializer(), ByteArraySerializer.Instance);
        var oracle = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var keyPool = BuildKeyPool(64, rng);

        for (int i = 0; i < 9300; i++)
        {
            var key = keyPool[rng.Next(keyPool.Length)];
            string opType;

            if (rng.Next(100) < 80)
            {
                int size = 50 + rng.Next(951);
                var v = new byte[size];
                rng.NextBytes(v);
                tree.Put(key, v);
                oracle[key] = v;
                opType = "PUT";
            }
            else
            {
                tree.Delete(key);
                oracle.Remove(key);
                opType = "DEL";
            }

            int treeCount = 0;
            foreach (var _ in tree) treeCount++;


            if (treeCount != oracle.Count)
                throw new InvalidOperationException($"Op#{i+1} {opType} key={key}: tree={treeCount} oracle={oracle.Count}");
        }
    }

    [Fact]
    public void AxisCopy_direct_copy_of_failing_stress_test_seed_2114412983()
    {
        const int seed = 2114412983;
        var rng = new Random(seed);
        using var tree = BPlusTree<string, byte[]>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 2048,
                CheckpointThreshold = 256,
                SyncMode            = WalSyncMode.Synchronous,
            },
            new CodecShapedOrdinalSerializer(), ByteArraySerializer.Instance);
        var oracle = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var keyPool = BuildKeyPool(64, rng);

        long opCount = 0; long lastAssertOpCount = 0;
        const int assertEveryNOps = 25;
        for (int i = 0; i < 200; i++)
        {
            var key = keyPool[rng.Next(keyPool.Length)];
            if (rng.Next(100) < 80)
            {
                int size = 50 + rng.Next(951);
                var v = new byte[size];
                rng.NextBytes(v);
                tree.Put(key, v);
                oracle[key] = v;
            }
            else
            {
                tree.Delete(key);
                oracle.Remove(key);
            }
            opCount++;
            if (opCount - lastAssertOpCount >= assertEveryNOps)
            {
                var scanned = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                using (var snap = tree.BeginSnapshot())
                    foreach (var (k, sv) in snap.Scan(null, null)) scanned[k] = sv;
                Console.Error.WriteLine($"[axisCopy op#{opCount}] scan-count={scanned.Count} oracle-count={oracle.Count}");
                scanned.Count.Should().Be(oracle.Count, $"op#{opCount} divergence");
                lastAssertOpCount = opCount;
            }
        }
    }

    [Fact]
    public void Axis7_seed2_keypool_64_random_workload()
    {
        // Mirrors the failing `Oracle_invariant_holds_under_randomized_upsert_delete_workload`
        // shape (key pool 64, 80/20 upsert/delete) under FailingSeed2 to reproduce
        // the key-missing class that survived the TreeEngine:506 fix.
        var verdict = RunAndCheck(new Config { Seed = FailingSeed2, KeyPoolSize = 64, UpsertPct = 80, ValueMin = 50, ValueMax = 1000, OpCount = 200, AssertEveryNOps = 25 });
        Console.Error.WriteLine($"[M140-NARROWING axis7 seed2-keypool64] {verdict}");
    }

    [Fact]
    public void Axis7a_seed2_keypool_64_no_delete()
    {
        var verdict = RunAndCheck(new Config { Seed = FailingSeed2, KeyPoolSize = 64, UpsertPct = 100, ValueMin = 50, ValueMax = 1000, OpCount = 200 });
        Console.Error.WriteLine($"[M140-NARROWING axis7a seed2-no-delete] {verdict}");
    }

    [Fact]
    public void Axis7b_seed2_keypool_64_forced_inline()
    {
        var verdict = RunAndCheck(new Config { Seed = FailingSeed2, KeyPoolSize = 64, UpsertPct = 80, ValueMin = 50, ValueMax = 100, OpCount = 200 });
        Console.Error.WriteLine($"[M140-NARROWING axis7b seed2-forced-inline-100B] {verdict}");
    }

    [Fact]
    public void Axis7c_seed2_keypool_64_direct_iteration()
    {
        var verdict = RunAndCheck(new Config { Seed = FailingSeed2, KeyPoolSize = 64, UpsertPct = 80, ValueMin = 50, ValueMax = 1000, OpCount = 200, ScanViaSnapshot = false });
        Console.Error.WriteLine($"[M140-NARROWING axis7c seed2-direct-iter] {verdict}");
    }

    [Fact]
    public void Axis6_smaller_page_4KB_lower_checkpoint()
    {
        var verdict = RunAndCheck(new Config { PageSize = 4096, CheckpointThresh = 64 });
        Console.Error.WriteLine($"[M140-NARROWING axis6 page-4KB] {verdict}");
    }

    // ─── Workload runner ────────────────────────────────────────────────────

    private sealed record Verdict(bool Failed, long FailingOp, string? Detail)
    {
        public override string ToString() => Failed
            ? $"FAILED at op#{FailingOp}: {Detail}"
            : $"PASSED ({OpCount} ops)";
    }

    private Verdict RunAndCheck(Config cfg)
    {
        var rng = new Random(cfg.Seed);
        using var tree = BPlusTree<string, byte[]>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = cfg.PageSize,
                BufferPoolCapacity  = 2048,
                CheckpointThreshold = cfg.CheckpointThresh,
                SyncMode            = WalSyncMode.Synchronous,
            },
            new CodecShapedOrdinalSerializer(), ByteArraySerializer.Instance);

        var oracle  = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var keyPool = BuildKeyPool(cfg.KeyPoolSize, rng);

        long opCount = 0;
        long lastAssertOpCount = 0;

        for (int i = 0; i < cfg.OpCount; i++)
        {
            var key = keyPool[rng.Next(keyPool.Length)];

            if (rng.Next(100) < cfg.UpsertPct)
            {
                int size = cfg.ValueMin + rng.Next(cfg.ValueMax - cfg.ValueMin + 1);
                var v = new byte[size];
                rng.NextBytes(v);
                tree.Put(key, v);
                oracle[key] = v;
            }
            else
            {
                tree.Delete(key);
                oracle.Remove(key);
            }

            opCount++;
            if (opCount - lastAssertOpCount >= cfg.AssertEveryNOps)
            {
                var verdict = CheckOracle(tree, oracle, opCount, cfg.ScanViaSnapshot);
                if (verdict != null) return new Verdict(true, opCount, verdict);
                lastAssertOpCount = opCount;
            }
        }

        var finalVerdict = CheckOracle(tree, oracle, opCount, cfg.ScanViaSnapshot);
        return finalVerdict != null
            ? new Verdict(true, opCount, finalVerdict)
            : new Verdict(false, opCount, null);
    }

    private static string? CheckOracle(
        BPlusTree<string, byte[]> tree,
        Dictionary<string, byte[]> oracle,
        long opCount,
        bool viaSnapshot)
    {
        var scanned = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        if (viaSnapshot)
        {
            using var snap = tree.BeginSnapshot();
            foreach (var (k, sv) in snap.Scan(null, null)) scanned[k] = sv;
        }
        else
        {
            foreach (var (k, sv) in tree) scanned[k] = sv;
        }

        if (scanned.Count != oracle.Count)
            return $"scan-count={scanned.Count} oracle-count={oracle.Count}";

        foreach (var (k, expected) in oracle)
        {
            if (!scanned.TryGetValue(k, out var actual))
                return $"key {k} missing from scan";
            if (!actual.AsSpan().SequenceEqual(expected))
                return $"value mismatch for key {k} (expected {expected.Length}B, got {actual.Length}B)";
        }
        return null;
    }

    private static string[] BuildKeyPool(int count, Random rng)
    {
        var keys = new List<string>(count);
        for (int i = 0; i < count - 4 && i < count; i++)
        {
            var bytes = new byte[16];
            rng.NextBytes(bytes);
            keys.Add("op." + new Guid(bytes).ToString());
        }
        for (int i = 0; i < 4 && keys.Count < count; i++)
            keys.Add($"topology.partition.{i}.transition");
        return keys.ToArray();
    }

    private sealed class CodecShapedOrdinalSerializer : IKeySerializer<string>, IValueSerializer<string>
    {
        public int FixedSize => -1;
        public int Serialize(string key, Span<byte> dst)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(key);
            BitConverter.TryWriteBytes(dst, bytes.Length);
            bytes.AsSpan().CopyTo(dst[4..]);
            return 4 + bytes.Length;
        }
        public string Deserialize(ReadOnlySpan<byte> src)
        {
            int len = BitConverter.ToInt32(src);
            return System.Text.Encoding.UTF8.GetString(src.Slice(4, len));
        }
        public int Compare(string x, string y) => string.CompareOrdinal(x, y);
        public int MeasureSize(string key) => 4 + System.Text.Encoding.UTF8.GetByteCount(key);
        public int GetSerializedSize(ReadOnlySpan<byte> data) => 4 + BitConverter.ToInt32(data);
    }
}
