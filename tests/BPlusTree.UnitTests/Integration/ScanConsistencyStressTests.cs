using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Integration;

/// <summary>
/// M139 P5 — oracle-backed randomized stress test for BPlusTree scan/point-read
/// consistency. Maintains an in-memory oracle <see cref="Dictionary{TKey,TValue}"/>
/// alongside the tree and asserts equality after every batch of operations.
///
/// Default CI budget: ~10 seconds. Override via <c>BEDROCK_BPT_FUZZ_MINUTES=N</c>.
/// Seed is random by default (printed for reproducibility); pin it via
/// <c>BEDROCK_BPT_FUZZ_SEED=&lt;int&gt;</c> when reproducing a flake.
///
/// Uses the trigger-shape <c>CodecShapedOrdinalSerializer</c> (4-byte LE key
/// length prefix) — the format that exposed the M139 LeafNode.TryInsert
/// upsert-data-loss bug. Deleting the M139 fix should re-surface failures here
/// within a few hundred operations.
/// </summary>
public sealed class ScanConsistencyStressTests : IDisposable
{
    private const int PageSize = 8192;

    private readonly string _dbPath  = Path.Combine(Path.GetTempPath(), $"bpt-stress-{Guid.NewGuid():N}.db");
    private readonly string _walPath = Path.Combine(Path.GetTempPath(), $"bpt-stress-{Guid.NewGuid():N}.wal");

    public void Dispose()
    {
        try { File.Delete(_dbPath); }  catch { }
        try { File.Delete(_walPath); } catch { }
    }

    // M141 P3 — un-skipped after closing the 12th-class bug:
    // Splitter.SplitInternalAndInsert used count-based midIndex and ignored
    // TryInsertSeparator's bool, silently dropping the pending separator + child
    // pointer for variable-size keys → orphaning the new leaf from tree parent
    // pointers while leaf chain still included it. Fix: byte-aware split point
    // selection + rebuild both halves from scratch.
    [Fact]
    public void Oracle_invariant_holds_under_randomized_upsert_delete_workload()
    {
        var (seed, budget) = ReadEnvConfig();
        Console.Error.WriteLine($"[stress] seed={seed} budget={budget.TotalSeconds:F1}s (override via BEDROCK_BPT_FUZZ_SEED, BEDROCK_BPT_FUZZ_MINUTES)");
        var rng = new Random(seed);

        using var tree = OpenTree(new CodecShapedOrdinalSerializer());
        var oracle = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var keyPool = BuildKeyPool(64, rng);

        long opCount = 0;
        long lastAssertOpCount = 0;
        const int assertEveryNOps = 25;
        var deadline = DateTime.UtcNow + budget;

        try
        {
            while (DateTime.UtcNow < deadline)
            {
                var key = keyPool[rng.Next(keyPool.Length)];

                // 80% upsert, 20% delete — biased toward the upsert-with-changing-size path.
                if (rng.Next(100) < 80)
                {
                    int size = 50 + rng.Next(951); // 50..1000
                    var value = new byte[size];
                    rng.NextBytes(value);
                    tree.Put(key, value);
                    oracle[key] = value;
                }
                else
                {
                    tree.Delete(key);
                    oracle.Remove(key);
                }

                opCount++;
                if (opCount - lastAssertOpCount >= assertEveryNOps)
                {
                    AssertOracleMatchesTree(tree, oracle, opCount, seed);
                    lastAssertOpCount = opCount;
                }
            }

            AssertOracleMatchesTree(tree, oracle, opCount, seed);
        }
        catch
        {
            Console.Error.WriteLine($"[stress] FAIL after {opCount} operations with seed={seed}. Reproduce with BEDROCK_BPT_FUZZ_SEED={seed}.");
            throw;
        }

        Console.Error.WriteLine($"[stress] PASS — {opCount} operations, {oracle.Count} keys live, seed={seed}.");
    }

    /// <summary>
    /// M139 P5 — diagnostic narrow repro for the value-mismatch failure surfaced
    /// by the randomized harness. Keep this test even after diagnosis: it's a
    /// minimal regression pin that runs in milliseconds.
    /// </summary>
    [Fact]
    public void Snapshot_scan_returns_latest_committed_value_for_repeated_upserts()
    {
        using var tree = OpenTree(new CodecShapedOrdinalSerializer());
        const string key = "op.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

        for (int i = 0; i < 30; i++)
        {
            int size = 200 + (i * 17 % 200);
            var v = new byte[size];
            for (int b = 0; b < size; b++) v[b] = (byte)(i & 0xFF);
            tree.Put(key, v);

            // Point-read agrees?
            tree.TryGet(key, out var pointRead).Should().BeTrue();
            pointRead.AsSpan().SequenceEqual(v).Should().BeTrue($"point-read at i={i} must return latest committed value");

            // Snapshot scan agrees?
            byte[]? scanned = null;
            using (var snap = tree.BeginSnapshot())
            {
                foreach (var (k, sv) in snap.Scan(null, null))
                    if (k == key) scanned = sv;
            }
            scanned.Should().NotBeNull($"snapshot scan at i={i} must include the key");
            scanned!.AsSpan().SequenceEqual(v).Should().BeTrue($"snapshot scan at i={i} must return latest committed value");
        }
    }

    /// <summary>
    /// Narrow-down: same-key delete-then-reinsert with growing values.
    /// Hypothesis: Delete leaves orphan slot; subsequent re-insert + upsert
    /// of changed size triggers the same class of leak the M139 fix addressed.
    /// </summary>
    [Fact]
    public void Delete_then_reinsert_with_growing_upsert_remains_consistent()
    {
        using var tree = OpenTree(new CodecShapedOrdinalSerializer());
        var keys = new[]
        {
            "op.aaaa1111-1111-1111-1111-aaaaaaaaaaaa",
            "op.bbbb2222-2222-2222-2222-bbbbbbbbbbbb",
            "op.cccc3333-3333-3333-3333-cccccccccccc",
            "op.dddd4444-4444-4444-4444-dddddddddddd",
        };
        var oracle = new Dictionary<string, byte[]>(StringComparer.Ordinal);

        for (int round = 0; round < 50; round++)
        {
            var key = keys[round % keys.Length];

            if (round % 7 == 6)
            {
                tree.Delete(key);
                oracle.Remove(key);
            }
            else
            {
                int size = 200 + ((round * 23) % 600);
                var v = new byte[size];
                for (int b = 0; b < size; b++) v[b] = (byte)(round & 0xFF);
                tree.Put(key, v);
                oracle[key] = v;
            }

            var scanned = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            using (var snap = tree.BeginSnapshot())
                foreach (var (k, sv) in snap.Scan(null, null)) scanned[k] = sv;

            scanned.Count.Should().Be(oracle.Count, $"round={round} op on {key}");
            foreach (var (k, expected) in oracle)
            {
                scanned.TryGetValue(k, out var actual).Should().BeTrue($"round={round} key={k} missing");
                actual.AsSpan().SequenceEqual(expected).Should().BeTrue($"round={round} key={k} value mismatch");
            }
        }
    }

    [Fact]
    public void Snapshot_scan_with_many_keys_and_upserts_matches_oracle()
    {
        using var tree = OpenTree(new CodecShapedOrdinalSerializer());
        var oracle = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var keys = new string[12];
        for (int i = 0; i < keys.Length; i++)
            keys[i] = "op." + new Guid(i + 1, 0, 0, new byte[8]).ToString();

        // Seed all keys.
        for (int i = 0; i < keys.Length; i++)
        {
            var v = new byte[300];
            for (int b = 0; b < v.Length; b++) v[b] = (byte)i;
            tree.Put(keys[i], v);
            oracle[keys[i]] = v;
        }

        // Now upsert each key with a different size 25 times.
        for (int round = 0; round < 25; round++)
        {
            for (int i = 0; i < keys.Length; i++)
            {
                int size = 250 + ((round * 13 + i * 7) % 200);
                var v = new byte[size];
                for (int b = 0; b < size; b++) v[b] = (byte)((round * 37 + i) & 0xFF);
                tree.Put(keys[i], v);
                oracle[keys[i]] = v;

                var scanned = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                using (var snap = tree.BeginSnapshot())
                    foreach (var (k, sv) in snap.Scan(null, null)) scanned[k] = sv;

                scanned.Count.Should().Be(oracle.Count, $"round={round} i={i} after Put({keys[i]})");
                foreach (var (k, expected) in oracle)
                {
                    scanned.TryGetValue(k, out var actual).Should().BeTrue($"round={round} i={i} key={k} missing from scan");
                    actual.AsSpan().SequenceEqual(expected).Should().BeTrue($"round={round} i={i} key={k} value mismatch");
                }
            }
        }
    }

    // M141 P3 — un-skipped (same fix as the randomized test above).
    [Fact]
    public void Oracle_invariant_holds_under_upsert_heavy_changing_size_workload()
    {
        var (seed, budget) = ReadEnvConfig();
        Console.Error.WriteLine($"[stress upsert-heavy] seed={seed} budget={budget.TotalSeconds:F1}s");
        var rng = new Random(seed);

        using var tree = OpenTree(new CodecShapedOrdinalSerializer());
        var oracle = new Dictionary<string, byte[]>(StringComparer.Ordinal);

        // Smaller key pool → more repeated upserts on the same key (the trigger pattern).
        var keyPool = BuildKeyPool(16, rng);

        long opCount = 0;
        long lastAssertOpCount = 0;
        const int assertEveryNOps = 10;
        var deadline = DateTime.UtcNow + budget;

        try
        {
            while (DateTime.UtcNow < deadline)
            {
                var key = keyPool[rng.Next(keyPool.Length)];

                // 95% upsert with constantly-changing size — maximum stress on the
                // in-place replace path that the M139 bug lived in.
                if (rng.Next(100) < 95)
                {
                    int size = 200 + rng.Next(401); // 200..600 — keeps multiple keys per leaf
                    var value = new byte[size];
                    rng.NextBytes(value);
                    tree.Put(key, value);
                    oracle[key] = value;
                }
                else
                {
                    tree.Delete(key);
                    oracle.Remove(key);
                }

                opCount++;
                if (opCount - lastAssertOpCount >= assertEveryNOps)
                {
                    AssertOracleMatchesTree(tree, oracle, opCount, seed);
                    lastAssertOpCount = opCount;
                }
            }

            AssertOracleMatchesTree(tree, oracle, opCount, seed);
        }
        catch
        {
            Console.Error.WriteLine($"[stress upsert-heavy] FAIL after {opCount} operations with seed={seed}. Reproduce with BEDROCK_BPT_FUZZ_SEED={seed}.");
            throw;
        }

        Console.Error.WriteLine($"[stress upsert-heavy] PASS — {opCount} operations, {oracle.Count} keys live, seed={seed}.");
    }

    private BPlusTree<string, byte[]> OpenTree(IKeySerializer<string> keySerializer)
        => BPlusTree<string, byte[]>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = PageSize,
                BufferPoolCapacity  = 2048,
                CheckpointThreshold = BPlusTreeDefaults.CheckpointThreshold,
                SyncMode            = WalSyncMode.Synchronous,
            },
            keySerializer, ByteArraySerializer.Instance);

    private static (int seed, TimeSpan budget) ReadEnvConfig()
    {
        int seed = int.TryParse(Environment.GetEnvironmentVariable("BEDROCK_BPT_FUZZ_SEED"), out var s)
            ? s
            : new Random().Next();

        TimeSpan budget = TimeSpan.FromSeconds(10);
        if (int.TryParse(Environment.GetEnvironmentVariable("BEDROCK_BPT_FUZZ_MINUTES"), out var m) && m > 0)
            budget = TimeSpan.FromMinutes(m);

        return (seed, budget);
    }

    private static string[] BuildKeyPool(int count, Random rng)
    {
        // Mix of "op." (Guid-shaped, matches System LSU operation records) and
        // "topology.partition.N.transition" (matches sidecar records).
        // Keys derived from the seeded rng so the workload is fully deterministic.
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

    private static void AssertOracleMatchesTree(
        BPlusTree<string, byte[]> tree,
        Dictionary<string, byte[]> oracle,
        long opCount,
        int seed)
    {
        // Snapshot scan — the path that exposed the M139 bug.
        var scanned = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        using (var snap = tree.BeginSnapshot())
        {
            foreach (var (k, v) in snap.Scan(null, null))
                scanned[k] = v;
        }

        scanned.Count.Should().Be(oracle.Count,
            $"[op#{opCount} seed={seed}] scan returned {scanned.Count} keys, oracle has {oracle.Count}");

        foreach (var (k, expected) in oracle)
        {
            scanned.TryGetValue(k, out var actual).Should().BeTrue(
                $"[op#{opCount} seed={seed}] key '{k}' present in oracle but missing from scan");
            actual.AsSpan().SequenceEqual(expected).Should().BeTrue(
                $"[op#{opCount} seed={seed}] value mismatch for key '{k}'");

            // Cross-check via point read.
            tree.TryGet(k, out var pointValue).Should().BeTrue(
                $"[op#{opCount} seed={seed}] key '{k}' missing from point-read");
            pointValue.AsSpan().SequenceEqual(expected).Should().BeTrue(
                $"[op#{opCount} seed={seed}] point-read value mismatch for key '{k}'");
        }
    }

    /// <summary>
    /// Mirrors <c>CodecKeySerializer</c>'s 4-byte LE length prefix shape — the
    /// format that exposed the M139 leaf-upsert data loss in repeated workloads.
    /// </summary>
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
