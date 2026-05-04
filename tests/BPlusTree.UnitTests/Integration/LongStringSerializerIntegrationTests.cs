using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Integration;

/// <summary>
/// Integration tests for BPlusTree&lt;long, long&gt; and BPlusTree&lt;string, string&gt; (Phase 82).
///
/// BPlusTree&lt;long, long&gt; uses Int64Serializer (FixedSize=8) — already supported.
/// BPlusTree&lt;string, string&gt; uses StringSerializer (FixedSize=-1) — requires variable-length
/// key support added in Phase 82.
/// </summary>
public class LongStringSerializerIntegrationTests : IDisposable
{
    private readonly string _dbPath  = Path.Combine(Path.GetTempPath(), $"p82_{Guid.NewGuid():N}.db");
    private readonly string _walPath = Path.Combine(Path.GetTempPath(), $"p82_{Guid.NewGuid():N}.wal");

    private BPlusTree<long, long> OpenLong() => BPlusTree<long, long>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 4096,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 4096,
        },
        Int64Serializer.Instance, Int64Serializer.Instance);

    private BPlusTree<string, string> OpenString() => BPlusTree<string, string>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 4096,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 4096,
        },
        StringSerializer.Instance, StringSerializer.Instance);

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
    }

    // ── Test 1: BPlusTree<long, long> basic ops ──────────────────────────────

    [Fact]
    public void LongLong_BasicOps()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = OpenLong();

        tree.Put(1L, 100L);
        tree.Put(2L, 200L);
        tree.Put(long.MinValue, -1L);
        tree.Put(long.MaxValue, long.MaxValue);

        tree.TryGet(1L,            out var v).Should().BeTrue(); v.Should().Be(100L);
        tree.TryGet(2L,            out v).Should().BeTrue();     v.Should().Be(200L);
        tree.TryGet(long.MinValue, out v).Should().BeTrue();     v.Should().Be(-1L);
        tree.TryGet(long.MaxValue, out v).Should().BeTrue();     v.Should().Be(long.MaxValue);
        tree.TryGet(999L,          out _).Should().BeFalse();

        tree.Count.Should().Be(4);
        tree.Delete(2L).Should().BeTrue();
        tree.Count.Should().Be(3);
        tree.TryGet(2L, out _).Should().BeFalse();
    }

    // ── Test 2: BPlusTree<string, string> basic ops ──────────────────────────

    [Fact]
    public void StringString_BasicOps()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = OpenString();

        tree.Put("apple",  "fruit");
        tree.Put("banana", "yellow");
        tree.Put("cherry", "red");
        tree.Put("",       "empty key");

        tree.TryGet("apple",  out var v).Should().BeTrue(); v.Should().Be("fruit");
        tree.TryGet("banana", out v).Should().BeTrue();     v.Should().Be("yellow");
        tree.TryGet("cherry", out v).Should().BeTrue();     v.Should().Be("red");
        tree.TryGet("",       out v).Should().BeTrue();     v.Should().Be("empty key");
        tree.TryGet("durian", out _).Should().BeFalse();

        tree.Count.Should().Be(4);

        // Overwrite
        tree.Put("apple", "updated");
        tree.TryGet("apple", out v).Should().BeTrue();
        v.Should().Be("updated");

        // Delete
        tree.Delete("banana").Should().BeTrue();
        tree.TryGet("banana", out _).Should().BeFalse();
        tree.Count.Should().Be(3);
    }

    // ── Test 3: BPlusTree<string, string> — split + scan ─────────────────────
    // Insert enough entries to force tree height > 1 (leaf splits into internal nodes).
    // With pageSize=4096 and ~10-20 bytes per key+value, ~150+ entries cause splits.

    [Fact]
    public void StringString_SplitAndScan()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = OpenString();

        // Insert 200 entries with keys like "key-000" … "key-199"
        var expected = new List<string>();
        for (int i = 0; i < 200; i++)
        {
            string k = $"key-{i:D3}";
            tree.Put(k, $"val-{i}");
            expected.Add(k);
        }
        expected.Sort(StringComparer.Ordinal);

        tree.Count.Should().Be(200);

        // Scan all entries — must be in ascending key order
        var scanned = tree.Scan().Select(p => p.Key).ToList();
        scanned.Should().HaveCount(200);
        scanned.Should().Equal(expected);

        // Range scan
        var rangeKeys = tree.Scan("key-050", "key-059").Select(p => p.Key).ToList();
        rangeKeys.Should().HaveCount(10);
        rangeKeys.First().Should().Be("key-050");
        rangeKeys.Last().Should().Be("key-059");
    }

    // ── Test 4: BPlusTree<string, string> — reverse scan ─────────────────────

    [Fact]
    public void StringString_ReverseScan()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = OpenString();

        var keys = new[] { "alpha", "beta", "gamma", "delta", "epsilon" };
        foreach (var k in keys)
            tree.Put(k, k.ToUpper());

        var forward  = tree.Scan().Select(p => p.Key).ToList();
        var backward = tree.ScanReverse().Select(p => p.Key).ToList();

        backward.Should().HaveCount(forward.Count);
        backward.Should().Equal(forward.AsEnumerable().Reverse());
    }

    // ── Test 5: BPlusTree<string, string> — transaction ─────────────────────

    [Fact]
    public void StringString_Transaction()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = OpenString();

        tree.Put("a", "1");
        tree.Put("b", "2");

        // Committed transaction: insert + update
        using (var scope = tree.BeginScope())
        {
            scope.TryInsert("c", "3").Should().BeTrue();
            scope.TryUpdate("a", "updated").Should().BeTrue();
            scope.TryDelete("b").Should().BeTrue();
            scope.Complete();
        }

        tree.TryGet("a", out var v).Should().BeTrue(); v.Should().Be("updated");
        tree.TryGet("b", out _).Should().BeFalse();
        tree.TryGet("c", out v).Should().BeTrue();     v.Should().Be("3");
        tree.Count.Should().Be(2);

        // Rolled-back transaction: none of its changes are visible
        using (var scope = tree.BeginScope())
        {
            scope.Insert("d", "4");
            scope.TryDelete("a").Should().BeTrue();
            // No Complete() → rollback on Dispose
        }

        tree.TryGet("a", out v).Should().BeTrue(); v.Should().Be("updated");
        tree.TryGet("d", out _).Should().BeFalse();
        tree.Count.Should().Be(2);
    }
}
