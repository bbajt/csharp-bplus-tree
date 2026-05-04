using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for ScanReverse — descending-order enumeration via the PrevLeafPageId chain.
/// Uses the public BPlusTree&lt;TKey,TValue&gt; API (Phase 76).
///
/// Properties verified:
///   1. Empty tree yields nothing.
///   2. Full reverse scan returns all keys in descending order.
///   3. Bounded reverse scan returns the correct slice in descending order.
///   4. Reverse scan is epoch-protected: concurrent CoW writes during iteration
///      do not corrupt the scan (frozen view).
///   5. tx.ScanReverse reads own uncommitted writes (shadow tree traversal).
/// </summary>
public class ReverseScanTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath       = _dbPath,
            WalFilePath        = _walPath,
            PageSize           = 4096,
            BufferPoolCapacity = 128,
            CheckpointThreshold = 4096,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
    }

    // ── Test 1: empty tree ─────────────────────────────────────────────────────

    [Fact]
    public void ScanReverse_EmptyTree_ReturnsEmpty()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        tree.ScanReverse().Should().BeEmpty();
    }

    // ── Test 2: full reverse scan matches forward scan reversed ────────────────

    [Fact]
    public void ScanReverse_AllKeys_DescendingOrder()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        // Insert 1–20, spanning multiple leaves at pageSize=4096
        // (leaf capacity ≈ 337; all 20 fit in one leaf, verifying single-leaf reverse)
        for (int i = 1; i <= 20; i++)
            tree.Put(i, i * 10);

        var forward = tree.Scan().ToList();
        var reverse = tree.ScanReverse().ToList();

        // Reverse must contain the same elements as forward, in reverse order
        reverse.Should().HaveCount(forward.Count);
        reverse.Should().Equal(forward.AsEnumerable().Reverse());

        // First element is the largest key
        reverse[0].Key.Should().Be(20);
        reverse[0].Value.Should().Be(200);
        // Last element is the smallest key
        reverse[^1].Key.Should().Be(1);
        reverse[^1].Value.Should().Be(10);
    }

    // ── Test 3: bounded range [startKey, endKey] in reverse ───────────────────

    [Fact]
    public void ScanReverse_WithRange_ReturnsSliceDescending()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 20; i++)
            tree.Put(i, i * 100);

        // Reverse scan of [5, 15] — inclusive on both ends
        var result = tree.ScanReverse(endKey: 15, startKey: 5).ToList();

        result.Should().HaveCount(11, "keys 5–15 inclusive = 11 entries");
        result[0].Key.Should().Be(15,  "first in reverse = largest in range");
        result[^1].Key.Should().Be(5,  "last in reverse  = smallest in range");

        // Keys must be strictly descending
        for (int i = 1; i < result.Count; i++)
            result[i].Key.Should().BeLessThan(result[i - 1].Key);

        // Keys outside the range must be absent
        result.Should().NotContain(r => r.Key < 5 || r.Key > 15);

        // Values must match
        foreach (var (key, value) in result)
            value.Should().Be(key * 100);
    }

    // ── Test 4: epoch-protected frozen view ───────────────────────────────────

    [Fact]
    public void ScanReverse_EpochProtected_FrozenView()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        // Seed 1–100 (one leaf at pageSize=4096)
        for (int i = 1; i <= 100; i++)
            tree.Put(i, i);

        // Open the reverse iterator and advance partway (holds an epoch)
        using var iter = tree.ScanReverse().GetEnumerator();
        iter.MoveNext().Should().BeTrue();
        iter.Current.Key.Should().Be(100, "first reverse entry = largest key");

        // Advance a few more times to confirm we are mid-scan
        for (int i = 0; i < 10; i++) iter.MoveNext();

        // Mutate while the scan is in-flight (CoW path because HasActiveSnapshots=true)
        tree.Put(9999, 9999);   // new key above range
        tree.Delete(1);         // remove key from range

        // Drain the rest of the iterator
        var remaining = new List<(int Key, int Value)>();
        remaining.Add(iter.Current);
        while (iter.MoveNext())
            remaining.Add(iter.Current);

        // Snapshot was frozen: 9999 must not appear; 1 must be present
        remaining.Should().NotContain(r => r.Key == 9999,
            "key inserted after scan open must not appear in frozen view");
        remaining.Should().Contain(r => r.Key == 1,
            "key deleted after scan open must still appear in frozen view");
    }

    // ── Test 5: transaction read-your-own-writes in reverse ───────────────────

    [Fact]
    public void ScanReverse_InTransaction_ReadYourOwnWrites()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        // Seed 1–10
        for (int i = 1; i <= 10; i++)
            tree.Put(i, i * 10);

        using var tx = tree.BeginTransaction();

        // Insert a key above the current range and delete key 5
        tx.Insert(50, 500);
        tx.TryDelete(5);

        var result = tx.ScanReverse().ToList();

        // 50 must be present (own insert visible)
        result.Should().Contain(r => r.Key == 50 && r.Value == 500,
            "own inserted key 50 must be visible via ScanReverse");

        // 5 must be absent (own delete visible)
        result.Should().NotContain(r => r.Key == 5,
            "own deleted key 5 must not appear in ScanReverse");

        // First key should be 50 (largest), order should be strictly descending
        result[0].Key.Should().Be(50);
        for (int i = 1; i < result.Count; i++)
            result[i].Key.Should().BeLessThan(result[i - 1].Key);

        // Rollback — live tree must be unchanged
        tx.Dispose();

        tree.TryGet(5,  out int v5).Should().BeTrue("key 5 must be restored after rollback");
        v5.Should().Be(50);
        tree.TryGet(50, out _).Should().BeFalse("key 50 must be absent after rollback");
    }
}
