using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for CountRange — exposed on BPlusTree, ISnapshot, ITransaction,
/// and BPlusTreeScope (Phase 80).
///
/// Properties verified:
///   1. Empty tree → CountRange == 0.
///   2. Full range [min, max] equals Count.
///   3. Partial range returns the exact key count.
///   4. No-match range (outside keys, inverted bounds) → 0.
///   5. Transaction CountRange reflects own uncommitted writes (read-your-own-writes);
///      rollback restores the live range count.
/// </summary>
public class CountRangeTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 4096,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 4096,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
    }

    // ── Test 1: empty tree ──────────────────────────────────────────────────

    [Fact]
    public void CountRange_EmptyTree_ReturnsZero()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        tree.CountRange(1, 100).Should().Be(0);
    }

    // ── Test 2: full range equals Count ─────────────────────────────────────

    [Fact]
    public void CountRange_FullRange_EqualsCount()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 10; i++)
            tree.Put(i, i * 10);

        tree.CountRange(1, 10).Should().Be(10, "full range [1,10] covers all 10 keys");
        tree.CountRange(1, 10).Should().Be(tree.Count);
    }

    // ── Test 3: partial range returns exact count ────────────────────────────

    [Fact]
    public void CountRange_PartialRange_ReturnsExactCount()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 10; i++)
            tree.Put(i, i);

        // [3, 7] = keys 3,4,5,6,7 → 5
        tree.CountRange(3, 7).Should().Be(5);

        // Snapshot sees same state
        using var snap = tree.BeginSnapshot();
        snap.CountRange(3, 7).Should().Be(5, "snapshot sees same partial range");

        // Single-key range
        tree.CountRange(5, 5).Should().Be(1, "single-key range [5,5]");

        // Absent key boundaries — keys 3–7 still in range [2, 8]
        tree.CountRange(2, 8).Should().Be(7, "[2,8] on keys 1–10: keys 2,3,4,5,6,7,8");
    }

    // ── Test 4: no-match range returns zero ──────────────────────────────────

    [Fact]
    public void CountRange_NoMatchingKeys_ReturnsZero()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 10; i++)
            tree.Put(i, i);

        // Entirely outside existing keys
        tree.CountRange(50, 60).Should().Be(0, "range [50,60] has no keys");

        // Inverted bounds (startKey > endKey) → 0
        tree.CountRange(7, 3).Should().Be(0, "inverted range [7,3] returns 0");

        // Range between two keys that doesn't include any
        tree.Put(100, 100); tree.Put(200, 200);
        tree.CountRange(101, 199).Should().Be(0, "gap between 100 and 200");
    }

    // ── Test 5: transaction CountRange reflects own uncommitted writes ───────

    [Fact]
    public void CountRange_Transaction_ReadsOwnWrites()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 10; i++)
            tree.Put(i, i);

        tree.CountRange(1, 10).Should().Be(10);

        using var tx = tree.BeginTransaction();
        tx.CountRange(1, 10).Should().Be(10, "tx starts with snapshot state");

        // Own inserts in range are visible
        tx.Insert(5, 999); // upsert — count unchanged
        tx.Insert(11, 11); // new key outside range
        tx.Insert(15, 15); // new key outside range
        tx.CountRange(1, 10).Should().Be(10, "upsert + out-of-range inserts: count unchanged");

        tx.Insert(7, 777); // upsert — count unchanged
        tx.Insert(8, 888); // upsert — count unchanged
        tx.CountRange(1, 10).Should().Be(10, "upserts do not change range count");

        // Own deletes in range are visible
        tx.TryDelete(1);
        tx.TryDelete(2);
        tx.CountRange(1, 10).Should().Be(8, "two deletes in range: 10 − 2 = 8");

        // Rollback — live range count restored
        tx.Dispose();

        tree.CountRange(1, 10).Should().Be(10, "rollback restores range count to 10");
    }
}
