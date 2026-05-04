using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for TryGetNext / TryGetPrev — O(log n) successor/predecessor queries.
/// Uses the public BPlusTree&lt;TKey,TValue&gt; API (Phase 78).
///
/// Properties verified:
///   1. Empty tree returns false for both directions.
///   2. TryGetNext on a key that exists returns the immediate successor.
///   3. TryGetNext on a key not in the tree returns the first key greater.
///   4. TryGetNext on the maximum key returns false.
///   5. TryGetPrev on a key that exists returns the immediate predecessor.
///   6. tx.TryGetNext sees own uncommitted insert (read-your-own-writes); rollback restores.
/// </summary>
public class SuccessorPredecessorTests : IDisposable
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
    public void TryGetNextAndPrev_EmptyTree_ReturnsFalse()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        tree.TryGetNext(5, out _, out _).Should().BeFalse("empty tree has no successor");
        tree.TryGetPrev(5, out _, out _).Should().BeFalse("empty tree has no predecessor");
    }

    // ── Test 2: successor of a key that exists ──────────────────────────────

    [Fact]
    public void TryGetNext_KeyExists_ReturnsImmediateSuccessor()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        // Insert keys 10, 20, 30, 40, 50
        for (int i = 1; i <= 5; i++)
            tree.Put(i * 10, i * 100);

        // Successor of 20 is 30
        tree.TryGetNext(20, out int nextKey, out int nextVal).Should().BeTrue();
        nextKey.Should().Be(30);
        nextVal.Should().Be(300);

        // Successor of 10 is 20
        tree.TryGetNext(10, out nextKey, out nextVal).Should().BeTrue();
        nextKey.Should().Be(20);
        nextVal.Should().Be(200);

        // Predecessor of 30 is 20
        tree.TryGetPrev(30, out int prevKey, out int prevVal).Should().BeTrue();
        prevKey.Should().Be(20);
        prevVal.Should().Be(200);

        // Predecessor of 50 is 40
        tree.TryGetPrev(50, out prevKey, out prevVal).Should().BeTrue();
        prevKey.Should().Be(40);
        prevVal.Should().Be(400);
    }

    // ── Test 3: successor/predecessor when key is absent from the tree ──────

    [Fact]
    public void TryGetNext_KeyAbsent_ReturnsFirstGreater()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        // Insert 10, 20, 30
        tree.Put(10, 1); tree.Put(20, 2); tree.Put(30, 3);

        // Successor of 15 (absent) = 20
        tree.TryGetNext(15, out int nextKey, out _).Should().BeTrue();
        nextKey.Should().Be(20, "first key > 15 is 20");

        // Predecessor of 25 (absent) = 20
        tree.TryGetPrev(25, out int prevKey, out _).Should().BeTrue();
        prevKey.Should().Be(20, "last key < 25 is 20");

        // Successor of 5 (below minimum) = 10
        tree.TryGetNext(5, out nextKey, out _).Should().BeTrue();
        nextKey.Should().Be(10);

        // Predecessor of 35 (above maximum) = 30
        tree.TryGetPrev(35, out prevKey, out _).Should().BeTrue();
        prevKey.Should().Be(30);
    }

    // ── Test 4: boundary conditions ─────────────────────────────────────────

    [Fact]
    public void TryGetNext_AtMaximum_ReturnsFalse_TryGetPrev_AtMinimum_ReturnsFalse()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        // Multi-key: no successor of max, no predecessor of min
        using (var tree = Open())
        {
            tree.Put(1, 10); tree.Put(5, 50); tree.Put(9, 90);
            tree.TryGetNext(9, out _, out _).Should().BeFalse("9 is the maximum — no successor");
            tree.TryGetPrev(1, out _, out _).Should().BeFalse("1 is the minimum — no predecessor");
        }

        // Single-element tree — dispose first tree before re-opening the same files
        File.Delete(_dbPath);
        File.Delete(_walPath);
        using (var tree2 = Open())
        {
            tree2.Put(42, 420);
            tree2.TryGetNext(42, out _, out _).Should().BeFalse("single key has no successor");
            tree2.TryGetPrev(42, out _, out _).Should().BeFalse("single key has no predecessor");
        }
    }

    // ── Test 5: multi-leaf tree — leaf-crossing successor/predecessor ───────

    [Fact]
    public void TryGetNext_MultiLeaf_CrossesLeafBoundary()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        // Insert 1..400 to force multiple leaves (capacity ~337 at pageSize=4096)
        for (int i = 1; i <= 400; i++)
            tree.Put(i, i * 10);

        // Successor of 337 (last slot of first leaf) crosses into the second leaf
        tree.TryGetNext(337, out int nextKey, out int nextVal).Should().BeTrue();
        nextKey.Should().Be(338);
        nextVal.Should().Be(3380);

        // Predecessor of 338 (first slot of second leaf) crosses back to the first leaf
        tree.TryGetPrev(338, out int prevKey, out int prevVal).Should().BeTrue();
        prevKey.Should().Be(337);
        prevVal.Should().Be(3370);

        // Chain: TryGetNext repeatedly from 398 → 399 → 400 → false
        tree.TryGetNext(398, out nextKey, out _).Should().BeTrue();
        nextKey.Should().Be(399);
        tree.TryGetNext(399, out nextKey, out _).Should().BeTrue();
        nextKey.Should().Be(400);
        tree.TryGetNext(400, out _, out _).Should().BeFalse("400 is the maximum");
    }

    // ── Test 6: transaction read-your-own-writes ────────────────────────────

    [Fact]
    public void TryGetNext_InTransaction_ReadYourOwnWrites()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        // Seed 10, 20, 30
        tree.Put(10, 100); tree.Put(20, 200); tree.Put(30, 300);

        using var tx = tree.BeginTransaction();

        // Insert 25 — successor of 20 changes from 30 to 25
        tx.Insert(25, 250);

        tx.TryGetNext(20, out int txNext, out int txNextVal).Should().BeTrue();
        txNext.Should().Be(25, "own insert 25 is now the successor of 20");
        txNextVal.Should().Be(250);

        // Predecessor of 30 changes from 20 to 25
        tx.TryGetPrev(30, out int txPrev, out int txPrevVal).Should().BeTrue();
        txPrev.Should().Be(25, "own insert 25 is now the predecessor of 30");
        txPrevVal.Should().Be(250);

        // Rollback — live tree must not contain 25
        tx.Dispose();

        tree.TryGetNext(20, out int liveNext, out _).Should().BeTrue();
        liveNext.Should().Be(30, "after rollback, successor of 20 is 30 again");
    }
}
