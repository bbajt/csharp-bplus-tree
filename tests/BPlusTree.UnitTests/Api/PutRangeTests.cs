using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Api;

/// <summary>
/// Tests for PutRange (bulk atomic insert) and InsertRange (transactional bulk insert),
/// and compile-time interface conformance checks for IReadableBPlusTree. Phase 91.
/// </summary>
public class PutRangeTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> CreateTree()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        return BPlusTree<int, int>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 256,
                CheckpointThreshold = 4096,
            },
            Int32Serializer.Instance, Int32Serializer.Instance);
    }

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
    }

    // ── Test 1: PutRange inserts all items ──

    [Fact]
    public void PutRange_InsertsAllItems()
    {
        using var tree = CreateTree();

        tree.PutRange(new[] { (1, 10), (2, 20), (3, 30) });

        tree.TryGet(1, out var v1).Should().BeTrue(); v1.Should().Be(10);
        tree.TryGet(2, out var v2).Should().BeTrue(); v2.Should().Be(20);
        tree.TryGet(3, out var v3).Should().BeTrue(); v3.Should().Be(30);
        tree.Count.Should().Be(3);
    }

    // ── Test 2: PutRange on empty collection is a no-op ──

    [Fact]
    public void PutRange_EmptyCollection_NoOp()
    {
        using var tree = CreateTree();

        var act = () => tree.PutRange(Enumerable.Empty<(int, int)>());

        act.Should().NotThrow();
        tree.Count.Should().Be(0);
    }

    // ── Test 3: PutRange is atomic — exception during enumeration rolls back all prior inserts ──

    [Fact]
    public void PutRange_Atomic_RollbackOnEnumerationException()
    {
        using var tree = CreateTree();
        tree.Put(5, 50);

        static IEnumerable<(int Key, int Value)> FaultingSource()
        {
            yield return (1, 10);
            yield return (2, 20);
            throw new InvalidOperationException("source fault");
        }

        var act = () => tree.PutRange(FaultingSource());

        act.Should().Throw<InvalidOperationException>();
        tree.TryGet(1, out _).Should().BeFalse("key 1 must have been rolled back");
        tree.TryGet(2, out _).Should().BeFalse("key 2 must have been rolled back");
        tree.TryGet(5, out var v5).Should().BeTrue(); v5.Should().Be(50, "original key unchanged");
    }

    // ── Test 4: KVP overload works ──

    [Fact]
    public void PutRange_KVP_Overload_Works()
    {
        using var tree = CreateTree();
        var dict = new Dictionary<int, int> { [1] = 10, [2] = 20 };

        tree.PutRange((IEnumerable<KeyValuePair<int, int>>)dict);

        tree.TryGet(1, out var v1).Should().BeTrue(); v1.Should().Be(10);
        tree.TryGet(2, out var v2).Should().BeTrue(); v2.Should().Be(20);
    }

    // ── Test 5: InsertRange within transaction — read-your-own-writes ──

    [Fact]
    public void InsertRange_WithinTransaction_ReadYourOwnWrites()
    {
        using var tree = CreateTree();
        tree.Put(1, 10);

        using var tx = tree.BeginTransaction();
        tx.InsertRange(new[] { (100, 1000), (200, 2000) });

        tx.TryGet(100, out var v100).Should().BeTrue(); v100.Should().Be(1000);
        tx.TryGet(200, out var v200).Should().BeTrue(); v200.Should().Be(2000);
        tx.TryGet(1, out var v1).Should().BeTrue();    v1.Should().Be(10);

        tx.Commit();

        tree.TryGet(100, out var after).Should().BeTrue(); after.Should().Be(1000);
    }

    // ── Test 6: PutRange with large batch spans multiple leaf pages ──

    [Fact]
    public void PutRange_LargeBatch_SingleTransaction()
    {
        using var tree = CreateTree();

        tree.PutRange(Enumerable.Range(1, 1000).Select(i => (i, i * 10)));

        tree.Count.Should().Be(1000);
        for (int i = 1; i <= 1000; i++)
        {
            tree.TryGet(i, out var v).Should().BeTrue();
            v.Should().Be(i * 10);
        }
        // Structural validity: Scan returns all 1000 keys in order
        tree.Scan().Select(p => p.Key).Should().BeInAscendingOrder();
    }

    // ── Test 7: BPlusTree implements IReadableBPlusTree ──

    [Fact]
    public void IReadableBPlusTree_BPlusTree_Assignable()
    {
        using var tree = CreateTree();
        tree.Put(1, 10);

        IReadableBPlusTree<int, int> readable = tree;

        readable.TryGet(1, out var v).Should().BeTrue(); v.Should().Be(10);
        readable.Scan().ToList().Should().ContainSingle();
    }

    // ── Test 8: ISnapshot implements IReadableBPlusTree ──

    [Fact]
    public void IReadableBPlusTree_Snapshot_Assignable()
    {
        using var tree = CreateTree();
        tree.Put(1, 10);

        using var snapshot = tree.BeginSnapshot();
        IReadableBPlusTree<int, int> readable = snapshot;

        readable.TryGet(1, out var v).Should().BeTrue(); v.Should().Be(10);
    }

    // ── Test 9: ITransaction implements IReadableBPlusTree ──

    [Fact]
    public void IReadableBPlusTree_Transaction_Assignable()
    {
        using var tree = CreateTree();
        tree.Put(1, 10);

        using var tx = tree.BeginTransaction();
        IReadableBPlusTree<int, int> readable = tx;

        readable.TryGet(1, out var v).Should().BeTrue(); v.Should().Be(10);
    }
}
