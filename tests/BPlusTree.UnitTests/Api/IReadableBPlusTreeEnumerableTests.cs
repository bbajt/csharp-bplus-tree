using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Api;

/// <summary>
/// Conformance tests for IEnumerable on IReadableBPlusTree (Phase 93 — deferral closure).
///
/// Tests verify:
///   1. ISnapshot implements IEnumerable — foreach yields snapshot-frozen state;
///      writes committed after snapshot open are not visible.
///   2. ITransaction implements IEnumerable — foreach yields shadow tree including
///      own uncommitted writes (read-your-own-writes).
/// </summary>
public class IReadableBPlusTreeEnumerableTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions { DataFilePath = _dbPath, WalFilePath = _walPath },
        Int32Serializer.Instance, Int32Serializer.Instance);

    // -------------------------------------------------------------------------
    // Test 1 — Snapshot.IEnumerable yields snapshot-frozen state
    // -------------------------------------------------------------------------
    [Fact]
    public void Snapshot_IEnumerable_YieldsSnapshotState()
    {
        using var tree = Open();
        tree.Put(1, 10);
        tree.Put(2, 20);
        tree.Put(3, 30);

        using var snapshot = tree.BeginSnapshot();

        // Write after snapshot — must NOT appear in foreach
        tree.Put(4, 40);

        var result = new List<(int Key, int Value)>();
        foreach (var pair in snapshot.Scan())   // explicit Scan() — ISnapshot now also implements IEnumerable<KVP> via IReadOnlyDictionary, making bare foreach ambiguous
            result.Add(pair);

        result.Should().HaveCount(3, "snapshot was opened before key 4 was inserted");
        result.Select(p => p.Key).Should().Equal(1, 2, 3);
        result.Select(p => p.Value).Should().Equal(10, 20, 30);
        result.Any(p => p.Key == 4).Should().BeFalse("key 4 committed after snapshot — must not appear");
    }

    // -------------------------------------------------------------------------
    // Test 2 — Transaction.IEnumerable yields own writes (RYOW)
    // -------------------------------------------------------------------------
    [Fact]
    public void Transaction_IEnumerable_YieldsOwnWrites()
    {
        using var tree = Open();
        tree.Put(1, 10);
        tree.Put(2, 20);

        using var tx = tree.BeginTransaction();
        tx.Insert(3, 30);   // uncommitted — must be visible via foreach

        var result = new List<(int Key, int Value)>();
        foreach (var pair in tx)   // IEnumerable from IReadableBPlusTree
            result.Add(pair);

        result.Should().HaveCount(3, "transaction sees own uncommitted insert (RYOW)");
        result.Any(p => p.Key == 3).Should().BeTrue("own insert must be visible via foreach");
        result.Select(p => p.Key).Should().BeInAscendingOrder();

        tx.Commit();
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath);  } catch { }
        try { File.Delete(_walPath); } catch { }
    }
}
