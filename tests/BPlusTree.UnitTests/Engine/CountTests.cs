using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for the Count property — exposed on BPlusTree, ISnapshot, ITransaction,
/// and BPlusTreeScope (Phase 79).
///
/// Properties verified:
///   1. Empty tree → Count == 0.
///   2. Count tracks inserts and deletes on the live tree.
///   3. Snapshot Count is frozen at open time; concurrent writes not reflected.
///   4. Transaction Count reflects own uncommitted writes (read-your-own-writes);
///      rollback restores the live count.
///   5. BPlusTreeScope Count delegates to the underlying transaction.
/// </summary>
public class CountTests : IDisposable
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
    public void Count_EmptyTree_ReturnsZero()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        tree.Count.Should().Be(0);
    }

    // ── Test 2: live Count tracks inserts and deletes ───────────────────────

    [Fact]
    public void Count_AfterInserts_MatchesActual()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 10; i++)
            tree.Put(i, i * 10);

        tree.Count.Should().Be(10);

        // Overwrite (upsert) does not change count
        tree.Put(5, 999);
        tree.Count.Should().Be(10, "upsert does not add a record");

        // Delete reduces count
        tree.Delete(1);
        tree.Delete(2);
        tree.Count.Should().Be(8);

        // Delete non-existent key does not change count
        tree.Delete(999);
        tree.Count.Should().Be(8);
    }

    // ── Test 3: snapshot Count is frozen at open time ───────────────────────

    [Fact]
    public void Count_Snapshot_FrozenAtOpenTime()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 20; i++)
            tree.Put(i, i);

        using var snap = tree.BeginSnapshot();
        snap.Count.Should().Be(20, "snapshot captures 20 records at open time");

        // Concurrent inserts and deletes after snapshot is open
        tree.Put(100, 100);
        tree.Put(101, 101);
        tree.Delete(1);

        // Snapshot count remains frozen
        snap.Count.Should().Be(20, "snapshot count must not reflect post-open mutations");

        // Live tree reflects the mutations
        tree.Count.Should().Be(21, "live tree: 20 − 1 + 2 = 21");
    }

    // ── Test 4: transaction Count reflects own uncommitted writes ───────────

    [Fact]
    public void Count_Transaction_ReadsOwnWrites()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 5; i++)
            tree.Put(i, i);

        tree.Count.Should().Be(5);

        using var tx = tree.BeginTransaction();
        tx.Count.Should().Be(5, "tx starts with snapshot count = 5");

        tx.Insert(10, 100);
        tx.Insert(11, 110);
        tx.Count.Should().Be(7, "own inserts visible: 5 + 2 = 7");

        tx.TryDelete(1);
        tx.Count.Should().Be(6, "own delete visible: 7 − 1 = 6");

        // Rollback — live tree and count must be restored
        tx.Dispose();

        tree.Count.Should().Be(5, "rollback restores count to 5");
    }

    // ── Test 5: BPlusTreeScope delegates to transaction count ───────────────

    [Fact]
    public void Count_Scope_ReadsOwnWrites()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        tree.Put(1, 10); tree.Put(2, 20); tree.Put(3, 30);
        tree.Count.Should().Be(3);

        using (var scope = tree.BeginScope())
        {
            scope.Count.Should().Be(3, "scope starts with 3 records");

            scope.Insert(4, 40);
            scope.Count.Should().Be(4, "own insert visible in scope");

            scope.TryDelete(1);
            scope.Count.Should().Be(3, "own delete visible in scope");

            scope.Complete();
        }

        // After committed scope, live count reflects committed changes
        tree.Count.Should().Be(3, "committed: 3 − 1 + 1 = 3");
    }
}
