using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for TryInsert — insert-if-absent semantic on BPlusTree, ITransaction,
/// and BPlusTreeScope (Phase 81).
///
/// Properties verified:
///   1. Absent key → true, value readable.
///   2. Existing key → false, original value unchanged, Count unchanged.
///   3. Transaction RYOW: own-insert blocks reinsert; own-delete re-enables insert; rollback.
///   4. Scope: commit persists insert; subsequent TryInsert on committed key → false.
/// </summary>
public class TryInsertTests : IDisposable
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

    // ── Test 1: absent key is inserted, returns true ─────────────────────────

    [Fact]
    public void TryInsert_AbsentKey_InsertsAndReturnsTrue()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        tree.TryInsert(1, 100).Should().BeTrue("key 1 is absent");
        tree.TryGet(1, out var v).Should().BeTrue();
        v.Should().Be(100);
        tree.Count.Should().Be(1);

        // Put can still overwrite after TryInsert
        tree.Put(1, 999);
        tree.TryGet(1, out v).Should().BeTrue();
        v.Should().Be(999);

        // TryInsert a second absent key
        tree.TryInsert(2, 200).Should().BeTrue("key 2 is absent");
        tree.Count.Should().Be(2);
    }

    // ── Test 2: existing key is not overwritten, returns false ───────────────

    [Fact]
    public void TryInsert_ExistingKey_ReturnsFalseNoOverwrite()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        tree.Put(5, 50);
        tree.Count.Should().Be(1);

        // TryInsert on existing key must not overwrite and must return false
        tree.TryInsert(5, 999).Should().BeFalse("key 5 already exists");
        tree.TryGet(5, out var v).Should().BeTrue();
        v.Should().Be(50, "original value must be preserved");
        tree.Count.Should().Be(1, "count must not change");

        // Multiple attempts all fail
        tree.TryInsert(5, 1).Should().BeFalse();
        tree.TryInsert(5, 2).Should().BeFalse();
        v = default;
        tree.TryGet(5, out v);
        v.Should().Be(50, "value still unchanged after multiple TryInsert attempts");
    }

    // ── Test 3: transaction read-your-own-writes ─────────────────────────────

    [Fact]
    public void TryInsert_Transaction_ReadsOwnWrites()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 5; i++)
            tree.Put(i, i * 10);

        using var tx = tree.BeginTransaction();

        // Existing key from snapshot — TryInsert must return false
        tx.TryInsert(3, 999).Should().BeFalse("key 3 exists in snapshot");
        tx.TryGet(3, out var v);
        v.Should().Be(30, "value 30 must be unchanged");

        // Insert a new absent key — TryInsert must return true
        tx.TryInsert(6, 60).Should().BeTrue("key 6 is absent");
        tx.TryGet(6, out v).Should().BeTrue();
        v.Should().Be(60);

        // Retry TryInsert on own-inserted key — must return false (RYOW)
        tx.TryInsert(6, 999).Should().BeFalse("own-inserted key 6 must block reinsert");
        tx.TryGet(6, out v);
        v.Should().Be(60, "own-inserted value unchanged");

        // Delete a key then TryInsert it — must return true (own-delete RYOW)
        tx.TryDelete(1).Should().BeTrue();
        tx.TryInsert(1, 111).Should().BeTrue("own-deleted key 1 can be reinserted");
        tx.TryGet(1, out v).Should().BeTrue();
        v.Should().Be(111);

        // Rollback — live tree restored, none of the tx mutations visible
        tx.Dispose();

        tree.TryGet(1, out v).Should().BeTrue();
        v.Should().Be(10, "rollback restores key 1 to original value 10");
        tree.TryGet(6, out _).Should().BeFalse("rollback removes own-inserted key 6");
        tree.Count.Should().Be(5, "rollback restores count to 5");
    }

    // ── Test 4: scope — commit persists; subsequent TryInsert blocked ────────

    [Fact]
    public void TryInsert_Scope_CommitAndPostCommit()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        tree.Put(1, 10);
        tree.Put(2, 20);

        using (var scope = tree.BeginScope())
        {
            scope.TryInsert(3, 30).Should().BeTrue("key 3 absent in scope");
            scope.TryInsert(1, 99).Should().BeFalse("key 1 exists in scope");
            scope.TryGet(1, out var v);
            v.Should().Be(10, "key 1 value unchanged in scope");

            scope.Complete();
        }

        // After committed scope: key 3 is visible
        tree.TryGet(3, out var v3).Should().BeTrue();
        v3.Should().Be(30);
        tree.Count.Should().Be(3);

        // TryInsert on a committed key → false
        tree.TryInsert(3, 999).Should().BeFalse("committed key 3 blocks TryInsert");
        tree.TryGet(3, out v3);
        v3.Should().Be(30, "committed value must be preserved");
    }
}
