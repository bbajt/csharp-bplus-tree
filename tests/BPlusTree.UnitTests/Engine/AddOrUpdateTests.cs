using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for AddOrUpdate — atomic read-modify-write on BPlusTree, ITransaction,
/// and BPlusTreeScope (Phase 84).
///
/// Properties verified:
///   1. Key absent → inserts addValue; returns addValue; TryGet confirms.
///   2. Key present → factory called with existing value; updated value stored; returns result.
///   3. Factory argument is the true existing value, not addValue.
///   4. Transaction RYOW: own-insert visible to addOrUpdate in same tx; commit persists result.
///   5. Scope: absent Complete → rollback → tree unchanged.
/// </summary>
public class AddOrUpdateTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath       = _dbPath,
            WalFilePath        = _walPath,
            PageSize           = 4096,
            BufferPoolCapacity = 64,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { File.Delete(_dbPath); }  catch (IOException) { }
        try { File.Delete(_walPath); } catch (IOException) { }
    }

    // ── Test 1: key absent — inserts addValue ─────────────────────────────────

    [Fact]
    public void AddOrUpdate_KeyAbsent_InsertsAddValue()
    {
        using var tree = Open();

        int result = tree.AddOrUpdate(42, addValue: 100, (_, _) => -1);

        result.Should().Be(100);
        tree.TryGet(42, out int v).Should().BeTrue();
        v.Should().Be(100);
        tree.Count.Should().Be(1);
    }

    // ── Test 2: key present — factory called, updated value stored ───────────

    [Fact]
    public void AddOrUpdate_KeyPresent_AppliesUpdateFactory()
    {
        using var tree = Open();
        tree.Put(10, 50);

        int result = tree.AddOrUpdate(10, addValue: 0, (_, existing) => existing + 1);

        result.Should().Be(51);
        tree.TryGet(10, out int v).Should().BeTrue();
        v.Should().Be(51);
        tree.Count.Should().Be(1, "AddOrUpdate must not create a duplicate");
    }

    // ── Test 3: factory receives the true existing value ─────────────────────

    [Fact]
    public void AddOrUpdate_UpdateFactory_ReceivesExistingValue()
    {
        using var tree = Open();
        tree.Put(7, 999);

        int capturedExisting = -1;
        tree.AddOrUpdate(7, addValue: 0, (_, existing) =>
        {
            capturedExisting = existing;
            return existing * 2;
        });

        capturedExisting.Should().Be(999, "factory must receive the stored value, not addValue");
        tree.TryGet(7, out int v).Should().BeTrue();
        v.Should().Be(1998);
    }

    // ── Test 4: transaction RYOW + commit ─────────────────────────────────────
    // Insert a key inside a transaction, then call AddOrUpdate on the same key
    // in the same transaction (RYOW). Factory must see the shadow-tree value.
    // After commit, the final value must be persisted.

    [Fact]
    public void AddOrUpdate_Transaction_RYOW_And_Commit()
    {
        using var tree = Open();

        using (var tx = tree.BeginTransaction())
        {
            tx.Insert(5, 10);                                // shadow: key 5 → 10

            // key 5 is visible in shadow tree (RYOW) → factory path, not addValue path
            int result = tx.AddOrUpdate(5, addValue: 0, (_, existing) => existing + 5);
            result.Should().Be(15);

            // Confirm shadow-tree state before commit
            tx.TryGet(5, out int shadow).Should().BeTrue();
            shadow.Should().Be(15);

            tx.Commit();
        }

        tree.TryGet(5, out int final).Should().BeTrue();
        final.Should().Be(15);
    }

    // ── Test 5: scope rollback — tree unchanged ───────────────────────────────

    [Fact]
    public void AddOrUpdate_Scope_Rollback_LeavesTreeUnchanged()
    {
        using var tree = Open();
        tree.Put(3, 300);

        using (var scope = tree.BeginScope())
        {
            scope.AddOrUpdate(3, addValue: 0, (_, existing) => existing + 1);
            scope.AddOrUpdate(99, addValue: 77, (_, _) => -1);
            // No scope.Complete() → rollback on Dispose
        }

        // key 3 must still have its original value
        tree.TryGet(3, out int v3).Should().BeTrue();
        v3.Should().Be(300);

        // key 99 must not exist
        tree.TryGet(99, out _).Should().BeFalse();
        tree.Count.Should().Be(1);
    }
}
