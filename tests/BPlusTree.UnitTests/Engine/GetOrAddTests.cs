using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for GetOrAdd — fetch-or-insert on BPlusTree, ITransaction,
/// and BPlusTreeScope (Phase 85).
///
/// Properties verified:
///   1. Key absent → inserted; returns addValue; TryGet confirms.
///   2. Key present → returns existing value; tree unchanged; Count unchanged.
///   3. Key present → returned value is stored value, not addValue.
///   4. Transaction RYOW: own-insert visible to GetOrAdd in same tx; commit persists.
///   5. Scope: absent Complete → rollback → key absent.
/// </summary>
public class GetOrAddTests : IDisposable
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

    // ── Test 1: key absent — inserts addValue and returns it ─────────────────

    [Fact]
    public void GetOrAdd_KeyAbsent_InsertsAndReturnsAddValue()
    {
        using var tree = Open();

        int result = tree.GetOrAdd(42, addValue: 100);

        result.Should().Be(100);
        tree.TryGet(42, out int v).Should().BeTrue();
        v.Should().Be(100);
        tree.Count.Should().Be(1);
    }

    // ── Test 2: key present — returns existing value, tree unchanged ──────────

    [Fact]
    public void GetOrAdd_KeyPresent_ReturnsExistingValue_NoWrite()
    {
        using var tree = Open();
        tree.Put(10, 50);
        long countBefore = tree.Count;

        int result = tree.GetOrAdd(10, addValue: 999);

        result.Should().Be(50);
        tree.Count.Should().Be(countBefore, "GetOrAdd must not insert a duplicate");
    }

    // ── Test 3: key present — returned value is stored value, not addValue ───

    [Fact]
    public void GetOrAdd_KeyPresent_DoesNotOverwrite()
    {
        using var tree = Open();
        tree.Put(7, 777);

        int result = tree.GetOrAdd(7, addValue: 0);

        result.Should().Be(777, "must return the stored value, not addValue");
        tree.TryGet(7, out int v).Should().BeTrue();
        v.Should().Be(777, "stored value must be unchanged");
    }

    // ── Test 4: transaction RYOW + commit ─────────────────────────────────────
    // Insert a key inside a transaction; GetOrAdd on the same key in the same
    // transaction must see the shadow-tree value (RYOW) and return it without
    // modifying the shadow tree further.

    [Fact]
    public void GetOrAdd_Transaction_RYOW_And_Commit()
    {
        using var tree = Open();

        using (var tx = tree.BeginTransaction())
        {
            tx.Insert(5, 55);                   // shadow: key 5 → 55

            // key 5 visible in shadow tree (RYOW) → present path → returns 55
            int result = tx.GetOrAdd(5, addValue: 0);
            result.Should().Be(55);

            // Shadow tree still has exactly one key
            tx.Count.Should().Be(1);

            tx.Commit();
        }

        tree.TryGet(5, out int final).Should().BeTrue();
        final.Should().Be(55);
        tree.Count.Should().Be(1);
    }

    // ── Test 5: scope rollback — tree unchanged ───────────────────────────────

    [Fact]
    public void GetOrAdd_Scope_Rollback_LeavesTreeUnchanged()
    {
        using var tree = Open();
        tree.Put(3, 300);

        using (var scope = tree.BeginScope())
        {
            // key 3 present → returns 300, no write
            int r1 = scope.GetOrAdd(3, addValue: 0);
            r1.Should().Be(300);

            // key 99 absent → inserted in shadow tree
            int r2 = scope.GetOrAdd(99, addValue: 77);
            r2.Should().Be(77);

            // No scope.Complete() → rollback on Dispose
        }

        // key 3 unchanged
        tree.TryGet(3, out int v3).Should().BeTrue();
        v3.Should().Be(300);

        // key 99 must not exist (rolled back)
        tree.TryGet(99, out _).Should().BeFalse();
        tree.Count.Should().Be(1);
    }
}
