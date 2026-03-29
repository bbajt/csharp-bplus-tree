using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Tests for TryGetAndDelete — atomic pop on BPlusTree, ITransaction,
/// and BPlusTreeScope (Phase 86).
///
/// Properties verified:
///   1. Key absent → false; value = default; tree unchanged.
///   2. Key present → true; correct value returned; key deleted; Count decremented.
///   3. Only the targeted key is removed; other keys intact.
///   4. Transaction RYOW: own-insert visible; TryGetAndDelete removes it; commit → absent.
///   5. Scope: absent Complete → rollback → deleted key is restored.
/// </summary>
public class TryGetAndDeleteTests : IDisposable
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

    // ── Test 1: key absent ────────────────────────────────────────────────────

    [Fact]
    public void TryGetAndDelete_KeyAbsent_ReturnsFalse()
    {
        using var tree = Open();
        tree.Put(1, 10);

        bool result = tree.TryGetAndDelete(99, out int value);

        result.Should().BeFalse();
        value.Should().Be(default(int));
        tree.Count.Should().Be(1, "absent-key call must not affect other entries");
    }

    // ── Test 2: key present — value returned and key deleted ──────────────────

    [Fact]
    public void TryGetAndDelete_KeyPresent_ReturnsValueAndDeletes()
    {
        using var tree = Open();
        tree.Put(42, 777);
        long countBefore = tree.Count;

        bool result = tree.TryGetAndDelete(42, out int value);

        result.Should().BeTrue();
        value.Should().Be(777);
        tree.TryGet(42, out _).Should().BeFalse("key must be gone after TryGetAndDelete");
        tree.Count.Should().Be(countBefore - 1);
    }

    // ── Test 3: only targeted key removed; siblings intact ────────────────────

    [Fact]
    public void TryGetAndDelete_KeyPresent_LeavesOtherKeysIntact()
    {
        using var tree = Open();
        tree.Put(1, 100);
        tree.Put(2, 200);
        tree.Put(3, 300);

        bool result = tree.TryGetAndDelete(2, out int value);

        result.Should().BeTrue();
        value.Should().Be(200);

        tree.TryGet(1, out int v1).Should().BeTrue(); v1.Should().Be(100);
        tree.TryGet(2, out _).Should().BeFalse();
        tree.TryGet(3, out int v3).Should().BeTrue(); v3.Should().Be(300);
        tree.Count.Should().Be(2);
    }

    // ── Test 4: transaction RYOW + commit ─────────────────────────────────────
    // Insert a key inside a transaction; TryGetAndDelete the same key in the same
    // transaction (RYOW); after commit the key must be absent.

    [Fact]
    public void TryGetAndDelete_Transaction_RYOW_And_Commit()
    {
        using var tree = Open();
        tree.Put(5, 50);   // pre-existing key

        using (var tx = tree.BeginTransaction())
        {
            tx.Insert(9, 90);   // shadow insert

            // RYOW: key 9 visible in shadow tree
            bool r1 = tx.TryGetAndDelete(9, out int v1);
            r1.Should().BeTrue();
            v1.Should().Be(90);

            // RYOW: key 5 visible in shadow tree (from live tree)
            bool r2 = tx.TryGetAndDelete(5, out int v2);
            r2.Should().BeTrue();
            v2.Should().Be(50);

            tx.Commit();
        }

        tree.TryGet(5, out _).Should().BeFalse("key 5 deleted in committed tx");
        tree.TryGet(9, out _).Should().BeFalse("key 9 inserted then deleted in same tx");
        tree.Count.Should().Be(0);
    }

    // ── Test 5: scope rollback — deleted key is restored ─────────────────────

    [Fact]
    public void TryGetAndDelete_Scope_Rollback_LeavesTreeUnchanged()
    {
        using var tree = Open();
        tree.Put(7, 70);
        tree.Put(8, 80);

        using (var scope = tree.BeginScope())
        {
            bool r = scope.TryGetAndDelete(7, out int v);
            r.Should().BeTrue();
            v.Should().Be(70);
            // No scope.Complete() → rollback on Dispose
        }

        // key 7 must be restored
        tree.TryGet(7, out int restored).Should().BeTrue();
        restored.Should().Be(70);
        // key 8 unaffected
        tree.TryGet(8, out int v8).Should().BeTrue();
        v8.Should().Be(80);
        tree.Count.Should().Be(2);
    }
}
