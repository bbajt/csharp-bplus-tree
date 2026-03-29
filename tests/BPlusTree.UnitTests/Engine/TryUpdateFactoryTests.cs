using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Tests for TryUpdate factory overload + BPlusTree.TryUpdate(key, newValue) exposure (Phase 87).
///
/// Properties verified:
///   1. Factory overload: key absent → false; factory not called; tree unchanged.
///   2. Factory overload: key present → factory applied; updated value stored; returns true.
///   3. Factory overload: factory receives the actual existing value.
///   4. Factory overload: transaction RYOW — own-insert visible to TryUpdate in same tx; commit persists.
///   5. BPlusTree.TryUpdate(key, newValue): absent → false; present → true; value updated.
/// </summary>
public class TryUpdateFactoryTests : IDisposable
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

    // ── Test 1: factory overload — key absent ─────────────────────────────────

    [Fact]
    public void TryUpdate_Factory_KeyAbsent_ReturnsFalse()
    {
        using var tree = Open();
        tree.Put(1, 10);

        bool factoryCalled = false;
        bool result = tree.TryUpdate(99, v => { factoryCalled = true; return v + 1; });

        result.Should().BeFalse();
        factoryCalled.Should().BeFalse("factory must not be called when key is absent");
        tree.TryGet(99, out _).Should().BeFalse();
        tree.Count.Should().Be(1);
    }

    // ── Test 2: factory overload — key present ────────────────────────────────

    [Fact]
    public void TryUpdate_Factory_KeyPresent_AppliesFactory()
    {
        using var tree = Open();
        tree.Put(5, 50);

        bool result = tree.TryUpdate(5, v => v * 2);

        result.Should().BeTrue();
        tree.TryGet(5, out int v).Should().BeTrue();
        v.Should().Be(100);
        tree.Count.Should().Be(1, "TryUpdate must not create a duplicate");
    }

    // ── Test 3: factory receives the actual existing value ────────────────────

    [Fact]
    public void TryUpdate_Factory_FactoryReceivesExistingValue()
    {
        using var tree = Open();
        tree.Put(7, 777);

        int captured = -1;
        tree.TryUpdate(7, v => { captured = v; return v + 1; });

        captured.Should().Be(777, "factory must receive the stored value");
        tree.TryGet(7, out int stored).Should().BeTrue();
        stored.Should().Be(778);
    }

    // ── Test 4: transaction RYOW + commit ─────────────────────────────────────

    [Fact]
    public void TryUpdate_Factory_Transaction_RYOW_And_Commit()
    {
        using var tree = Open();

        using (var tx = tree.BeginTransaction())
        {
            tx.Insert(3, 30);   // shadow: key 3 → 30

            // RYOW: key 3 visible in shadow tree → factory path
            bool r = tx.TryUpdate(3, v => v + 5);
            r.Should().BeTrue();

            tx.TryGet(3, out int shadow).Should().BeTrue();
            shadow.Should().Be(35);

            tx.Commit();
        }

        tree.TryGet(3, out int final).Should().BeTrue();
        final.Should().Be(35);
    }

    // ── Test 5: BPlusTree.TryUpdate(key, newValue) newly exposed ─────────────

    [Fact]
    public void BPlusTree_TryUpdate_NewValue_ExposedOnPublicApi()
    {
        using var tree = Open();

        // Absent key → false
        bool r1 = tree.TryUpdate(42, 999);
        r1.Should().BeFalse();
        tree.TryGet(42, out _).Should().BeFalse("TryUpdate must not insert");

        // Present key → true, value updated
        tree.Put(42, 100);
        bool r2 = tree.TryUpdate(42, 200);
        r2.Should().BeTrue();
        tree.TryGet(42, out int v).Should().BeTrue();
        v.Should().Be(200);
        tree.Count.Should().Be(1);
    }
}
