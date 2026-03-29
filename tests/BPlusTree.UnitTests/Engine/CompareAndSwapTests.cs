using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Tests for TryCompareAndSwap — atomic conditional update on BPlusTree, ITransaction,
/// and IEnumerable on BPlusTree (Phase 92).
///
/// Properties verified:
///   1. Key absent → false; no phantom insert.
///   2. Key present, value matches → true; value updated to newValue.
///   3. Key present, value mismatches → false; value unchanged.
///   4. Transaction RYOW: own-insert visible; CAS updates it; commit persists result.
///   5. Custom IEqualityComparer is used for comparison.
///   6. IEnumerable: foreach yields all key-value pairs in ascending key order.
/// </summary>
public class CompareAndSwapTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions { DataFilePath = _dbPath, WalFilePath = _walPath },
        Int32Serializer.Instance, Int32Serializer.Instance);

    // -------------------------------------------------------------------------
    // Test 1 — Key absent → false; no phantom insert
    // -------------------------------------------------------------------------
    [Fact]
    public void TryCompareAndSwap_KeyAbsent_ReturnsFalse()
    {
        using var tree = Open();

        tree.TryCompareAndSwap(1, 10, 99).Should().BeFalse();
        tree.TryGet(1, out _).Should().BeFalse("no phantom insert expected");
        tree.Count.Should().Be(0);
    }

    // -------------------------------------------------------------------------
    // Test 2 — Key present, value matches → true; value updated
    // -------------------------------------------------------------------------
    [Fact]
    public void TryCompareAndSwap_Match_UpdatesValueAndReturnsTrue()
    {
        using var tree = Open();
        tree.Put(1, 10);

        tree.TryCompareAndSwap(1, 10, 99).Should().BeTrue();
        tree.TryGet(1, out int v).Should().BeTrue();
        v.Should().Be(99);
    }

    // -------------------------------------------------------------------------
    // Test 3 — Key present, value mismatches → false; value unchanged
    // -------------------------------------------------------------------------
    [Fact]
    public void TryCompareAndSwap_Mismatch_ReturnsFalseAndLeavesValueUnchanged()
    {
        using var tree = Open();
        tree.Put(1, 10);

        tree.TryCompareAndSwap(1, 42, 99).Should().BeFalse("42 ≠ 10");
        tree.TryGet(1, out int v).Should().BeTrue();
        v.Should().Be(10, "value must be unchanged");
    }

    // -------------------------------------------------------------------------
    // Test 4 — Transaction RYOW: own insert visible; CAS updates; commit persists
    // -------------------------------------------------------------------------
    [Fact]
    public void TryCompareAndSwap_InTransaction_ReadYourOwnWrites()
    {
        using var tree = Open();

        using var tx = tree.BeginTransaction();
        tx.Insert(1, 10);
        tx.TryCompareAndSwap(1, 10, 99).Should().BeTrue("tx sees own insert");
        tx.TryGet(1, out int v).Should().BeTrue();
        v.Should().Be(99, "RYOW: own CAS visible within tx");
        tx.Commit();

        tree.TryGet(1, out int committed).Should().BeTrue();
        committed.Should().Be(99, "committed value must persist");
    }

    // -------------------------------------------------------------------------
    // Test 5 — Custom IEqualityComparer is used for comparison
    // -------------------------------------------------------------------------
    [Fact]
    public void TryCompareAndSwap_CustomComparer_UsedForEquality()
    {
        using var tree = Open();
        var comparer = new EvenOddEqualityComparer();

        // 4 is even; expected=2 is also even → match → update
        tree.Put(1, 4);
        tree.TryCompareAndSwap(1, 2, 99, comparer).Should().BeTrue("2 and 4 are both even");
        tree.TryGet(1, out int v1).Should().BeTrue();
        v1.Should().Be(99);

        // 4 is even; expected=3 is odd → mismatch → no update
        tree.Put(2, 4);
        tree.TryCompareAndSwap(2, 3, 99, comparer).Should().BeFalse("3 is odd, 4 is even");
        tree.TryGet(2, out int v2).Should().BeTrue();
        v2.Should().Be(4, "value must be unchanged");
    }

    // -------------------------------------------------------------------------
    // Test 6 — IEnumerable: foreach yields all pairs in ascending key order
    // -------------------------------------------------------------------------
    [Fact]
    public void IEnumerable_Foreach_YieldsAllKeysAscending()
    {
        using var tree = Open();
        tree.Put(3, 30);
        tree.Put(1, 10);
        tree.Put(2, 20);

        var result = new List<(int Key, int Value)>();
        foreach (var pair in tree)
            result.Add(pair);

        result.Select(p => p.Key).Should().Equal(1, 2, 3);
        result.Select(p => p.Value).Should().Equal(10, 20, 30);
    }

    // -------------------------------------------------------------------------
    // Helper — custom comparer: even numbers equal, odd numbers equal
    // -------------------------------------------------------------------------
    private sealed class EvenOddEqualityComparer : IEqualityComparer<int>
    {
        public bool Equals(int x, int y) => (x % 2) == (y % 2);
        public int GetHashCode(int obj) => obj % 2;
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath);  } catch { }
        try { File.Delete(_walPath); } catch { }
    }
}
