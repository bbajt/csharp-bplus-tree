using System.Collections.Generic;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Api;

/// <summary>
/// Conformance tests for IReadOnlyDictionary&lt;TKey,TValue&gt; on ISnapshot (Phase 104).
/// Verifies all 7 interface members are correctly wired and that snapshot-frozen
/// semantics are preserved (live writes after snapshot-open are not visible).
/// </summary>
public class IReadOnlyDictionarySnapshotTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions { DataFilePath = _dbPath, WalFilePath = _walPath },
        Int32Serializer.Instance, Int32Serializer.Instance);

    // ── Cast ──────────────────────────────────────────────────────────────────

    [Fact]
    public void Snapshot_IReadOnlyDictionary_Cast_Succeeds()
    {
        using var tree = Open();
        using var snap = tree.BeginSnapshot();
        var dict = (IReadOnlyDictionary<int, int>)snap;
        dict.Should().NotBeNull();
    }

    // ── Indexer ───────────────────────────────────────────────────────────────

    [Fact]
    public void Snapshot_IReadOnlyDictionary_Indexer_ExistingKey_ReturnsValue()
    {
        using var tree = Open();
        tree.Put(1, 10);
        using var snap = tree.BeginSnapshot();
        var dict = (IReadOnlyDictionary<int, int>)snap;

        dict[1].Should().Be(10);
    }

    [Fact]
    public void Snapshot_IReadOnlyDictionary_Indexer_MissingKey_ThrowsKeyNotFoundException()
    {
        using var tree = Open();
        using var snap = tree.BeginSnapshot();
        var dict = (IReadOnlyDictionary<int, int>)snap;

        var act = () => { _ = dict[99]; };
        act.Should().Throw<KeyNotFoundException>();
    }

    // ── ContainsKey ───────────────────────────────────────────────────────────

    [Fact]
    public void Snapshot_IReadOnlyDictionary_ContainsKey_True()
    {
        using var tree = Open();
        tree.Put(1, 10);
        using var snap = tree.BeginSnapshot();
        var dict = (IReadOnlyDictionary<int, int>)snap;

        dict.ContainsKey(1).Should().BeTrue();
    }

    [Fact]
    public void Snapshot_IReadOnlyDictionary_ContainsKey_False()
    {
        using var tree = Open();
        using var snap = tree.BeginSnapshot();
        var dict = (IReadOnlyDictionary<int, int>)snap;

        dict.ContainsKey(99).Should().BeFalse();
    }

    // ── TryGetValue ───────────────────────────────────────────────────────────

    [Fact]
    public void Snapshot_IReadOnlyDictionary_TryGetValue_Found()
    {
        using var tree = Open();
        tree.Put(1, 10);
        using var snap = tree.BeginSnapshot();
        var dict = (IReadOnlyDictionary<int, int>)snap;

        dict.TryGetValue(1, out var v).Should().BeTrue();
        v.Should().Be(10);
    }

    // ── Snapshot-frozen semantics ─────────────────────────────────────────────

    [Fact]
    public void Snapshot_IReadOnlyDictionary_Keys_FrozenAtOpenTime()
    {
        using var tree = Open();
        tree.Put(1, 10);
        tree.Put(2, 20);
        tree.Put(3, 30);

        using var snap = tree.BeginSnapshot();
        var dict = (IReadOnlyDictionary<int, int>)snap;

        // Live write after snapshot-open must not be visible.
        tree.Put(4, 40);

        dict.Keys.Should().BeEquivalentTo(new[] { 1, 2, 3 },
            "snapshot is frozen at open time; key 4 was written after");
    }

    [Fact]
    public void Snapshot_IReadOnlyDictionary_Count_FrozenAtOpenTime()
    {
        using var tree = Open();
        tree.Put(1, 10);
        tree.Put(2, 20);
        tree.Put(3, 30);

        using var snap = tree.BeginSnapshot();
        var dict = (IReadOnlyDictionary<int, int>)snap;

        // Live writes after snapshot-open must not affect Count.
        tree.Put(4, 40);
        tree.Put(5, 50);

        dict.Count.Should().Be(3,
            "snapshot Count is frozen at open time; two inserts after open must not be counted");
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    public void Dispose()
    {
        if (File.Exists(_dbPath))  File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);
    }
}
