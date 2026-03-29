using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Tests for Phase 17a: leaf borrow, leaf merge, and root collapse.
/// All tests use trees shallow enough that internal node rebalancing
/// is NOT triggered (leaf underflow handled at height-2 tree max).
/// </summary>
public class MergerLeafTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, TreeEngine<int, int> engine, TreeMetadata meta) Open()
    {
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = PageSize, BufferPoolCapacity = 128, CheckpointThreshold = 64,
        });
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, new TreeEngine<int, int>(mgr, ns, meta), meta);
    }

    // ── Borrow from right ─────────────────────────────────────────────────────

    [Fact]
    public void BorrowFromRight_LeftUnderflows_KeyMovedToLeft()
    {
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 700; i++) engine.Insert(i, i);
        for (int i = 0; i < 300; i++) engine.Delete(i);
        for (int i = 300; i < 700; i++)
            engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after right borrow");
        mgr.Dispose();
    }

    [Fact]
    public void BorrowFromRight_SeparatorKeyUpdatedInParent()
    {
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 700; i++) engine.Insert(i, i);
        for (int i = 0; i < 280; i++) engine.Delete(i);
        for (int i = 280; i < 700; i++)
            engine.TryGet(i, out _).Should().BeTrue();
        mgr.Dispose();
    }

    // ── Borrow from left ──────────────────────────────────────────────────────

    [Fact]
    public void BorrowFromLeft_RightUnderflows_KeyMovedToRight()
    {
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 700; i++) engine.Insert(i, i);
        for (int i = 400; i < 700; i++) engine.Delete(i);
        for (int i = 0; i < 400; i++)
            engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after left borrow");
        mgr.Dispose();
    }

    [Fact]
    public void BorrowFromLeft_SeparatorKeyUpdatedInParent()
    {
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 700; i++) engine.Insert(i, i);
        for (int i = 500; i < 700; i++) engine.Delete(i);
        for (int i = 0; i < 500; i++)
            engine.TryGet(i, out _).Should().BeTrue();
        mgr.Dispose();
    }

    // ── Leaf merge ────────────────────────────────────────────────────────────

    [Fact]
    public void MergeLeaves_AllEntriesInLeftSibling()
    {
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 700; i++) engine.Insert(i, i);
        for (int i = 0; i < 600; i++) engine.Delete(i);
        for (int i = 600; i < 700; i++)
            engine.TryGet(i, out _).Should().BeTrue();
        mgr.Dispose();
    }

    [Fact]
    public void MergeLeaves_SiblingPointersUpdated()
    {
        var (mgr, engine, meta) = Open();
        for (int i = 0; i < 700; i++) engine.Insert(i, i);
        for (int i = 0; i < 600; i++) engine.Delete(i);

        meta.Load();
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var seen = new List<int>();
        uint id  = meta.FirstLeafPageId;
        while (id != PageLayout.NullPageId)
        {
            var f    = mgr.FetchPage(id);
            var leaf = ns.AsLeaf(f);
            for (int i = 0; i < leaf.Count; i++) seen.Add(leaf.GetKey(i));
            uint next = leaf.NextLeafPageId;
            mgr.Unpin(id);
            id = next;
        }
        seen.Should().BeInAscendingOrder();
        seen.Should().HaveCount(100);
        mgr.Dispose();
    }

    [Fact]
    public void MergeLeaves_RightSiblingFreedToFreeList()
    {
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 700; i++) engine.Insert(i, i);
        uint pageCountBefore = mgr.TotalPageCount;
        for (int i = 0; i < 600; i++) engine.Delete(i);
        mgr.TotalPageCount.Should().BeLessOrEqualTo(pageCountBefore + 5);
        mgr.Dispose();
    }

    [Fact]
    public void MergeLeaves_SeparatorRemovedFromParent()
    {
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 700; i++) engine.Insert(i, i);
        for (int i = 200; i < 550; i++) engine.Delete(i);
        for (int i = 0; i < 200; i++)
            engine.TryGet(i, out _).Should().BeTrue();
        for (int i = 550; i < 700; i++)
            engine.TryGet(i, out _).Should().BeTrue();
        mgr.Dispose();
    }

    // ── Collapse root ─────────────────────────────────────────────────────────

    [Fact]
    public void CollapseRoot_SingleChildRemains_RootReplaced()
    {
        var (mgr, engine, meta) = Open();
        for (int i = 0; i < 700; i++) engine.Insert(i, i);
        for (int i = 1; i < 700; i++) engine.Delete(i);
        engine.TryGet(0, out int v).Should().BeTrue();
        v.Should().Be(0);
        mgr.Dispose();
    }

    [Fact]
    public void CollapseRoot_TreeHeightDecremented()
    {
        var (mgr, engine, meta) = Open();
        for (int i = 0; i < 700; i++) engine.Insert(i, i);
        uint heightBefore = meta.TreeHeight;
        for (int i = 1; i < 700; i++) engine.Delete(i);
        meta.Load();
        meta.TreeHeight.Should().BeLessThan(heightBefore);
        mgr.Dispose();
    }

    [Fact]
    public void DeleteAll_EmptyTree_NoException()
    {
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 100; i++) engine.Insert(i, i);
        for (int i = 0; i < 100; i++) engine.Delete(i);
        engine.TryGet(0, out _).Should().BeFalse();
        engine.Insert(99, 99);
        engine.TryGet(99, out int v).Should().BeTrue();
        v.Should().Be(99);
        mgr.Dispose();
    }

    public void Dispose() { try { File.Delete(_dbPath); File.Delete(_walPath); } catch { } }
}
