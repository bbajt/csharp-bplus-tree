using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Phase 17c integration tests: full Merger end-to-end with recursive propagation.
/// These are the definitive Merger tests — 17a and 17b are sub-component tests.
/// </summary>
public class MergerIntegrationTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, TreeEngine<int, int> engine) Open()
    {
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = PageSize, BufferPoolCapacity = 256, CheckpointThreshold = 128,
        });
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, new TreeEngine<int, int>(mgr, ns, meta));
    }

    [Fact]
    public void BorrowFromSibling_LeavesAllKeysAccessible()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 800; i++) engine.Insert(i, i);
        engine.Delete(0);
        for (int i = 1; i < 800; i++)
            engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after borrow");
        mgr.Dispose();
    }

    [Fact]
    public void MergeLeaves_LeavesAllKeysAccessible()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 800; i++) engine.Insert(i, i);
        for (int i = 0; i < 400; i++) engine.Delete(i);
        for (int i = 400; i < 800; i++)
            engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after merge");
        mgr.Dispose();
    }

    [Fact]
    public void DeleteAll_ResultsInEmptyTree()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 100; i++) engine.Insert(i, i);
        for (int i = 0; i < 100; i++) engine.Delete(i);
        for (int i = 0; i < 100; i++)
            engine.TryGet(i, out _).Should().BeFalse();
        mgr.Dispose();
    }

    [Fact]
    public void CollapseRoot_WhenSingleChildRemains()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 700; i++) engine.Insert(i, i);
        for (int i = 0; i < 699; i++) engine.Delete(i);
        engine.TryGet(699, out int v).Should().BeTrue();
        v.Should().Be(699);
        mgr.Dispose();
    }

    [Fact]
    public void LeafSiblingChain_IntactAfterMerge()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 800; i++) engine.Insert(i, i);
        for (int i = 0; i < 400; i++) engine.Delete(i);

        var meta = new TreeMetadata(mgr);
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
        seen.Should().HaveCount(400);
        mgr.Dispose();
    }

    [Fact]
    public void DeepTree_RecursiveMerge_AllLevelsRebalanced()
    {
        // Height-3 tree. Delete enough to trigger recursive merge propagation.
        var (mgr, engine) = Open();
        for (int i = 0; i < 10_000; i++) engine.Insert(i, i);
        for (int i = 0; i < 9_800; i++) engine.Delete(i);
        var keys = engine.Scan().Select(x => x.Key).ToList();
        keys.Should().HaveCount(200);
        keys.Should().BeInAscendingOrder();
        mgr.Dispose();
    }

    [Fact]
    public void InsertDeleteInsert_TreeStaysConsistent()
    {
        // Insert 1000, delete 800, insert 800 new keys.
        // Final state: 1000 keys (200 original + 800 new).
        var (mgr, engine) = Open();
        for (int i = 0;    i < 1000; i++) engine.Insert(i, i);
        for (int i = 0;    i < 800;  i++) engine.Delete(i);
        for (int i = 1000; i < 1800; i++) engine.Insert(i, i);
        engine.TryGet(999, out _).Should().BeTrue();
        for (int i = 1000; i < 1800; i++) engine.TryGet(i, out _).Should().BeTrue();
        mgr.Dispose();
    }

    public void Dispose() { try { File.Delete(_dbPath); File.Delete(_walPath); } catch { } }
}
