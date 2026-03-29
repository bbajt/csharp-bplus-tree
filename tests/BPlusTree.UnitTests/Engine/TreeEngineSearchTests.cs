using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

public class TreeEngineSearchTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    // Helper: build a single-leaf tree manually (no Insert API yet)
    private (PageManager, TreeEngine<int, int>) BuildSingleLeafTree(params (int key, int val)[] entries)
    {
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = PageSize, BufferPoolCapacity = 64, CheckpointThreshold = 16,
        });
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();

        var leafFrame = mgr.AllocatePage(PageType.Leaf);
        var leaf = ns.AsLeaf(leafFrame);
        leaf.Initialize();
        foreach (var (k, v) in entries) leaf.TryInsert(k, v);
        mgr.MarkDirtyAndUnpin(leafFrame.PageId); // no WAL in this test
        meta.SetRoot(leafFrame.PageId, treeHeight: 1);
        meta.SetFirstLeaf(leafFrame.PageId);
        meta.Flush();

        var engine = new TreeEngine<int, int>(mgr, ns, meta);
        return (mgr, engine);
    }

    [Fact]
    public void TryGet_EmptyTree_ReturnsFalse()
    {
        var mgr = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = PageSize, BufferPoolCapacity = 64, CheckpointThreshold = 16,
        });
        var engine = new TreeEngine<int, int>(mgr,
            new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance),
            new TreeMetadata(mgr));
        engine.TryGet(1, out _).Should().BeFalse();
        mgr.Dispose();
    }

    [Fact]
    public void TryGet_PresentKey_ReturnsTrue()
    {
        var (mgr, engine) = BuildSingleLeafTree((10, 100), (20, 200));
        engine.TryGet(10, out int v).Should().BeTrue();
        v.Should().Be(100);
        mgr.Dispose();
    }

    [Fact]
    public void TryGet_AbsentKey_ReturnsFalse()
    {
        var (mgr, engine) = BuildSingleLeafTree((10, 100), (20, 200));
        engine.TryGet(99, out _).Should().BeFalse();
        mgr.Dispose();
    }

    public void Dispose() { try { File.Delete(_dbPath); File.Delete(_walPath); } catch { } }
}
