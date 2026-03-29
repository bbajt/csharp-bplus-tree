using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

public class SplitterTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, TreeEngine<int, int> engine) Open()
    {
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = PageSize, BufferPoolCapacity = 128, CheckpointThreshold = 64,
        });
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, new TreeEngine<int, int>(mgr, ns, meta));
    }

    [Fact]
    public void Insert_BeyondSingleLeafCapacity_AllKeysRetrievable()
    {
        // 8192 - 48 = 8144 free; each int-int entry = 8 bytes + 4 slot = 12 bytes
        // ~678 entries per leaf. Insert 700 to force a split.
        var (mgr, engine) = Open();
        for (int i = 0; i < 700; i++) engine.Insert(i, i);
        for (int i = 0; i < 700; i++)
            engine.TryGet(i, out int v).Should().BeTrue($"Missing key {i}");
        mgr.Dispose();
    }

    [Fact]
    public void Insert_ForcesRootSplit_TreeHeightIncreases()
    {
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = PageSize, BufferPoolCapacity = 128, CheckpointThreshold = 64,
        });
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        var engine = new TreeEngine<int, int>(mgr, ns, meta);

        for (int i = 0; i < 700; i++) engine.Insert(i, i);
        meta.TreeHeight.Should().BeGreaterThan(1u);
        mgr.Dispose();
    }

    [Fact]
    public void LeafSiblingPointers_AreCorrectAfterSplit()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 700; i++) engine.Insert(i, i);

        // Walk the leaf chain from the first leaf; all keys must appear in sorted order
        var meta = new TreeMetadata(mgr);
        meta.Load();
        var seen = new List<int>();
        uint leafId = meta.FirstLeafPageId;
        while (leafId != PageLayout.NullPageId)
        {
            var frame = mgr.FetchPage(leafId);
            var ns = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
            var leaf = ns.AsLeaf(frame);
            for (int i = 0; i < leaf.Count; i++) seen.Add(leaf.GetKey(i));
            uint next = leaf.NextLeafPageId;
            mgr.Unpin(leafId);
            leafId = next;
        }
        seen.Should().BeInAscendingOrder();
        seen.Should().HaveCount(700);
        mgr.Dispose();
    }

    [Fact]
    public void Insert_1000_RandomOrder_AllRetrievable()
    {
        var (mgr, engine) = Open();
        var keys = Enumerable.Range(0, 1000).OrderBy(_ => Guid.NewGuid()).ToList();
        keys.ForEach(k => engine.Insert(k, k * 2));
        keys.ForEach(k =>
        {
            engine.TryGet(k, out int v).Should().BeTrue();
            v.Should().Be(k * 2);
        });
        mgr.Dispose();
    }

    public void Dispose() { try { File.Delete(_dbPath); File.Delete(_walPath); } catch { } }
}
