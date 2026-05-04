using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

public class TreeEngineInsertNoSplitTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, TreeEngine<int, int> engine, TreeMetadata meta) Open()
    {
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = PageSize, BufferPoolCapacity = 64, CheckpointThreshold = 16,
        });
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, new TreeEngine<int, int>(mgr, ns, meta), meta);
    }

    [Fact]
    public void Insert_IntoEmptyTree_ThenGet_Succeeds()
    {
        var (mgr, engine, _) = Open();
        engine.Insert(42, 420);
        engine.TryGet(42, out int v).Should().BeTrue();
        v.Should().Be(420);
        mgr.Dispose();
    }

    [Fact]
    public void Insert_MultipleKeys_AllRetrievable()
    {
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 50; i++) engine.Insert(i, i * 10);
        for (int i = 0; i < 50; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} not found");
            v.Should().Be(i * 10);
        }
        mgr.Dispose();
    }

    [Fact]
    public void Insert_UpdatesExistingKey()
    {
        var (mgr, engine, _) = Open();
        engine.Insert(5, 50);
        engine.Insert(5, 99);
        engine.TryGet(5, out int v);
        v.Should().Be(99);
        mgr.Dispose();
    }

    [Fact]
    public void Insert_ReturnsTrue_ForNewKey()
    {
        var (mgr, engine, _) = Open();
        engine.Insert(1, 10).Should().BeTrue();
        mgr.Dispose();
    }

    [Fact]
    public void Insert_ReturnsFalse_ForExistingKey()
    {
        var (mgr, engine, _) = Open();
        engine.Insert(1, 10);
        engine.Insert(1, 99).Should().BeFalse();
        mgr.Dispose();
    }

    [Fact]
    public void Insert_UpdatesTotalRecordCount()
    {
        var (mgr, engine, meta) = Open();
        engine.Insert(1, 10);
        engine.Insert(2, 20);
        meta.TotalRecordCount.Should().Be(2UL);
        mgr.Dispose();
    }

    public void Dispose() { try { File.Delete(_dbPath); File.Delete(_walPath); } catch { } }
}
