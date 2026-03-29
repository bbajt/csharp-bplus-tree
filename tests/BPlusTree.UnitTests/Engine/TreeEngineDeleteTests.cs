using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

public class TreeEngineDeleteTests : IDisposable
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
    public void Delete_ExistingKey_ReturnsTrue()
    {
        var (mgr, engine) = Open();
        engine.Insert(5, 50);
        engine.Delete(5).Should().BeTrue();
        mgr.Dispose();
    }

    [Fact]
    public void Delete_MissingKey_ReturnsFalse()
    {
        var (mgr, engine) = Open();
        engine.Insert(5, 50);
        engine.Delete(99).Should().BeFalse();
        mgr.Dispose();
    }

    [Fact]
    public void Delete_KeyNotRetrievableAfterDelete()
    {
        var (mgr, engine) = Open();
        engine.Insert(7, 70);
        engine.Delete(7);
        engine.TryGet(7, out _).Should().BeFalse();
        mgr.Dispose();
    }

    [Fact]
    public void Delete_UpdatesRecordCount()
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
        engine.Insert(1, 10); engine.Insert(2, 20);
        engine.Delete(1);
        meta.TotalRecordCount.Should().Be(1UL);
        mgr.Dispose();
    }

    [Fact]
    public void Delete_AfterSplit_TreeRemainsConsistent()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 1000; i++) engine.Insert(i, i);
        for (int i = 0; i < 500; i++) engine.Delete(i);
        for (int i = 500; i < 1000; i++)
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} missing");
        mgr.Dispose();
    }

    [Fact]
    public void Insert_Delete_Mix_MatchesReferenceDictionary()
    {
        var (mgr, engine) = Open();
        var reference = new Dictionary<int, int>();
        var rng = new Random(42);
        for (int i = 0; i < 500; i++)
        {
            int k = rng.Next(200), v = rng.Next(1000);
            engine.Insert(k, v);
            reference[k] = v;
        }
        foreach (var k in reference.Keys.Take(100).ToList())
        {
            engine.Delete(k);
            reference.Remove(k);
        }
        foreach (var (k, expected) in reference)
        {
            engine.TryGet(k, out int actual).Should().BeTrue($"key {k} missing");
            actual.Should().Be(expected);
        }
        mgr.Dispose();
    }

    public void Dispose() { try { File.Delete(_dbPath); File.Delete(_walPath); } catch { } }
}
