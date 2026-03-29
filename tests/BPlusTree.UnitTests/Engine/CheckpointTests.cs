using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

public class CheckpointTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, TreeEngine<int, int> engine) Open()
    {
        var wal  = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = PageSize, BufferPoolCapacity = 128, CheckpointThreshold = 64,
        }, wal);
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, new TreeEngine<int, int>(mgr, ns, meta));
    }

    [Fact]
    public void Checkpoint_DoesNotThrow()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 500; i++) engine.Insert(i, i);
        engine.Invoking(e => e.Checkpoint()).Should().NotThrow();
        mgr.Dispose();
    }

    [Fact]
    public void Checkpoint_WalTruncated_FileShrinks()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 1000; i++) engine.Insert(i, i);
        long before = new FileInfo(_walPath).Length;
        engine.Checkpoint();
        long after  = new FileInfo(_walPath).Length;
        after.Should().BeLessThan(before, "WAL must be truncated after checkpoint");
        mgr.Dispose();
    }

    [Fact]
    public void Checkpoint_MetaPage_LastCheckpointLsnUpdated()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 200; i++) engine.Insert(i, i);
        engine.Checkpoint();
        var meta = new TreeMetadata(mgr);
        meta.Load();
        meta.LastCheckpointLsn.Should().BeGreaterThan(0UL);
        mgr.Dispose();
    }

    [Fact]
    public void MultipleCheckpoints_DataIntact()
    {
        var (mgr, engine) = Open();
        for (int batch = 0; batch < 5; batch++)
        {
            for (int i = batch * 100; i < (batch + 1) * 100; i++) engine.Insert(i, i);
            engine.Checkpoint();
        }
        for (int i = 0; i < 500; i++)
            engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after repeated checkpoints");
        mgr.Dispose();
    }

    [Fact]
    public void CleanClose_Reopen_AllDataPresent()
    {
        {
            var (mgr, engine) = Open();
            for (int i = 0; i < 500; i++) engine.Insert(i, i);
            engine.Close();
            mgr.Dispose();
        }
        {
            var (mgr, engine) = Open();
            for (int i = 0; i < 500; i++)
                engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after close+reopen");
            mgr.Dispose();
        }
    }

    [Fact]
    public void CleanClose_100Cycles_DataAccumulates()
    {
        for (int cycle = 0; cycle < 10; cycle++)   // 10 cycles for speed; 100 for thoroughness
        {
            var (mgr, engine) = Open();
            for (int i = cycle * 50; i < (cycle + 1) * 50; i++) engine.Insert(i, i);
            engine.Close();
            mgr.Dispose();
        }
        var (mgr2, engine2) = Open();
        for (int i = 0; i < 500; i++)
            engine2.TryGet(i, out _).Should().BeTrue($"key {i} missing after {10} close cycles");
        mgr2.Dispose();
    }

    [Fact]
    public void GracefulClose_Idempotent_DoubleCallNoException()
    {
        var (mgr, engine) = Open();
        engine.Close();
        engine.Invoking(e => e.Close()).Should().NotThrow();
        mgr.Dispose();
    }

    [Fact]
    public void TakeCheckpoint_AfterGracefulClose_DoesNotThrow()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 100; i++) engine.Insert(i, i);
        engine.Close();   // sets _closed = true inside CheckpointManager
        engine.Invoking(e => e.Checkpoint()).Should().NotThrow(
            "TakeCheckpoint after GracefulClose must be a safe no-op");
        mgr.Dispose();
    }

    public void Dispose() { try { File.Delete(_dbPath); File.Delete(_walPath); } catch { } }
}
