using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Api;

public class BPlusTreeDisposedGuardTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    /// <summary>Opens a tree, disposes it, and returns the disposed instance.</summary>
    private BPlusTree<int, int> OpenAndDispose()
    {
        var tree = BPlusTree<int, int>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 64,
                CheckpointThreshold = 128,
            },
            Int32Serializer.Instance,
            Int32Serializer.Instance);
        tree.Dispose();
        return tree;
    }

    [Fact]
    public void Delete_AfterDispose_ThrowsObjectDisposedException()
        => OpenAndDispose().Invoking(t => t.Delete(1))
            .Should().Throw<ObjectDisposedException>();

    [Fact]
    public void ContainsKey_AfterDispose_ThrowsObjectDisposedException()
        => OpenAndDispose().Invoking(t => t.ContainsKey(1))
            .Should().Throw<ObjectDisposedException>();

    [Fact]
    public void Scan_AfterDispose_ThrowsObjectDisposedException()
        => OpenAndDispose().Invoking(t => t.Scan())
            .Should().Throw<ObjectDisposedException>();

    [Fact]
    public void Checkpoint_AfterDispose_ThrowsObjectDisposedException()
        => OpenAndDispose().Invoking(t => t.Checkpoint())
            .Should().Throw<ObjectDisposedException>();

    [Fact]
    public void Compact_AfterDispose_ThrowsObjectDisposedException()
        => OpenAndDispose().Invoking(t => t.Compact())
            .Should().Throw<ObjectDisposedException>();

    [Fact]
    public void GetStatistics_AfterDispose_ThrowsObjectDisposedException()
        => OpenAndDispose().Invoking(t => t.GetStatistics())
            .Should().Throw<ObjectDisposedException>();

    [Fact]
    public void Close_IsIdempotent_DoesNotThrowOnSecondCall()
    {
        var tree = BPlusTree<int, int>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 64,
                CheckpointThreshold = 128,
            },
            Int32Serializer.Instance,
            Int32Serializer.Instance);

        tree.Close();
        tree.Invoking(t => t.Close())
            .Should().NotThrow("Close() must be idempotent like Dispose()");
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); }  catch { }
        try { File.Delete(_walPath); } catch { }
    }
}
