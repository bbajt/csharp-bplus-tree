using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Api;

public class BPlusTreeApiTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions { DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = 8192, BufferPoolCapacity = 128, CheckpointThreshold = 32 },
        Int32Serializer.Instance, Int32Serializer.Instance);

    [Fact]
    public void Put_TryGet_RoundTrip()
    {
        using var tree = Open();
        tree.Put(42, 420);
        tree.TryGet(42, out int v).Should().BeTrue();
        v.Should().Be(420);
    }

    [Fact]
    public void ContainsKey_ReturnsTrueForInsertedKey()
    {
        using var tree = Open();
        tree.Put(7, 70);
        tree.ContainsKey(7).Should().BeTrue();
        tree.ContainsKey(99).Should().BeFalse();
    }

    [Fact]
    public void Delete_RemovesKey()
    {
        using var tree = Open();
        tree.Put(5, 50);
        tree.Delete(5);
        tree.ContainsKey(5).Should().BeFalse();
    }

    [Fact]
    public void Scan_ReturnsAllInOrder()
    {
        using var tree = Open();
        for (int i = 9; i >= 0; i--) tree.Put(i, i * 10);
        var keys = tree.Scan().Select(x => x.Key).ToList();
        keys.Should().BeInAscendingOrder();
        keys.Should().HaveCount(10);
    }

    [Fact]
    public void GetStatistics_TreeHeight_IsPopulated()
    {
        using var tree = Open();
        for (int i = 0; i < 100; i++) tree.Put(i, i);
        tree.GetStatistics().TreeHeight.Should().BeGreaterThan(0u);
    }

    [Fact]
    public void DoubleDispose_DoesNotThrow()
    {
        var tree = Open();
        tree.Dispose();
        tree.Invoking(t => t.Dispose()).Should().NotThrow();
    }

    [Fact]
    public void CloseAndReopen_AllDataStillPresent()
    {
        {
            using var tree = Open();
            for (int i = 0; i < 100; i++) tree.Put(i, i);
        }
        {
            using var tree = Open();
            for (int i = 0; i < 100; i++)
            {
                tree.TryGet(i, out int v).Should().BeTrue($"key {i} missing after reopen");
                v.Should().Be(i);
            }
        }
    }

    public void Dispose() { try { File.Delete(_dbPath); File.Delete(_walPath); } catch { } }
}
