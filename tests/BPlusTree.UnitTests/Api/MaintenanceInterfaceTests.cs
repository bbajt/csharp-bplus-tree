using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Api;

/// <summary>
/// Tests for IBPlusTreeMaintenance (Phase Q-H).
/// </summary>
public class MaintenanceInterfaceTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 4096,
            BufferPoolCapacity  = 64,
            CheckpointThreshold = 256,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
    }

    [Fact]
    public void BPlusTree_Implements_IBPlusTreeMaintenance()
    {
        using var store = Open();
        IBPlusTreeMaintenance maintenance = store;
        maintenance.Should().NotBeNull();
    }

    [Fact]
    public void Maintenance_Flush_DoesNotThrow()
    {
        using var store = Open();
        store.Put(1, 1);
        IBPlusTreeMaintenance maintenance = store;

        var act = () => maintenance.Flush();
        act.Should().NotThrow();
    }

    [Fact]
    public void Maintenance_Checkpoint_DoesNotThrow()
    {
        using var store = Open();
        store.Put(1, 1);
        IBPlusTreeMaintenance maintenance = store;

        var act = () => maintenance.Checkpoint();
        act.Should().NotThrow();
    }

    [Fact]
    public void Maintenance_GetStatistics_ViaInterface_ReturnsStats()
    {
        using var store = Open();
        for (int i = 0; i < 10; i++) store.Put(i, i);

        IBPlusTreeMaintenance maintenance = store;
        var stats = maintenance.GetStatistics();

        stats.TotalRecords.Should().Be(10);
    }
}
