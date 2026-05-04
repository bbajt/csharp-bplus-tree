using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Api;

/// <summary>
/// Tests for BPlusTree.GetStatistics() (Phase P-E).
/// </summary>
public class StatisticsTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 4096,
            BufferPoolCapacity  = 256,
            CheckpointThreshold = 64,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
    }

    [Fact]
    public void GetStatistics_EmptyTree_TotalRecordsIsZero()
    {
        using var store = Open();
        var stats = store.GetStatistics();

        stats.TotalRecords.Should().Be(0);
        stats.TotalPages.Should().BeGreaterThan(0, "even an empty tree has at least a root page");
        stats.FreePages.Should().BeGreaterOrEqualTo(0);
    }

    [Fact]
    public void GetStatistics_AfterInserts_ReflectsInsertedCount()
    {
        using var store = Open();
        const int count = 1000;
        for (int i = 0; i < count; i++)
            store.Put(i, i);

        var stats = store.GetStatistics();

        stats.TotalRecords.Should().Be((ulong)count);
        stats.TotalPages.Should().BeGreaterThan(0);
        stats.TreeHeight.Should().BeGreaterOrEqualTo(1u);
        stats.WalSizeBytes.Should().BeGreaterThan(0, "WAL must have written something");
    }

    [Fact]
    public void GetStatistics_AfterDelete_TotalRecordsDecreases()
    {
        using var store = Open();
        for (int i = 0; i < 100; i++) store.Put(i, i);
        var before = store.GetStatistics();

        store.Delete(50);
        var after = store.GetStatistics();

        after.TotalRecords.Should().Be(before.TotalRecords - 1);
    }

    [Fact]
    public void GetStatistics_WalSizeBytes_NonNegative()
    {
        using var store = Open();
        store.Put(1, 1);

        var stats = store.GetStatistics();

        stats.WalSizeBytes.Should().BeGreaterOrEqualTo(0);
    }

    [Fact]
    public void GetStatistics_AfterDispose_ThrowsObjectDisposedException()
    {
        var store = Open();
        store.Dispose();

        var act = () => store.GetStatistics();
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void GetStatistics_NewFields_OccupancyFractionInRange()
    {
        using var store = Open();
        for (int i = 0; i < 500; i++) store.Put(i, i);

        var stats = store.GetStatistics();

        stats.BufferPoolOccupancyFraction.Should().BeInRange(0.0, 1.0);
        stats.DirtyPageCount.Should().BeGreaterOrEqualTo(0);
        stats.ActiveTransactionCount.Should().Be(0, "no open transactions");
    }

    [Fact]
    public void GetStatistics_AfterReads_BufferPoolHitsIncrease()
    {
        using var store = Open();
        const int count = 200;
        for (int i = 0; i < count; i++) store.Put(i, i);

        // Read the same keys — pages should be in the pool, generating hits.
        for (int i = 0; i < count; i++) store.TryGet(i, out _);

        var stats = store.GetStatistics();
        stats.BufferPoolHits.Should().BeGreaterThan(0, "repeated reads should produce pool hits");
        stats.BufferPoolMisses.Should().BeGreaterOrEqualTo(0);
    }

    [Fact]
    public void GetStatistics_AfterDelete_FreePagesIncreases()
    {
        using var store = Open();
        for (int i = 0; i < 100; i++) store.Put(i, i);
        var statsA = store.GetStatistics();

        for (int i = 0; i < 50; i++) store.Delete(i);
        store.Checkpoint();
        var statsB = store.GetStatistics();

        statsB.FreePages.Should().BeGreaterOrEqualTo(statsA.FreePages,
            "deleting records should not decrease the free-page count");
    }

    [Fact]
    public void GetStatistics_OpenTransaction_ActiveCountIsOne()
    {
        using var store = Open();
        store.Put(1, 1);

        using var tx = store.BeginTransaction();
        var stats = store.GetStatistics();
        stats.ActiveTransactionCount.Should().Be(1);

        tx.Commit();
        var statsAfter = store.GetStatistics();
        statsAfter.ActiveTransactionCount.Should().Be(0);
    }

    [Fact]
    public void GetStatistics_OpenSnapshot_ActiveSnapshotCountIsOne()
    {
        using var store = Open();
        store.Put(1, 1);

        using var snap = store.BeginSnapshot();
        var statsDuring = store.GetStatistics();
        statsDuring.ActiveSnapshotCount.Should().Be(1, "one snapshot is open");

        snap.Dispose();
        var statsAfter = store.GetStatistics();
        statsAfter.ActiveSnapshotCount.Should().Be(0, "snapshot was disposed");
    }

    [Fact]
    public void GetStatistics_AfterReopen_FreePagesAccurate()
    {
        int freePagesBeforeReopen;
        {
            using var store = Open();
            for (int i = 0; i < 100; i++) store.Put(i, i);
            for (int i = 0; i < 50;  i++) store.Delete(i);
            freePagesBeforeReopen = store.GetStatistics().FreePages;
        }

        // After close+reopen, FreePages must match — LoadFromMeta walks the chain.
        using var reopened = Open();
        reopened.GetStatistics().FreePages.Should().Be(freePagesBeforeReopen,
            "FreeList.Count must be restored by walking the chain on open");
    }
}
