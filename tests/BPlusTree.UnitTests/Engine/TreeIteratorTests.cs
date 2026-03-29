using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

public class TreeIteratorTests : IDisposable
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
    public void Scan_EmptyTree_ReturnsEmpty()
    {
        var (mgr, engine) = Open();
        engine.Scan().Should().BeEmpty();
        mgr.Dispose();
    }

    [Fact]
    public void Scan_AllKeys_ReturnsSortedOrder()
    {
        var (mgr, engine) = Open();
        int[] keys = { 50, 10, 30, 20, 40 };
        foreach (var k in keys) engine.Insert(k, k * 10);
        var result = engine.Scan().Select(x => x.Key).ToList();
        result.Should().BeInAscendingOrder();
        result.Should().HaveCount(5);
        mgr.Dispose();
    }

    [Fact]
    public void Scan_WithStartKey_SkipsBelowStart()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 10; i++) engine.Insert(i, i);
        var result = engine.Scan(startKey: 5).Select(x => x.Key).ToList();
        result.Min().Should().BeGreaterOrEqualTo(5);
        mgr.Dispose();
    }

    [Fact]
    public void Scan_WithEndKey_StopsAtEnd()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 10; i++) engine.Insert(i, i);
        var result = engine.Scan(endKey: 5).Select(x => x.Key).ToList();
        result.Max().Should().BeLessOrEqualTo(5);
        mgr.Dispose();
    }

    [Fact]
    public void Scan_WithRange_ReturnsOnlyRangeKeys()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 20; i++) engine.Insert(i, i);
        var result = engine.Scan(startKey: 5, endKey: 10).Select(x => x.Key).ToList();
        result.Should().AllSatisfy(k => k.Should().BeInRange(5, 10));
        result.Should().HaveCount(6); // 5,6,7,8,9,10
        mgr.Dispose();
    }

    [Fact]
    public void Scan_AcrossMultipleLeaves_AllKeysReturned()
    {
        var (mgr, engine) = Open();
        for (int i = 0; i < 2000; i++) engine.Insert(i, i);
        var result = engine.Scan().ToList();
        result.Should().HaveCount(2000);
        result.Select(x => x.Key).Should().BeInAscendingOrder();
        mgr.Dispose();
    }

    [Fact]
    public void Scan_AfterDelete_LeafChainIntact()
    {
        var (mgr, engine) = Open();

        // Insert enough keys to create multiple leaf pages
        for (int i = 0; i < 100; i++)
        {
            engine.Insert(i, i * 10);
        }

        // Delete some keys to trigger leaf rebalancing
        for (int i = 0; i < 50; i++)
        {
            engine.Delete(i);
        }

        // Verify scan works correctly
        var result = engine.Scan().ToList();
        result.Should().HaveCount(50);
        result.Select(x => x.Key).Should().BeInAscendingOrder();

        // Verify the tree's metadata leaf chain is consistent
        var meta = new TreeMetadata(mgr);
        meta.Load();
        var ns = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);

        // Walk the leaf chain manually to check consistency
        var seen = new List<int>();
        uint id = meta.FirstLeafPageId;
        int leafCount = 0;

        while (id != PageLayout.NullPageId)
        {
            var f = mgr.FetchPage(id);
            var leaf = ns.AsLeaf(f);
            for (int i = 0; i < leaf.Count; i++)
            {
                seen.Add(leaf.GetKey(i));
            }
            uint next = leaf.NextLeafPageId;
            mgr.Unpin(id);
            id = next;
            leafCount++;
        }

        seen.Should().HaveCount(50);
        seen.Should().BeInAscendingOrder();

        mgr.Dispose();
    }

    [Fact]
    public void Scan_DoesNotAllocatePerRecord()
    {
        // Verify that iterating through a scan result does not allocate
        // LeafNode wrappers per record (the Phase 37 regression signal).
        // The expected allocation is only from the iteration infrastructure
        // (TreeIterator itself, created once and pooled after first scan).

        var (mgr, engine) = Open();
        for (int i = 0; i < 2_000; i++) engine.Insert(i, i);

        // Warm-up: let the iterator get pooled
        foreach (var _ in engine.Scan()) { }

        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();

        long before = GC.GetAllocatedBytesForCurrentThread();
        int count = 0;
        foreach (var _ in engine.Scan()) count++;
        long after = GC.GetAllocatedBytesForCurrentThread();

        count.Should().Be(2_000);

        // Post-warmup: TreeIterator comes from pool (0 bytes).
        // MoveNext uses static accessors (0 bytes per record).
        // Phase 68: Scan now delegates to BeginSnapshot() internally for epoch protection.
        // Fixed per-scan allocations: Scan state machine + Snapshot object + ScanFromSnapshot
        // state machine ≈ 280 bytes total. TreeIterator is still pooled (0 bytes).
        // The guard is that allocation must NOT scale with record count — 280 bytes for
        // 2,000 records is 0.14 bytes/record, effectively zero.
        // If allocation grows linearly with count, AsLeaf() is being called per record.
        // Allow 400 bytes total for the fixed per-scan snapshot infrastructure.
        long allocated = after - before;
        allocated.Should().BeLessThan(400,
            $"Scan must not allocate per record. Got {allocated} bytes for 2,000-record scan. " +
            $"If > 400, AsLeaf() is still being called in MoveNext().");

        mgr.Dispose();
    }

    public void Dispose() { try { File.Delete(_dbPath); File.Delete(_walPath); } catch { } }
}
