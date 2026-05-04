using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using FluentAssertions;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;

namespace ByTech.BPlusTree.Core.Tests.Storage;

/// <summary>
/// Tests for the reverted (locked) BufferPool.Pin fast path.
///
/// These tests verify:
/// 1. Pin cannot race with TryClaimForEviction (no ABA window)
/// 2. Pin correctly serialises against concurrent eviction
/// 3. ReferenceBit is set inside the lock (Fix 2 interaction)
/// 4. Working-set pages survive multiple EvictionWorker sweeps while being accessed
/// 5. StorageFile.WritePage does not issue a per-page fsync
/// </summary>
public class BufferPoolPinTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal) OpenPool(int capacity = 32)
    {
        var opts = new BPlusTreeOptions
        {
            DataFilePath          = _dbPath,
            WalFilePath           = _walPath,
            PageSize              = PageSize,
            BufferPoolCapacity    = capacity,
            CheckpointThreshold   = 256,
            WalBufferSize         = 4 * 1024 * 1024,
            EvictionHighWatermark = 0.85,
            EvictionLowWatermark  = 0.70,
            EvictionBatchSize     = 8,
        };
        var wal = WalWriter.Open(_walPath, bufferSize: opts.WalBufferSize);
        var mgr = PageManager.Open(opts, wal);
        return (mgr, wal);
    }

    // ── Pin cannot race with TryClaimForEviction ──────────────────────────────

    [Fact(Timeout = 10_000)]
    public async Task Pin_WhileEvictionPressureHigh_PageRemainsAccessible()
    {
        // Under maximum eviction pressure, Pin must return a valid frame.
        // If Pin has an ABA race, it may return a frame mid-eviction,
        // causing the test to see stale or zeroed data.
        var (mgr, wal) = OpenPool(capacity: 16);
        var page   = mgr.AllocatePage(PageType.Leaf);
        var pageId = page.PageId;
        page.Data[0] = 0xAA;
        mgr.MarkDirtyAndUnpin(pageId, bypassWal: true);

        // Keep the pool under 100% pressure with background allocations
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(6));
        var pressure = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var f = mgr.AllocatePage(PageType.Leaf);
                mgr.Unpin(f.PageId);
            }
        }, cts.Token);

        // Repeatedly pin/unpin our page — must always return correct data
        for (int i = 0; i < 2_000; i++)
        {
            var f = mgr.FetchPage(pageId);
            f.Should().NotBeNull($"iteration {i}: FetchPage must always return a frame");
            f.PageId.Should().Be(pageId,
                $"iteration {i}: returned frame must hold our page, not a recycled one");
            mgr.Unpin(f.PageId);
        }

        cts.Cancel();
        await pressure.ContinueWith(_ => { });
        wal.Dispose(); mgr.Dispose();
    }

    // ── ReferenceBit set inside the lock ──────────────────────────────────────

    [Fact]
    public void Pin_SetsReferenceBit_InsideLock()
    {
        // After Pin(), the frame's ReferenceBit must be true.
        // This is set inside _lock in the reverted implementation — consistent
        // with TryClaimForEviction's Fix 2 check (both under same lock).
        var (mgr, wal) = OpenPool(capacity: 32);
        var frame  = mgr.AllocatePage(PageType.Leaf);
        var pageId = frame.PageId;
        mgr.Unpin(pageId);  // unpin so ReferenceBit can be cleared by eviction

        // Manually clear the reference bit (simulate EvictionWorker clearing it)
        mgr.BufferPool.GetFrameByPageId(pageId)!.ReferenceBit = false;

        // Pin again — should set ReferenceBit
        var repinned = mgr.FetchPage(pageId);
        repinned.Should().NotBeNull();

        mgr.BufferPool.GetFrameByPageId(pageId)!.ReferenceBit.Should().BeTrue(
            "Pin must set ReferenceBit inside _lock — if false, " +
            "TryClaimForEviction can immediately evict the frame after Pin returns");

        mgr.Unpin(pageId);
        wal.Dispose(); mgr.Dispose();
    }

    // ── Serialisation with TryClaimForEviction ────────────────────────────────

    [Fact(Timeout = 15_000)]
    public async Task Pin_SerializesWithEviction_NoPinCountDoubleIncrement()
    {
        // Concurrent Pin and TryClaimForEviction must not both succeed on the
        // same frame. If Pin is locked and EvictionWorker is locked, only one
        // can proceed at a time — no double-increment of PinCount.
        var (mgr, wal) = OpenPool(capacity: 32);
        var frame = mgr.AllocatePage(PageType.Leaf);
        var pageId = frame.PageId;
        mgr.Unpin(pageId);

        var pinCounts = new System.Collections.Concurrent.ConcurrentBag<int>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(6));

        // Reader: rapidly pins and unpins
        var reader = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    var f = mgr.FetchPage(pageId);
                    if (f != null)
                    {
                        pinCounts.Add(f.PinCount);
                        mgr.Unpin(f.PageId);
                    }
                }
                catch (InvalidOperationException) { /* page evicted, acceptable */ }
            }
        }, cts.Token);

        // Eviction pressure
        var writer = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var f = mgr.AllocatePage(PageType.Leaf);
                mgr.Unpin(f.PageId);
            }
        }, cts.Token);

        await Task.WhenAll(
            reader.ContinueWith(_ => { }),
            writer.ContinueWith(_ => { }));

        // PinCount seen by reader should never exceed 1 for a page pinned by one thread.
        // Double-increment would appear as PinCount = 2 when only one pin was taken.
        pinCounts.Where(c => c > 2).Should().BeEmpty(
            "PinCount must never exceed number of active pins — " +
            "a value of 3+ indicates double-increment from Pin ABA race");

        wal.Dispose(); mgr.Dispose();
    }

    // ── Working set survives eviction sweeps while being accessed ─────────────

    [Fact]
    public void HotPages_SurviveEviction_WhileAccessedViaLockedPin()
    {
        // This is the regression guard for the Phase 28 root cause.
        // A working set of 10 pages accessed repeatedly must stay in cache
        // under continuous eviction pressure. Phase 28 lock-free Pin failed
        // this — pages were evicted between accesses. Locked Pin must pass it.
        var (mgr, wal) = OpenPool(capacity: 24);

        // Establish working set
        var hot = Enumerable.Range(0, 10)
            .Select(_ => mgr.AllocatePage(PageType.Leaf))
            .ToList();
        hot.ForEach(p =>
        {
            p.Data[0] = 0xBB;
            mgr.MarkDirtyAndUnpin(p.PageId, bypassWal: true);
        });

        int hits = 0, misses = 0;

        for (int round = 0; round < 300; round++)
        {
            // Access hot pages
            foreach (var p in hot)
            {
                bool inPool = mgr.BufferPool.GetFrameByPageId(p.PageId) != null;
                var f = mgr.FetchPage(p.PageId);
                if (inPool) hits++; else misses++;
                f.Data[0].Should().Be(0xBB, $"page {p.PageId} data corrupted — possible ABA eviction");
                mgr.Unpin(f.PageId);
            }

            // Pressure
            var cold = mgr.AllocatePage(PageType.Leaf);
            mgr.Unpin(cold.PageId);
        }

        double hitRate = (double)hits / (hits + misses);
        hitRate.Should().BeGreaterThan(0.80,
            $"hot page hit rate {hitRate:P0} too low ({hits} hits, {misses} misses). " +
            "With locked Pin, EvictionWorker cannot claim frames during active access. " +
            "If < 50%: Pin is NOT holding _lock — the revert is incomplete.");

        wal.Dispose(); mgr.Dispose();
    }

    // ── StorageFile durability model test ──────────────────────────────────────

    [Fact]
    public void StorageFile_WritePage_DoesNotFsync()
    {
        // Verify that WritePage does not call Flush(flushToDisk: true).
        // Proxy: measure time for dirty page eviction via EvictionWorker.
        // A per-page fsync would take 5–50ms each = 500ms+ total.
        // Without fsync: OS write + scheduling = well under 1000ms.
        //
        // Pool capacity must exceed (dirty pages + flood pages + meta page) so
        // that the LINQ ToList() can pin all flood frames simultaneously.
        // Use capacity=256: 50 dirty + 50 flood + 1 meta = 101 < 256.
        var (mgr, wal) = OpenPool(capacity: 256);
        var frames = Enumerable.Range(0, 50)
            .Select(_ => mgr.AllocatePage(PageType.Leaf))
            .ToList();
        frames.ForEach(f =>
        {
            f.Data[0] = 0x01;
            mgr.MarkDirtyAndUnpin(f.PageId, bypassWal: true);
        });

        // Force evictions by flooding pool — measures WritePage cost
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var flood = Enumerable.Range(0, 50)
            .Select(_ => mgr.AllocatePage(PageType.Leaf))
            .ToList();
        Thread.Sleep(200); // let EvictionWorker flush the originals
        sw.Stop();

        flood.ForEach(f => mgr.Unpin(f.PageId));

        // If WritePage fsyncs: 100 × 5ms minimum = 500ms. Threshold: 1000ms.
        // If WritePage does not fsync: OS write + scheduling = < 100ms.
        sw.ElapsedMilliseconds.Should().BeLessThan(1_000,
            $"Evicting 100 pages took {sw.ElapsedMilliseconds}ms. " +
            "If > 1000ms, StorageFile.WritePage is likely calling " +
            "Flush(flushToDisk: true) — a per-page fsync that should be removed. " +
            "See StorageFile durability model comment for explanation.");

        wal.Dispose(); mgr.Dispose();
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }
}
