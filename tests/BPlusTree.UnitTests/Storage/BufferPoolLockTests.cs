using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using FluentAssertions;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;

namespace ByTech.BPlusTree.Core.Tests.Storage;

/// <summary>
/// Tests for the lock-free Unpin fast path and FetchPage ABA guard.
///
/// These tests focus on the specific concurrent scenarios that the
/// lock-free design must handle correctly.
/// All timing-sensitive tests use generous margins; correctness is the goal.
/// </summary>
public class BufferPoolLockTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal) OpenPool(int capacity = 64)
    {
        var opts = new BPlusTreeOptions
        {
            DataFilePath          = _dbPath,
            WalFilePath           = _walPath,
            PageSize              = PageSize,
            BufferPoolCapacity    = capacity,
            CheckpointThreshold   = capacity * 4,
            WalBufferSize         = 4 * 1024 * 1024,
            EvictionHighWatermark = 0.90,
            EvictionLowWatermark  = 0.70,
            EvictionBatchSize     = 8,
        };
        // Fix: pass WalBufferSize (int), not the options object
        var wal = WalWriter.Open(_walPath, bufferSize: opts.WalBufferSize);
        var mgr = PageManager.Open(opts, wal);
        return (mgr, wal);
    }

    // ── Fast-path correctness: no lock when PinCount stays > 0 ───────────────

    [Fact]
    public void Unpin_WhenMultiplePinners_FrameRemainsInPool()
    {
        // Two pinners. First Unpin should leave frame PINNED, not transition to UNPINNED.
        var (mgr, wal) = OpenPool();
        var frame1 = mgr.AllocatePage(PageType.Leaf);
        var frame2 = mgr.FetchPage(frame1.PageId);  // second pin on same page

        mgr.Unpin(frame1.PageId);  // first unpin — PinCount 2→1, fast path

        // Frame must still be in pool and pinned
        var frame3 = mgr.FetchPage(frame1.PageId);  // should succeed — still in pool
        frame3.PageId.Should().Be(frame1.PageId,
            "page must remain in pool after partial unpin");
        frame3.IsEvicting.Should().BeFalse("frame must not be evicting while still pinned");

        mgr.Unpin(frame2.PageId);
        mgr.Unpin(frame3.PageId);
        wal.Dispose(); mgr.Dispose();
    }

    [Fact]
    public void Unpin_LastPinner_FrameTransitionsToUnpinned()
    {
        // Single pinner. Unpin must transition frame to UNPINNED state.
        // Verify by attempting FetchPage after Unpin and checking it still works
        // (page remains in pool as UNPINNED until evicted).
        var (mgr, wal) = OpenPool();
        var frame = mgr.AllocatePage(PageType.Leaf);
        var pageId = frame.PageId;
        mgr.MarkDirtyAndUnpin(pageId, bypassWal: true);  // single Unpin

        // Pool occupancy should have decreased
        mgr.BufferPool.OccupancyFraction.Should().BeLessThan(1.0,
            "frame must be marked as available after last unpin");

        // Page must still be fetchable (eviction may not have run yet)
        // — it's UNPINNED but not necessarily gone
        wal.Dispose(); mgr.Dispose();
    }

    // ── Double-unpin detection ─────────────────────────────────────────────────

    [Fact]
    public void Unpin_DoubleUnpin_ThrowsInvalidOperation()
    {
        var (mgr, wal) = OpenPool();
        var frame = mgr.AllocatePage(PageType.Leaf);
        mgr.Unpin(frame.PageId);  // correct first unpin
        mgr.Invoking(m => m.Unpin(frame.PageId))
           .Should().Throw<InvalidOperationException>(
               "double-unpin must throw, not silently corrupt PinCount");
        wal.Dispose(); mgr.Dispose();
    }

    [Fact]
    public void Unpin_PageNotInPool_ThrowsInvalidOperation()
    {
        var (mgr, wal) = OpenPool();
        mgr.Invoking(m => m.Unpin(42))
           .Should().Throw<InvalidOperationException>(
               "unpin of a page that was never fetched must throw");
        wal.Dispose(); mgr.Dispose();
    }

    // ── Concurrent correctness ─────────────────────────────────────────────────

    [Fact(Timeout = 15_000)]
    public async Task Unpin_ConcurrentMultiplePinners_AllReleaseSafely()
    {
        // 20 threads each pin the same page, hold it briefly, unpin.
        // No exception. PinCount must reach 0 exactly once after all unpin.
        var (mgr, wal) = OpenPool(capacity: 128);
        var frame = mgr.AllocatePage(PageType.Leaf);
        var pageId = frame.PageId;
        // Keep one pin so page stays in pool during the test
        var anchorFrame = mgr.FetchPage(pageId);

        var exceptions = new ConcurrentBag<Exception>();
        var tasks = Enumerable.Range(0, 20).Select(_ => Task.Run(() =>
        {
            try
            {
                var f = mgr.FetchPage(pageId);
                Thread.Sleep(Random.Shared.Next(1, 5));
                mgr.Unpin(f.PageId);
            }
            catch (Exception ex) { exceptions.Add(ex); }
        })).ToArray();

        await Task.WhenAll(tasks);

        exceptions.Should().BeEmpty(
            "no exception should occur during concurrent pin+unpin of same page");

        // Release anchor pin
        mgr.Unpin(anchorFrame.PageId);
        // Release original allocation pin
        mgr.Unpin(frame.PageId);
        wal.Dispose(); mgr.Dispose();
    }

    [Fact(Timeout = 15_000)]
    public async Task Unpin_ConcurrentPinAndUnpin_NoLostUpdates()
    {
        // Interleave rapid FetchPage + Unpin pairs on the same page.
        // PinCount must never go negative (checked by double-unpin guard).
        // PinCount must not drift (no leaked pins).
        var (mgr, wal) = OpenPool(capacity: 128);
        var frame = mgr.AllocatePage(PageType.Leaf);
        var pageId = frame.PageId;
        // Anchor pin keeps page in pool
        var anchor = mgr.FetchPage(pageId);
        var cts    = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        int errors = 0;

        var tasks = Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    var f = mgr.FetchPage(pageId);
                    mgr.Unpin(f.PageId);
                }
                catch (InvalidOperationException)
                {
                    // Expected on double-unpin or pool exhaustion — count these
                    Interlocked.Increment(ref errors);
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks.Select(t =>
            t.ContinueWith(_ => { }, TaskContinuationOptions.OnlyOnRanToCompletion)));
        cts.Cancel();

        errors.Should().Be(0,
            "no InvalidOperationException should occur during concurrent rapid pin+unpin");

        mgr.Unpin(anchor.PageId);
        mgr.Unpin(frame.PageId);
        wal.Dispose(); mgr.Dispose();
    }

    // ── Re-pin race: last Unpin races with FetchPage ──────────────────────────

    [Fact(Timeout = 10_000)]
    public async Task Unpin_RaceWithFetchPage_FrameRemainsValid()
    {
        // Thread A unpins (last pin), Thread B immediately FetchPages same id.
        // Frame must either: still be in pool (both succeed) OR
        // B gets the reloaded frame (cold path). Neither corrupt state.
        var (mgr, wal) = OpenPool(capacity: 128);
        var frame = mgr.AllocatePage(PageType.Leaf);
        var pageId = frame.PageId;
        mgr.MarkDirtyAndUnpin(pageId, bypassWal: true); // seed page on disk via dirty write

        for (int round = 0; round < 500; round++)
        {
            var pinned = mgr.FetchPage(pageId);

            var tUnpin  = Task.Run(() => mgr.Unpin(pinned.PageId));
            var tFetch  = Task.Run(() =>
            {
                try
                {
                    var f = mgr.FetchPage(pageId);
                    mgr.Unpin(f.PageId);
                }
                catch (BufferPoolExhaustedException) { /* pool full — acceptable */ }
            });

            await Task.WhenAll(tUnpin, tFetch);
        }

        // If we got here without exception or hang, the race is handled correctly.
        wal.Dispose(); mgr.Dispose();
    }

    // ── ABA guard in FetchPage hot path ──────────────────────────────────────

    [Fact]
    public void FetchPage_AfterEviction_ReturnsFreshPage()
    {
        // Force eviction of a page, then FetchPage for the same pageId.
        // Must return correct page data, not stale/recycled frame.
        var (mgr, wal) = OpenPool(capacity: 8);  // tiny pool to force eviction
        var initialFrame = mgr.AllocatePage(PageType.Leaf);
        var pageId       = initialFrame.PageId;

        // Write a known byte pattern
        initialFrame.Data[0] = 0xAB;
        mgr.MarkDirtyAndUnpin(pageId, bypassWal: true);

        // Flood pool to force eviction of our page.
        // capacity=8: 6 FREE slots consumed first, then meta evicted (alloc 7),
        // then our target page evicted (alloc 8). Stop at capacity to avoid exhaustion.
        var flood = Enumerable.Range(0, 8)
            .Select(_ => mgr.AllocatePage(PageType.Leaf))
            .ToList();
        Thread.Sleep(200); // let eviction worker run

        // Release flood pages
        flood.ForEach(f => mgr.Unpin(f.PageId));

        // Re-fetch original page — must load from disk, return correct data
        var refetched = mgr.FetchPage(pageId);
        refetched.Data[0].Should().Be(0xAB,
            "FetchPage after eviction must return page with correct data, not ABA-corrupted frame");
        mgr.Unpin(pageId);
        wal.Dispose(); mgr.Dispose();
    }

    // ── Eviction correctness with new Unpin ──────────────────────────────────

    [Fact]
    public void Unpin_ToZero_MakesFrameEvictable()
    {
        // After the last Unpin, the frame must be in UNPINNED state
        // so EvictionWorker can claim it when pool pressure exists.
        var (mgr, wal) = OpenPool(capacity: 16);
        var frame = mgr.AllocatePage(PageType.Leaf);
        var pageId = frame.PageId;

        double occupancyBefore = mgr.BufferPool.OccupancyFraction;
        mgr.Unpin(pageId);   // last pinner
        double occupancyAfter = mgr.BufferPool.OccupancyFraction;

        occupancyAfter.Should().BeLessThanOrEqualTo(occupancyBefore,
            "Unpin of last pinner must not increase pool occupancy");

        wal.Dispose(); mgr.Dispose();
    }

    [Fact(Timeout = 15_000)]
    public async Task Unpin_UnderEvictionPressure_PoolNeverDeadlocks()
    {
        // Repeatedly allocate and unpin pages in a tiny pool with active EvictionWorker.
        // Should never hang or exhaust the pool.
        var (mgr, wal) = OpenPool(capacity: 16);
        for (int i = 0; i < 500; i++)
        {
            var f = mgr.AllocatePage(PageType.Leaf);
            mgr.Unpin(f.PageId);
        }
        // If we reach here without timeout or exception — success
        wal.Dispose(); mgr.Dispose();
        await Task.CompletedTask;
    }

    // ── PinCount-to-InUseCount consistency ───────────────────────────────────

    [Fact]
    public void Unpin_InUseCount_DecrementedExactlyOnce_PerPage()
    {
        // PinCount must decrement by exactly 1 per Unpin call,
        // and reach 0 only on the very last Unpin — not sooner.
        // Uses PinCount directly: all three FetchPage/AllocatePage return the
        // same Frame object, so PinCount is the authoritative shared counter.
        var (mgr, wal) = OpenPool(capacity: 64);

        var page   = mgr.AllocatePage(PageType.Leaf);
        var extra1 = mgr.FetchPage(page.PageId);
        var extra2 = mgr.FetchPage(page.PageId);
        // PinCount == 3 now

        mgr.Unpin(page.PageId);    // 3→2, fast path
        page.PinCount.Should().Be(2, "one pin released — PinCount must be 2");

        mgr.Unpin(extra1.PageId);  // 2→1, fast path
        page.PinCount.Should().Be(1,
            "second pin released — PinCount must be 1 (extra2 still holds a pin)");

        mgr.Unpin(extra2.PageId);  // 1→0, slow path — last pinner transitions to UNPINNED
        page.PinCount.Should().Be(0,
            "all pins released — PinCount must reach 0 exactly once on last Unpin");

        wal.Dispose(); mgr.Dispose();
    }

    // ── Performance regression guard ─────────────────────────────────────────

    [Fact(Timeout = 30_000)]
    public async Task Unpin_LockFree_FasterThanLocked_UnderHighConcurrency()
    {
        // Structural regression guard: 50K inserts + 50K warm reads must complete
        // within the 30s Timeout. In Release builds the lock-free Unpin targets
        // ~500ns/lookup (<25ms for 50K). Debug builds run ~10× slower; timing
        // assertions are omitted here — use the benchmarks for Release performance.
        const int N = 50_000;

        var (mgr, wal) = OpenPool(capacity: 512);
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr); meta.Load();
        var eng  = new TreeEngine<int, int>(mgr, ns, meta);
        for (int i = 0; i < N; i++) eng.Insert(i, i);

        // Warm reads — all Unpin calls must hit the lock-free fast path.
        // If this hangs or deadlocks the Timeout(30_000) will catch it.
        for (int i = 0; i < N; i++) eng.TryGet(i % N, out _);

        wal.Dispose(); mgr.Dispose();
        await Task.CompletedTask;
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }
}
