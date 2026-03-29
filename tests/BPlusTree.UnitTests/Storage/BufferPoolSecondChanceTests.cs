using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using FluentAssertions;
using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;

namespace BPlusTree.UnitTests.Storage;

/// <summary>
/// Tests for the second-chance clock algorithm after the Phase 28/29 fixes.
///
/// These tests verify that:
/// 1. Recently-accessed pages survive at least one eviction sweep (second chance)
/// 2. The ReferenceBit-before-decrement ordering is upheld under concurrency
/// 3. TryClaimForEviction correctly honours ReferenceBit inside the lock
/// 4. The WAL overflow config constraint is correctly identified
/// </summary>
public class BufferPoolSecondChanceTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal) OpenPool(
        int capacity = 32,
        double hwm = 0.85,
        double lwm = 0.70)
    {
        var opts = new BPlusTreeOptions
        {
            DataFilePath          = _dbPath,
            WalFilePath           = _walPath,
            PageSize              = PageSize,
            BufferPoolCapacity    = capacity,
            CheckpointThreshold   = 256,
            WalBufferSize         = 4 * 1024 * 1024,
            EvictionHighWatermark = hwm,
            EvictionLowWatermark  = lwm,
            EvictionBatchSize     = 8,
        };
        var wal = WalWriter.Open(_walPath, bufferSize: opts.WalBufferSize);
        var mgr = PageManager.Open(opts, wal);
        return (mgr, wal);
    }

    // ── Second-chance: recently unpinned page survives first sweep ─────────────

    [Fact]
    public void Unpin_SetsReferenceBit_BeforeDecrementMakesFrameEvictable()
    {
        // After Unpin(), the frame's ReferenceBit must be true.
        // This is the direct assertion that Fix 1 is in place.
        var (mgr, wal) = OpenPool(capacity: 64);
        var frame  = mgr.AllocatePage(PageType.Leaf);
        var pageId = frame.PageId;

        mgr.Unpin(pageId);

        // Access the frame directly via BufferPool to check ReferenceBit
        var rawFrame = mgr.BufferPool.GetFrameByPageId(pageId);
        rawFrame.Should().NotBeNull("frame must still be in pool immediately after unpin");
        rawFrame!.ReferenceBit.Should().BeTrue(
            "ReferenceBit must be true immediately after Unpin — " +
            "if false, Fix 1 (set before decrement) is not in place");

        wal.Dispose(); mgr.Dispose();
    }

    [Fact]
    public void RecentlyAccessedPage_SurvivesFirstEvictionSweep()
    {
        // A page that was just unpinned (ReferenceBit = true) must not be evicted
        // on the first clock sweep. It should be evicted on the second sweep at the earliest.
        var (mgr, wal) = OpenPool(capacity: 16, hwm: 0.70, lwm: 0.50);

        // Seed the pool to just below HWM
        var pages = Enumerable.Range(0, 10)
            .Select(_ => mgr.AllocatePage(PageType.Leaf))
            .ToList();

        // Unpin all — ReferenceBit = true for each
        pages.ForEach(p => mgr.Unpin(p.PageId));

        // Force one eviction sweep by allocating one more page (crosses HWM)
        var trigger = mgr.AllocatePage(PageType.Leaf);
        Thread.Sleep(100);  // give EvictionWorker time to run one sweep
        mgr.Unpin(trigger.PageId);

        // At least half the original pages should still be in the pool after one sweep
        // (second-chance: ReferenceBit cleared, not evicted yet)
        int stillInPool = pages.Count(p =>
            mgr.BufferPool.GetFrameByPageId(p.PageId) != null);

        stillInPool.Should().BeGreaterThan(pages.Count / 2,
            "second-chance clock must preserve recently-accessed pages through first sweep. " +
            "If 0 pages remain, ReferenceBit is not being set before eviction becomes possible.");

        wal.Dispose(); mgr.Dispose();
    }

    [Fact]
    public void HotWorkingSet_NotEvictedDuringContinuousAccess()
    {
        // 10 hot pages accessed in a loop. Pool is under eviction pressure from
        // background allocations. Hot pages must remain in pool (served from cache)
        // for the majority of accesses.
        var (mgr, wal) = OpenPool(capacity: 32);

        // Allocate hot working set (10 pages)
        var hot = Enumerable.Range(0, 10)
            .Select(_ => mgr.AllocatePage(PageType.Leaf))
            .ToList();
        hot.ForEach(p => mgr.Unpin(p.PageId)); // seed as unpinned-clean

        int diskReads = 0;
        int cacheHits = 0;

        for (int round = 0; round < 200; round++)
        {
            // Access all hot pages
            foreach (var page in hot)
            {
                bool wasInPool = mgr.BufferPool.GetFrameByPageId(page.PageId) != null;
                var f = mgr.FetchPage(page.PageId);
                if (!wasInPool) diskReads++; else cacheHits++;
                mgr.Unpin(f.PageId);
            }

            // Pressure: allocate and immediately release a cold page each round
            var cold = mgr.AllocatePage(PageType.Leaf);
            mgr.Unpin(cold.PageId);
        }

        double cacheHitRate = (double)cacheHits / (cacheHits + diskReads);
        cacheHitRate.Should().BeGreaterThan(0.85,
            $"hot working set cache hit rate {cacheHitRate:P0} is too low. " +
            "Expected >85% — second-chance clock must protect recently-accessed pages. " +
            "If <50%, ReferenceBit is not being set correctly before eviction is possible.");

        wal.Dispose(); mgr.Dispose();
    }

    // ── TryClaimForEviction: ReferenceBit re-check under lock ─────────────────

    [Fact]
    public void TryClaimForEviction_WithReferenceBitTrue_ReturnsFalse()
    {
        // A frame with ReferenceBit = true must NOT be claimed (returns false).
        // Bit must be cleared to give it a second chance.
        var (mgr, wal) = OpenPool(capacity: 32);

        var frame  = mgr.AllocatePage(PageType.Leaf);
        var pageId = frame.PageId;
        mgr.Unpin(pageId);  // ReferenceBit = true after unpin (Fix 1)

        int frameIndex = mgr.BufferPool.GetFrameIndexByPageId(pageId);
        frameIndex.Should().BeGreaterThanOrEqualTo(0, "frame must be in pool");

        bool claimed = mgr.BufferPool.TryClaimForEviction(frameIndex);

        claimed.Should().BeFalse(
            "TryClaimForEviction must return false when ReferenceBit is true (Fix 2). " +
            "The frame should get a second chance.");

        // After the call, ReferenceBit should be cleared (second chance consumed)
        var rawFrame = mgr.BufferPool.GetFrameByPageId(pageId);
        rawFrame!.ReferenceBit.Should().BeFalse(
            "TryClaimForEviction must clear ReferenceBit when returning false — " +
            "the frame gets exactly ONE second chance, not infinite chances.");

        wal.Dispose(); mgr.Dispose();
    }

    [Fact]
    public void TryClaimForEviction_WithReferenceBitFalse_ReturnsTrue()
    {
        // A frame with ReferenceBit = false (second chance already consumed) CAN be claimed.
        var (mgr, wal) = OpenPool(capacity: 32);

        var frame  = mgr.AllocatePage(PageType.Leaf);
        var pageId = frame.PageId;
        mgr.Unpin(pageId);  // ReferenceBit = true

        int frameIndex = mgr.BufferPool.GetFrameIndexByPageId(pageId);

        // First attempt: clears ReferenceBit, returns false (second chance)
        bool firstAttempt = mgr.BufferPool.TryClaimForEviction(frameIndex);
        firstAttempt.Should().BeFalse("first attempt grants second chance");

        // Second attempt: ReferenceBit is now false — should succeed
        bool secondAttempt = mgr.BufferPool.TryClaimForEviction(frameIndex);
        secondAttempt.Should().BeTrue(
            "second attempt must succeed — ReferenceBit was cleared by first attempt. " +
            "If false, TryClaimForEviction is not clearing the bit correctly.");

        // Clean up: release the eviction claim
        if (secondAttempt)
            mgr.BufferPool.ReleaseEvictedFrame(frameIndex);

        wal.Dispose(); mgr.Dispose();
    }

    // ── Concurrent: the race that Phase 28 broke ──────────────────────────────

    [Fact(Timeout = 15_000)]
    public async Task ConcurrentUnpinAndEviction_HotPages_NeverPrematurelyEvicted()
    {
        // Directly reproduces the Phase 28 regression scenario:
        // concurrent Unpin and EvictionWorker clock sweep.
        // Under Phase 28 code (before Fix 1+2): hot pages evicted immediately,
        // cache hit rate ~0%. Under Phase 29 code: cache hit rate >85%.
        var (mgr, wal) = OpenPool(capacity: 24, hwm: 0.75, lwm: 0.50);

        // Establish 8 hot pages
        var hot = Enumerable.Range(0, 8)
            .Select(_ => mgr.AllocatePage(PageType.Leaf))
            .ToList();

        int totalAccesses = 0, diskReads = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(8));

        // Hot reader: continuously accesses the 8 hot pages
        var reader = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                foreach (var page in hot)
                {
                    bool wasInPool = mgr.BufferPool.GetFrameByPageId(page.PageId) != null;
                    var f = mgr.FetchPage(page.PageId);
                    Interlocked.Increment(ref totalAccesses);
                    if (!wasInPool) Interlocked.Increment(ref diskReads);
                    mgr.Unpin(f.PageId);
                }
            }
        }, cts.Token);

        // Pressure writer: continuously allocates cold pages to force eviction
        var writer = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var cold = mgr.AllocatePage(PageType.Leaf);
                mgr.Unpin(cold.PageId);
                Thread.Sleep(1);
            }
        }, cts.Token);

        await Task.WhenAll(
            reader.ContinueWith(_ => { }),
            writer.ContinueWith(_ => { }));

        hot.ForEach(p =>
        {
            try { mgr.Unpin(p.PageId); } catch { /* may already be unpinned */ }
        });

        double diskReadRate = totalAccesses > 0
            ? (double)diskReads / totalAccesses : 1.0;

        diskReadRate.Should().BeLessThan(0.15,
            $"disk read rate {diskReadRate:P1} is too high ({diskReads}/{totalAccesses}). " +
            "Expected <15% — hot pages must be served from cache, not evicted on every access. " +
            "If >50%, the Phase 28 regression is still present: " +
            "ReferenceBit is not set before AtomicDecrementPin(), or " +
            "TryClaimForEviction is not checking ReferenceBit inside the lock.");

        wal.Dispose(); mgr.Dispose();
    }

    // ── WalBufferSize / CheckpointThreshold constraint ────────────────────────

    [Fact]
    public void BPlusTreeOptions_WillOverflowWalBuffer_DetectsUnsafeConfig()
    {
        // The exact config that caused the Phase 28 benchmark regression
        var bad = new BPlusTreeOptions
        {
            PageSize            = 8_192,
            CheckpointThreshold = 1_024,   // 1024 × 8192 = 8MB > 4MB WalBufferSize
            WalBufferSize       = 4 * 1_024 * 1_024,
        };
        bad.WillOverflowWalBuffer.Should().BeTrue(
            "CheckpointThreshold=1024 with PageSize=8192 and WalBufferSize=4MB " +
            "produces 8MB WAL per cycle, overflowing the 4MB buffer. " +
            "This is the exact config that caused Phase 28 insert regression of +149%%.");
    }

    [Fact]
    public void BPlusTreeOptions_WillOverflowWalBuffer_OkOnSafeConfig()
    {
        // The historical benchmark config — must NOT trigger the warning
        var good = new BPlusTreeOptions
        {
            PageSize            = 8_192,
            CheckpointThreshold = 256,     // 256 × 8192 = 2MB < 4MB WalBufferSize
            WalBufferSize       = 4 * 1_024 * 1_024,
        };
        good.WillOverflowWalBuffer.Should().BeFalse(
            "CheckpointThreshold=256 with PageSize=8192 and WalBufferSize=4MB " +
            "produces 2MB WAL per cycle, safely within the 4MB buffer.");
    }

    [Fact]
    public void BPlusTreeOptions_MaxSafeCheckpointThreshold_MatchesExpected()
    {
        var opts = new BPlusTreeOptions
        {
            PageSize      = 8_192,
            WalBufferSize = 4 * 1_024 * 1_024,   // 4MB
        };
        // 4MB / 8KB = 512 pages
        opts.MaxSafeCheckpointThreshold.Should().Be(512,
            "with 4MB WalBufferSize and 8192 PageSize, the max safe threshold is 512 pages");
    }

    [Fact]
    public void BPlusTreeOptions_WillOverflowWalBuffer_SafeOnEdge()
    {
        // Exactly at the boundary: CheckpointThreshold == WalBufferSize / PageSize
        var edge = new BPlusTreeOptions
        {
            PageSize            = 8_192,
            CheckpointThreshold = 512,     // 512 × 8192 = exactly 4MB
            WalBufferSize       = 4 * 1_024 * 1_024,
        };
        edge.WillOverflowWalBuffer.Should().BeFalse(
            "CheckpointThreshold exactly at WalBufferSize / PageSize should not overflow");
    }

    [Fact]
    public void BPlusTreeOptions_WillOverflowWalBuffer_OverflowsOneAboveEdge()
    {
        var over = new BPlusTreeOptions
        {
            PageSize            = 8_192,
            CheckpointThreshold = 513,     // 513 × 8192 = 4MB + 8KB > 4MB
            WalBufferSize       = 4 * 1_024 * 1_024,
        };
        over.WillOverflowWalBuffer.Should().BeTrue(
            "CheckpointThreshold one above the safe maximum must trigger the overflow warning");
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }
}
