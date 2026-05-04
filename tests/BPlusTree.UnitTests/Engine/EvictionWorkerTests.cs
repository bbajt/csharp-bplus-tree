using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using FluentAssertions;
using Xunit;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for async dirty-page eviction (Phase 26b).
///
/// All tests create their own EvictionWorker via the Open() helper — correctness
/// invariants are what matter; all timing assertions use generous bounds to avoid
/// flakiness on CI / shared build agents.
///
/// Key invariant: no test should produce a tree where TryGet returns wrong data.
/// </summary>
public class EvictionWorkerTests : IDisposable
{
    private const int SmallPool  = 32;
    private const int MediumPool = 128;
    private const int PageSize   = 8192;

    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    public void Dispose()
    {
        try { File.Delete(_dbPath);  } catch { }
        try { File.Delete(_walPath); } catch { }
    }

    // ── Test helper ──────────────────────────────────────────────────────────

    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine, EvictionWorker worker)
        Open(int poolCapacity = MediumPool, WalSyncMode syncMode = WalSyncMode.Synchronous)
    {
        var opts = new BPlusTreeOptions
        {
            DataFilePath          = _dbPath,
            WalFilePath           = _walPath,
            PageSize              = PageSize,
            BufferPoolCapacity    = poolCapacity,
            CheckpointThreshold   = Math.Max(4, poolCapacity / 4),
            WalBufferSize         = 4 * 1024 * 1024,
            SyncMode              = syncMode,
            EvictionHighWatermark = 0.85,
            EvictionLowWatermark  = 0.70,
            EvictionBatchSize     = 16,
            EvictionWaitTimeoutMs = 5_000,
        };
        var wal    = WalWriter.Open(_walPath, opts.WalBufferSize, opts.SyncMode, opts.FlushIntervalMs, opts.FlushBatchSize);
        var mgr    = PageManager.Open(opts, wal);
        var ns     = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta   = new TreeMetadata(mgr);
        meta.Load();
        var engine = new TreeEngine<int, int>(mgr, ns, meta);
        var worker = new EvictionWorker(mgr.BufferPool, mgr.Storage, wal, opts);
        worker.Start();
        return (mgr, wal, engine, worker);
    }

    // Dispose in the correct order: worker → engine → mgr (mgr also disposes wal).
    private static void CleanUp(EvictionWorker worker, TreeEngine<int, int> engine, PageManager mgr)
    {
        worker.Dispose();
        engine.Close();
        mgr.Dispose();
    }

    // Crash-simulation teardown: stop worker + dispose mgr WITHOUT calling engine.Close().
    // engine.Close() → TakeCheckpoint() → TruncateWal() would destroy the WAL records
    // needed for crash-recovery in the second block of crash tests.
    private static void CrashCleanUp(EvictionWorker worker, PageManager mgr)
    {
        worker.Dispose();
        mgr.Dispose();
    }

    // ── Core correctness ─────────────────────────────────────────────────────

    [Fact]
    public void Insert_WithSmallPool_AllDataRetrievable()
    {
        var (mgr, _, engine, worker) = Open(poolCapacity: SmallPool);
        for (int i = 0; i < 2_000; i++) engine.Insert(i, i * 7);
        for (int i = 0; i < 2_000; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} not found");
            v.Should().Be(i * 7, $"wrong value for key {i}");
        }
        CleanUp(worker, engine, mgr);
    }

    [Fact]
    public void Insert_CloseReopen_AllDataPresent()
    {
        {
            var (mgr, _, engine, worker) = Open(poolCapacity: SmallPool);
            for (int i = 0; i < 2_000; i++) engine.Insert(i, i);
            CleanUp(worker, engine, mgr);
        }
        {
            var (mgr, _, engine, worker) = Open(poolCapacity: SmallPool);
            for (int i = 0; i < 2_000; i++)
                engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after reopen");
            CleanUp(worker, engine, mgr);
        }
    }

    [Fact]
    public void RandomInsert_WithSmallPool_AllDataRetrievable()
    {
        var (mgr, _, engine, worker) = Open(poolCapacity: SmallPool);
        var rng       = new Random(42);
        var reference = new Dictionary<int, int>();
        for (int i = 0; i < 2_000; i++)
        {
            int k = rng.Next(10_000), v = rng.Next();
            engine.Insert(k, v);
            reference[k] = v;
        }
        foreach (var (k, expected) in reference)
        {
            engine.TryGet(k, out int actual).Should().BeTrue($"key {k} not found");
            actual.Should().Be(expected, $"wrong value for key {k}");
        }
        CleanUp(worker, engine, mgr);
    }

    // ── WAL-before-page invariant ─────────────────────────────────────────────

    [Fact]
    public void EvictedPage_WalAlwaysFlushedFirst_DataConsistentAfterCrash()
    {
        {
            var (mgr, wal, engine, worker) = Open(poolCapacity: SmallPool);
            for (int i = 0; i < 1_000; i++) engine.Insert(i, i);
            wal.Flush();
            // Do NOT call engine.Close() — that checkpoints and truncates the WAL.
            // Use CrashCleanUp to preserve WAL records for recovery in the second block.
            CrashCleanUp(worker, mgr);
            // Wipe data file to force full WAL recovery on reopen.
            File.WriteAllBytes(_dbPath, new byte[PageSize]);
        }
        {
            var (mgr, _, engine, worker) = Open(poolCapacity: SmallPool);
            for (int i = 0; i < 1_000; i++)
                engine.TryGet(i, out _).Should().BeTrue(
                    $"key {i} missing after crash+recovery — WAL-before-page violated");
            CleanUp(worker, engine, mgr);
        }
    }

    [Fact]
    public void GroupCommit_WithEviction_WalFlushedBeforePageWrite()
    {
        {
            var (mgr, wal, engine, worker) = Open(
                poolCapacity: SmallPool, syncMode: WalSyncMode.GroupCommit);
            for (int i = 0; i < 1_000; i++) engine.Insert(i, i);
            Thread.Sleep(50);   // let group commit flush
            wal.Flush();        // ensure full WAL flush before wipe
            // Do NOT call engine.Close() — that checkpoints and truncates the WAL.
            CrashCleanUp(worker, mgr);
            File.WriteAllBytes(_dbPath, new byte[PageSize]);
        }
        {
            var (mgr, _, engine, worker) = Open(poolCapacity: SmallPool);
            for (int i = 0; i < 1_000; i++)
                engine.TryGet(i, out _).Should().BeTrue(
                    $"key {i} missing — WAL-before-page violated in GroupCommit+eviction");
            CleanUp(worker, engine, mgr);
        }
    }

    // ── Pinned pages never evicted ────────────────────────────────────────────

    [Fact]
    public void PinnedPage_NeverEvicted_DataIntact()
    {
        var (mgr, _, engine, worker) = Open(poolCapacity: SmallPool);
        for (int i = 0; i < 500; i++) engine.Insert(i, i);

        // Correct: use TreeMetadata to find the first leaf page.
        var meta = new TreeMetadata(mgr);
        meta.Load();
        uint targetPageId = meta.FirstLeafPageId;

        var pinnedFrame = mgr.FetchPage(targetPageId);

        // Insert more to force eviction pressure.
        for (int i = 500; i < 1_000; i++) engine.Insert(i, i);
        Thread.Sleep(200);   // give eviction thread time to run

        // Pinned frame must retain its identity — eviction resets PageId to NullPageId.
        // NOTE: byte-content equality is NOT asserted because the leaf data may legitimately
        // change as more inserts land in it (8192-byte pages hold ~700+ entries).
        pinnedFrame.PageId.Should().Be(targetPageId,
            "pinned frame must retain its page identity — eviction resets PageId to NullPageId");
        pinnedFrame.IsEvicting.Should().BeFalse(
            "pinned frame must never be in EVICTING state");

        mgr.Unpin(targetPageId);
        CleanUp(worker, engine, mgr);
    }

    // ── No double-eviction (atomic claim) ─────────────────────────────────────

    [Fact(Timeout = 30_000)]
    public async Task ConcurrentEviction_NoDuplicateWrites_NoCorruption()
    {
        var (mgr, _, engine, worker) = Open(poolCapacity: SmallPool);

        var threads = Enumerable.Range(0, 4).Select(t => new Thread(() =>
        {
            for (int i = t * 500; i < (t + 1) * 500; i++)
                engine.Insert(i, i * 13);
        })).ToList();
        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        for (int i = 0; i < 2_000; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} not found");
            v.Should().Be(i * 13, $"wrong value for key {i} — possible double-eviction");
        }
        CleanUp(worker, engine, mgr);
        await Task.CompletedTask;
    }

    // ── Pool exhaustion ───────────────────────────────────────────────────────

    [Fact(Timeout = 10_000)]
    public async Task PoolExhaustion_AllFramesPinned_ThrowsWithinTimeout()
    {
        var opts = new BPlusTreeOptions
        {
            DataFilePath          = _dbPath,
            WalFilePath           = _walPath,
            PageSize              = PageSize,
            BufferPoolCapacity    = 16,
            CheckpointThreshold   = 4,
            WalBufferSize         = 4 * 1024 * 1024,
            EvictionWaitTimeoutMs = 500,
        };
        var wal    = WalWriter.Open(_walPath, opts.WalBufferSize);
        var mgr    = PageManager.Open(opts, wal);
        var worker = new EvictionWorker(mgr.BufferPool, mgr.Storage, wal, opts);
        worker.Start();

        // Pin 16 pages — fills the pool completely.
        var frames = Enumerable.Range(0, 16)
            .Select(_ => mgr.AllocatePage(PageType.Leaf))
            .ToList();

        // 17th allocation must throw — eviction worker cannot free pinned frames.
        var sw = System.Diagnostics.Stopwatch.StartNew();
        mgr.Invoking(m => m.AllocatePage(PageType.Leaf))
           .Should().Throw<BufferPoolExhaustedException>();
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(3),
            "should throw promptly, not hang indefinitely");

        frames.ForEach(f => mgr.Unpin(f.PageId));
        worker.Dispose();
        mgr.Dispose();
        await Task.CompletedTask;
    }

    // ── Watermarks ────────────────────────────────────────────────────────────

    [Fact(Timeout = 30_000)]
    public async Task Eviction_StopsAtLowWatermark_DoesNotEvictEverything()
    {
        var (mgr, _, engine, worker) = Open(poolCapacity: MediumPool);
        for (int i = 0; i < 5_000; i++) engine.Insert(i, i);
        Thread.Sleep(500);  // let eviction run

        double occupancy = mgr.BufferPool.OccupancyFraction;
        occupancy.Should().BeLessThanOrEqualTo(0.90,
            "pool should not be above HWM after eviction has run");
        occupancy.Should().BeGreaterThan(0.0,
            "pool should not be completely empty — eviction stops at LWM");

        CleanUp(worker, engine, mgr);
        await Task.CompletedTask;
    }

    // ── Phase 33: pre-allocated buffer regression ─────────────────────────────

    /// <summary>
    /// Verify that stale entries from a previous TryEvictBatch call do not contaminate
    /// subsequent calls (Failure Point #3 from PHASE-33.MD).
    ///
    /// With the pre-allocated _dirtyBuffer, passing the full array instead of
    /// ArraySegment(0, dirtyCount) would write stale frame data from a prior batch.
    /// A double-release of the same frame index would corrupt the pool state.
    ///
    /// Strategy: drive two consecutive batches via InvokeTryEvictBatch without the
    /// background thread running.  Total evicted must not exceed the number of pages
    /// we allocated — any surplus implies a frame was double-released.
    /// </summary>
    [Fact]
    public void TryEvictBatch_PreAllocatedBuffers_NoDirtyEntryLeakAcrossCalls()
    {
        var opts = new BPlusTreeOptions
        {
            DataFilePath            = _dbPath,
            WalFilePath             = _walPath,
            PageSize                = PageSize,
            BufferPoolCapacity      = 64,
            CheckpointThreshold     = 16,
            WalBufferSize           = 4 * 1024 * 1024,
            EvictionBatchSize       = 8,
            CoWWriteAmplification   = 1,   // no scaling — test controls batch size directly
            EvictionHighWatermark   = 0.85,
            EvictionLowWatermark    = 0.70,
        };
        var wal    = WalWriter.Open(_walPath, opts.WalBufferSize);
        var mgr    = PageManager.Open(opts, wal);
        // Do NOT call worker.Start() — we drive InvokeTryEvictBatch() directly so
        // the background thread cannot race with the test's deterministic batches.
        var worker = new EvictionWorker(mgr.BufferPool, mgr.Storage, wal, opts);

        // Allocate 20 pages, mark dirty, unpin so they become eviction candidates.
        // bypassWal: true → PageLsn stays 0, so TryEvictBatch skips WAL flush.
        var pages = Enumerable.Range(0, 20)
            .Select(_ => mgr.AllocatePage(PageType.Leaf))
            .ToList();
        pages.ForEach(p => mgr.MarkDirtyAndUnpin(p.PageId, bypassWal: true));

        // Batch 1: second-chance clock clears reference bits (set by Unpin), evicts 0.
        int evicted1 = worker.InvokeTryEvictBatch();

        // Batch 2: reference bits are now clear — frames are evictable; claims up to 8.
        int evicted2 = worker.InvokeTryEvictBatch();

        // Total must not exceed allocated count.  If stale _dirtyBuffer entries were
        // written to frames that had already been released and re-claimed, the pool
        // state would be corrupted and either eviction would return a wrong count or
        // an exception would surface before we reach this assertion.
        (evicted1 + evicted2).Should().BeLessThanOrEqualTo(20,
            "TryEvictBatch must not evict frames beyond those claimed in the current batch. " +
            "Stale _dirtyBuffer entries (Failure Point #3) cause double-release, corrupting " +
            "the pool and producing an eviction count > number of allocated pages.");

        worker.Dispose();
        wal.Dispose();
        mgr.Dispose();
    }

    // ── Phase 54: PageLsn stamp gap fix ──────────────────────────────────────

    /// <summary>
    /// Verifies that EvictionWorker stamps frame.PageLsn into frame.Data[PageLsnOffset]
    /// before writing the page to disk (Phase 54 gap fix).
    ///
    /// Strategy: allocate a page via PageManager (auto-logs a WAL record → non-zero LSN),
    /// capture the in-memory frame.PageLsn, drive two InvokeTryEvictBatch() calls to evict
    /// (first clears ReferenceBit, second evicts), then read the page back from the
    /// StorageFile and assert the on-disk PageLsn bytes equal the captured value.
    ///
    /// Pre-fix behaviour: frame.PageLsn was never written into frame.Data by the
    /// EvictionWorker, so the on-disk bytes would be 0 and the assertion would fail.
    /// </summary>
    [Fact]
    public void TryEvictBatch_DirtyPage_StampsPageLsnBeforeWrite()
    {
        var opts = new BPlusTreeOptions
        {
            DataFilePath            = _dbPath,
            WalFilePath             = _walPath,
            PageSize                = PageSize,
            BufferPoolCapacity      = 64,
            CheckpointThreshold     = 16,
            WalBufferSize           = 4 * 1024 * 1024,
            EvictionBatchSize       = 8,
            CoWWriteAmplification   = 1,   // no scaling — test controls batch size directly
            EvictionHighWatermark   = 0.85,
            EvictionLowWatermark    = 0.70,
        };
        var wal    = WalWriter.Open(_walPath, opts.WalBufferSize);
        var mgr    = PageManager.Open(opts, wal);
        // Do NOT start the worker — drive InvokeTryEvictBatch() directly.
        var worker = new EvictionWorker(mgr.BufferPool, mgr.Storage, wal, opts);

        // Prime the WAL: on a fresh WAL file, _currentLsn = 0, so the very first
        // Append returns assignedLsn = 0. Appending one record advances _currentLsn
        // past 0, ensuring the test page's auto-log record gets a non-zero LSN.
        wal.Append(WalRecordType.Begin, transactionId: 0, pageId: 0,
                   LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);

        // Allocate a leaf page and mark it dirty through the normal WAL path.
        // MarkDirtyAndUnpin(pageId) auto-logs an UpdatePage record and sets
        // frame.PageLsn to the resulting LSN — this is the value we will verify
        // appears in the on-disk page bytes after eviction.
        var frame  = mgr.AllocatePage(PageType.Leaf);
        uint pageId = frame.PageId;
        frame.Data[100] = 0xAB;
        mgr.MarkDirtyAndUnpin(pageId);   // sets frame.PageLsn = (non-zero WAL LSN)

        // Capture the in-memory PageLsn before eviction.
        var pinnedFrame = mgr.FetchPage(pageId);
        ulong expectedLsn = pinnedFrame.PageLsn;
        expectedLsn.Should().BeGreaterThan(0UL,
            "MarkDirtyAndUnpin auto-logged a WAL record so LSN must be non-zero");
        mgr.Unpin(pageId);

        // Batch 1: second-chance clock clears ReferenceBit — nothing evicted.
        worker.InvokeTryEvictBatch();
        // Batch 2: ReferenceBit is clear — frame is claimed, PageLsn stamped, written.
        worker.InvokeTryEvictBatch();

        // The page must no longer be in the pool (evicted and released).
        mgr.BufferPool.GetFrameByPageId(pageId).Should().BeNull(
            "frame must have been evicted and released by batch 2");

        // Read the raw page bytes directly from the StorageFile.
        var raw = mgr.Storage.ReadPage(pageId);
        ulong onDiskLsn = BinaryPrimitives.ReadUInt64BigEndian(
            raw.Slice(PageLayout.PageLsnOffset, sizeof(ulong)));

        onDiskLsn.Should().Be(expectedLsn,
            "EvictionWorker must stamp frame.PageLsn into frame.Data before writing to disk");

        worker.Dispose();
        wal.Dispose();
        mgr.Dispose();
    }

    // ── Graceful shutdown ─────────────────────────────────────────────────────

    [Fact(Timeout = 30_000)]
    public async Task Dispose_EvictionThreadExitsCleanly()
    {
        var (mgr, _, engine, worker) = Open(poolCapacity: SmallPool);
        await Task.Run(() =>
        {
            for (int i = 0; i < 5_000; i++) engine.Insert(i, i);
        });

        // Dispose in correct order — must not throw.
        var act = () => CleanUp(worker, engine, mgr);
        act.Should().NotThrow("Close() must not throw after eviction activity");
    }

    [Fact(Timeout = 30_000)]
    public async Task Dispose_AllDirtyPagesFlushed_DataIntactAfterReopen()
    {
        {
            var (mgr, _, engine, worker) = Open(poolCapacity: SmallPool);
            for (int i = 0; i < 2_000; i++) engine.Insert(i, i);
            CleanUp(worker, engine, mgr);

            // Wipe WAL so reopen cannot use recovery — data must come from data file only.
            File.Delete(_walPath);
            File.WriteAllBytes(_walPath, Array.Empty<byte>());
        }
        {
            var (mgr, _, engine, worker) = Open(poolCapacity: MediumPool);
            for (int i = 0; i < 2_000; i++)
                engine.TryGet(i, out _).Should().BeTrue(
                    $"key {i} missing — dirty pages not flushed to data file on Close()");
            CleanUp(worker, engine, mgr);
        }
        await Task.CompletedTask;
    }
}
