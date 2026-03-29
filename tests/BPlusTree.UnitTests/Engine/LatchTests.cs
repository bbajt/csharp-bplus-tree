using System.IO;
using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Unit tests for PageLatchManager and LatchCoupling in isolation.
/// No tree or page manager involved — pure concurrency primitive tests.
/// All tests have a hard Timeout to detect deadlocks.
/// </summary>
public class LatchTests
{
    // ── PageLatchManager ──────────────────────────────────────────────────────

    [Fact(Timeout = 10_000)]
    public async Task ReadLatch_MultipleThreads_AllAcquiredConcurrently()
    {
        await Task.Run(() =>
        {
            var mgr     = new PageLatchManager();
            int readers = 0;
            var threads = Enumerable.Range(0, 10).Select(_ => new Thread(() =>
            {
                using var latch = mgr.AcquireReadLatch(1);
                Interlocked.Increment(ref readers);
                Thread.Sleep(20); // hold latch briefly
            })).ToList();
            threads.ForEach(t => t.Start());
            Thread.Sleep(30); // let all threads acquire
            readers.Should().Be(10, "all 10 readers must hold latch simultaneously");
            threads.ForEach(t => t.Join());
        });
    }

    [Fact(Timeout = 10_000)]
    public async Task WriteLatch_Exclusive_BlocksAllOtherReaders()
    {
        await Task.Run(() =>
        {
            var mgr   = new PageLatchManager();
            bool writerDone = false;
            var writer = new Thread(() =>
            {
                using var latch = mgr.AcquireWriteLatch(1);
                Thread.Sleep(100);
                writerDone = true;
            });
            var reader = new Thread(() =>
            {
                Thread.Sleep(20); // let writer acquire first
                using var latch = mgr.AcquireReadLatch(1);
                writerDone.Should().BeTrue("reader must wait for writer to finish");
            });
            writer.Start(); reader.Start();
            writer.Join(); reader.Join();
        });
    }

    [Fact(Timeout = 10_000)]
    public async Task WriteLatch_Exclusive_BlocksOtherWriters()
    {
        await Task.Run(() =>
        {
            var mgr    = new PageLatchManager();
            int writes = 0;
            var t1     = new Thread(() => { using var l = mgr.AcquireWriteLatch(1); writes++; Thread.Sleep(50); });
            var t2     = new Thread(() => { Thread.Sleep(10); using var l = mgr.AcquireWriteLatch(1); writes++; });
            t1.Start(); t2.Start();
            t1.Join(); t2.Join();
            writes.Should().Be(2);
        });
    }

    [Fact(Timeout = 10_000)]
    public async Task ReadLatch_Dispose_ReleasesLatch_WriterCanProceed()
    {
        await Task.Run(() =>
        {
            var mgr      = new PageLatchManager();
            var readHeld = new ManualResetEventSlim(false);
            var readDone = new ManualResetEventSlim(false);
            var writer   = new Thread(() =>
            {
                readHeld.Wait();
                using var w = mgr.AcquireWriteLatch(1); // blocks until reader disposes
                readDone.Set();
            });
            var reader = new Thread(() =>
            {
                using var r = mgr.AcquireReadLatch(1);
                readHeld.Set();
                Thread.Sleep(50);
            }); // dispose releases here
            writer.Start(); reader.Start();
            writer.Join(); reader.Join();
            readDone.IsSet.Should().BeTrue();
        });
    }

    [Fact(Timeout = 10_000)]
    public async Task Remove_FreesLatch_NewAcquireCreatesNewLatch()
    {
        await Task.Run(() =>
        {
            var mgr = new PageLatchManager();
            using (var latch = mgr.AcquireReadLatch(5)) { }
            mgr.Remove(5);
            // After Remove, acquiring again must succeed (creates a fresh latch)
            mgr.Invoking(m => m.AcquireReadLatch(5).Dispose()).Should().NotThrow();
        });
    }

    // ── LatchCoupling ─────────────────────────────────────────────────────────

    [Fact(Timeout = 10_000)]
    public async Task CrabReadDown_ReleasesParentLatch_HoldsChildLatch()
    {
        await Task.Run(() =>
        {
            var mgr      = new PageLatchManager(timeout: TimeSpan.FromSeconds(5));
            var coupling = new LatchCoupling(mgr);

            var parentLatch = mgr.AcquireReadLatch(10);  // pretend we hold parent
            var childLatch  = coupling.CrabReadDown(20, parentLatch);

            // Parent (page 10) latch should now be released — another writer can acquire it
            bool parentFree = false;
            var t = new Thread(() =>
            {
                using var w = mgr.AcquireWriteLatch(10);
                parentFree = true;
            });
            t.Start(); t.Join(TimeSpan.FromSeconds(2));
            parentFree.Should().BeTrue("parent latch must be released after CrabReadDown");

            childLatch.Dispose();
        });
    }

    [Fact(Timeout = 10_000)]
    public async Task CrabWriteDown_SafeNode_ReleasesAncestorLatches()
    {
        await Task.Run(() =>
        {
            var mgr      = new PageLatchManager(timeout: TimeSpan.FromSeconds(5));
            var coupling = new LatchCoupling(mgr);

            // Stack-style buffer: fill two ancestor write latches, then crab down safely.
            WriteLatchHandle[] buf = new WriteLatchHandle[LatchCoupling.MaxTreeHeight];
            int count = 0;
            buf[count++] = mgr.AcquireWriteLatch(1);
            buf[count++] = mgr.AcquireWriteLatch(2);

            var childLatch = coupling.CrabWriteDown(childPageId: 3, isSafe: true, buf, ref count);

            // After safe crab-down, count must be reset to 0 (all ancestors released).
            count.Should().Be(0, "CrabWriteDown must reset count to 0 when isSafe=true");

            // Verify pages 1 and 2 are free (write-acquirable by another thread).
            bool p1Free = false, p2Free = false;
            var t = new Thread(() =>
            {
                using var l1 = mgr.AcquireWriteLatch(1); p1Free = true;
                using var l2 = mgr.AcquireWriteLatch(2); p2Free = true;
            });
            t.Start(); t.Join(TimeSpan.FromSeconds(2));
            p1Free.Should().BeTrue(); p2Free.Should().BeTrue();
            childLatch.Dispose();
        });
    }

    [Fact(Timeout = 10_000)]
    public async Task CrabWriteDown_UnsafeNode_RetainsAncestorLatches()
    {
        await Task.Run(() =>
        {
            var mgr      = new PageLatchManager(timeout: TimeSpan.FromSeconds(5));
            var coupling = new LatchCoupling(mgr);

            WriteLatchHandle[] buf = new WriteLatchHandle[LatchCoupling.MaxTreeHeight];
            int count = 0;
            buf[count++] = mgr.AcquireWriteLatch(1);

            var childLatch = coupling.CrabWriteDown(childPageId: 2, isSafe: false, buf, ref count);

            // Ancestor count must remain 1 (unsafe = we might split/merge upward).
            count.Should().Be(1, "ancestor latches must be retained when isSafe=false");

            LatchCoupling.ReleaseAll(buf, ref count);
            childLatch.Dispose();
        });
    }

    // ── Phase 33: zero-allocation regression ──────────────────────────────────

    /// <summary>
    /// A single non-splitting insert must not allocate heap objects from the latch
    /// coupling layer.  This is the regression test for the
    ///   List&lt;IDisposable&gt; ancestors → Span&lt;WriteLatchHandle&gt; (InlineArray)
    /// change in Phase 33.
    ///
    /// Uses GC.GetTotalAllocatedBytes() for before/after comparison.
    /// The only permitted allocation is the WAL record (pre-allocated buffer, but
    /// metadata structs may be boxed).  Threshold 1024 bytes covers that overhead.
    /// If List&lt;IDisposable&gt; or WriteLatchHandle class is still in use, allocated
    /// will be ~200–400 bytes of list/class overhead per insert.
    /// </summary>
    [Fact]
    public void CrabWriteDown_DoesNotAllocate_BeyondStructSize()
    {
        // Strategy: use GC.GetAllocatedBytesForCurrentThread() (current-thread only) and
        // measure over N=1000 update iterations.  Per-thread measurement is immune to
        // parallel xUnit test classes allocating heavily on other threads.
        // If List<IDisposable>/WriteLatchHandle class is still active, allocation is
        // ~300 bytes × N = 300 KB total → perInsert ≈ 300 bytes (above the 100-byte threshold).
        //
        // Update iterations (re-inserting the same key) traverse the full latch coupling
        // path (root → leaf) without causing splits, so all allocation comes from the
        // latch coupling layer alone.
        //
        // No WAL: eliminates byte[] WAL record allocations from the signal.
        // No EvictionWorker: no background thread to add cross-thread noise.
        string dbPath = Path.GetTempFileName();
        try
        {
            var opts = new BPlusTreeOptions
            {
                DataFilePath        = dbPath,
                WalFilePath         = dbPath + ".wal", // unused — no WAL opened
                PageSize            = 8192,
                BufferPoolCapacity  = 1024,
                CheckpointThreshold = 256,
            };
            var mgr  = PageManager.Open(opts, wal: null);
            var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
            var meta = new TreeMetadata(mgr);
            meta.Load();
            var eng  = new TreeEngine<int, int>(mgr, ns, meta);

            // Warm up: prime the JIT and build a stable tree structure.
            for (int i = 0; i < 200; i++) eng.Insert(i, i);

            GC.Collect(2, GCCollectionMode.Forced, blocking: true);
            GC.WaitForPendingFinalizers();

            const int N = 1000;
            // GetAllocatedBytesForCurrentThread() counts only THIS thread's allocations,
            // making the measurement immune to parallel xUnit test classes running on
            // other threads and allocating heavily during the same window.
            long before = GC.GetAllocatedBytesForCurrentThread();

            // N updates of existing key 42: in-place value replacement, no split.
            // Each iteration traverses the full write-latch-coupling path (root→leaf).
            for (int i = 0; i < N; i++) eng.Insert(42, i);

            long total     = GC.GetAllocatedBytesForCurrentThread() - before;
            long perInsert = total / N;

            // With InlineArray + WriteLatchHandle struct: perInsert ≈ 0 bytes.
            // With List<IDisposable> / WriteLatchHandle class: perInsert ≈ 300 bytes.
            // Threshold 100 bytes/insert comfortably separates the two cases while
            // allowing for Debug-mode metadata/boxing overhead on the current thread.
            perInsert.Should().BeLessThan(100,
                $"Per-insert heap allocation from latch coupling must be near zero. " +
                $"Got {perInsert} bytes/insert (total {total} bytes / {N} iterations). " +
                $"If ≥ 200 bytes/insert, List<IDisposable> or WriteLatchHandle class is " +
                $"still in use — Phase 33 regression.");

            eng.Close();
            mgr.Dispose();
        }
        finally
        {
            try { File.Delete(dbPath); } catch { }
        }
    }

    // ── Phase 34: ReadLatchHandle struct regression ────────────────────────────

    /// <summary>
    /// Verify that the latch returned by CrabReadDown is live (not already disposed).
    /// If the traversal loop disposes the leaf latch before returning, the caller holds
    /// a released lock — ExitReadLock without a held read lock throws
    /// SynchronizationLockException.
    ///
    /// Tested indirectly: 20 concurrent tasks performing 500 TryGet each.
    /// Any double-dispose on the leaf latch surfaces as a SynchronizationLockException.
    /// </summary>
    [Fact(Timeout = 20_000)]
    public async Task CrabReadDown_LeafLatch_NotDisposedBeforeReturn()
    {
        string dbPath = Path.GetTempFileName();
        try
        {
            var opts = new BPlusTreeOptions
            {
                DataFilePath       = dbPath,
                WalFilePath        = dbPath + ".wal",
                PageSize           = 8192,
                BufferPoolCapacity = 1024,
                CheckpointThreshold = 256,
            };
            var mgr  = PageManager.Open(opts, wal: null);
            var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
            var meta = new TreeMetadata(mgr);
            meta.Load();
            var eng  = new TreeEngine<int, int>(mgr, ns, meta);

            for (int i = 0; i < 1000; i++) eng.Insert(i, i);

            var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();
            var tasks = Enumerable.Range(0, 20).Select(_ => Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 500; i++) eng.TryGet(i % 1000, out _);
                }
                catch (Exception ex) { exceptions.Add(ex); }
            })).ToArray();

            await Task.WhenAll(tasks);

            exceptions.Should().BeEmpty(
                "CrabReadDown must return a live (non-disposed) leaf latch. " +
                "SynchronizationLockException indicates a latch was released twice, " +
                "meaning the leaf latch was disposed inside the traversal loop before return.");

            eng.Close();
            mgr.Dispose();
        }
        finally
        {
            try { File.Delete(dbPath); } catch { }
            try { File.Delete(dbPath + ".wal"); } catch { }
        }
    }

    /// <summary>
    /// Verify that ReadLatchHandle struct conversion eliminated read-path latch boxing.
    /// Uses GC.GetAllocatedBytesForCurrentThread() (per-thread — immune to xUnit parallelism).
    ///
    /// With IDisposable boxing: ~3 × 32 bytes × N TryGet = ~96 bytes/TryGet.
    /// With ReadLatchHandle struct: ~0 bytes/TryGet from latch layer.
    /// Threshold 50 bytes/TryGet separates the two cases.
    /// </summary>
    [Fact]
    public void TryGet_ReadPath_AllocatesNoLatchObjects()
    {
        string dbPath = Path.GetTempFileName();
        try
        {
            var opts = new BPlusTreeOptions
            {
                DataFilePath       = dbPath,
                WalFilePath        = dbPath + ".wal",
                PageSize           = 8192,
                BufferPoolCapacity = 1024,
                CheckpointThreshold = 256,
            };
            var mgr  = PageManager.Open(opts, wal: null);
            var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
            var meta = new TreeMetadata(mgr);
            meta.Load();
            var eng  = new TreeEngine<int, int>(mgr, ns, meta);

            // Use 2000 inserts to force a 2-level tree (leaf capacity ~1000 for 8KB int/int).
            // With a 2-level tree each TryGet calls CrabReadDown twice (root → leaf),
            // giving 2 × 32 = 64 bytes from boxing if still present.
            // Other read-path sources contribute ~72 bytes/TryGet (measured, Debug mode).
            // With boxing: ~72 + 64 = ~136 bytes/TryGet; without boxing: ~72 bytes/TryGet.
            // Threshold 90 sits cleanly between: 72 < 90 ✅, 136 > 90 ✅.
            for (int i = 0; i < 2000; i++) eng.Insert(i, i);

            // Warm up JIT and stabilise tree structure
            for (int i = 0; i < 200; i++) eng.TryGet(i, out _);

            GC.Collect(2, GCCollectionMode.Forced, blocking: true);
            GC.WaitForPendingFinalizers();

            const int N = 1000;
            long before = GC.GetAllocatedBytesForCurrentThread();

            for (int i = 0; i < N; i++) eng.TryGet(i % 2000, out _);

            long total     = GC.GetAllocatedBytesForCurrentThread() - before;
            long perLookup = total / N;

            // Phase 36: node wrappers eliminated — expected ≈ 0 bytes/TryGet.
            // Threshold 20 bytes/TryGet × 1000 ops = 20,000 bytes total.
            // If AsLeaf() or AsInternal() is still in the read path, allocation ≈ 72 bytes/TryGet
            // (32+40 bytes wrapper objects) → total ≈ 72,000 bytes → fails 20,000 threshold.
            perLookup.Should().BeLessThan(20,
                $"Per-TryGet heap allocation must be near zero after Phase 36 node wrapper elimination. " +
                $"Got {perLookup} bytes/TryGet (total {total} bytes / {N} calls). " +
                $"If ≥ 70 bytes/TryGet, AsLeaf() or AsInternal() is still in the read path.");

            eng.Close();
            mgr.Dispose();
        }
        finally
        {
            try { File.Delete(dbPath); } catch { }
            try { File.Delete(dbPath + ".wal"); } catch { }
        }
    }

    /// <summary>
    /// Verify that the static read-path eliminates LeafNode and InternalNode wrapper
    /// allocations on a multi-level tree (2+ internal levels, matching benchmark depth).
    /// Phase 34's test only covered 2 levels; this covers the deeper tree path. (Phase 36)
    /// </summary>
    [Fact]
    public void TryGet_MultiLevel_AllocatesNoNodeWrappers()
    {
        string dbPath = Path.GetTempFileName();
        try
        {
            var opts = new BPlusTreeOptions
            {
                DataFilePath        = dbPath,
                WalFilePath         = dbPath + ".wal",
                PageSize            = 8192,
                BufferPoolCapacity  = 2048,
                CheckpointThreshold = 256,
            };
            var mgr  = PageManager.Open(opts, wal: null);
            var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
            var meta = new TreeMetadata(mgr);
            meta.Load();
            var eng  = new TreeEngine<int, int>(mgr, ns, meta);

            // 200K inserts forces at least 2 internal node levels for 8KB int/int tree.
            for (int i = 0; i < 200_000; i++) eng.Insert(i, i);

            // Warm up
            for (int i = 0; i < 100; i++) eng.TryGet(i, out _);

            GC.Collect(2, GCCollectionMode.Forced, blocking: true);
            GC.WaitForPendingFinalizers();

            const int N = 1_000;
            long before = GC.GetAllocatedBytesForCurrentThread();

            for (int i = 0; i < N; i++) eng.TryGet(i % 200_000, out _);

            long total  = GC.GetAllocatedBytesForCurrentThread() - before;
            long perOp  = total / N;

            // With static path: ≈ 0 bytes/TryGet. Allow 20 bytes/TryGet for residual overhead.
            // If AsLeaf/AsInternal still used: ≈ 104 bytes/TryGet (2×32 + 1×40) → total ≈ 104,000.
            perOp.Should().BeLessThanOrEqualTo(20,
                $"TryGet on a multi-level tree must not allocate LeafNode or InternalNode wrappers. " +
                $"Got {perOp} bytes/TryGet (total {total} / {N} ops). " +
                $"If > 20 bytes/TryGet, the static read path is not in use — Phase 36 regression.");

            eng.Close();
            mgr.Dispose();
        }
        finally
        {
            try { File.Delete(dbPath); } catch { }
            try { File.Delete(dbPath + ".wal"); } catch { }
        }
    }

    [Fact(Timeout = 10_000)]
    public async Task ReleaseAll_ReleasesAllLatches_CountReset()
    {
        await Task.Run(() =>
        {
            var mgr = new PageLatchManager();

            WriteLatchHandle[] buf = new WriteLatchHandle[LatchCoupling.MaxTreeHeight];
            int count = 0;
            buf[count++] = mgr.AcquireWriteLatch(1);
            buf[count++] = mgr.AcquireWriteLatch(2);
            buf[count++] = mgr.AcquireWriteLatch(3);

            LatchCoupling.ReleaseAll(buf, ref count);

            count.Should().Be(0, "ReleaseAll must reset count to 0");
            // All three pages must now be acquirable.
            for (uint p = 1; p <= 3; p++)
                mgr.Invoking(m => m.AcquireWriteLatch(p).Dispose()).Should().NotThrow();
        });
    }
}
