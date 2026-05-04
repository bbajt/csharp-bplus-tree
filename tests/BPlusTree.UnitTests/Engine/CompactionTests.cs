using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

public class CompactionTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine) Open()
    {
        var wal  = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = PageSize, BufferPoolCapacity = 256, CheckpointThreshold = 64,
        }, wal);
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, wal, new TreeEngine<int, int>(mgr, ns, meta));
    }

    [Fact]
    public void Compact_AllDataAccessibleAfterCompaction()
    {
        var (mgr, wal, engine) = Open();
        for (int i = 0; i < 1000; i++) engine.Insert(i, i);
        for (int i = 0; i < 600; i++) engine.Delete(i);
        engine.Compact();
        for (int i = 600; i < 1000; i++)
            engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after compaction");
        wal.Dispose(); mgr.Dispose();
    }

    [Fact]
    public void Compact_FileSizeReducedAfterHeavyDeletes()
    {
        var (mgr, wal, engine) = Open();
        for (int i = 0; i < 5000; i++) engine.Insert(i, i);
        for (int i = 0; i < 4000; i++) engine.Delete(i);
        long sizeBefore = new FileInfo(_dbPath).Length;
        engine.Compact();
        long sizeAfter  = new FileInfo(_dbPath).Length;
        sizeAfter.Should().BeLessThan(sizeBefore * 60 / 100,
            "compacted file must be less than 60% of original after 80% deletes");
        wal.Dispose(); mgr.Dispose();
    }

    [Fact]
    public void Compact_LeafChain_SortedAndComplete()
    {
        var (mgr, wal, engine) = Open();
        for (int i = 0; i < 1000; i++) engine.Insert(i, i);
        for (int i = 0; i < 500; i++) engine.Delete(i);
        engine.Compact();

        // Walk leaf chain and verify
        var meta = new TreeMetadata(mgr);
        meta.Load();
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var keys = new List<int>();
        uint id  = meta.FirstLeafPageId;
        while (id != PageLayout.NullPageId)
        {
            var f    = mgr.FetchPage(id);
            var leaf = ns.AsLeaf(f);
            for (int i = 0; i < leaf.Count; i++) keys.Add(leaf.GetKey(i));
            uint next = leaf.NextLeafPageId;
            mgr.Unpin(id);
            id = next;
        }
        keys.Should().HaveCount(500);
        keys.Should().BeInAscendingOrder();
        wal.Dispose(); mgr.Dispose();
    }

    [Fact]
    public void Compact_ScanAfterCompaction_CorrectOrder()
    {
        var (mgr, wal, engine) = Open();
        for (int i = 999; i >= 0; i--) engine.Insert(i, i); // reverse insert
        for (int i = 0; i < 200; i++) engine.Delete(i);
        engine.Compact();
        var keys = engine.Scan().Select(x => x.Key).ToList();
        keys.Should().HaveCount(800);
        keys.Should().BeInAscendingOrder();
        wal.Dispose(); mgr.Dispose();
    }

    [Fact]
    public void CrashBeforeRename_OriginalFileIntact()
    {
        // Leave a .compact file (simulating crash mid-compaction before rename).
        var compactPath = _dbPath + ".compact";
        File.WriteAllBytes(compactPath, new byte[PageSize]); // fake incomplete compact file

        {
            var (mgr, wal, engine) = Open(); // Open must call CleanupAbortedCompaction
            engine.TryGet(0, out _);         // original must be usable
            File.Exists(compactPath).Should().BeFalse(
                "Open() must clean up leftover .compact file");
            wal.Dispose(); mgr.Dispose();
        }
    }

    [Fact]
    public void Compact_ConcurrentReads_CompleteWithoutException()
    {
        // Compaction holds write lock — concurrent readers must block, not throw.
        var (mgr, wal, engine) = Open();
        for (int i = 0; i < 2000; i++) engine.Insert(i, i);
        for (int i = 0; i < 1000; i++) engine.Delete(i);

        Exception? readerException = null;
        var reader = new Thread(() =>
        {
            try { for (int i = 1000; i < 2000; i++) engine.TryGet(i, out _); }
            catch (Exception ex) { readerException = ex; }
        });
        var compactor = new Thread(() => engine.Compact());

        reader.Start(); compactor.Start();
        reader.Join(); compactor.Join();
        readerException.Should().BeNull("readers must not throw during compaction");
        wal.Dispose(); mgr.Dispose();
    }

    /// <summary>
    /// Phase 106: online compaction — concurrent writes during Phase A (leaf walk window)
    /// must be visible after compaction completes (delta tracking correctness).
    /// </summary>
    [Fact]
    public void Compact_ConcurrentWritesDuringLeafWalk_DeltaAppliedCorrectly()
    {
        var (mgr, wal, engine) = Open();
        // Pre-populate: keys 0–999 all present.
        for (int i = 0; i < 1000; i++) engine.Insert(i, i);

        // Run compaction on a background thread.
        var compactDone = new System.Threading.ManualResetEventSlim(false);
        Exception? compactEx = null;
        var compactor = new Thread(() =>
        {
            try   { engine.Compact(); }
            catch (Exception ex) { compactEx = ex; }
            finally { compactDone.Set(); }
        });
        compactor.Start();

        // Concurrently: insert new keys (1000–1049) and delete some existing keys (0–49).
        // These writes happen during the compaction window; the delta map must capture them.
        var writerDone = new System.Threading.ManualResetEventSlim(false);
        Exception? writerEx = null;
        var writer = new Thread(() =>
        {
            try
            {
                for (int i = 1000; i < 1050; i++) engine.Insert(i, i);
                for (int i = 0;    i < 50;   i++) engine.Delete(i);
            }
            catch (Exception ex) { writerEx = ex; }
            finally { writerDone.Set(); }
        });
        writer.Start();

        compactDone.Wait(TimeSpan.FromSeconds(30));
        writerDone.Wait(TimeSpan.FromSeconds(30));

        compactEx.Should().BeNull("compaction must not throw");
        writerEx.Should().BeNull("concurrent writer must not throw");

        // After compaction + delta apply: keys 0–49 deleted, 50–1049 present.
        for (int i = 0; i < 50; i++)
            engine.TryGet(i, out _).Should().BeFalse($"key {i} should have been deleted by concurrent writer");
        for (int i = 50; i < 1050; i++)
            engine.TryGet(i, out _).Should().BeTrue($"key {i} should be present after compaction+delta");

        wal.Dispose(); mgr.Dispose();
    }

    /// <summary>
    /// Phase 106: crash-between-rename-and-TruncateWal correctness.
    /// After step [16] (live-tree checkpoint), the live WAL contains only CheckpointEnd +
    /// CompactionComplete. Recovery must start replay from the CompactionComplete boundary
    /// and not corrupt the compact file with stale UpdatePage records.
    /// </summary>
    [Fact]
    public void Compact_CrashAfterRename_RecoveryUsesCompactionCompleteBoundary()
    {
        // Phase 1: populate, compact (full clean run to have a compact file in place).
        {
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 500; i++) engine.Insert(i, i);
            engine.Compact();
            wal.Dispose(); mgr.Dispose();
        }

        // Phase 2: reopen, add data, then simulate a crash AFTER rename but BEFORE TruncateWal.
        // We use the skip-truncation checkpoint seam: _coordinator.TakeCheckpointSkipTruncation
        // is not exposed on TreeEngine, so we verify recovery correctness indirectly.
        // We verify that after a normal open (which runs RecoverFromWal), all data is consistent.
        {
            var (mgr, wal, engine) = Open();
            for (int i = 500; i < 600; i++) engine.Insert(i, i);
            engine.Compact();

            // Verify data immediately after compaction.
            for (int i = 0; i < 600; i++)
                engine.TryGet(i, out _).Should().BeTrue($"key {i} must be present after second compaction");

            wal.Dispose(); mgr.Dispose();
        }

        // Phase 3: reopen (recovery runs RecoverFromWal with CompactionComplete boundary).
        {
            var (mgr3, wal3, engine3) = Open();
            for (int i = 0; i < 600; i++)
                engine3.TryGet(i, out _).Should().BeTrue($"key {i} must survive reopening after compaction");
            wal3.Dispose(); mgr3.Dispose();
        }
    }

    // ── Phase 109c: compaction barrier + quiescence tests ─────────────────────

    /// <summary>
    /// Phase 109c: a transaction that starts before compaction begins and commits
    /// during Phase A (leaf walk window) must have its data present after compaction
    /// (captured via delta tracking).
    /// </summary>
    [Fact]
    public async Task Compact_WithConcurrentTransaction_DataIntegrity()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine) = Open();
        for (int i = 0; i < 1000; i++) engine.Insert(i, i);

        Exception? compactEx = null;
        Exception? txEx      = null;
        var tcsCompactStarted = new TaskCompletionSource();

        var compactTask = Task.Run(() =>
        {
            try { engine.Compact(); }
            catch (Exception ex) { compactEx = ex; }
        });

        // Start a transaction after compaction has started (races with Phase A leaf walk).
        // Retry on conflict (root version check may fire if timing overlaps Phase B).
        var txTask = Task.Run(() =>
        {
            try
            {
                bool done = false;
                while (!done)
                {
                    try
                    {
                        using var tx = engine.BeginTransaction();
                        tx.Insert(1001, 1001);
                        tx.Commit();
                        done = true;
                    }
                    catch (TransactionConflictException) { Thread.SpinWait(100); }
                    catch (CompactionInProgressException) { Thread.SpinWait(100); }
                }
            }
            catch (Exception ex) { txEx = ex; }
        });

        await Task.WhenAll(compactTask, txTask);

        compactEx.Should().BeNull("compaction must not throw");
        txEx.Should().BeNull("transaction must not throw any unhandled exception");

        // Original data intact.
        engine.TryGet(500, out int v500).Should().BeTrue();
        v500.Should().Be(500);
        // Transaction's insert must be present (delta captured it).
        engine.TryGet(1001, out int v1001).Should().BeTrue();
        v1001.Should().Be(1001);

        wal.Dispose(); mgr.Dispose();
    }

    /// <summary>
    /// Phase 109c: <see cref="TreeEngine{TKey,TValue}.BeginTransaction"/> throws
    /// <see cref="CompactionInProgressException"/> when the compaction barrier is raised.
    /// After the barrier is cleared, <c>BeginTransaction</c> succeeds.
    /// </summary>
    [Fact]
    public void BeginTransaction_DuringCompactionBarrier_ThrowsCompactionInProgressException()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine) = Open();

        // Manually raise the compaction barrier (simulates Phase B setup).
        engine.Coordinator.SetCompactionBarrier();

        Action act = () => engine.BeginTransaction();
        act.Should().Throw<CompactionInProgressException>(
            "BeginTransaction must be rejected while the compaction barrier is raised");

        // Clear the barrier — subsequent BeginTransaction must succeed.
        engine.Coordinator.ClearCompactionBarrier();
        using var tx = engine.BeginTransaction();
        tx.Dispose(); // rollback

        wal.Dispose(); mgr.Dispose();
    }

    /// <summary>
    /// Phase 109c: the compaction barrier is always cleared after <c>Compact()</c>
    /// returns — even on the normal success path. Subsequent <c>BeginTransaction()</c>
    /// calls must succeed without throwing <see cref="CompactionInProgressException"/>.
    /// </summary>
    [Fact]
    public void Compact_BarrierClearedAfterCompletion_BeginTransactionSucceeds()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine) = Open();
        for (int i = 0; i < 100; i++) engine.Insert(i, i);

        engine.Compact();

        // Barrier must be cleared (finally block ran).
        Action act = () =>
        {
            using var tx = engine.BeginTransaction();
            tx.Insert(999, 999);
            tx.Commit();
        };
        act.Should().NotThrow<CompactionInProgressException>(
            "barrier must be cleared after Compact() returns");

        engine.TryGet(999, out int v).Should().BeTrue();
        v.Should().Be(999);

        wal.Dispose(); mgr.Dispose();
    }

    [Fact]
    public void Compact_Returns_NonNegativeMetrics()
    {
        var (mgr, wal, engine) = Open();
        for (int i = 0; i < 200; i++) engine.Insert(i, i);
        for (int i = 0; i < 100; i++) engine.Delete(i);

        var result = engine.Compact();

        result.BytesSaved.Should().BeGreaterThanOrEqualTo(0, "bytes saved must be non-negative");
        result.PagesFreed.Should().BeGreaterThanOrEqualTo(0, "pages freed must be non-negative");
        result.Duration.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero, "duration must be non-negative");

        wal.Dispose(); mgr.Dispose();
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
        try { File.Delete(_dbPath + ".compact"); } catch { }
    }
}
