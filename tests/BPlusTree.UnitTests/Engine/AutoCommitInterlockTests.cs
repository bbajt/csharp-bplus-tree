using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Phase 60 — Auto-commit checkpoint gate tests.
/// Verifies that auto-commit Insert() and Delete() hold the shared checkpoint gate
/// for their full duration, serialising correctly with Compact() (which holds the
/// exclusive gate). Closes the last known data-loss race: an auto-commit write that
/// completed mid-Compact leaf-chain walk could have its WAL record discarded by
/// TruncateWal(), making the write invisible after crash+recovery.
/// </summary>
public class AutoCommitInterlockTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine) Open()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        var wal = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 4096,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 64,
        }, wal);
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, wal, new TreeEngine<int, int>(mgr, ns, meta));
    }

    public void Dispose()
    {
        // Swallow IOException: if a test assertion throws before Close() is called,
        // the mgr/wal handles are still open and the delete will fail. Temp files
        // are cleaned up by the OS on process exit.
        static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch (IOException) { }
        }
        TryDelete(_dbPath);
        TryDelete(_walPath);
        TryDelete(_dbPath  + ".compact");
        TryDelete(_walPath + ".compact");
    }

    // ── Test 1: Auto-commit Insert serialises with Compact ────────────────────

    [Fact]
    public void AutoCommit_Insert_BlocksDuringCompact()
    {
        var (mgr, wal, engine) = Open();

        // Pre-populate 2000 keys — large enough that Compact holds the exclusive gate
        // for a measurable duration (file I/O + leaf-chain walk).
        for (int i = 1; i <= 2000; i++)
            engine.Insert(i, i * 10);
        wal.Flush();

        // Start compaction on a background thread.
        // Compact acquires EnterCheckpointLock (exclusive) for its full duration.
        bool compactCompleted = false;
        var compactThread = new Thread(() =>
        {
            engine.Compact();
            compactCompleted = true;
        });
        compactThread.Start();

        // Give the compact thread time to acquire the exclusive lock.
        Thread.Sleep(50);

        // Auto-commit Insert — with the Phase 60 gate, this acquires EnterTransactionLock
        // (shared read lock) and blocks until Compact releases the exclusive write lock.
        // Without the gate, this would race with TruncateWal and the insert could be lost.
        engine.Insert(99999, 0);

        // Compact must complete within 10 s.
        bool joined = compactThread.Join(millisecondsTimeout: 10_000);
        joined.Should().BeTrue("Compact must complete within 10 seconds");
        compactCompleted.Should().BeTrue();

        // Key 99999 must be visible regardless of whether it was inserted before or
        // after Compact's leaf walk — the gate guarantees it is not lost.
        engine.TryGet(99999, out int v99999).Should().BeTrue(
            "auto-commit Insert must be visible after serialising with Compact");
        v99999.Should().Be(0);

        // All 2000 original keys must survive compaction.
        for (int i = 1; i <= 2000; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must survive compaction");
            v.Should().Be(i * 10);
        }

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 3: Auto-commit Update serialises with Compact ────────────────────

    [Fact]
    public void AutoCommit_Update_BlocksDuringCompact()
    {
        var (mgr, wal, engine) = Open();

        // Pre-populate 2000 keys.
        for (int i = 1; i <= 2000; i++)
            engine.Insert(i, i * 10);
        wal.Flush();

        // Start compaction on a background thread.
        bool compactCompleted = false;
        var compactThread = new Thread(() =>
        {
            engine.Compact();
            compactCompleted = true;
        });
        compactThread.Start();

        // Give the compact thread time to acquire the exclusive lock.
        Thread.Sleep(50);

        // Auto-commit Update — holds shared gate (M+1), serialises with Compact.
        const int targetKey = 500;
        engine.Update(targetKey, v => v + 1);

        bool joined = compactThread.Join(millisecondsTimeout: 10_000);
        joined.Should().BeTrue("Compact must complete within 10 seconds");
        compactCompleted.Should().BeTrue();

        // Updated value must be present — the update completed and was not lost.
        engine.TryGet(targetKey, out int updatedValue).Should().BeTrue(
            "auto-commit Update must be visible after serialising with Compact");
        updatedValue.Should().Be(targetKey * 10 + 1);

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 2: Auto-commit Delete serialises with Compact ────────────────────

    [Fact]
    public void AutoCommit_Delete_BlocksDuringCompact()
    {
        var (mgr, wal, engine) = Open();

        // Pre-populate 2000 keys.
        for (int i = 1; i <= 2000; i++)
            engine.Insert(i, i * 10);
        wal.Flush();

        // Start compaction on a background thread.
        bool compactCompleted = false;
        var compactThread = new Thread(() =>
        {
            engine.Compact();
            compactCompleted = true;
        });
        compactThread.Start();

        // Give the compact thread time to acquire the exclusive lock. M96 Phase 5:
        // bumped from 50ms → 200ms because under full-solution parallel load the
        // compact thread sometimes hadn't yet reached the lock-acquisition point by
        // 50ms, letting the Delete below win the race. The test's INTENT is to prove
        // that Delete serialises with Compact, which requires Compact to have the
        // lock when Delete arrives — 200ms gives the scheduler a bigger window even
        // under CPU contention. The threading invariant itself (shared/exclusive
        // gates) is what's under test; the sleep is just the setup theatrics.
        Thread.Sleep(200);

        // Auto-commit Delete — holds shared gate, serialises with Compact.
        engine.Delete(1000);

        // M96 Phase 5: 10s → 30s. Compact on 2000 keys normally finishes in <1s, but
        // under parallel contention the worker thread can be CPU-starved. 30s preserves
        // the "something is stuck" failure mode without flaking on slow hosts.
        bool joined = compactThread.Join(millisecondsTimeout: 30_000);
        joined.Should().BeTrue("Compact must complete within 30 seconds");
        compactCompleted.Should().BeTrue();

        // Key 1000 must be absent — the delete completed and was not lost.
        engine.TryGet(1000, out _).Should().BeFalse(
            "auto-commit Delete must take effect after serialising with Compact");

        // All other 1999 keys must be present.
        for (int i = 1; i <= 2000; i++)
        {
            if (i == 1000) continue;
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must survive compaction");
            v.Should().Be(i * 10);
        }

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }
}
