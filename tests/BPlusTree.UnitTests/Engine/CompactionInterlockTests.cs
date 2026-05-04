using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Phase 58 / Phase 73 — Compaction/Transaction Interlock tests.
/// Verifies that Compact() holds the writer mutex for its entire duration (Phase 73),
/// preventing concurrent transactions from starting while compaction is in progress.
/// The exclusive checkpoint lock is held only for the short atomic swap
/// (WAL record + rename + reload + TruncateWal).
/// </summary>
public class CompactionInterlockTests : IDisposable
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
        // the mgr/wal handles are still open and the delete will fail. The temp files
        // will be cleaned up by the OS on process exit.
        static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch (IOException) { }
        }
        TryDelete(_dbPath);
        TryDelete(_walPath);
        TryDelete(_dbPath  + ".compact");
        TryDelete(_walPath + ".compact");
    }

    // ── Test 1: Active transaction defers Compact() until commit ─────────────

    [Fact]
    public void Compact_WaitsForActiveTransaction_ThenCompletes()
    {
        var (mgr, wal, engine) = Open();

        // Pre-populate so there is something to compact.
        for (int i = 1; i <= 20; i++)
            engine.Insert(i, i * 10);
        wal.Flush();

        // Begin a transaction and write a page — acquires EnterTransactionLock (shared gate).
        var tx = engine.BeginTransaction();
        tx.Insert(999, 0);

        // Start compaction on a background thread.
        // It must block at EnterWriterLock() until the transaction releases the writer mutex.
        bool compactionCompleted = false;
        var compactThread = new Thread(() =>
        {
            engine.Compact();
            compactionCompleted = true;
        });
        compactThread.Start();

        // Give the compact thread time to start and block on the mutex.
        Thread.Sleep(150);

        // Compaction must NOT have completed yet.
        // Phase 109a: Compact() blocks at WaitForTxQuiescence (barrier+drain) while
        // the transaction is in-flight, not at the writer mutex.
        compactionCompleted.Should().BeFalse(
            "Compact() must block while a transaction is in-flight (quiescence not yet reached)");

        // WAL must still contain records (TruncateWal has not run).
        wal.Flush();
        new FileInfo(_walPath).Length.Should().BeGreaterThan(WalRecordLayout.FileHeaderSize,
            "WAL must not be truncated while a transaction is in-flight");

        // Commit the transaction — releases EnterTransactionLock.
        tx.Commit();
        tx.Dispose();

        // Compaction must now complete.
        bool joined = compactThread.Join(millisecondsTimeout: 5000);
        joined.Should().BeTrue("Compact() must complete within 5 seconds after tx commits");
        compactionCompleted.Should().BeTrue("compaction must have completed");

        // WAL must be truncated to epoch header after compaction.
        new FileInfo(_walPath).Length.Should().Be(WalRecordLayout.FileHeaderSize,
            "Compact() calls TruncateWal() — WAL must be shrunk to the epoch header");

        // All records (including the tx record) must be readable.
        for (int i = 1; i <= 20; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue();
            v.Should().Be(i * 10);
        }
        engine.TryGet(999, out int txVal).Should().BeTrue();
        txVal.Should().Be(0);

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 2: Compact with no active transactions succeeds ─────────────────

    [Fact]
    public void Compact_WithNoActiveTransactions_Succeeds()
    {
        var (mgr, wal, engine) = Open();

        const int recordCount = 20;
        for (int i = 1; i <= recordCount; i++)
            engine.Insert(i, i * 100);

        // No active transaction — Compact() must complete immediately.
        engine.Compact();

        // All records must be readable after compaction.
        for (int i = 1; i <= recordCount; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must survive compaction");
            v.Should().Be(i * 100);
        }

        // WAL must be truncated to epoch header.
        new FileInfo(_walPath).Length.Should().Be(WalRecordLayout.FileHeaderSize,
            "Compact() must truncate the WAL to the epoch header");

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 3: LSN is preserved across Compact() (Phase 57 epoch header) ────

    [Fact]
    public void Compact_PreservesLsnMonotonicity()
    {
        var (mgr, wal, engine) = Open();

        for (int i = 1; i <= 10; i++)
            engine.Insert(i, i);
        wal.Flush();

        var lsnBeforeCompact = wal.CurrentLsn.Value;
        lsnBeforeCompact.Should().BeGreaterThan(0UL, "inserts must advance the WAL LSN");

        engine.Compact();

        // Phase 57: TruncateWal preserves _currentLsn — LSN must never reset to 0.
        // Note: Compact() appends one CompactionComplete record before TruncateWal,
        // so CurrentLsn after compact = lsnBeforeCompact + sizeof(CompactionComplete) ≥ lsnBeforeCompact.
        wal.CurrentLsn.Value.Should().BeGreaterThan(0UL,
            "Compact() must preserve LSN monotonicity — CurrentLsn must not reset to 0");

        // Inserts after compaction must advance the LSN from the preserved base.
        for (int i = 11; i <= 15; i++)
            engine.Insert(i, i);
        wal.Flush();

        wal.CurrentLsn.Value.Should().BeGreaterThan(lsnBeforeCompact,
            "post-compaction inserts must produce WAL records with higher LSNs");

        // All 15 records must be readable.
        for (int i = 1; i <= 15; i++)
            engine.TryGet(i, out _).Should().BeTrue($"key {i} must be readable after compaction + inserts");

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 4: Open snapshot does not block Compact() (Phase 73) ────────────
    // A snapshot holds an epoch token — not the writer mutex. Compaction acquires
    // the writer mutex, not the snapshot epoch. They must not deadlock or stall.
    // Verifies: (1) compaction completes while snapshot is open, (2) snapshot's
    // frozen view is correct post-compaction, (3) live tree is correct.

    [Fact]
    public void Compact_WithOpenSnapshot_CompactionSucceeds()
    {
        var (mgr, wal, engine) = Open();

        for (int i = 1; i <= 20; i++)
            engine.Insert(i, i * 10);
        wal.Flush();

        // Open a snapshot BEFORE compaction — holds only a read epoch, not the writer mutex.
        using var snap = engine.BeginSnapshot();

        // Compaction must complete even while the snapshot is open.
        engine.Compact();

        // Live tree (post-compaction) must contain all records.
        for (int i = 1; i <= 20; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must be in live tree after compaction");
            v.Should().Be(i * 10);
        }

        // Snapshot's frozen view must also be correct (captured before compaction started).
        for (int i = 1; i <= 20; i++)
        {
            snap.TryGet(i, out int sv).Should().BeTrue($"key {i} must be visible in snapshot");
            sv.Should().Be(i * 10);
        }

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }
}
