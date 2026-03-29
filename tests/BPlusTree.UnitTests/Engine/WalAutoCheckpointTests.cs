using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Tests for WAL size-based auto-checkpoint (Phase 56).
/// All tests use pollIntervalMs=10 so the background thread fires quickly.
/// </summary>
public class WalAutoCheckpointTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine,
             NodeSerializer<int, int> ns, TreeMetadata meta) Open()
    {
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
        return (mgr, wal, new TreeEngine<int, int>(mgr, ns, meta), ns, meta);
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))  File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);
    }

    // ── Test 1: WAL exceeds threshold → checkpoint fires ─────────────────────

    [Fact]
    public void AutoCheckpoint_WalExceedsThreshold_CheckpointFires()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _, _) = Open();

        // Insert records to build up WAL content.
        for (int i = 1; i <= 30; i++)
            engine.Insert(i, i * 10);

        var walSizeAfterInserts = (long)wal.CurrentLsn.Value;
        walSizeAfterInserts.Should().BeGreaterThan(0, "inserts must produce WAL records");

        // Start auto-checkpoint with threshold below current WAL size → fires on first poll.
        engine.StartAutoCheckpoint(walSizeAfterInserts / 2, pollIntervalMs: 10);

        // Wait up to 3 s for TruncateWal() to shrink the file to just the epoch header.
        bool truncated = SpinWait.SpinUntil(
            () => new FileInfo(_walPath).Length == WalRecordLayout.FileHeaderSize,
            TimeSpan.FromSeconds(3));

        truncated.Should().BeTrue("auto-checkpoint should have truncated the WAL within 3 seconds");
        new FileInfo(_walPath).Length.Should().Be(WalRecordLayout.FileHeaderSize,
            "TruncateWal shrinks the file to the epoch header only");

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 2: Active transaction defers checkpoint until commit ─────────────

    [Fact]
    public void AutoCheckpoint_ActiveTransaction_DeferredUntilCommit()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _, _) = Open();

        // Pre-populate so WAL has content above threshold.
        for (int i = 1; i <= 20; i++)
            engine.Insert(i, i * 10);

        var walSizeAfterInserts = (long)wal.CurrentLsn.Value;
        walSizeAfterInserts.Should().BeGreaterThan(0);

        // Start a transaction and write a page — this acquires a write lock.
        var tx = engine.BeginTransaction();
        tx.Insert(999, 0);   // acquires write lock on the leaf page

        // Start auto-checkpoint with threshold below current WAL size.
        // The lock held by tx must prevent it from firing.
        engine.StartAutoCheckpoint(walSizeAfterInserts / 2, pollIntervalMs: 10);

        // Give the background thread several poll cycles while the lock is held.
        Thread.Sleep(150);

        // Checkpoint must NOT have fired: WAL file must still be larger than the epoch header.
        wal.Flush();   // ensure any buffered records are on disk before checking file size
        new FileInfo(_walPath).Length.Should().BeGreaterThan(WalRecordLayout.FileHeaderSize,
            "auto-checkpoint must not fire while a transaction holds a write lock");

        // Commit the transaction — releases the shared checkpoint gate.
        tx.Commit();
        tx.Dispose();

        // Now the checkpoint should fire.
        bool truncated = SpinWait.SpinUntil(
            () => new FileInfo(_walPath).Length == WalRecordLayout.FileHeaderSize,
            TimeSpan.FromSeconds(3));
        truncated.Should().BeTrue("auto-checkpoint should fire after lock is released");

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 3: Threshold = 0 → auto-checkpoint disabled ─────────────────────

    [Fact]
    public void AutoCheckpoint_Disabled_WhenThresholdIsZero()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _, _) = Open();

        // StartAutoCheckpoint with threshold=0 is a no-op.
        engine.StartAutoCheckpoint(walSizeThresholdBytes: 0, pollIntervalMs: 10);

        for (int i = 1; i <= 20; i++)
            engine.Insert(i, i * 10);

        // Wait long enough for the background thread to fire if it were running.
        Thread.Sleep(150);

        // WAL must NOT be truncated — file must be larger than the epoch header.
        wal.Flush();
        new FileInfo(_walPath).Length.Should().BeGreaterThan(WalRecordLayout.FileHeaderSize,
            "no auto-checkpoint thread is started when threshold is 0");

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }
}
