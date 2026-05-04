using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Wal;

/// <summary>
/// Phase 57 — WAL Epoch Header + Checkpoint/Transaction Mutex tests.
/// </summary>
public class WalEpochTests : IDisposable
{
    private readonly string _walPath = Path.GetTempFileName();
    private readonly string _dbPath  = Path.GetTempFileName();

    public void Dispose()
    {
        if (File.Exists(_walPath)) File.Delete(_walPath);
        if (File.Exists(_dbPath))  File.Delete(_dbPath);
    }

    // ── Test 1: LSN is monotonically increasing across TruncateWal ───────────

    [Fact]
    public void TruncateWal_LsnContinuesMonotonically()
    {
        using var wal = WalWriter.Open(_walPath, bufferSize: 64 * 1024);

        // Write some records and capture the LSN before truncation.
        wal.Append(WalRecordType.Begin, 1, 0, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
        wal.Append(WalRecordType.Commit, 1, 0, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
        wal.Flush();

        var lsnBeforeTruncation = wal.CurrentLsn.Value;
        lsnBeforeTruncation.Should().BeGreaterThan(0UL, "records should advance the LSN");

        // Truncate: file shrinks to epoch header; _currentLsn must NOT reset.
        wal.TruncateWal();

        wal.CurrentLsn.Value.Should().Be(lsnBeforeTruncation,
            "TruncateWal must preserve _currentLsn to keep LSNs monotonically increasing");

        // Post-truncation file should be exactly FileHeaderSize bytes.
        new FileInfo(_walPath).Length.Should().Be(WalRecordLayout.FileHeaderSize,
            "WAL file must be truncated to the epoch header only");

        // Write a new record after truncation — LSN must continue from where it left off.
        var postLsn = wal.Append(WalRecordType.Begin, 2, 0, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
        postLsn.Value.Should().Be(lsnBeforeTruncation,
            "first post-truncation record LSN must equal the preserved pre-truncation LSN");
    }

    // ── Test 2: WalReader reads post-truncation records correctly ─────────────

    [Fact]
    public void WalReader_AfterTruncation_ReadsPostTruncationRecords()
    {
        using var wal = WalWriter.Open(_walPath, bufferSize: 64 * 1024);

        // Write and truncate.
        wal.Append(WalRecordType.Begin, 1, 0, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
        wal.Flush();
        wal.TruncateWal();

        var lsnBase = wal.CurrentLsn.Value;  // preserved across truncation

        // Write a post-truncation record.
        var rec1Lsn = wal.Append(WalRecordType.Begin, 2, 0, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
        wal.Flush();

        // WalReader must find the record via ReadForward from LSN.None.
        var reader  = new WalReader(_walPath);
        var records = reader.ReadForward(LogSequenceNumber.None).ToList();

        records.Should().HaveCount(1, "only the post-truncation Begin record should be readable");
        records[0].Lsn.Value.Should().Be(rec1Lsn.Value,
            "the record's LSN must match the post-truncation assigned LSN");
        records[0].TransactionId.Should().Be(2u);

        // ReadAt must also work using the absolute LSN.
        var fetched = reader.ReadAt(rec1Lsn);
        fetched.Should().NotBeNull("ReadAt must locate the record by absolute LSN");
        fetched!.Value.TransactionId.Should().Be(2u);
    }

    // ── Test 3: Checkpoint/transaction mutex — checkpoint waits for tx ────────

    [Fact]
    public void Checkpoint_WaitsForActiveTransaction_MutexWorks()
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
        var ns     = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta   = new TreeMetadata(mgr);
        meta.Load();
        var engine = new TreeEngine<int, int>(mgr, ns, meta);

        // Pre-populate so WAL has content.
        for (int i = 1; i <= 20; i++)
            engine.Insert(i, i * 10);
        wal.Flush();

        // Begin a transaction — this acquires EnterTransactionLock (shared gate).
        var tx = engine.BeginTransaction();
        tx.Insert(999, 0);  // acquires page write lock

        // Run a checkpoint attempt on a background thread.
        // It must block in EnterCheckpointLock until the transaction commits.
        bool checkpointCompleted = false;
        var checkpointThread = new Thread(() =>
        {
            engine.Checkpoint();
            checkpointCompleted = true;
        });
        checkpointThread.Start();

        // Give the checkpoint thread time to start and block on the mutex.
        Thread.Sleep(100);

        // Checkpoint must NOT have completed yet (blocked by tx shared lock).
        checkpointCompleted.Should().BeFalse(
            "checkpoint must block while transaction holds the shared gate");

        // Commit — releases EnterTransactionLock; checkpoint can now proceed.
        tx.Commit();
        tx.Dispose();

        // Wait for checkpoint to complete.
        bool joined = checkpointThread.Join(millisecondsTimeout: 3000);
        joined.Should().BeTrue("checkpoint thread must complete within 3 seconds after tx commits");
        checkpointCompleted.Should().BeTrue("checkpoint must complete after transaction releases the gate");

        // WAL must have been truncated to epoch header only.
        new FileInfo(_walPath).Length.Should().Be(WalRecordLayout.FileHeaderSize,
            "checkpoint must truncate the WAL to the epoch header");

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }
}
