using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// All 8 crash/recovery scenarios.  Each test simulates a crash at a precise point,
/// then reopens and verifies the tree is consistent and correct.
///
/// Crash simulation strategy: manipulate WAL and data files directly
/// (truncate, corrupt bytes, delete data file) rather than using a mock,
/// because the recovery path must handle real filesystem artifacts.
/// </summary>
public class RecoveryTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    /// <summary>
    /// Open or reopen the database. RecoverFromWal() is called automatically inside
    /// the TreeEngine constructor when a WalWriter is present.
    /// </summary>
    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine) Open()
    {
        var wal  = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = PageSize,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 256,
        }, wal);
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        // TreeEngine ctor calls RecoverFromWal(), which reloads meta + free list.
        return (mgr, wal, new TreeEngine<int, int>(mgr, ns, meta));
    }

    // ── Scenario 1: crash after WAL flush, before data page flush ────────────
    [Fact]
    public void Scenario1_WalFlushed_DataPageNotFlushed_RecoveryRestoresData()
    {
        // Write phase: insert 100 keys, flush WAL explicitly, then simulate crash
        // by overwriting the data file with a blank page (all zeros).
        // WAL records for all inserts survive; page data is lost.
        {
            if (File.Exists(_dbPath)) File.Delete(_dbPath);
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 100; i++) engine.Insert(i, i);
            wal.Flush();      // WAL on disk
            wal.Dispose(); mgr.Dispose();
            // Simulate crash: overwrite data file so only one blank page remains.
            File.WriteAllBytes(_dbPath, new byte[PageSize]);
        }
        // Recovery phase: Open() invokes RecoverFromWal() which replays all WAL records.
        {
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 100; i++)
                engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after Scenario 1 recovery");
            AssertTreeValid(mgr);
            wal.Dispose(); mgr.Dispose();
        }
    }

    // ── Scenario 2: WAL-Before-Page invariant enforced structurally ──────────
    [Fact]
    public void Scenario2_DirectDirtyWithoutWal_ThrowsInvalidOperation()
    {
        var (mgr, wal, _) = Open();
        var frame = mgr.AllocatePage(PageType.Leaf);
        // Calling with bypassWal:false when a WAL is attached must throw —
        // callers must either supply a walLsn or use the auto-log overload.
        mgr.Invoking(m => m.MarkDirtyAndUnpin(frame.PageId, bypassWal: false))
           .Should().Throw<InvalidOperationException>(
               "WAL-Before-Page invariant must be enforced structurally");
        // Unpin to avoid buffer-pool pin leak.
        mgr.Unpin(frame.PageId);
        wal.Dispose(); mgr.Dispose();
    }

    // ── Scenario 3: crash mid-checkpoint (Begin written, End not written) ────
    [Fact]
    public void Scenario3_CrashMidCheckpoint_ReplayFromPreviousCheckpoint()
    {
        // Step 1: insert 100 keys and take a COMPLETE checkpoint — establishes a clean baseline.
        // Step 2: insert 100 more keys, then explicitly write a CheckpointBegin record
        //         (simulating the start of a second checkpoint that never completes).
        //         Truncate the WAL to a partial CheckpointBegin header.
        // Recovery: no CheckpointEnd found → replay from LSN 0 → all 200 keys restored.
        {
            if (File.Exists(_dbPath)) File.Delete(_dbPath);
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 100; i++) engine.Insert(i, i);
            engine.Checkpoint();      // complete checkpoint #1 — all pages on disk, WAL truncated

            for (int i = 100; i < 200; i++) engine.Insert(i, i);
            wal.Flush();

            // Simulate start of a second checkpoint: write CheckpointBegin, then crash.
            wal.Append(WalRecordType.CheckpointBegin, 0, 0, LogSequenceNumber.None,
                       ReadOnlySpan<byte>.Empty);
            wal.Flush();

            // Truncate WAL to just the header of the last CheckpointBegin record.
            long beginOffset = FindLastCheckpointBeginOffset(_walPath);
            wal.Dispose(); mgr.Dispose();
            using var fs = new FileStream(_walPath, FileMode.Open, FileAccess.Write, FileShare.None);
            fs.SetLength(beginOffset + WalRecordLayout.FixedHeaderSize);
        }
        {
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 200; i++)
                engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after Scenario 3 recovery");
            AssertTreeValid(mgr);
            wal.Dispose(); mgr.Dispose();
        }
    }

    // ── Scenario 4: crash after CheckpointEnd, before WAL truncation ─────────
    [Fact]
    public void Scenario4_CrashAfterCheckpointEnd_BeforeWalTruncation_RecoveryIdempotent()
    {
        // Manually run checkpoint steps 1-5 (write CheckpointEnd + fsync WAL) but
        // skip step 6 (TruncateWal) to simulate a crash between WAL fsync and rename/truncate.
        // Recovery must replay forward from the CheckpointEnd it finds, which is a no-op
        // for page data (all pages were flushed in step 2) — idempotent. ✓
        {
            if (File.Exists(_dbPath)) File.Delete(_dbPath);
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 200; i++) engine.Insert(i, i);

            // Load a fresh metadata view to pass to the test-seam checkpoint manager.
            var metaForCkpt = new TreeMetadata(mgr);
            metaForCkpt.Load();
            var ckptMgr = new CheckpointManager(mgr, wal, metaForCkpt, _walPath);
            ckptMgr.TakeCheckpointSkipTruncation();   // leaves WAL un-truncated

            wal.Dispose(); mgr.Dispose();
        }
        {
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 200; i++)
                engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after Scenario 4 recovery");
            AssertTreeValid(mgr);
            wal.Dispose(); mgr.Dispose();
        }
    }

    // ── Scenario 5: truncated WAL tail (partial last record) ─────────────────
    [Fact]
    public void Scenario5_TruncatedWalTail_RecoveryStopsGracefully()
    {
        {
            if (File.Exists(_dbPath)) File.Delete(_dbPath);
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 100; i++) engine.Insert(i, i);
            wal.Flush();
            wal.Dispose(); mgr.Dispose();
            // Corrupt WAL tail: remove last 5 bytes.
            var bytes = File.ReadAllBytes(_walPath);
            File.WriteAllBytes(_walPath, bytes[..^5]);
        }
        // Recovery must not throw; it applies all records up to the truncated one.
        // Phase 42: RecoverFromWal() now recomputes TotalRecordCount from the actual
        // leaf chain after any WAL replay, so the count-desync (leaf chain = 100,
        // metadata = 99) that caused AssertTreeValid to fail in Phase 41 is fixed.
        Action recover = () =>
        {
            var (mgr, wal, _) = Open();
            AssertTreeValid(mgr);
            wal.Dispose(); mgr.Dispose();
        };
        recover.Should().NotThrow("corrupted WAL tail must be silently stopped, not thrown");
    }

    // ── Scenario 6: empty WAL (brand-new database, no prior writes) ──────────
    [Fact]
    public void Scenario6_EmptyWal_RecoveryIsNoOp_TreeUsable()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        File.WriteAllBytes(_walPath, Array.Empty<byte>());
        var (mgr, wal, engine) = Open();
        engine.Invoking(e => e.TryGet(0, out _)).Should().NotThrow();
        engine.Insert(42, 99);
        engine.TryGet(42, out int v).Should().BeTrue();
        v.Should().Be(99);
        AssertTreeValid(mgr);
        wal.Dispose(); mgr.Dispose();
    }

    // ── Scenario 9: GroupCommit + FlushUpTo → crash before checkpoint → WAL replay ──
    [Fact]
    public void Scenario9_GroupCommitFlush_CrashBeforeCheckpoint_WalReplayRestoresData()
    {
        // Phase 1: write 200 keys in GroupCommit mode, flush WAL, then crash (no checkpoint).
        // FlushIntervalMs=10000 ensures the background thread won't auto-flush during inserts.
        {
            if (File.Exists(_dbPath)) File.Delete(_dbPath);
            var wal = WalWriter.Open(_walPath, bufferSize: 512 * 1024,
                syncMode: WalSyncMode.GroupCommit, flushIntervalMs: 10_000, flushBatchSize: 65_536);
            var mgr = PageManager.Open(new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = PageSize,
                BufferPoolCapacity  = 128,
                CheckpointThreshold = 256,
            }, wal);
            var ns     = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
            var meta   = new TreeMetadata(mgr);
            meta.Load();
            var engine = new TreeEngine<int, int>(mgr, ns, meta);
            for (int i = 0; i < 200; i++) engine.Insert(i, i * 10);

            // Flush WAL — this is the operation under test. All 200 records are now durable.
            wal.FlushUpTo(wal.CurrentLsn.Value);

            // Crash simulation: dispose without checkpoint (no engine.Close()).
            wal.Dispose(); mgr.Dispose();

            // Wipe data file to force full WAL replay on re-open.
            File.WriteAllBytes(_dbPath, new byte[PageSize]);
        }

        // Phase 2: re-open in Synchronous mode — recovery replays all 200 WAL records.
        {
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 200; i++)
            {
                engine.TryGet(i, out int v).Should().BeTrue($"key {i} missing after Scenario 9 recovery");
                v.Should().Be(i * 10);
            }
            AssertTreeValid(mgr);
            wal.Dispose(); mgr.Dispose();
        }
    }

    // ── Recovery idempotency ─────────────────────────────────────────────────
    [Fact]
    public void Recovery_Idempotent_RunTwice_SameState()
    {
        {
            if (File.Exists(_dbPath)) File.Delete(_dbPath);
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 100; i++) engine.Insert(i, i);
            wal.Flush();
            wal.Dispose(); mgr.Dispose();
        }
        // First recovery (WAL not truncated — no checkpoint was taken).
        {
            var (mgr1, wal1, _) = Open();
            wal1.Dispose(); mgr1.Dispose();
        }
        // Second recovery on the same WAL: pages already on disk, records skipped (idempotent).
        {
            var (mgr2, wal2, engine2) = Open();
            for (int i = 0; i < 100; i++)
                engine2.TryGet(i, out _).Should().BeTrue($"key {i} missing after double recovery");
            AssertTreeValid(mgr2);
            wal2.Dispose(); mgr2.Dispose();
        }
    }

    // ── Crash during split, recovery produces consistent tree ────────────────
    [Fact]
    public void CrashDuringSplit_RecoveryProducesConsistentTree()
    {
        {
            if (File.Exists(_dbPath)) File.Delete(_dbPath);
            var (mgr, wal, engine) = Open();
            // Insert enough to guarantee many splits across multiple tree levels.
            for (int i = 0; i < 700; i++) engine.Insert(i, i);
            wal.Flush();
            wal.Dispose(); mgr.Dispose();
            // Wipe data file to force full WAL replay (all splits must be redone).
            File.WriteAllBytes(_dbPath, new byte[PageSize]);
        }
        {
            var (mgr, wal, engine) = Open();
            var keys = engine.Scan().Select(x => x.Key).ToList();
            keys.Should().HaveCount(700);
            keys.Should().BeInAscendingOrder();
            AssertTreeValid(mgr);
            wal.Dispose(); mgr.Dispose();
        }
    }

    // ── PostRecoveryValidation firing contract ───────────────────────────────
    [Fact]
    public void PostRecoveryValidation_FiresAfterReplay_NotOnCleanOpen()
    {
        // Phase 1: insert 50 records, then blank the data file.
        // mgr.Dispose() flushes dirty pages (now stamps PageLsn into each page).
        // Overwriting the data file afterwards simulates a crash where page data
        // is genuinely lost — forcing the Redo Pass guard to fire (frame.PageLsn = 0
        // from the blank file < any WAL record LSN → anyReplayed = true).
        {
            if (File.Exists(_dbPath)) File.Delete(_dbPath);
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 50; i++) engine.Insert(i, i * 10);
            wal.Flush();
            wal.Dispose(); mgr.Dispose();
            // Blank the data file: all on-disk PageLsn values become 0.
            File.WriteAllBytes(_dbPath, new byte[PageSize]);
        }

        var opts = new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = PageSize,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 256,
        };

        // Phase 2: recovery open — delegate MUST fire (anyReplayed = true).
        // Construct CheckpointManager directly so the custom delegate can be registered
        // before RecoverFromWal() is called. TreeEngine constructs its own internal
        // CheckpointManager and offers no injection point.
        bool firedOnRecovery = false;
        {
            var wal     = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
            var mgr     = PageManager.Open(opts, wal);
            var meta    = new TreeMetadata(mgr);
            meta.Load();

            var ckptMgr = new CheckpointManager(mgr, wal, meta, _walPath);
            ckptMgr.PostRecoveryValidation = () => firedOnRecovery = true;
            ckptMgr.RecoverFromWal();

            firedOnRecovery.Should().BeTrue(
                "WAL has 50 insert records → anyReplayed = true → delegate must fire");

            // GracefulClose: after Change A, TruncateWal runs after _metadata.Flush(),
            // so the UpdateMeta record is discarded. WAL is left with only CheckpointEnd.
            ckptMgr.GracefulClose();
            wal.Dispose(); mgr.Dispose();
        }

        // Phase 3: clean open after graceful close — delegate MUST NOT fire (anyReplayed = false).
        // After Change A, GracefulClose() leaves WAL containing only CheckpointEnd.
        // RecoverFromWal() finds CheckpointEnd, scans forward — nothing past it — anyReplayed = false.

        bool firedOnCleanOpen = false;
        {
            var wal     = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
            var mgr     = PageManager.Open(opts, wal);
            var meta    = new TreeMetadata(mgr);
            meta.Load();

            var ckptMgr = new CheckpointManager(mgr, wal, meta, _walPath);
            ckptMgr.PostRecoveryValidation = () => firedOnCleanOpen = true;
            ckptMgr.RecoverFromWal();

            firedOnCleanOpen.Should().BeFalse(
                "graceful close → WAL has only CheckpointEnd → anyReplayed = false → delegate must not fire");

            wal.Dispose(); mgr.Dispose();
        }
    }

    // ── Phase 50: PageLsn persistence tests ──────────────────────────────────

    /// <summary>
    /// Stamp-restore round-trip: PageLsn written into page data before flush must be
    /// read back into frame.PageLsn after a cold page load on the next open.
    /// </summary>
    [Fact]
    public void PageLsn_PersistedToDisk_SurvivesGracefulClose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        // Insert enough records to dirty the root page, then graceful-close.
        {
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 100; i++) engine.Insert(i, i * 10);
            engine.Close();   // GracefulClose: checkpoint + flush stamps PageLsn into frame.Data
            wal.Dispose();
            mgr.Dispose();
        }

        // Reopen a raw PageManager — no TreeEngine, RecoverFromWal is NOT called.
        var wal2 = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr2 = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = PageSize,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 256,
        }, wal2);

        var meta2       = new TreeMetadata(mgr2);
        meta2.Load();
        uint rootPageId = meta2.RootPageId;
        rootPageId.Should().NotBe(PageLayout.NullPageId, "tree must have a root after inserts");

        // Cold load the root page: restore reads PageLsn from frame.Data bytes 20–27.
        var frame = mgr2.FetchPage(rootPageId);
        frame.PageLsn.Should().BeGreaterThan(0,
            "PageLsn must survive flush-to-disk and be restored into frame on cold page load");
        mgr2.Unpin(rootPageId);
        wal2.Dispose();
        mgr2.Dispose();
    }

    /// <summary>
    /// After a graceful close (all pages flushed with current PageLsn stamped on disk),
    /// RecoverFromWal must apply zero after-images — the Redo Pass guard skips every record.
    /// Verified via PostRecoveryValidation: the delegate fires only when anyReplayed = true.
    /// </summary>
    [Fact]
    public void RedoPass_CleanOpen_ZeroRecordsApplied()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        {
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 1000; i++) engine.Insert(i, i * 10);
            engine.Close();   // GracefulClose: all pages on disk at current PageLsn
            wal.Dispose();
            mgr.Dispose();
        }

        // Reopen — TreeEngine ctor calls RecoverFromWal() with no delegate yet set.
        var (mgr2, wal2, engine2) = Open();

        // Now set the delegate and call RecoverFromWal() again on the already-recovered tree.
        // Since all pages are at the correct PageLsn, the Redo Pass guard skips every record
        // and anyReplayed stays false → delegate must NOT fire.
        bool delegateFired = false;
        engine2.CheckpointManager!.PostRecoveryValidation = () => delegateFired = true;
        engine2.CheckpointManager!.RecoverFromWal();

        delegateFired.Should().BeFalse(
            "graceful close leaves all pages at correct LSN — Redo Pass applies zero records");

        engine2.Close();
        wal2.Dispose();
        mgr2.Dispose();
    }

    /// <summary>
    /// Crash open: with a small buffer pool (10 pages) some pages are flushed to disk by
    /// eviction mid-insert, some are not. After crash + reopen, the Redo Pass replays only
    /// the pages whose on-disk LSN lags behind the WAL. All records must be readable.
    /// </summary>
    [Fact]
    public void RedoPass_CrashOpen_OnlyDirtyPagesReplayed()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        // Write phase: tiny buffer pool forces frequent eviction (pages flushed mid-insert).
        {
            var wal = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
            var mgr = PageManager.Open(new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = PageSize,
                BufferPoolCapacity  = 10,   // forces eviction during inserts
                CheckpointThreshold = 4096,
            }, wal);
            var ns     = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
            var meta   = new TreeMetadata(mgr);
            meta.Load();
            var engine = new TreeEngine<int, int>(mgr, ns, meta);
            for (int i = 0; i < 200; i++) engine.Insert(i, i * 10);
            wal.Flush();      // WAL on disk
            wal.Dispose();
            mgr.Dispose();    // flush remaining dirty pages; no checkpoint
        }

        // Recovery phase: standard open (RecoverFromWal runs in TreeEngine ctor).
        var (mgr2, wal2, engine2) = Open();
        for (int i = 0; i < 200; i++)
        {
            engine2.TryGet(i, out int v).Should().BeTrue($"key {i} missing after crash+recovery");
            v.Should().Be(i * 10);
        }
        AssertTreeValid(mgr2);
        engine2.Close();
        wal2.Dispose();
        mgr2.Dispose();
    }

    /// <summary>
    /// Guard unit test: after a checkpoint all pages on disk have PageLsn == the LSN of
    /// their most recent WAL record. On a subsequent cold load, frame.PageLsn (restored
    /// from disk) must be >= every WAL record LSN for that page — the guard would skip them.
    /// </summary>
    [Fact]
    public void RedoPass_GuardSkipsAlreadyCurrentPage()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        // Write phase: insert records and force a checkpoint so all pages are flushed.
        ulong rootPageId;
        {
            var (mgr, wal, engine) = Open();
            for (int i = 0; i < 50; i++) engine.Insert(i, i * 10);
            engine.Close();   // GracefulClose includes checkpoint — stamps PageLsn on every page
            var meta = new TreeMetadata(mgr);
            meta.Load();
            rootPageId = meta.RootPageId;
            wal.Dispose();
            mgr.Dispose();
        }

        // Find the highest WAL record LSN for the root page before the checkpoint truncated the WAL.
        // After GracefulClose the WAL is truncated to just the CheckpointEnd record, so we inspect
        // the on-disk frame.PageLsn directly against a re-read of the page.
        var wal2 = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr2 = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = PageSize,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 256,
        }, wal2);

        // Cold load the root page — restore reads PageLsn from bytes 20–27 in frame.Data.
        var frame = mgr2.FetchPage((uint)rootPageId);
        ulong restoredLsn = frame.PageLsn;
        mgr2.Unpin((uint)rootPageId);

        // The restored LSN must be > 0 (page was written at least once) and the guard
        // frame.PageLsn < record.Lsn must be false for any record at or below restoredLsn.
        restoredLsn.Should().BeGreaterThan(0,
            "root page must have been written and its LSN stamped to disk");

        // Re-read the WAL from disk: any UpdatePage/UpdateMeta record for the root page
        // must have Lsn.Value <= restoredLsn (guard would skip it — page already current).
        var reader = new WalReader(_walPath);
        foreach (var record in reader.ReadForward(LogSequenceNumber.None))
        {
            if (record.PageId != (uint)rootPageId) continue;
            if (record.Type != WalRecordType.UpdatePage && record.Type != WalRecordType.UpdateMeta) continue;
            record.Lsn.Value.Should().BeLessOrEqualTo(restoredLsn,
                $"WAL record LSN {record.Lsn.Value} exceeds on-disk PageLsn {restoredLsn} — guard would incorrectly apply it");
        }

        wal2.Dispose();
        mgr2.Dispose();
    }

    // ── Transactional recovery scenarios ─────────────────────────────────────

    /// <summary>
    /// Committed transaction survives crash + WAL replay.
    ///
    /// Write phase: insert 50 keys (auto-commit), then insert key 9999 via a
    /// committed transaction. Flush WAL so it is durable, then simulate crash by
    /// overwriting the data file. Recovery must replay all WAL records including
    /// the transactional UpdatePage / UpdateMeta records and restore the committed tree.
    /// </summary>
    [Fact]
    public void Recovery_CommittedTransaction_SurvivesCrash()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        {
            var (mgr, wal, engine) = Open();
            for (int i = 1; i <= 50; i++) engine.Insert(i, i * 10);

            using var tx = engine.BeginTransaction();
            tx.Insert(9999, 777);
            tx.Commit();

            wal.Flush();            // WAL durable on disk
            wal.Dispose();
            mgr.Dispose();          // closes handles; does NOT take a checkpoint
            File.WriteAllBytes(_dbPath, new byte[PageSize]);  // simulate data loss
        }
        {
            var (mgr, wal, engine) = Open();     // RecoverFromWal runs in ctor
            engine.TryGet(9999, out int v).Should().BeTrue(
                "committed transactional insert must survive crash+recovery");
            v.Should().Be(777);
            for (int i = 1; i <= 50; i++)
                engine.TryGet(i, out _).Should().BeTrue($"auto-commit key {i} must survive recovery");
            AssertTreeValid(mgr);
            engine.Close(); wal.Dispose(); mgr.Dispose();
        }
    }

    /// <summary>
    /// Uncommitted transaction data is absent after crash + WAL replay.
    ///
    /// Write phase: insert 50 keys (auto-commit), then insert key 9999 via a
    /// transaction that is NEVER committed. Flush WAL (Begin + UpdatePage records
    /// are durable, but no Commit record). Simulate crash. Recovery replays the
    /// shadow page UpdatePage records, but since ApplyCoWTxCommit was never called
    /// there is no UpdateMeta for the root change — the live root still refers to
    /// the original tree. Key 9999 must be invisible after recovery.
    /// </summary>
    [Fact]
    public void Recovery_UncommittedTransaction_DataAbsentAfterCrash()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        {
            var (mgr, wal, engine) = Open();
            for (int i = 1; i <= 50; i++) engine.Insert(i, i * 10);

            var tx = engine.BeginTransaction();
            tx.Insert(9999, 777);
            // Deliberate: no tx.Commit() and no tx.Dispose() — simulates crash mid-tx.

            wal.Flush();            // Begin + UpdatePage records durable; no Commit
            wal.Dispose();
            mgr.Dispose();
            File.WriteAllBytes(_dbPath, new byte[PageSize]);
        }
        {
            var (mgr, wal, engine) = Open();
            engine.TryGet(9999, out _).Should().BeFalse(
                "uncommitted transactional insert must not be visible after crash+recovery");
            for (int i = 1; i <= 50; i++)
                engine.TryGet(i, out _).Should().BeTrue($"auto-commit key {i} must survive recovery");
            AssertTreeValid(mgr);
            engine.Close(); wal.Dispose(); mgr.Dispose();
        }
    }

    /// <summary>
    /// Committed transaction with a leaf split survives crash + WAL replay.
    ///
    /// Uses PageSize=4096 so the leaf holds ~337 int/int records. Pre-populates
    /// 337 keys (fills the leaf), then commits a transaction that inserts key 338,
    /// forcing a split. The split creates shadow pages and an UpdateMeta record for
    /// the new root. After crash + recovery all records including key 338 must be
    /// present and the tree must be structurally valid.
    /// </summary>
    [Fact]
    public void Recovery_CommittedTransactionWithSplit_SurvivesCrash()
    {
        const int splitPageSize = 4096;
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        {
            var wal = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
            var mgr = PageManager.Open(new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = splitPageSize,
                BufferPoolCapacity  = 128,
                CheckpointThreshold = 4096,
            }, wal);
            var ns     = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
            var meta   = new TreeMetadata(mgr);
            meta.Load();
            var engine = new TreeEngine<int, int>(mgr, ns, meta);

            for (int i = 1; i <= 337; i++) engine.Insert(i, i * 10);   // fills leaf

            using var tx = engine.BeginTransaction();
            tx.Insert(338, 3380);   // triggers split in CoW path
            tx.Commit();

            wal.Flush();
            wal.Dispose();
            mgr.Dispose();
            File.WriteAllBytes(_dbPath, new byte[splitPageSize]);
        }
        {
            var wal = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
            var mgr = PageManager.Open(new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = splitPageSize,
                BufferPoolCapacity  = 128,
                CheckpointThreshold = 4096,
            }, wal);
            var ns     = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
            var meta   = new TreeMetadata(mgr);
            meta.Load();
            var engine = new TreeEngine<int, int>(mgr, ns, meta);

            engine.TryGet(338, out int v).Should().BeTrue(
                "committed transactional insert with split must survive crash+recovery");
            v.Should().Be(3380);
            for (int i = 1; i <= 337; i++)
                engine.TryGet(i, out _).Should().BeTrue($"pre-split key {i} must survive recovery");
            AssertTreeValid(mgr);
            engine.Close(); wal.Dispose(); mgr.Dispose();
        }
    }

    // ── Transactional recovery — M+10 ────────────────────────────────────────

    /// <summary>
    /// Committed transactional delete that triggers a leaf merge + root collapse
    /// survives crash + WAL replay.
    ///
    /// Setup: 338 keys → two leaves of 169 each (pageSize=4096, threshold=169).
    /// Transaction: delete keys 1 and 2 from the left leaf (169 → 167 &lt; 169).
    /// Neither sibling has &gt; 169 keys so the Merger merges both leaves into one
    /// and the single-child root collapses. The Merger emits UpdatePage records for
    /// the shadow leaf, the right sibling (now freed), and the parent, plus the
    /// deferred-free entry. ApplyCoWTxCommit writes the final UpdateMeta.
    /// Recovery must replay this multi-record sequence and produce a valid tree.
    /// </summary>
    [Fact]
    public void Recovery_CommittedDelete_MergeAndCollapse_SurvivesCrash()
    {
        const int splitPageSize = 4096;
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        {
            var wal = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
            var mgr = PageManager.Open(new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = splitPageSize,
                BufferPoolCapacity  = 128,
                CheckpointThreshold = 4096,
            }, wal);
            var ns     = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
            var meta   = new TreeMetadata(mgr);
            meta.Load();
            var engine = new TreeEngine<int, int>(mgr, ns, meta);

            // Fill to 338 keys — triggers split into two leaves of 169 each.
            for (int i = 1; i <= 338; i++) engine.Insert(i, i * 10);

            // Delete 2 from the left leaf (169 → 167 < threshold 169) → merge + root collapse.
            using var tx = engine.BeginTransaction();
            tx.TryDelete(1).Should().BeTrue();
            tx.TryDelete(2).Should().BeTrue();
            tx.Commit();

            wal.Flush();
            wal.Dispose();
            mgr.Dispose();
            File.WriteAllBytes(_dbPath, new byte[splitPageSize]);
        }
        {
            var wal = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
            var mgr = PageManager.Open(new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = splitPageSize,
                BufferPoolCapacity  = 128,
                CheckpointThreshold = 4096,
            }, wal);
            var ns     = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
            var meta   = new TreeMetadata(mgr);
            meta.Load();
            var engine = new TreeEngine<int, int>(mgr, ns, meta);

            engine.TryGet(1, out _).Should().BeFalse("deleted key 1 must be absent after recovery");
            engine.TryGet(2, out _).Should().BeFalse("deleted key 2 must be absent after recovery");
            for (int i = 3; i <= 338; i++)
                engine.TryGet(i, out _).Should().BeTrue($"key {i} must survive recovery");
            AssertTreeValid(mgr);
            engine.Close(); wal.Dispose(); mgr.Dispose();
        }
    }

    /// <summary>
    /// Committed transactional TryUpdate survives crash + WAL replay.
    ///
    /// The CoW path shadows the write path, updates the value in the shadow leaf,
    /// and installs the shadow root via ApplyCoWTxCommit. The WAL contains
    /// Begin → UpdatePage(shadowLeaf) → UpdatePage(shadowAncestors) → Commit →
    /// UpdateMeta(newRoot). Recovery must replay the updated value.
    /// </summary>
    [Fact]
    public void Recovery_CommittedUpdate_SurvivesCrash()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        {
            var (mgr, wal, engine) = Open();
            for (int i = 1; i <= 50; i++) engine.Insert(i, i * 10);

            using var tx = engine.BeginTransaction();
            tx.TryUpdate(25, 9999).Should().BeTrue();
            tx.Commit();

            wal.Flush();
            wal.Dispose();
            mgr.Dispose();
            File.WriteAllBytes(_dbPath, new byte[PageSize]);
        }
        {
            var (mgr, wal, engine) = Open();
            engine.TryGet(25, out int v).Should().BeTrue("updated key must survive recovery");
            v.Should().Be(9999, "updated value must be replayed correctly");
            for (int i = 1; i <= 50; i++)
            {
                if (i == 25) continue;
                engine.TryGet(i, out int other).Should().BeTrue($"key {i} must survive recovery");
                other.Should().Be(i * 10);
            }
            AssertTreeValid(mgr);
            engine.Close(); wal.Dispose(); mgr.Dispose();
        }
    }

    /// <summary>
    /// Multiple sequential committed transactions each survive crash + WAL replay.
    ///
    /// Each committed transaction writes its own Begin → UpdatePage → Commit →
    /// UpdateMeta chain. Recovery must apply all UpdateMeta records in LSN order
    /// so that the final root reflects the last committed state. Both inserted
    /// keys must be visible after recovery.
    /// </summary>
    [Fact]
    public void Recovery_MultipleCommittedTransactions_AllSurviveCrash()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        {
            var (mgr, wal, engine) = Open();
            for (int i = 1; i <= 50; i++) engine.Insert(i, i * 10);

            using (var tx1 = engine.BeginTransaction())
            {
                tx1.Insert(9999, 111);
                tx1.Commit();
            }
            using (var tx2 = engine.BeginTransaction())
            {
                tx2.Insert(8888, 222);
                tx2.Commit();
            }

            wal.Flush();
            wal.Dispose();
            mgr.Dispose();
            File.WriteAllBytes(_dbPath, new byte[PageSize]);
        }
        {
            var (mgr, wal, engine) = Open();
            engine.TryGet(9999, out int v1).Should().BeTrue("first committed tx must survive recovery");
            v1.Should().Be(111);
            engine.TryGet(8888, out int v2).Should().BeTrue("second committed tx must survive recovery");
            v2.Should().Be(222);
            for (int i = 1; i <= 50; i++)
                engine.TryGet(i, out _).Should().BeTrue($"auto-commit key {i} must survive recovery");
            AssertTreeValid(mgr);
            engine.Close(); wal.Dispose(); mgr.Dispose();
        }
    }

    /// <summary>
    /// Mixed committed and uncommitted transactions: only the committed one
    /// is visible after crash + WAL replay.
    ///
    /// tx1 commits (UpdateMeta for root swap written). tx2 does not commit
    /// (no Commit WAL record, no UpdateMeta for its shadow root). Recovery
    /// replays tx2's shadow UpdatePage records but the live root still points
    /// to tx1's shadow root — tx2's key is invisible.
    /// </summary>
    [Fact]
    public void Recovery_Mixed_OnlyCommittedVisible_AfterCrash()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        {
            var (mgr, wal, engine) = Open();
            for (int i = 1; i <= 50; i++) engine.Insert(i, i * 10);

            using (var tx1 = engine.BeginTransaction())
            {
                tx1.Insert(9999, 111);
                tx1.Commit();
            }

            // tx2: insert but never commit — simulates crash mid-transaction.
            var tx2 = engine.BeginTransaction();
            tx2.Insert(8888, 222);
            // Deliberate: no tx2.Commit() and no tx2.Dispose().

            wal.Flush();   // Begin+UpdatePage for tx2 durable; no Commit record
            wal.Dispose();
            mgr.Dispose();
            File.WriteAllBytes(_dbPath, new byte[PageSize]);
        }
        {
            var (mgr, wal, engine) = Open();
            engine.TryGet(9999, out int v1).Should().BeTrue(
                "committed tx1 insert must survive recovery");
            v1.Should().Be(111);
            engine.TryGet(8888, out _).Should().BeFalse(
                "uncommitted tx2 insert must not be visible after recovery");
            for (int i = 1; i <= 50; i++)
                engine.TryGet(i, out _).Should().BeTrue($"auto-commit key {i} must survive recovery");
            AssertTreeValid(mgr);
            engine.Close(); wal.Dispose(); mgr.Dispose();
        }
    }

    // ── Phase 67: orphaned shadow pages freed after crash ────────────────────

    /// <summary>
    /// Verifies that shadow pages allocated by a crashed transaction are freed
    /// during crash recovery and reused by subsequent operations — file does not grow.
    ///
    /// Setup: fill a single leaf (337 keys, pageSize=4096). Begin a transaction that
    /// inserts key 338 (triggers SplitRoot: 2 AllocPage WAL records written).
    /// Flush WAL for durability, then crash (no Commit, no Dispose → no Abort record).
    ///
    /// Recovery (Phase 3 open): Analysis Pass collects pages 2+3 from AllocPage WAL
    /// records of the crashed transaction. Undo Pass restores before-images. FreePage
    /// returns pages 2+3 to the free list.
    ///
    /// A committed re-insert of the same split-triggering key must reuse the freed
    /// pages — file size must not grow beyond the post-crash size.
    /// </summary>
    [Fact]
    public void Recovery_CrashedTransaction_AllocatedPagesFreed()
    {
        const int splitPageSize = 4096;
        if (File.Exists(_dbPath))  File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);

        (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine) OpenSmall()
        {
            var w = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
            var m = PageManager.Open(new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = splitPageSize,
                BufferPoolCapacity  = 128,
                CheckpointThreshold = 4096,
            }, w);
            var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
            var meta = new TreeMetadata(m);
            meta.Load();
            return (m, w, new TreeEngine<int, int>(m, ns, meta));
        }

        // Phase 1: fill tree to single-leaf capacity (337 keys, pageSize=4096).
        {
            var (mgr, wal, engine) = OpenSmall();
            for (int i = 0; i < 337; i++) engine.Insert(i, i);
            mgr.Dispose(); wal.Dispose();
        }

        // Phase 2: transaction that triggers SplitRoot (key 338 overflows the full leaf).
        //   SplitRoot calls AllocatePage twice → TrackAllocatedPage twice → 2 AllocPage WAL records.
        //   Flush WAL for durability, then crash (no Commit, no Dispose → no Abort record).
        {
            var (mgr, wal, engine) = OpenSmall();
            var tx = engine.BeginTransaction();   // NOT 'using' — crash = no tx.Dispose()
            tx.Insert(338, 338);
            wal.Flush();                           // AllocPage records hit disk before crash
            mgr.Dispose(); wal.Dispose();          // crash: files closed; tx abandoned, no Abort written
        }

        long fileSizeAtCrash = new FileInfo(_dbPath).Length;  // 4 pages = 16384 bytes

        // Phase 3: recovery + verification.
        {
            var (mgr, wal, engine) = OpenSmall();  // RecoverFromWal(): undo restores tree; frees pages 2+3

            engine.TryGet(338, out _).Should().BeFalse("uncommitted key must be absent after recovery");
            for (int i = 0; i < 337; i++)
            {
                engine.TryGet(i, out var v).Should().BeTrue($"key {i} must survive recovery");
                v.Should().Be(i);
            }

            // Committed re-insert of the same split-triggering key.
            // With the fix: reuses freed pages 2+3 → file does not grow.
            // Without the fix: orphaned pages force new allocations → file grows to 24576 bytes.
            using var tx2 = engine.BeginTransaction();
            tx2.Insert(338, 338);
            tx2.Commit();
            mgr.Dispose(); wal.Dispose();
        }

        long fileSizeAfterReinsert = new FileInfo(_dbPath).Length;
        fileSizeAfterReinsert.Should().Be(fileSizeAtCrash,
            "freed shadow pages must be reused by the subsequent split — file must not grow");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Post-recovery structural validation: walks the leaf chain and asserts strict
    /// ascending key order. Catches any recovery bug that produces key-order corruption
    /// before it surfaces as a wrong query result.
    /// </summary>
    private static void AssertTreeValid(PageManager mgr)
    {
        var ns     = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta   = new TreeMetadata(mgr);
        meta.Load();
        var result = new TreeValidator<int, int>(mgr, ns, meta).Validate();
        result.IsValid.Should().BeTrue(
            result.Errors.Count > 0
                ? string.Join("; ", result.Errors)
                : "tree structural validation failed after recovery");
    }

    /// <summary>
    /// Returns the byte offset (LSN value) of the last CheckpointBegin record in the WAL.
    /// WAL record LSNs are byte offsets in the file, so record.Lsn.Value IS the seek position.
    /// </summary>
    private static long FindLastCheckpointBeginOffset(string walPath)
    {
        long lastOffset = -1;
        var reader = new WalReader(walPath);
        foreach (var record in reader.ReadForward(LogSequenceNumber.None))
        {
            if (record.Type == WalRecordType.CheckpointBegin)
                lastOffset = (long)record.Lsn.Value;
        }
        if (lastOffset < 0)
            throw new InvalidOperationException("No CheckpointBegin record found in WAL.");
        return lastOffset;
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); }  catch { }
        try { File.Delete(_walPath); } catch { }
    }
}
