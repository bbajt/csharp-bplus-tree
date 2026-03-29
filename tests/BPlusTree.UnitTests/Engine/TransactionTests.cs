using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Tests for multi-operation atomic transactions (Phase 49).
/// PageSize=8192 with at most a few hundred records ensures no splits/merges
/// occur within transactional operations (Option B scope).
/// </summary>
public class TransactionTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine,
             NodeSerializer<int, int> ns, TreeMetadata meta) Open()
    {
        var wal  = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = PageSize,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 4096,
        }, wal);
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, wal, new TreeEngine<int, int>(mgr, ns, meta), ns, meta);
    }

    /// <summary>Simulate crash: flush WAL to disk but do NOT checkpoint.</summary>
    private static void SimulateCrash(WalWriter wal, PageManager mgr)
    {
        wal.Flush();      // ensure WAL buffer is on disk
        wal.Dispose();
        mgr.Dispose();    // flushes dirty pages; no checkpoint taken
    }

    // ── Test 1: Committed transaction survives reopen ─────────────────────────

    [Fact]
    public void Transaction_Commit_AllOperationsPersist()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        using (var tx = engine.BeginTransaction())
        {
            tx.Insert(1, 100);
            tx.Insert(2, 200);
            tx.Insert(3, 300);
            tx.Commit();
        }

        engine.Close();
        wal.Dispose();
        mgr.Dispose();

        // Reopen and verify all three records survived.
        var (mgr2, wal2, engine2, _, _) = Open();
        engine2.TryGet(1, out int v1).Should().BeTrue();
        v1.Should().Be(100);
        engine2.TryGet(2, out int v2).Should().BeTrue();
        v2.Should().Be(200);
        engine2.TryGet(3, out int v3).Should().BeTrue();
        v3.Should().Be(300);
        engine2.Close();
        wal2.Dispose();
        mgr2.Dispose();
    }

    // ── Test 2: Rollback (Dispose without Commit) undoes all operations ───────

    [Fact]
    public void Transaction_Dispose_WithoutCommit_AllOperationsRolledBack()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        // Pre-existing auto-commit record.
        engine.Insert(10, 1000);

        using (var tx = engine.BeginTransaction())
        {
            tx.Insert(1, 100);
            tx.Insert(2, 200);
            // Dispose without Commit → rollback
        }

        engine.TryGet(1, out _).Should().BeFalse("key 1 must be rolled back");
        engine.TryGet(2, out _).Should().BeFalse("key 2 must be rolled back");
        engine.TryGet(10, out int v10).Should().BeTrue("pre-existing record must be unaffected");
        v10.Should().Be(1000);

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 3: Committed transaction survives crash ──────────────────────────

    [Fact]
    public void Transaction_Commit_SurvivesCrash()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 100; i++) engine.Insert(i, i * 10);

        using (var tx = engine.BeginTransaction())
        {
            tx.Insert(9001, 90010);
            tx.Insert(9002, 90020);
            tx.TryDelete(50);
            tx.Commit();
        }

        SimulateCrash(wal, mgr); // crash AFTER commit

        // Recovery: Redo Pass replays committed changes. Undo Pass: ATT empty (committed).
        var (mgr2, wal2, engine2, _, _) = Open();
        engine2.TryGet(9001, out int vr1).Should().BeTrue("committed insert must survive crash");
        vr1.Should().Be(90010);
        engine2.TryGet(9002, out int vr2).Should().BeTrue("committed insert must survive crash");
        vr2.Should().Be(90020);
        engine2.TryGet(50, out _).Should().BeFalse("committed delete must survive crash");
        engine2.Close();
        wal2.Dispose();
        mgr2.Dispose();
    }

    // ── Test 4: Uncommitted transaction is undone on recovery ─────────────────

    [Fact]
    public void Transaction_CrashMidTransaction_UndoneOnRecovery()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 100; i++) engine.Insert(i, i * 10);

        var tx = engine.BeginTransaction(); // intentionally not disposed — crash simulation
        tx.Insert(9001, 90010);
        tx.Insert(9002, 90020);
        // NO Commit — crash while transaction is open

        SimulateCrash(wal, mgr); // crash BEFORE commit

        // Recovery: Redo Pass applies after-images; Analysis Pass detects open txId;
        // Undo Pass reverses both inserts using before-images from WAL records.
        var (mgr2, wal2, engine2, _, _) = Open();
        engine2.TryGet(9001, out _).Should().BeFalse("uncommitted insert must be undone by recovery");
        engine2.TryGet(9002, out _).Should().BeFalse("uncommitted insert must be undone by recovery");

        // Auto-commit records must be unaffected.
        for (int i = 1; i <= 100; i++)
        {
            engine2.TryGet(i, out int v).Should().BeTrue($"auto-commit key {i} must survive");
            v.Should().Be(i * 10);
        }

        engine2.Close();
        wal2.Dispose();
        mgr2.Dispose();
    }

    // ── Test 5: Two concurrent transactions on different leaves — independent rollback ──
    // Updated in Phase 55: pre-populate a 2-leaf tree so tx1 and tx2 operate on
    // different leaf pages (no write-write conflict). Left leaf = 1..169, right = 170..338.

    [Fact]
    public void Transaction_MultipleTransactions_IndependentRollback()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        // Use pageSize=4096 (same as Tests 7–12) so 338 inserts force a leaf split.
        var (mgr, wal, engine, ns, meta) = Open(pageSize: 4096);
        for (int i = 1; i <= 338; i++) engine.Insert(i, i * 10);

        // tx1 modifies a key on the LEFT leaf; tx2 modifies a key on the RIGHT leaf.
        // Different pages → no conflict.
        using var tx1 = engine.BeginTransaction();
        tx1.TryUpdate(1, 9999);       // left leaf — acquires write lock on left leaf page

        using (var tx2 = engine.BeginTransaction())
        {
            tx2.TryUpdate(170, 8888); // right leaf — acquires write lock on right leaf page
            // Dispose without Commit → rollback; key 170 reverts to original value 1700
        }

        tx1.Commit();

        engine.TryGet(1, out int v1).Should().BeTrue("tx1 committed — key 1 must be present");
        v1.Should().Be(9999, "tx1 committed — value must be updated");
        engine.TryGet(170, out int v170).Should().BeTrue("key 170 must be present (pre-populated)");
        v170.Should().Be(1700, "tx2 was rolled back — key 170 must revert to original value");

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 6: Empty commit writes exactly Begin + Commit WAL records ────────

    [Fact]
    public void Transaction_EmptyCommit_WritesOnlyBeginAndCommit()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        uint txId;
        using (var tx = engine.BeginTransaction())
        {
            txId = tx.TransactionId;
            tx.Commit(); // no operations performed
        }

        // Read WAL BEFORE engine.Close() — GracefulClose truncates the WAL.
        var reader  = new WalReader(_walPath);
        var records = reader.ReadForward(LogSequenceNumber.None)
                            .Where(r => r.TransactionId == txId)
                            .ToList();

        engine.Close();
        wal.Dispose();
        mgr.Dispose();

        records.Should().HaveCount(2, "an empty transaction must produce exactly Begin + Commit");
        records[0].Type.Should().Be(WalRecordType.Begin,  "first record must be Begin");
        records[1].Type.Should().Be(WalRecordType.Commit, "second record must be Commit");
    }

    // ── Open overload for split tests (small page to force splits at low record counts) ──

    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine,
             NodeSerializer<int, int> ns, TreeMetadata meta) Open(int pageSize)
    {
        var wal  = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = pageSize,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 64,
        }, wal);
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, wal, new TreeEngine<int, int>(mgr, ns, meta), ns, meta);
    }

    // ── Test 7: Commit with split — all data persists after reopen ────────────

    [Fact]
    public void Transaction_Commit_WithSplit_AllDataPersists()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open(pageSize: 4096);

        // 300 auto-commit records — tree stays at height 1 (root is the only leaf)
        for (int i = 1; i <= 300; i++) engine.Insert(i, i * 10);

        // Transaction: 400 more records — crosses the ~337 leaf capacity threshold,
        // forcing SplitRoot (height 1 → 2) and at least one further leaf split
        using (var tx = engine.BeginTransaction())
        {
            for (int i = 301; i <= 700; i++) tx.Insert(i, i * 10);
            tx.Commit();
        }

        engine.Close(); wal.Dispose(); mgr.Dispose();

        // Reopen and verify all 700 records survived
        var (mgr2, wal2, engine2, ns2, meta2) = Open(pageSize: 4096);
        for (int i = 1; i <= 700; i++)
        {
            engine2.TryGet(i, out int v).Should().BeTrue($"key {i} must be present after commit");
            v.Should().Be(i * 10);
        }
        new TreeValidator<int, int>(mgr2, ns2, meta2).Validate().IsValid
            .Should().BeTrue("tree must be structurally valid after commit with splits");
        engine2.Close(); wal2.Dispose(); mgr2.Dispose();
    }

    // ── Test 8: Rollback with split — structure reverted ─────────────────────

    [Fact]
    public void Transaction_Rollback_WithSplit_StructureReverted()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open(pageSize: 4096);

        // 285 auto-commit records — tree stays at height 1 (~285 of 289 slots used)
        for (int i = 1; i <= 285; i++) engine.Insert(i, i * 10);

        // Capture pre-transaction structural state
        uint preRootPageId = meta.RootPageId;
        uint preHeight     = meta.TreeHeight;

        // Transaction: 400 records — SplitRoot fires (height 1 → 2), RootPageId changes.
        // Dispose without Commit triggers rollback.
        using (var tx = engine.BeginTransaction())
        {
            for (int i = 286; i <= 685; i++) tx.Insert(i, i * 10);
            // no tx.Commit() — Dispose triggers rollback
        }

        // Transactional keys must be absent
        for (int i = 286; i <= 685; i++)
            engine.TryGet(i, out _).Should().BeFalse($"key {i} must be absent after rollback");

        // Auto-commit keys must be intact
        for (int i = 1; i <= 285; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must survive rollback");
            v.Should().Be(i * 10);
        }

        // Structural state must be fully reverted.
        // These assertions depend on the metadata in-memory reload in Transaction.Dispose()
        // (step 2: _engine.ReloadMetadata() when MetaPageId is in _writeOrder).
        meta.RootPageId.Should().Be(preRootPageId,
            "root page ID must be restored to pre-split value after rollback");
        meta.TreeHeight.Should().Be(preHeight,
            "tree height must be restored (height 1) after rollback of SplitRoot");

        // Note: TotalRecordCount is NOT asserted here. The auto-commit _metadata.Flush()
        // calls that happen per-insert (before the split fires) advance TotalRecordCount
        // outside the transaction's WAL chain. After rollback, TotalRecordCount reflects
        // the number of successful inserts up to (but not including) the first split —
        // it does not revert to 300. This is a known Phase 51 limitation (Option B scope).

        new TreeValidator<int, int>(mgr, ns, meta).Validate().IsValid
            .Should().BeTrue("tree must be structurally valid after rollback");

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 9: Commit with merge (+ CollapseRoot) — changes persist after reopen ─

    [Fact]
    public void Transaction_Commit_WithMerge_AllChangePersist()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open(pageSize: 4096);

        // 290 auto-commit inserts → SplitRoot fires → height 2
        // Both leaves land at threshold 145 (left: 1..145, right: 146..290)
        // (capacity=289 with 6-byte slot; split gives ceil(289/2)=145 to left,
        //  then 290th key goes to right → both at threshold 145)
        for (int i = 1; i <= 290; i++) engine.Insert(i, i * 10);

        // Transaction: delete key 1 → left leaf underflows (144 < 145)
        // Right leaf at threshold (145) → cannot borrow → MergeLeaves fires
        // Parent loses its only separator → CollapseRoot fires → height 1
        using (var tx = engine.BeginTransaction())
        {
            tx.TryDelete(1);
            tx.Commit();
        }

        engine.Close(); wal.Dispose(); mgr.Dispose();

        // Reopen and verify post-merge state
        var (mgr2, wal2, engine2, ns2, meta2) = Open(pageSize: 4096);
        engine2.TryGet(1, out _).Should().BeFalse("key 1 was deleted and committed");
        for (int i = 2; i <= 290; i++)
        {
            engine2.TryGet(i, out int v).Should().BeTrue($"key {i} must persist after commit");
            v.Should().Be(i * 10);
        }
        meta2.TreeHeight.Should().Be(1, "CollapseRoot fired on commit — height must be 1");
        new TreeValidator<int, int>(mgr2, ns2, meta2).Validate().IsValid
            .Should().BeTrue("tree must be structurally valid after merge commit");
        engine2.Close(); wal2.Dispose(); mgr2.Dispose();
    }

    // ── Test 10: Rollback with merge (no CollapseRoot) — structure reverted ────

    [Fact]
    public void Transaction_Rollback_WithMerge_StructureReverted()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open(pageSize: 4096);

        // 675 auto-commit inserts → 3 leaves: L1=1..169 (169), L2=170..338 (169), L3=339..675 (337)
        // Root has 2 separators (170, 339). Height 2.
        for (int i = 1; i <= 675; i++) engine.Insert(i, i * 10);

        uint preRootPageId = meta.RootPageId;
        uint preHeight     = meta.TreeHeight;

        // Transaction: delete key 1 → L1 underflows (168 < 169)
        // L2 at threshold (169) → cannot borrow → MergeLeaves(L1, L2) fires
        // Root loses separator 170 but keeps separator 339 → no CollapseRoot
        // Then rollback
        using (var tx = engine.BeginTransaction())
        {
            tx.TryDelete(1);
            // no Commit — Dispose triggers rollback
        }

        // All 675 keys must be intact
        for (int i = 1; i <= 675; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must survive rollback");
            v.Should().Be(i * 10);
        }

        // Structural state must be fully reverted
        meta.RootPageId.Should().Be(preRootPageId, "root page must be restored after rollback");
        meta.TreeHeight.Should().Be(preHeight, "tree height must be unchanged (no CollapseRoot)");

        new TreeValidator<int, int>(mgr, ns, meta).Validate().IsValid
            .Should().BeTrue("tree must be structurally valid after merge rollback");

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 11: Rollback with CollapseRoot — height reverted ─────────────────

    [Fact]
    public void Transaction_Rollback_WithCollapseRoot_HeightReverted()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open(pageSize: 4096);

        // 338 auto-commit inserts → height 2, 2 leaves both at threshold 169
        for (int i = 1; i <= 338; i++) engine.Insert(i, i * 10);

        uint preRootPageId = meta.RootPageId;
        uint preHeight     = meta.TreeHeight;   // 2

        // Transaction: delete key 1 → merge → CollapseRoot (height 2 → 1)
        // Then rollback
        using (var tx = engine.BeginTransaction())
        {
            tx.TryDelete(1);
            // no Commit — Dispose triggers rollback
        }

        // All 338 keys including key 1 must be present
        for (int i = 1; i <= 338; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must survive rollback");
            v.Should().Be(i * 10);
        }

        // Height and root must be restored — CollapseRoot was rolled back
        meta.RootPageId.Should().Be(preRootPageId,
            "root page must be restored to pre-transaction value");
        meta.TreeHeight.Should().Be(preHeight,
            "tree height must be restored to 2 after CollapseRoot rollback");

        new TreeValidator<int, int>(mgr, ns, meta).Validate().IsValid
            .Should().BeTrue("tree must be structurally valid after CollapseRoot rollback");

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 12: Commit with borrow — separator updated and persists ───────────

    [Fact]
    public void Transaction_Commit_WithBorrow_SeparatorUpdated()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open(pageSize: 4096);

        // 400 auto-commit inserts → height 2
        // L1 = 1..169 (169 keys, at threshold), L2 = 170..400 (231 keys, can donate)
        for (int i = 1; i <= 400; i++) engine.Insert(i, i * 10);

        // Transaction: delete key 1 → L1 underflows (168 < 169)
        // L2 has 231 > 169 → BorrowFromRight fires: key 170 moves to L1
        // Parent separator updated from 170 to 171
        using (var tx = engine.BeginTransaction())
        {
            tx.TryDelete(1);
            tx.Commit();
        }

        engine.Close(); wal.Dispose(); mgr.Dispose();

        // Reopen and verify borrow result
        var (mgr2, wal2, engine2, ns2, meta2) = Open(pageSize: 4096);
        engine2.TryGet(1, out _).Should().BeFalse("key 1 was deleted");
        engine2.TryGet(170, out int v170).Should().BeTrue("key 170 moved to left leaf via borrow");
        v170.Should().Be(1700);
        for (int i = 2; i <= 400; i++)
        {
            engine2.TryGet(i, out int v).Should().BeTrue($"key {i} must persist after borrow commit");
            v.Should().Be(i * 10);
        }
        new TreeValidator<int, int>(mgr2, ns2, meta2).Validate().IsValid
            .Should().BeTrue("tree must be structurally valid after borrow commit");
        engine2.Close(); wal2.Dispose(); mgr2.Dispose();
    }

    // ── Test 13: Sequential write-write on same page — lock released between transactions ──

    [Fact]
    public void Transaction_SequentialWriteSamePage_NoConflict()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open(pageSize: 4096);
        for (int i = 1; i <= 338; i++) engine.Insert(i, i * 10);

        // tx1 writes key 1 (left leaf) and commits — releases write lock.
        using (var tx1 = engine.BeginTransaction())
        {
            tx1.TryUpdate(1, 9999);
            tx1.Commit();
        }

        // tx2 writes key 1 (same page, same key) — tx1 has released its lock, no conflict.
        using (var tx2 = engine.BeginTransaction())
        {
            Action act = () => tx2.TryUpdate(1, 7777);
            act.Should().NotThrow<TransactionConflictException>(
                "tx1 committed and released its lock before tx2 started");
            tx2.Commit();
        }

        engine.TryGet(1, out int v1).Should().BeTrue();
        v1.Should().Be(7777, "tx2 committed its update");

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 14: Concurrent write-write on same page — TransactionConflictException ──

    [Fact]
    public void Transaction_ConcurrentWriteSamePage_ThrowsConflictException()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open(pageSize: 4096);
        for (int i = 1; i <= 338; i++) engine.Insert(i, i * 10);

        // tx1 writes key 1 (left leaf) and holds the write lock without committing.
        var tx1 = engine.BeginTransaction();
        tx1.TryUpdate(1, 9999); // acquires write lock on left leaf page

        // tx2 attempts to write a different key on the SAME left leaf page.
        // Key 50 is in range 1..169 (same leaf as key 1).
        var tx2 = engine.BeginTransaction();
        Action act = () => tx2.TryUpdate(50, 7777);
        var ex = act.Should().Throw<TransactionConflictException>(
            "tx1 holds the write lock on the left leaf — tx2 must detect the conflict").Which;

        ex.TxId.Should().Be(tx2.TransactionId,
            "the exception must identify tx2 as the conflicting (aborted) transaction");
        ex.OwnerTxId.Should().Be(tx1.TransactionId,
            "the exception must identify tx1 as the lock owner");

        // tx2 must be disposed (Abort WAL record written) before tx1 commits.
        tx2.Dispose();

        // tx1 commits — releases the write lock.
        tx1.Commit();
        tx1.Dispose();

        // tx3 (fresh transaction) can now write the same page.
        using var tx3 = engine.BeginTransaction();
        Action act3 = () => tx3.TryUpdate(50, 5555);
        act3.Should().NotThrow<TransactionConflictException>(
            "tx1 committed and released its lock");
        tx3.Commit();

        engine.TryGet(50, out int v50).Should().BeTrue();
        v50.Should().Be(5555, "tx3 committed its update");

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 15: Rollback releases lock — subsequent transaction succeeds ──

    [Fact]
    public void Transaction_RollbackReleasesWriteLock_NoConflictOnRetry()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open(pageSize: 4096);
        for (int i = 1; i <= 338; i++) engine.Insert(i, i * 10);

        // tx1 writes key 1 (left leaf), no commit.
        using (var tx1 = engine.BeginTransaction())
        {
            tx1.TryUpdate(1, 9999); // acquires write lock on left leaf page
            // Dispose without Commit → rollback → lock released
        }

        // After tx1 rolled back: key 1 should be restored and lock released.
        engine.TryGet(1, out int v1After).Should().BeTrue();
        v1After.Should().Be(10, "tx1 was rolled back — key 1 must revert to original value 10");

        // tx2 (fresh transaction) can now write the same page — no conflict.
        using var tx2 = engine.BeginTransaction();
        Action act = () => tx2.TryUpdate(1, 7777);
        act.Should().NotThrow<TransactionConflictException>(
            "tx1 rolled back and released its write lock");
        tx2.Commit();

        engine.TryGet(1, out int v1Final).Should().BeTrue();
        v1Final.Should().Be(7777, "tx2 committed its update");

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 16: DeleteRange commit — keys in range removed, outside intact ──

    [Fact]
    public void Transaction_DeleteRange_CommitDeletesAllKeysInRange()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open(pageSize: 4096);

        for (int i = 1; i <= 20; i++) engine.Insert(i, i * 10);

        int deleted;
        using (var tx = engine.BeginTransaction())
        {
            deleted = tx.DeleteRange(5, 15);
            tx.Commit();
        }

        deleted.Should().Be(11, "keys 5,6,...,15 = 11 keys deleted");

        // Keys 5–15 must be absent
        for (int i = 5; i <= 15; i++)
            engine.TryGet(i, out _).Should().BeFalse($"key {i} must be deleted");

        // Keys 1–4 and 16–20 must still be present
        for (int i = 1; i <= 4; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must survive range delete");
            v.Should().Be(i * 10);
        }
        for (int i = 16; i <= 20; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must survive range delete");
            v.Should().Be(i * 10);
        }

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 17: DeleteRange rollback — no keys deleted ───────────────────────

    [Fact]
    public void Transaction_DeleteRange_Rollback_NoKeysDeleted()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open(pageSize: 4096);

        for (int i = 1; i <= 20; i++) engine.Insert(i, i * 10);

        using (var tx = engine.BeginTransaction())
        {
            tx.DeleteRange(5, 15);
            // Dispose without Commit → rollback
        }

        // All 20 keys must be present
        for (int i = 1; i <= 20; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must survive rollback");
            v.Should().Be(i * 10);
        }

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 18: DeleteRange empty range — returns 0, tree unchanged ──────────

    [Fact]
    public void Transaction_DeleteRange_EmptyRange_ReturnsZero()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open(pageSize: 4096);

        for (int i = 1; i <= 10; i++) engine.Insert(i, i * 10);

        int deleted;
        using (var tx = engine.BeginTransaction())
        {
            deleted = tx.DeleteRange(50, 60); // range entirely outside tree
            tx.Commit();
        }

        deleted.Should().Be(0, "no keys exist in range [50,60]");

        // All 10 keys must still be present
        for (int i = 1; i <= 10; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must be unaffected");
            v.Should().Be(i * 10);
        }

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    [Fact]
    public void Dispose_WithoutCommit_ReleasesWriterLock()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _, _) = Open();

        engine.Insert(1, 10);

        // Rollback a transaction (Dispose without Commit).
        var tx = engine.BeginTransaction();
        tx.Insert(2, 20);
        tx.Dispose();   // rollback — must release writer lock

        // If the writer lock leaked, BeginTransaction() would block here
        // and the thread would never complete within the join timeout.
        bool completed = false;
        var t = new System.Threading.Thread(() =>
        {
            using var tx2 = engine.BeginTransaction();
            tx2.Insert(3, 30);
            tx2.Commit();
            completed = true;
        });
        t.Start();
        t.Join(5_000).Should().BeTrue(
            "BeginTransaction must acquire the lock within 5 s; a deadlock means rollback " +
            "did not release the writer lock");
        completed.Should().BeTrue("second transaction must commit successfully");

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Shadow-page reuse: V-B ────────────────────────────────────────────────

    /// <summary>
    /// Insert 100 sequential keys in one transaction.
    /// Before V-B fix: each insert CoW-copied the full root→leaf path → O(100 × H) shadow pages.
    /// After fix: pages already owned by the transaction are reused in-place → far fewer allocations.
    /// Verifies correctness (all keys readable after commit), not exact allocation counts.
    /// </summary>
    [Fact]
    public void Transaction_MultiInsert_ShadowPagesNotDuplicated()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _, _) = Open();

        var tx = engine.BeginTransaction();
        for (int i = 0; i < 100; i++)
            tx.Insert(i, i);
        tx.Commit();
        tx.Dispose();

        for (int i = 0; i < 100; i++)
        {
            engine.TryGet(i, out int val).Should().BeTrue($"key {i} must be readable after commit");
            val.Should().Be(i);
        }

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); }  catch { }
        try { File.Delete(_walPath); } catch { }
    }
}
