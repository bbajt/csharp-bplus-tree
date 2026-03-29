using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Tests for Insert() and Delete() CoW write paths added in M+4b.
/// </summary>
public class CowInsertDeleteTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine, TreeMetadata meta) Open(
        int pageSize = 4096)
    {
        if (File.Exists(_dbPath))  File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);

        var wal  = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = pageSize,
            BufferPoolCapacity  = 256,
            CheckpointThreshold = 16384,
        }, wal);
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, wal, new TreeEngine<int, int>(mgr, ns, meta), meta);
    }

    public void Dispose()
    {
        static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch (IOException) { }
        }
        TryDelete(_dbPath);
        TryDelete(_walPath);
    }

    // ── Test 1: CoW Insert — new key, shadow root installed ───────────────────

    [Fact]
    public void Insert_CoW_NewKeyInserted_TreeValid()
    {
        var (mgr, wal, engine, meta) = Open();

        for (int i = 1; i <= 200; i++)
            engine.Insert(i, i * 10);

        uint oldRoot = meta.RootPageId;

        // Open a snapshot to force the CoW path (in-place is skipped when a snapshot is open).
        using var snapshot = engine.BeginSnapshot();
        bool inserted = engine.Insert(9999, 999);

        inserted.Should().BeTrue();
        engine.TryGet(9999, out int v).Should().BeTrue();
        v.Should().Be(999);
        meta.RootPageId.Should().NotBe(oldRoot, "CoW Insert must install a new shadow root");

        var ns        = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var validator = new TreeValidator<int, int>(mgr, ns, meta);
        validator.Validate().IsValid.Should().BeTrue("tree must be structurally valid after CoW Insert");

        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 2: CoW Insert causing a leaf split ────────────────────────────────

    [Fact]
    public void Insert_CoW_CausingSplit_TreeValid()
    {
        // PageSize=4096 → leaf capacity ≈ 337 int/int entries.
        // Inserting 400 keys forces at least one leaf split.
        var (mgr, wal, engine, meta) = Open(pageSize: 4096);

        for (int i = 1; i <= 400; i++)
            engine.Insert(i, i * 10);

        var ns        = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var validator = new TreeValidator<int, int>(mgr, ns, meta);
        validator.Validate().IsValid.Should().BeTrue("tree must be structurally valid after split-inducing inserts");

        for (int i = 1; i <= 400; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must be present");
            v.Should().Be(i * 10);
        }

        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 3: CoW Delete — key removed, shadow root installed ───────────────

    [Fact]
    public void Delete_CoW_KeyDeleted_TreeValid()
    {
        var (mgr, wal, engine, meta) = Open();

        for (int i = 1; i <= 200; i++)
            engine.Insert(i, i * 10);

        uint oldRoot = meta.RootPageId;

        // Open a snapshot to force the CoW path (in-place is skipped when a snapshot is open).
        using var snapshot = engine.BeginSnapshot();
        bool deleted = engine.Delete(100);

        deleted.Should().BeTrue();
        engine.TryGet(100, out _).Should().BeFalse("key 100 must be absent after delete");
        meta.RootPageId.Should().NotBe(oldRoot, "CoW Delete must install a new shadow root");

        var ns        = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var validator = new TreeValidator<int, int>(mgr, ns, meta);
        validator.Validate().IsValid.Should().BeTrue("tree must be structurally valid after CoW Delete");

        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 4: CoW Delete causing a leaf merge ────────────────────────────────

    [Fact]
    public void Delete_CoW_CausingMerge_TreeValid()
    {
        // PageSize=4096 → leaf capacity ≈ 337 entries.
        // Insert 338 keys to produce a 2-leaf tree, then delete until underflow+merge triggers.
        var (mgr, wal, engine, meta) = Open(pageSize: 4096);

        const int total = 338;
        for (int i = 1; i <= total; i++)
            engine.Insert(i, i * 10);

        // Delete from the second leaf (upper half) until a merge occurs.
        // Deleting ≈ half of the second leaf's entries causes underflow.
        var ns = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        int deleted = 0;
        for (int i = total; i >= total / 2; i--)
        {
            engine.Delete(i);
            deleted++;
        }

        var validator = new TreeValidator<int, int>(mgr, ns, meta);
        validator.Validate().IsValid.Should().BeTrue("tree must be structurally valid after merge-inducing deletes");

        // All remaining keys must be present.
        int remaining = total - deleted;
        for (int i = 1; i <= remaining; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must still be present");
            v.Should().Be(i * 10);
        }

        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 6: Gap 6 / Phase 109a — Cross-thread transactions with disjoint key ranges ─────
    // Originally tested that per-transaction writer lock (Phase 71) prevented false page-level
    // conflicts when two threads write disjoint ranges. Under Phase 109a (commit-serialized
    // multi-writer), the writer lock is no longer held for the full transaction lifetime.
    // Instead, the root-version check at commit prevents lost updates: the second committer
    // sees a stale root and throws TransactionConflictException, then retries.
    // Both threads must eventually succeed via the retry-until-commit protocol.

    [Fact]
    public void Transaction_CrossThread_DisjointRanges_BothSucceed()
    {
        var (mgr, wal, engine, meta) = Open(pageSize: 4096);

        // Thread A writes keys 0–99; Thread B writes keys 10000–10099.
        // These are guaranteed to map to entirely different leaf pages.
        // Under Phase 109a, one commits first; the other gets TransactionConflictException
        // and retries from a fresh snapshot — both eventually persist.
        const int countA = 100;
        const int countB = 100;

        Exception? exA = null;
        Exception? exB = null;

        var tA = new Thread(() =>
        {
            try
            {
                bool done = false;
                while (!done)
                {
                    try
                    {
                        using var tx = engine.BeginTransaction();
                        for (int i = 0; i < countA; i++)
                            tx.Insert(i, i);
                        tx.Commit();
                        done = true;
                    }
                    catch (TransactionConflictException) { /* retry */ }
                }
            }
            catch (Exception ex) { exA = ex; }
        });

        var tB = new Thread(() =>
        {
            try
            {
                bool done = false;
                while (!done)
                {
                    try
                    {
                        using var tx = engine.BeginTransaction();
                        for (int i = 10_000; i < 10_000 + countB; i++)
                            tx.Insert(i, i);
                        tx.Commit();
                        done = true;
                    }
                    catch (TransactionConflictException) { /* retry */ }
                }
            }
            catch (Exception ex) { exB = ex; }
        });

        tA.Start();
        tB.Start();
        tA.Join();
        tB.Join();

        exA.Should().BeNull("Thread A must not throw any non-conflict exception");
        exB.Should().BeNull("Thread B must not throw any non-conflict exception");

        // All keys from both transactions must be present after retries.
        for (int i = 0; i < countA; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must be present");
            v.Should().Be(i);
        }
        for (int i = 10_000; i < 10_000 + countB; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must be present");
            v.Should().Be(i);
        }

        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 5: Gap 7 — Snapshot root epoch-protected from concurrent retirement ──────
    // Regression for Gap 7 (Phase 70). Without Fix A, tx1 captures the snapshot root R
    // without an epoch. tx2 commits, retiring R via RetirePage → SweepRetiredPages frees R
    // immediately (no active epoch). tx1.Insert then traverses from freed page R →
    // PageNotFoundException(NullPageId). With Fix A, tx1 holds epoch E1 at construction,
    // so SweepRetiredPages defers R's retirement until tx1 exits its epoch (at Commit/Dispose).
    //
    // Phase 109a update: tx1 makes writes (Insert 50), so the root-version check at
    // Commit() applies. tx2 committed after tx1 started → CurrentRootId != snapshotRootId
    // → TransactionConflictException (no crash — epoch protection works; conflict is safe).
    // The test now verifies (a) no PageNotFoundException (epoch guard intact), and
    // (b) TransactionConflictException is thrown (not a crash).

    [Fact]
    public void Transaction_SnapshotRoot_EpochProtectedFromRetirement()
    {
        var (mgr, wal, engine, meta) = Open(pageSize: 4096);

        // Seed the tree with 10 keys (height=1, single root leaf R).
        for (int i = 0; i < 10; i++)
            engine.Insert(i, i);

        // tx1 opens: captures TxRootId = R (with Fix A: epoch E1 registered, R protected).
        var tx1 = engine.BeginTransaction();

        // tx2 overwrites key 5 — CopyWritePathAndAllocShadows copies the leaf,
        // shadow leaf SL becomes the new root; original R goes into _obsoletePages.
        // tx2.Commit() calls RetirePage(R) → SweepRetiredPages:
        //   WITHOUT Fix A: no active epoch → R freed → tx1.Insert crashes.
        //   WITH    Fix A: epoch E1 active → R deferred until tx1 releases epoch.
        using var tx2 = engine.BeginTransaction();
        tx2.Insert(5, 999);
        tx2.Commit();

        // tx1.Insert must not crash (epoch protection keeps R accessible).
        // However, tx1.Commit() must throw TransactionConflictException (Phase 109a root
        // version check): tx1 made writes, and tx2 committed since tx1's snapshot → lost
        // update prevention. This is safe and controlled — no PageNotFoundException.
        var act = () =>
        {
            tx1.Insert(50, 50);
            tx1.Commit();
        };
        act.Should().Throw<TransactionConflictException>(
            "tx1 made writes with a stale snapshot — root version check fires (Phase 109a)");

        // Dispose tx1 BEFORE wal/mgr — tx1 is not committed, rollback writes to WAL.
        tx1.Dispose();

        wal.Dispose();
        mgr.Dispose();
    }
}
