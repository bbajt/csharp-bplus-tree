using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Tests for Phase 109a commit-serialized multi-writer.
///
/// Core invariants verified:
///   1. Two non-conflicting transactions starting from the same snapshot cannot both
///      commit — the second sees a stale root (root version check) and must retry.
///   2. Two transactions touching the same page conflict via page write locks.
///   3. Write-only transactions with an empty read set detect stale root via the
///      root version check (SSI would miss these).
///   4. Retry-until-success protocol: concurrent transactions each eventually commit.
///   5. Nested transactions on the same thread still work (depth-tracked reentrancy).
/// </summary>
public class MultiWriterTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine) Open()
    {
        var wal = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr = PageManager.Open(new BPlusTreeOptions
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
        return (mgr, wal, new TreeEngine<int, int>(mgr, ns, meta));
    }

    private static void Cleanup(WalWriter wal, PageManager mgr)
    {
        wal.Flush();
        mgr.Dispose();
        wal.Dispose();
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { }
        try { File.Delete(_walPath); } catch { }
    }

    // ── Test 1: Two non-conflicting transactions — second detects stale root ──

    [Fact]
    public void TwoTransactions_NonConflicting_SecondRetriesAndBothKeysPersist()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine) = Open();

        // T1 starts, inserts key 1, commits first.
        var t1 = engine.BeginTransaction();
        t1.Insert(1, 100);

        // T2 starts from same snapshot root.
        var t2 = engine.BeginTransaction();
        t2.Insert(2, 200);

        // T1 commits — live root advances.
        t1.Commit();
        t1.Dispose();

        // T2 sees stale root → TransactionConflictException.
        Action commitT2 = () => t2.Commit();
        commitT2.Should().Throw<TransactionConflictException>();
        t2.Dispose();

        // Retry T2: fresh transaction, fresh snapshot.
        using (var t2b = engine.BeginTransaction())
        {
            t2b.Insert(2, 200);
            t2b.Commit();
        }

        // Both keys must be present.
        engine.TryGet(1, out int v1).Should().BeTrue();
        v1.Should().Be(100);
        engine.TryGet(2, out int v2).Should().BeTrue();
        v2.Should().Be(200);

        Cleanup(wal, mgr);
    }

    // ── Test 2: Same-page conflict via page write lock ───────────────────────

    [Fact]
    public void TwoTransactions_SamePage_SecondThrowsPageConflict()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine) = Open();

        // Seed a key so a leaf page exists.
        engine.Insert(5, 50);

        var t1 = engine.BeginTransaction();
        t1.TryUpdate(5, 500); // locks the leaf page for t1

        var t2 = engine.BeginTransaction();

        // T2 trying to write the same page should throw page-level conflict.
        Action act = () => t2.TryUpdate(5, 999);
        act.Should().Throw<TransactionConflictException>();
        t2.Dispose();

        t1.Commit();
        t1.Dispose();

        engine.TryGet(5, out int v).Should().BeTrue();
        v.Should().Be(500);

        Cleanup(wal, mgr);
    }

    // ── Test 3: Write-only transaction detects stale root (root version check) ─

    [Fact]
    public void WriteOnlyTransaction_StaleRoot_ThrowsConflict_RetrySucceeds()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine) = Open();

        // Seed a key so a non-empty root exists.
        engine.Insert(1, 10);

        // T1 and T2 both start from the same snapshot root.
        // T2 writes to a different key (key 3) AFTER T1 commits, so T2 avoids page-lock
        // conflicts with T1. However, T2's _snapshotRootId was captured before T1 committed,
        // so CurrentRootId != _snapshotRootId at T2.Commit() — root version check fires.
        // SSI passes (empty read set → no page-retire conflict).
        var t1 = engine.BeginTransaction();
        var t2 = engine.BeginTransaction(); // same snapshot root as T1

        t1.Insert(2, 20);

        // T1 commits — live root advances. T1's page lock on page 1 is released.
        t1.Commit();
        t1.Dispose();

        // T2 inserts AFTER T1's page locks are released, so no page-lock conflict.
        // T2's _snapshotRootId is still the old root (captured before T1 committed).
        t2.Insert(3, 30); // T2 has a write → _txRootId != _snapshotRootId

        // Root version check: _txRootId != _snapshotRootId (T2 has a shadow),
        // AND CurrentRootId != _snapshotRootId (T1 advanced the root) → conflict.
        Action commitT2 = () => t2.Commit();
        commitT2.Should().Throw<TransactionConflictException>();
        t2.Dispose();

        // Retry T2 with a fresh snapshot.
        using (var t2b = engine.BeginTransaction())
        {
            t2b.Insert(3, 30);
            t2b.Commit();
        }

        engine.TryGet(2, out int v2).Should().BeTrue();
        v2.Should().Be(20);
        engine.TryGet(3, out int v3).Should().BeTrue();
        v3.Should().Be(30);

        Cleanup(wal, mgr);
    }

    // ── Test 4: Concurrent retry loop — both keys eventually persisted ────────

    [Fact]
    public async Task ConcurrentRetryLoop_BothThreadsInsertSuccessfully()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine) = Open();

        var barrier   = new Barrier(2);
        Exception? exA = null, exB = null;

        var taskA = Task.Run(() =>
        {
            try
            {
                barrier.SignalAndWait();
                bool inserted = false;
                while (!inserted)
                {
                    try
                    {
                        using var tx = engine.BeginTransaction();
                        tx.Insert(100, 1000);
                        tx.Commit();
                        inserted = true;
                    }
                    catch (TransactionConflictException) { /* retry */ }
                }
            }
            catch (Exception ex) { exA = ex; }
        });

        var taskB = Task.Run(() =>
        {
            try
            {
                barrier.SignalAndWait();
                bool inserted = false;
                while (!inserted)
                {
                    try
                    {
                        using var tx = engine.BeginTransaction();
                        tx.Insert(200, 2000);
                        tx.Commit();
                        inserted = true;
                    }
                    catch (TransactionConflictException) { /* retry */ }
                }
            }
            catch (Exception ex) { exB = ex; }
        });

        await Task.WhenAll(taskA, taskB);

        exA.Should().BeNull();
        exB.Should().BeNull();
        engine.TryGet(100, out int v100).Should().BeTrue();
        v100.Should().Be(1000);
        engine.TryGet(200, out int v200).Should().BeTrue();
        v200.Should().Be(2000);

        Cleanup(wal, mgr);
    }

    // ── Test 6: Auto-commit concurrent with transaction — no corruption ───────

    [Fact]
    public async Task AutoCommit_ConcurrentWithTransaction_NoCorruption()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine) = Open();

        // Pre-seed keys 0–99.
        for (int i = 0; i < 100; i++)
            engine.Insert(i, i);

        Exception? exA = null, exB = null;

        // Task A: 100 auto-commit Puts (keys 100–199), each retried on conflict
        // (mirrors the BPlusTree.Put retry loop built on top of engine.Insert).
        var taskA = Task.Run(() =>
        {
            try
            {
                for (int i = 100; i < 200; i++)
                {
                    bool done = false;
                    while (!done)
                    {
                        try { engine.Insert(i, i); done = true; }
                        catch (TransactionConflictException) { Thread.SpinWait(100); }
                    }
                }
            }
            catch (Exception ex) { exA = ex; }
        });

        // Task B: single transaction inserting keys 200–299, with retry on conflict.
        var taskB = Task.Run(() =>
        {
            try
            {
                bool done = false;
                while (!done)
                {
                    try
                    {
                        using var tx = engine.BeginTransaction();
                        for (int i = 200; i < 300; i++)
                            tx.Insert(i, i);
                        tx.Commit();
                        done = true;
                    }
                    catch (TransactionConflictException) { /* retry */ }
                }
            }
            catch (Exception ex) { exB = ex; }
        });

        await Task.WhenAll(taskA, taskB);

        exA.Should().BeNull("auto-commit task must not throw any unhandled exception");
        exB.Should().BeNull("transaction task must not throw any unhandled exception");

        // All 300 keys must be present and correct.
        for (int i = 0; i < 300; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue($"key {i} must be present");
            v.Should().Be(i);
        }

        Cleanup(wal, mgr);
    }

    // ── Test 7: Auto-commit without active transactions succeeds ──────────────

    [Fact]
    public void AutoCommit_NoActiveTransactions_Succeeds()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine) = Open();

        // No open transactions — auto-commit must proceed immediately without retrying.
        engine.Insert(1, 100);
        engine.TryGet(1, out int v).Should().BeTrue();
        v.Should().Be(100);

        Cleanup(wal, mgr);
    }

    // ── Test 5: Nested transactions on same thread (reentrancy) ───────────────

    [Fact]
    public void NestedTransactions_SameThread_BothKeysPersistAfterRetry()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine) = Open();

        // T_outer starts.
        var tOuter = engine.BeginTransaction();
        tOuter.Insert(1, 10);

        // T_inner starts on the same thread (same snapshot root — no deadlock).
        var tInner = engine.BeginTransaction();
        tInner.Insert(2, 20);

        // Commit inner first — advances root.
        tInner.Commit();
        tInner.Dispose();

        // Outer now sees stale root → conflict.
        Action commitOuter = () => tOuter.Commit();
        commitOuter.Should().Throw<TransactionConflictException>();
        tOuter.Dispose();

        // Retry outer.
        using (var tOuter2 = engine.BeginTransaction())
        {
            tOuter2.Insert(1, 10);
            tOuter2.Commit();
        }

        engine.TryGet(1, out int v1).Should().BeTrue();
        v1.Should().Be(10);
        engine.TryGet(2, out int v2).Should().BeTrue();
        v2.Should().Be(20);

        Cleanup(wal, mgr);
    }
}
