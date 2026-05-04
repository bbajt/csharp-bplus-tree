using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Api;

/// <summary>
/// Integration tests for BeginTransaction, BeginSnapshot, and conflict detection
/// through the public BPlusTree&lt;TKey,TValue&gt; API (Phase M+8).
///
/// All tests use BPlusTree.Open() — the full public stack — rather than internal
/// engine components. PageSize=8192 keeps 200 records in a single leaf (no splits).
/// </summary>
public class BPlusTreeTransactionTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 8192,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 4096,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
    }

    // ── Test 1: committed insert is visible after commit ──

    [Fact]
    public void Transaction_Insert_Commit_KeyVisible()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 100; i++) tree.Put(i, i * 10);

        using (var tx = tree.BeginTransaction())
        {
            tx.Insert(9999, 777);
            tx.Commit();
        }

        tree.TryGet(9999, out int v).Should().BeTrue("committed insert must be visible");
        v.Should().Be(777);
        tree.TryGet(100, out int existing).Should().BeTrue("pre-existing key must remain");
        existing.Should().Be(1000);
    }

    // ── Test 2: rolled-back insert is not visible ──

    [Fact]
    public void Transaction_Insert_Rollback_KeyAbsent()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 100; i++) tree.Put(i, i * 10);

        var tx = tree.BeginTransaction();
        tx.Insert(9999, 777);
        tx.Dispose();   // rollback — no Commit()

        tree.TryGet(9999, out _).Should().BeFalse("rolled-back insert must not be visible");
        tree.GetStatistics().TotalRecords.Should().Be(100, "record count must be unchanged after rollback");
    }

    // ── Test 3: committed delete removes the key ──

    [Fact]
    public void Transaction_Delete_Commit_KeyGone()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 100; i++) tree.Put(i, i * 10);

        using (var tx = tree.BeginTransaction())
        {
            tx.TryDelete(50).Should().BeTrue();
            tx.Commit();
        }

        tree.TryGet(50, out _).Should().BeFalse("committed delete must remove the key");
        tree.TryGet(49, out _).Should().BeTrue("adjacent key must remain");
        tree.TryGet(51, out _).Should().BeTrue("adjacent key must remain");
        tree.GetStatistics().TotalRecords.Should().Be(99);
    }

    // ── Test 4: tx.TryGet and tx.Scan see the transaction's own uncommitted writes ──

    [Fact]
    public void Transaction_TryGet_And_Scan_ReadYourOwnWrites()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 100; i++) tree.Put(i, i * 10);

        using var tx = tree.BeginTransaction();

        tx.Insert(9999, 777);
        tx.TryDelete(50);

        // TryGet reads from the transaction's shadow tree.
        tx.TryGet(9999, out int inserted).Should().BeTrue("own insert must be visible via tx.TryGet");
        inserted.Should().Be(777);
        tx.TryGet(50, out _).Should().BeFalse("own delete must be reflected via tx.TryGet");
        tx.TryGet(1, out int existing).Should().BeTrue("unmodified key must be visible");
        existing.Should().Be(10);

        // Scan also reads from the shadow tree.
        var results = tx.Scan().ToList();
        results.Should().Contain((9999, 777));
        results.Should().NotContain(r => r.Key == 50);
        results.Count.Should().Be(100, "100 = original 100 - 1 deleted + 1 inserted");

        tx.Commit();
    }

    // ── Test 5: snapshot returns a frozen view while the tree is written ──

    [Fact]
    public void Transaction_Snapshot_FrozenDuringConcurrentWrites()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 100; i++) tree.Put(i, i * 10);

        using var snap = tree.BeginSnapshot();

        // Writes after the snapshot is opened.
        tree.Put(9999, 777);
        tree.Delete(50);

        snap.TryGet(9999, out _).Should().BeFalse("key written after snapshot open must not be visible");
        snap.TryGet(50, out int v).Should().BeTrue("key deleted after snapshot open must still be visible");
        v.Should().Be(500);
        snap.Scan().Count().Should().Be(100, "snapshot count frozen at 100");

        // Live tree reflects the changes.
        tree.TryGet(9999, out _).Should().BeTrue("new key must be in the live tree");
        tree.TryGet(50, out _).Should().BeFalse("deleted key must be gone from the live tree");
    }

    // ── Test 6: two transactions on the same page throw TransactionConflictException ──

    [Fact]
    public void Transaction_ConflictException_TwoTransactionsOnSamePage()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        // 100 keys all reside in one leaf at pageSize=8192.
        for (int i = 1; i <= 100; i++) tree.Put(i, i * 10);

        var tx1 = tree.BeginTransaction();
        var tx2 = tree.BeginTransaction();

        // tx1 locks the leaf via CaptureBeforeImage.
        tx1.TryDelete(1);

        // tx2 tries to operate on the same leaf — must conflict.
        tx2.Invoking(t => t.TryDelete(50))
            .Should().Throw<TransactionConflictException>(
                "tx2 must be rejected when tx1 holds the leaf write lock");

        tx1.Commit();
        tx2.Dispose();
    }

    // ── Test 7: DeleteRange via transaction commits atomically ──

    [Fact]
    public void Transaction_DeleteRange_Commit()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 100; i++) tree.Put(i, i);

        int deleted;
        using (var tx = tree.BeginTransaction())
        {
            deleted = tx.DeleteRange(10, 20);
            tx.Commit();
        }

        deleted.Should().Be(11, "keys 10–20 inclusive = 11 keys");
        for (int i = 10; i <= 20; i++)
            tree.TryGet(i, out _).Should().BeFalse($"key {i} must be gone after DeleteRange commit");
        tree.TryGet(9, out _).Should().BeTrue("key below range must remain");
        tree.TryGet(21, out _).Should().BeTrue("key above range must remain");
        tree.GetStatistics().TotalRecords.Should().Be(89);
    }

    // ── Scope tests (Phase 72) ────────────────────────────────────────────────

    // Test 8: Complete() commits all scope operations.
    [Fact]
    public void Scope_Complete_CommitsInserts()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        using (var scope = tree.BeginScope())
        {
            scope.Insert(1, 100);
            scope.Insert(2, 200);
            scope.Complete();
        }

        tree.TryGet(1, out int v1).Should().BeTrue("key 1 must be committed");
        v1.Should().Be(100);
        tree.TryGet(2, out int v2).Should().BeTrue("key 2 must be committed");
        v2.Should().Be(200);
    }

    // Test 9: Dispose without Complete() rolls back all scope operations.
    [Fact]
    public void Scope_NoComplete_RollsBack()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        tree.Put(1, 999);

        using (var scope = tree.BeginScope())
        {
            scope.Insert(2, 200);
            scope.Insert(3, 300);
            // No Complete() — Dispose rolls back.
        }

        tree.TryGet(2, out _).Should().BeFalse("key 2 must be absent after rollback");
        tree.TryGet(3, out _).Should().BeFalse("key 3 must be absent after rollback");
        tree.TryGet(1, out int v1).Should().BeTrue("pre-existing key 1 must be unaffected");
        v1.Should().Be(999);
    }

    // Test 10: Exception in using-block body rolls back via Dispose (no Complete called).
    [Fact]
    public void Scope_ExceptionInBody_RollsBack()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        var act = () =>
        {
            using var scope = tree.BeginScope();
            scope.Insert(42, 420);
            throw new InvalidOperationException("simulated failure");
            // Complete() never reached — scope.Dispose() rolls back.
        };

        act.Should().Throw<InvalidOperationException>("exception propagates out of scope");

        tree.TryGet(42, out _).Should().BeFalse("key 42 must be absent — scope rolled back on exception");
    }

    // ── Async commit tests (Phase 75) ────────────────────────────────────────────

    // Test 12: await using scope with Complete() → CommitAsync → FlushAsync (non-blocking fsync).
    [Fact]
    public async Task Scope_Async_Complete_CommitsInserts()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        await using (var scope = tree.BeginScope())
        {
            scope.Insert(1, 100);
            scope.Insert(2, 200);
            scope.Complete();
            // DisposeAsync → CommitAsync → FlushAsync
        }

        tree.TryGet(1, out int v1).Should().BeTrue("key 1 must be committed via async path");
        v1.Should().Be(100);
        tree.TryGet(2, out int v2).Should().BeTrue("key 2 must be committed via async path");
        v2.Should().Be(200);
    }

    // Test 13: await using scope without Complete() → rollback (synchronous) on DisposeAsync.
    [Fact]
    public async Task Scope_Async_NoComplete_RollsBack()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        tree.Put(1, 999);

        await using (var scope = tree.BeginScope())
        {
            scope.Insert(2, 200);
            // No Complete() — DisposeAsync rolls back via Dispose()
        }

        tree.TryGet(2, out _).Should().BeFalse("key 2 must be absent after async rollback");
        tree.TryGet(1, out int v1).Should().BeTrue("pre-existing key 1 must be unaffected");
        v1.Should().Be(999);
    }

    // Test 14: Direct ITransaction.CommitAsync() — key must be durable after await.
    [Fact]
    public async Task Transaction_CommitAsync_KeyVisible()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 10; i++) tree.Put(i, i * 10);

        using var tx = tree.BeginTransaction();
        tx.Insert(42, 420);
        await tx.CommitAsync();

        tree.TryGet(42, out int v).Should().BeTrue("key 42 must be visible after CommitAsync");
        v.Should().Be(420);
        tree.TryGet(1, out int existing).Should().BeTrue("pre-existing key must remain");
        existing.Should().Be(10);
    }

    // Test 11: Mix of Insert/TryUpdate/TryDelete in one scope — Complete commits all.
    [Fact]
    public void Scope_MultipleOperations_CommitAll()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        for (int i = 1; i <= 10; i++) tree.Put(i, i * 10);

        using (var scope = tree.BeginScope())
        {
            scope.Insert(99, 990);           // new key
            scope.TryUpdate(5, 555);         // overwrite existing
            scope.TryDelete(3);              // remove existing
            scope.Complete();
        }

        tree.TryGet(99, out int v99).Should().BeTrue("inserted key 99 must be present");
        v99.Should().Be(990);
        tree.TryGet(5, out int v5).Should().BeTrue("updated key 5 must be present");
        v5.Should().Be(555);
        tree.TryGet(3, out _).Should().BeFalse("deleted key 3 must be absent");
        tree.TryGet(1, out int v1).Should().BeTrue("untouched key 1 must be present");
        v1.Should().Be(10);
    }
}
