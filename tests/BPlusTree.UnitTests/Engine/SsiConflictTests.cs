using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Tests for SSI Phase 2: read-write conflict detection at commit (Phase 89).
///
/// Under the per-transaction writer lock (Phase 71), cross-thread transactions
/// are fully serialized — conflicts can only arise from same-thread nested
/// transactions where an inner writer commits (retiring pages) while an outer
/// reader is still open.
///
/// Properties verified:
///   1. Read-only transaction with no concurrent writers → no conflict.
///   2. Inner writer retires the same leaf the outer reader read → conflict at commit.
///   3. Writer committed BEFORE reader started → reader sees new version → no conflict.
///   4. Writer modifies an unrelated page (not in reader's ReadSet) → no conflict.
///   5. Write-only transaction (empty ReadSet) → SSI check is bypassed → no conflict.
/// </summary>
public class SsiConflictTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath       = _dbPath,
            WalFilePath        = _walPath,
            PageSize           = 4096,
            BufferPoolCapacity = 64,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { File.Delete(_dbPath); }  catch (IOException) { }
        try { File.Delete(_walPath); } catch (IOException) { }
    }

    // ── Test 1: read-only tx, no concurrent writers → no conflict ─────────────

    [Fact]
    public void Ssi_ReadOnlyTx_NoConflict()
    {
        using var tree = Open();
        tree.Put(1, 10);
        tree.Put(2, 20);

        using var tx = tree.BeginTransaction();
        tx.TryGet(1, out _);
        tx.Scan().ToList();   // build up a read set

        // No writers → no pages retired after our snapshot → no conflict
        Action commit = () => tx.Commit();
        commit.Should().NotThrow<TransactionConflictException>();
    }

    // ── Test 2: inner writer retires the leaf the outer reader read → CONFLICT ─

    [Fact]
    public void Ssi_InnerWriterRetiresSamePage_OuterReaderConflicts()
    {
        using var tree = Open();
        tree.Put(1, 10);   // creates initial leaf page

        // Outer transaction: reads key 1 → leaf page enters ReadSet
        using var outerTx = tree.BeginTransaction();
        outerTx.TryGet(1, out _);

        // Inner transaction (same thread — writer depth incremented via _txWriterDepth):
        // writes key 1 → CoW creates a shadow leaf, retires the original leaf
        using (var innerTx = tree.BeginTransaction())
        {
            innerTx.Insert(1, 99);   // shadows the leaf outerTx read
            innerTx.Commit();        // retires original leaf at epoch > outerTx._snapshotEpoch
        }

        // outerTx.ReadSet contains the now-retired leaf → SSI conflict
        Action outerCommit = () => outerTx.Commit();
        outerCommit.Should().Throw<TransactionConflictException>(
            "the leaf page outerTx read was retired by innerTx after outerTx started");
    }

    // ── Test 3: writer committed BEFORE reader started → no conflict ───────────

    [Fact]
    public void Ssi_WriterCommittedBeforeReaderStarted_NoConflict()
    {
        using var tree = Open();
        tree.Put(1, 10);

        // Writer commits first, retiring the initial leaf
        using (var writerTx = tree.BeginTransaction())
        {
            writerTx.Insert(1, 99);
            writerTx.Commit();
        }

        // Reader starts AFTER the write committed → its snapshotEpoch > retireEpoch
        // → it reads the new leaf (not the retired one)
        using var readerTx = tree.BeginTransaction();
        readerTx.TryGet(1, out int v);
        v.Should().Be(99);

        // The new leaf is in ReaderSet; it has NOT been retired → no conflict
        Action commit = () => readerTx.Commit();
        commit.Should().NotThrow<TransactionConflictException>();
    }

    // ── Test 4: writer modifies unrelated page → no conflict ──────────────────

    [Fact]
    public void Ssi_WriterModifiesUnrelatedPage_NoConflict()
    {
        using var tree = Open();

        // Seed 500 keys to guarantee multiple leaf pages (4096-byte page, int-int entries).
        // Key 0 lands on the first leaf; key 499 lands on the last leaf.
        for (int i = 0; i < 500; i++)
            tree.Put(i, i);

        // Outer reader: reads only key 0 → first leaf enters ReadSet
        using var readerTx = tree.BeginTransaction();
        readerTx.TryGet(0, out _);

        // Inner writer: updates key 499 → CoW retires the last leaf (different from first)
        using (var writerTx = tree.BeginTransaction())
        {
            writerTx.Insert(499, 999);
            writerTx.Commit();
        }

        // readerTx's ReadSet = {first leaf}; last leaf was retired → no overlap → no conflict
        Action commit = () => readerTx.Commit();
        commit.Should().NotThrow<TransactionConflictException>(
            "the retired page (last leaf) was not in readerTx's ReadSet (first leaf)");
    }

    // ── Test 5: write-only tx (empty ReadSet) → SSI bypassed, root version check fires ─────────
    // Phase 109a: SSI (FindConflictingPage) still returns 0 for write-only transactions —
    // the empty ReadSet means no page-retire check is performed.
    // However, the root version check at commit fires if another transaction committed
    // between this transaction's snapshot and commit. This is by design — write-only
    // transactions must retry on TransactionConflictException just like read-write ones.

    [Fact]
    public void Ssi_WriteOnlyTx_SsiBypassedButRootVersionCheckFires()
    {
        using var tree = Open();

        // Seed 500 keys so key 0 (first leaf) and key 499 (last leaf) are on different pages.
        for (int i = 0; i < 500; i++)
            tree.Put(i, i);

        // Outer tx: write-only (no reads → empty ReadSet → SSI bypass).
        using var outerTx = tree.BeginTransaction();
        outerTx.Insert(500, 500);

        // Inner tx: commits, advancing the live root.
        using (var innerTx = tree.BeginTransaction())
        {
            innerTx.Insert(0, 999);
            innerTx.Commit();
        }

        // outerTx: SSI (FindConflictingPage) returns 0 (empty ReadSet).
        // But root version check fires: live root changed since outerTx's snapshot.
        // Phase 109a ensures write-only transactions are no longer a lost-update vector.
        Action commit = () => outerTx.Commit();
        commit.Should().Throw<TransactionConflictException>(
            "root version check fires for write-only transactions with a stale snapshot (Phase 109a)");
    }
}
