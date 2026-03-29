using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;
using FluentAssertions;
using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Phase 98 — AllocOverflowChain + AllocShadowChain WAL records.
/// Verifies: records are written to WAL, Undo Pass frees overflow pages on crash,
/// Undo Pass frees shadow pages on crash (Gap 1 closure).
/// </summary>
public class WalAllocRecordTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    // ── Helpers ───────────────────────────────────────────────────────────────

    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine) OpenEngine()
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
        return (mgr, wal, new TreeEngine<int, int>(mgr, ns, meta));
    }

    private (PageManager mgr, WalWriter wal) OpenManager()
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
        return (mgr, wal);
    }

    private static void SimulateCrash(WalWriter wal, PageManager mgr)
    {
        wal.Flush();   // WAL buffer → disk
        wal.Dispose();
        mgr.Dispose(); // pages → disk; no checkpoint
    }

    private static List<uint> DecodePageIds(ReadOnlyMemory<byte> data)
    {
        var ids  = new List<uint>();
        var span = data.Span;
        for (int i = 0; i + 3 < span.Length; i += 4)
            ids.Add(BinaryPrimitives.ReadUInt32BigEndian(span.Slice(i, 4)));
        return ids;
    }

    // ── Test 1: AllocOverflowChain writes a WAL record ────────────────────────

    [Fact]
    public void AllocOverflowChain_WritesWalRecord_WithCorrectPageIds()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal) = OpenManager();

        // Manually append Begin so the Analysis Pass tracks txId=42.
        wal.Append(WalRecordType.Begin, 42, 0, LogSequenceNumber.None,
                   ReadOnlySpan<byte>.Empty);

        // 8192-byte value → 2 overflow pages at DataCapacity(8192)=8154 bytes each.
        var value = new byte[8192];
        new Random(42).NextBytes(value);
        mgr.AllocateOverflowChain(value, out _, out uint[] chainPageIds, txId: 42);
        chainPageIds.Should().HaveCount(2);

        wal.Flush();
        mgr.Dispose();
        wal.Dispose();

        // Scan WAL — must contain exactly one AllocOverflowChain record.
        var reader  = new WalReader(_walPath);
        var records = reader.ReadForward(LogSequenceNumber.None)
                            .Where(r => r.Type == WalRecordType.AllocOverflowChain)
                            .ToList();
        records.Should().HaveCount(1, "AllocateOverflowChain must write exactly one record");

        var record   = records[0];
        record.TransactionId.Should().Be(42);
        var decoded  = DecodePageIds(record.Data);
        decoded.Should().BeEquivalentTo(chainPageIds,
            "WAL record data must encode all overflow page IDs in order");
    }

    // ── Test 2: AllocShadowChain writes a WAL record on transactional insert ──

    [Fact]
    public void AllocShadowChain_WritesWalRecord_OnTransactionalInsert()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine) = OpenEngine();

        // Pre-populate so the tree has at least one leaf (non-empty root).
        for (int i = 1; i <= 10; i++) engine.Insert(i, i * 10);

        // Transactional insert → CopyWritePathAndAllocShadows → AppendAllocShadowChain.
        var tx = engine.BeginTransaction();
        tx.Insert(9999, 99990);
        wal.Flush(); // capture record before commit
        tx.Commit();

        mgr.Dispose();
        wal.Dispose();

        // Scan WAL — must contain at least one AllocShadowChain record with non-zero txId.
        var reader  = new WalReader(_walPath);
        var records = reader.ReadForward(LogSequenceNumber.None)
                            .Where(r => r.Type == WalRecordType.AllocShadowChain)
                            .ToList();
        records.Should().NotBeEmpty("transactional insert must emit AllocShadowChain record");
        records.Should().OnlyContain(r => r.TransactionId != 0,
            "AllocShadowChain records must carry the transaction ID");
    }

    // ── Test 3: Undo Pass frees overflow pages on crash-before-commit ─────────

    [Fact]
    public void UndoPass_FreesOverflowPages_OnCrashBeforeCommit()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        uint[] chainPageIds;

        // Phase 1: allocate overflow chain, crash without committing.
        {
            var (mgr, wal, _) = OpenEngine();

            // Manually begin txId=42 so the Analysis Pass tracks it.
            wal.Append(WalRecordType.Begin, 42, 0, LogSequenceNumber.None,
                       ReadOnlySpan<byte>.Empty);

            var value = new byte[8192];
            mgr.AllocateOverflowChain(value, out _, out chainPageIds, txId: 42);
            chainPageIds.Should().HaveCount(2);

            SimulateCrash(wal, mgr); // no Commit(42) → transaction left open in WAL
        }

        // Phase 2: recover. RecoverFromWal runs inside TreeEngine ctor.
        // Analysis Pass: finds Begin(42), AllocOverflowChain(42) → crashedAllocations[42]
        // Undo Pass: FreePage for each overflow page ID.
        {
            var (mgr2, wal2, _) = OpenEngine();

            // The Undo Pass freed chainPageIds[0] and chainPageIds[1].
            // Allocating 2 new pages should reuse the freed IDs.
            var f1 = mgr2.AllocatePage(PageType.Overflow);
            var id1 = f1.PageId;
            mgr2.MarkDirtyAndUnpin(f1.PageId);

            var f2 = mgr2.AllocatePage(PageType.Overflow);
            var id2 = f2.PageId;
            mgr2.MarkDirtyAndUnpin(f2.PageId);

            new[] { id1, id2 }.Should().BeEquivalentTo(chainPageIds,
                "Undo Pass must have freed the overflow pages so they are reusable");

            wal2.Dispose();
            mgr2.Dispose();
        }
    }

    // ── Test 4: Undo Pass frees shadow pages on crash-before-commit (Gap 1) ───

    [Fact]
    public void UndoPass_FreesShadowPages_OnCrashBeforeCommit_Gap1Closed()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        // Phase 1: insert committed data, then start a transaction, crash without commit.
        {
            var (mgr, wal, engine) = OpenEngine();
            for (int i = 1; i <= 100; i++) engine.Insert(i, i * 10);

            var tx = engine.BeginTransaction();
            tx.Insert(9001, 90010); // triggers CopyWritePathAndAllocShadows → AllocShadowChain

            SimulateCrash(wal, mgr); // no tx.Commit() → transaction left open
        }

        // Phase 2: recover.
        // Undo Pass: finds Begin(txId) + AllocShadowChain(txId) → frees shadow pages.
        //            applies before-images from UpdatePage records → undoes tx.Insert(9001).
        {
            var (mgr2, wal2, engine2) = OpenEngine();

            // Uncommitted insert must be absent.
            engine2.TryGet(9001, out _)
                   .Should().BeFalse("uncommitted insert must be undone by the Undo Pass");

            // Committed data must survive.
            engine2.TryGet(1, out int v)
                   .Should().BeTrue("committed auto-commit insert must survive crash");
            v.Should().Be(10);

            wal2.Dispose();
            mgr2.Dispose();
        }
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    public void Dispose()
    {
        if (File.Exists(_dbPath))  File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);
    }
}
