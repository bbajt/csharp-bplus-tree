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
/// Phase 105 — CoW overflow chain retirement WAL record (Gap 3 closure).
///
/// T1: auto-commit CoW overflow→inline — WAL contains FreeOverflowChain(txId=0).
/// T2: transactional CoW overflow→inline — WAL contains FreeOverflowChain(txId>0)
///     appearing AFTER the Commit record for the same txId.
/// T3: crash-after-commit recovery — old overflow chain pages freed on reopen
///     (reused by the next allocation).
/// </summary>
public class RetirementWalTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    // ── Helpers ───────────────────────────────────────────────────────────────

    private (PageManager mgr, WalWriter wal, TreeEngine<int, byte[]> engine) OpenEngine()
    {
        var wal  = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = PageSize,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = int.MaxValue,
        }, wal);
        var ns   = new NodeSerializer<int, byte[]>(Int32Serializer.Instance, ByteArraySerializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, wal, new TreeEngine<int, byte[]>(mgr, ns, meta));
    }

    private static void SimulateCrash(WalWriter wal, PageManager mgr)
    {
        wal.Flush();
        wal.Dispose();
        mgr.Dispose();
    }

    private static List<uint> DecodePageIds(ReadOnlyMemory<byte> data)
    {
        var ids  = new List<uint>();
        var span = data.Span;
        for (int i = 0; i + 3 < span.Length; i += 4)
            ids.Add(BinaryPrimitives.ReadUInt32BigEndian(span.Slice(i, 4)));
        return ids;
    }

    // ── T1: auto-commit CoW — FreeOverflowChain(txId=0) written to WAL ────────

    [Fact]
    public void AutoCommitCow_OverflowToInline_WalContainsFreeOverflowChainRecord()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        List<uint> originalChainIds;

        {
            var (mgr, wal, engine) = OpenEngine();

            // Insert a large value to create an overflow chain for key 1.
            var large = new byte[32_000];
            new Random(1).NextBytes(large);
            engine.Insert(1, large);
            wal.Flush();

            // Capture the overflow chain IDs from the WAL.
            var reader   = new WalReader(_walPath);
            var allocRec = reader.ReadForward(LogSequenceNumber.None)
                                 .FirstOrDefault(r => r.Type == WalRecordType.AllocOverflowChain);
            allocRec.Should().NotBeNull("large insert must write AllocOverflowChain");
            originalChainIds = DecodePageIds(allocRec!.Data);
            originalChainIds.Should().NotBeEmpty();

            // Open a snapshot — forces the CoW path for the next auto-commit write.
            var snap = engine.BeginSnapshot();

            // Overwrite key 1 with a small inline value.
            // Auto-commit CoW path: RetireOverflowChain(..., writeWalRecord: true)
            // appends FreeOverflowChain(txId=0) to the WAL buffer.
            var small = new byte[4];
            engine.Insert(1, small);

            snap.Dispose();

            // Flush to make all WAL records durable.
            wal.Flush();
            mgr.Dispose();
            wal.Dispose();
        }

        // Verify FreeOverflowChain(txId=0) record exists in the WAL.
        var walReader    = new WalReader(_walPath);
        var freeRecords  = walReader.ReadForward(LogSequenceNumber.None)
                                    .Where(r => r.Type == WalRecordType.FreeOverflowChain)
                                    .ToList();

        freeRecords.Should().NotBeEmpty(
            "auto-commit CoW overflow→inline must write a FreeOverflowChain WAL record");

        var cowRecord = freeRecords.FirstOrDefault(r => r.TransactionId == 0);
        cowRecord.Should().NotBeNull("auto-commit FreeOverflowChain must use txId=0");

        var decoded = DecodePageIds(cowRecord!.Data);
        decoded.Should().BeEquivalentTo(originalChainIds,
            "FreeOverflowChain must contain the original overflow chain page IDs");
    }

    // ── T2: transactional CoW — FreeOverflowChain written AFTER Commit ────────

    [Fact]
    public void Transaction_CowOverflowToInline_WalContainsFreeOverflowChainAfterCommit()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        {
            var (mgr, wal, engine) = OpenEngine();

            // Insert a large overflow value for key 1 (auto-commit, in-place path).
            var large = new byte[32_000];
            new Random(2).NextBytes(large);
            engine.Insert(1, large);
            wal.Flush();

            // Begin a transaction; overwrite key 1 with a small inline value.
            // Transactional CoW path: RetireOverflowChain(..., tx.TrackObsoleteOverflowPage)
            // tracks IDs in _obsoleteOverflowPageIds.
            // Transaction.Commit() writes [Commit][FreeOverflowChain] then flushes.
            var tx    = engine.BeginTransaction();
            var small = new byte[4];
            tx.TryUpdate(1, small);
            tx.Commit();

            mgr.Dispose();
            wal.Dispose();
        }

        // Scan all WAL records; find Commit + FreeOverflowChain positions.
        var walReader = new WalReader(_walPath);
        var records   = walReader.ReadForward(LogSequenceNumber.None).ToList();

        // Find the Commit record for the transaction (txId > 0).
        var commitRec = records.FirstOrDefault(
            r => r.Type == WalRecordType.Commit && r.TransactionId > 0);
        commitRec.Should().NotBeNull("transaction must write a Commit WAL record");

        // Find FreeOverflowChain record for the same txId.
        var freeRec = records.FirstOrDefault(
            r => r.Type == WalRecordType.FreeOverflowChain
              && r.TransactionId == commitRec!.TransactionId);
        freeRec.Should().NotBeNull(
            "transactional CoW overflow→inline must write FreeOverflowChain in Commit()");

        // FreeOverflowChain must appear AFTER the Commit record in the WAL.
        int commitIdx = records.IndexOf(commitRec!);
        int freeIdx   = records.IndexOf(freeRec!);
        freeIdx.Should().BeGreaterThan(commitIdx,
            "FreeOverflowChain must be written after Commit so EvictionWorker " +
            "cannot make it durable without Commit");

        // The record must contain at least one page ID.
        DecodePageIds(freeRec!.Data).Should().NotBeEmpty(
            "FreeOverflowChain record must contain the overflow chain page IDs");
    }

    // ── T3: crash-after-commit recovery — chain pages freed on reopen ─────────

    [Fact]
    public void CrashAfterCommit_CowRetiredOverflowPages_FreedOnReopen()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        List<uint> originalChainIds;

        // Phase 1: write overflow value, open snapshot, CoW-overwrite → retire chain.
        // FreeOverflowChain WAL record written. Then crash (WAL flushed, no checkpoint).
        {
            var (mgr, wal, engine) = OpenEngine();

            var large = new byte[32_000];
            new Random(3).NextBytes(large);
            engine.Insert(1, large);

            // Capture chain IDs from WAL.
            wal.Flush();
            var reader   = new WalReader(_walPath);
            var allocRec = reader.ReadForward(LogSequenceNumber.None)
                                 .FirstOrDefault(r => r.Type == WalRecordType.AllocOverflowChain);
            allocRec.Should().NotBeNull();
            originalChainIds = DecodePageIds(allocRec!.Data);
            originalChainIds.Should().NotBeEmpty();

            // Open snapshot → forces auto-commit CoW path.
            var snap  = engine.BeginSnapshot();
            var small = new byte[4];
            engine.Insert(1, small);   // CoW: retires old chain + writes FreeOverflowChain WAL
            snap.Dispose();

            SimulateCrash(wal, mgr);   // WAL flushed to disk; data file not checkpointed
        }

        // Phase 2: recover. Redo Pass applies UpdatePage+UpdateMeta (new value installed).
        // FreeOverflowChain Redo applied after ReloadFreeList → old chain pages freed.
        {
            var (mgr2, wal2, engine2) = OpenEngine();

            // Key 1 must have the new small value (Redo applied the CoW write).
            engine2.TryGet(1, out var recovered)
                   .Should().BeTrue("key 1 must survive crash+recovery");
            recovered.Should().HaveCount(4, "new small value must be recovered");

            // Old overflow chain pages must be back in the free list.
            // Allocating that many Overflow pages should reuse them.
            var allocatedIds = new List<uint>();
            for (int i = 0; i < originalChainIds.Count; i++)
            {
                var frame = mgr2.AllocatePage(PageType.Overflow);
                allocatedIds.Add(frame.PageId);
                mgr2.MarkDirtyAndUnpin(frame.PageId);
            }

            allocatedIds.Should().BeEquivalentTo(originalChainIds,
                "Redo Pass must have freed the overflow chain pages so they are reused");

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
