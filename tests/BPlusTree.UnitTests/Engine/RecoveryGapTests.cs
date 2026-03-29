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
/// Phase 102 — Crash-recovery gap closures.
///
/// Gap 2: FreeOverflowChain WAL record (WalRecordType.FreeOverflowChain = 13).
///   T1 verifies the record is written by PageManager.FreeOverflowChain.
///   T2 verifies the Redo Pass re-applies chain frees on crash-after-WAL-fsync-before-free.
///
/// Gap 1: Auto-commit CoW shadow page orphaning.
///   T3 verifies orphaned shadow pages are freed on recovery when AllocShadowChain(txId=0)
///      appears in WAL without a following UpdateMeta.
///   T4 verifies that completed CoW writes (AllocShadowChain + UpdateMeta both in WAL)
///      do NOT have their shadow pages freed — they are live tree nodes.
/// </summary>
public class RecoveryGapTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    // ── Helpers ───────────────────────────────────────────────────────────────

    private (PageManager mgr, WalWriter wal, TreeEngine<int, byte[]> engine) OpenByteEngine()
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

    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine) OpenIntEngine()
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
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, wal, new TreeEngine<int, int>(mgr, ns, meta));
    }

    private static void SimulateCrash(WalWriter wal, PageManager mgr)
    {
        wal.Flush();    // WAL buffer → disk (no fsync of data file; no checkpoint)
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

    // ── T1: Gap 2 — FreeOverflowChain writes WAL record ──────────────────────

    [Fact]
    public void FreeOverflowChain_WritesWalRecord_WithCorrectPageIds()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        List<uint> chainIds;

        // Insert a 32 KB value → overflow chain.
        // Then upsert with a tiny value → FreeOverflowChain called on old chain.
        {
            var (mgr, wal, engine) = OpenByteEngine();

            var largeValue = new byte[32_000];
            new Random(1).NextBytes(largeValue);
            engine.Insert(1, largeValue);

            // Delete the overflow entry — unconditionally calls FreeOverflowChain on the chain.
            engine.Delete(1);

            wal.Flush();
            mgr.Dispose();
            wal.Dispose();
        }

        // Scan WAL for AllocOverflowChain to get original chain IDs.
        var reader = new WalReader(_walPath);
        var allocRecords = reader.ReadForward(LogSequenceNumber.None)
                                 .Where(r => r.Type == WalRecordType.AllocOverflowChain)
                                 .ToList();
        allocRecords.Should().NotBeEmpty("a large insert must write AllocOverflowChain");
        chainIds = DecodePageIds(allocRecords[0].Data);
        chainIds.Should().NotBeEmpty();

        // Verify FreeOverflowChain record exists with the same page IDs.
        var freeRecords = reader.ReadForward(LogSequenceNumber.None)
                                .Where(r => r.Type == WalRecordType.FreeOverflowChain)
                                .ToList();
        freeRecords.Should().HaveCount(1, "upsert overflow→inline must write exactly one FreeOverflowChain record");

        var decoded = DecodePageIds(freeRecords[0].Data);
        decoded.Should().BeEquivalentTo(chainIds,
            "FreeOverflowChain record must contain the original overflow chain page IDs");
    }

    // ── T2: Gap 2 — Redo Pass re-applies FreeOverflowChain on recovery ────────

    [Fact]
    public void Recovery_FreeOverflowChain_Redo_FreesOrphanedChainPages()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        List<uint> chainIds;

        // Phase 1: insert a 32 KB overflow value; capture chain IDs from WAL;
        // then crash without running FreeOverflowChain.
        {
            var (mgr, wal, engine) = OpenByteEngine();

            var largeValue = new byte[32_000];
            new Random(2).NextBytes(largeValue);
            engine.Insert(1, largeValue);

            // Flush WAL so overflow page WAL records are on disk.
            wal.Flush();

            // Find chain IDs from WAL.
            var reader = new WalReader(_walPath);
            var allocRecs = reader.ReadForward(LogSequenceNumber.None)
                                  .Where(r => r.Type == WalRecordType.AllocOverflowChain)
                                  .ToList();
            allocRecs.Should().NotBeEmpty();
            chainIds = DecodePageIds(allocRecs[0].Data);
            chainIds.Should().NotBeEmpty();

            SimulateCrash(wal, mgr);
        }

        // Phase 2: manually append a FreeOverflowChain WAL record (simulates the record
        // that would have been written by FreeOverflowChain before the crash, but whose
        // corresponding FreePage calls never executed).
        {
            var wal2 = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
            wal2.AppendFreeOverflowChain(0, chainIds.ToArray());
            wal2.Flush();
            wal2.Dispose();
        }

        // Phase 3: recover. Redo Pass applies FreeOverflowChain → chain pages freed.
        {
            var (mgr3, wal3, _) = OpenByteEngine();

            // Allocate pages — freed chain IDs should be reused.
            var allocatedIds = new List<uint>();
            for (int i = 0; i < chainIds.Count; i++)
            {
                var frame = mgr3.AllocatePage(PageType.Overflow);
                allocatedIds.Add(frame.PageId);
                mgr3.MarkDirtyAndUnpin(frame.PageId);
            }

            allocatedIds.Should().BeEquivalentTo(chainIds,
                "Redo Pass must have freed the overflow chain pages so they are reused");

            wal3.Dispose();
            mgr3.Dispose();
        }
    }

    // ── T3: Gap 1 — orphaned auto-commit CoW shadows freed on recovery ────────

    [Fact]
    public void Recovery_AutoCommitCoW_OrphanedShadowPages_Freed()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        uint shadowA, shadowB;

        // Phase 1: allocate two pages (simulate shadow pages),
        // write AllocShadowChain(txId=0) for them, then crash WITHOUT UpdateMeta.
        {
            var (mgr, wal, _) = OpenByteEngine();

            // Allocate two "shadow" pages — these represent what CopyWritePathAndAllocShadows
            // would allocate during an auto-commit CoW write.
            var frameA = mgr.AllocatePage(PageType.Leaf);
            shadowA = frameA.PageId;
            mgr.MarkDirtyAndUnpin(frameA.PageId);

            var frameB = mgr.AllocatePage(PageType.Leaf);
            shadowB = frameB.PageId;
            mgr.MarkDirtyAndUnpin(frameB.PageId);

            // Write AllocShadowChain(txId=0) for the two shadow pages — no UpdateMeta follows.
            wal.AppendAllocShadowChain(0, new[] { shadowA, shadowB });

            SimulateCrash(wal, mgr); // crash before UpdateMeta
        }

        // Phase 2: recover. Analysis Pass collects AllocShadowChain(txId=0) into
        // pendingAutoShadows; no UpdateMeta clears it → Undo Pass frees both pages.
        {
            var (mgr2, wal2, _) = OpenByteEngine();

            // Allocate 2 pages — the freed shadow IDs should be reused.
            var frame1 = mgr2.AllocatePage(PageType.Leaf);
            var id1 = frame1.PageId;
            mgr2.MarkDirtyAndUnpin(id1);

            var frame2 = mgr2.AllocatePage(PageType.Leaf);
            var id2 = frame2.PageId;
            mgr2.MarkDirtyAndUnpin(id2);

            new[] { id1, id2 }.Should().BeEquivalentTo(new[] { shadowA, shadowB },
                "Analysis Pass must have freed orphaned auto-commit CoW shadow pages");

            wal2.Dispose();
            mgr2.Dispose();
        }
    }

    // ── T4: Gap 1 — completed CoW write leaves shadow pages live ─────────────

    [Fact]
    public void Recovery_AutoCommitCoW_ShadowPages_Live_WhenWriteCompleted()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        // Phase 1: populate tree, open snapshot (forces CoW path),
        // insert a key while snapshot is open → AllocShadowChain(txId=0) + UpdateMeta
        // both written to WAL. Crash after.
        {
            var (mgr, wal, engine) = OpenIntEngine();

            // Populate so the tree has a real leaf with entries.
            for (int i = 1; i <= 50; i++)
                engine.Insert(i, i * 10);

            // Open snapshot to force CoW on the next insert.
            var snap = engine.BeginSnapshot();

            // Auto-commit insert with snapshot active → CopyWritePathAndAllocShadows
            // → AllocShadowChain(txId=0) written, then UpdateMeta written.
            engine.Insert(9001, 90010);

            snap.Dispose();

            SimulateCrash(wal, mgr);
        }

        // Verify AllocShadowChain(txId=0) and UpdateMeta are both in WAL.
        {
            var walReader = new WalReader(_walPath);
            var records   = walReader.ReadForward(LogSequenceNumber.None).ToList();

            records.Any(r => r.Type == WalRecordType.AllocShadowChain && r.TransactionId == 0)
                   .Should().BeTrue("CoW insert must emit AllocShadowChain(txId=0)");
            records.Any(r => r.Type == WalRecordType.UpdateMeta && r.TransactionId == 0)
                   .Should().BeTrue("completed write must emit UpdateMeta(txId=0)");
        }

        // Phase 2: recover. pendingAutoShadows should be cleared by UpdateMeta
        // → shadow pages are NOT freed → inserted key is readable.
        {
            var (mgr2, wal2, engine2) = OpenIntEngine();

            engine2.TryGet(9001, out int v)
                   .Should().BeTrue("completed CoW insert must survive crash+recovery");
            v.Should().Be(90010);

            // Original data also intact.
            engine2.TryGet(1, out int v1)
                   .Should().BeTrue();
            v1.Should().Be(10);

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
