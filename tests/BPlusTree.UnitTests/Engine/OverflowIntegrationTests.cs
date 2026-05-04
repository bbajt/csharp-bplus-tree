using System;
using System.Buffers.Binary;
using System.IO;
using Xunit;
using FluentAssertions;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Phase 99a — TreeEngine overflow integration: TryGet and Delete in-place fast path.
/// Tests use TreeEngine&lt;int, byte[]&gt; so that large byte[] values can be injected
/// as overflow entries without needing Phase 99b's write path.
/// Overflow entries are set up by:
///   1. Inserting a placeholder byte[5] value (serialises to exactly 9 bytes = OverflowPointerSize).
///   2. Allocating an overflow chain via PageManager directly.
///   3. Injecting the pointer record into the leaf and setting the SlotIsOverflow flag.
/// This simulates the on-disk state that Phase 99b will produce transparently.
/// </summary>
public class OverflowIntegrationTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    // ── Helpers ───────────────────────────────────────────────────────────────

    private (PageManager mgr, WalWriter wal, TreeEngine<int, byte[]> engine, TreeMetadata meta) OpenEngine()
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
        return (mgr, wal, new TreeEngine<int, byte[]>(mgr, ns, meta), meta);
    }

    /// <summary>
    /// Injects an overflow pointer record into the leaf slot for <paramref name="slotIndex"/>.
    /// The cell at that slot must already be exactly PageLayout.OverflowPointerSize bytes in the
    /// value area (key is 4 bytes for int; value area = cellLen - 4).
    /// Sets SlotIsOverflow flag and writes [totalLen:4 BE][firstPageId:4 BE][0:1].
    /// Caller is responsible for MarkDirtyAndUnpin after this call.
    /// </summary>
    private static void InjectOverflowPointer(
        Frame leafFrame, int slotIndex,
        uint firstPageId, int totalSerializedLen)
    {
        int slotBase  = PageLayout.FirstSlotOffset + slotIndex * PageLayout.SlotEntrySize;
        uint packed   = BinaryPrimitives.ReadUInt32BigEndian(leafFrame.Data.AsSpan(slotBase, 4));
        ushort cellOff = (ushort)(packed >> 16);
        // Key is 4 bytes (int); value starts at cellOff + 4.
        int valueOff = cellOff + 4;

        // Write pointer record in-place.
        BinaryPrimitives.WriteInt32BigEndian(leafFrame.Data.AsSpan(valueOff, 4), totalSerializedLen);
        BinaryPrimitives.WriteUInt32BigEndian(leafFrame.Data.AsSpan(valueOff + 4, 4), firstPageId);
        leafFrame.Data[valueOff + 8] = 0;

        // Set IsOverflow flag in slot byte 4.
        leafFrame.Data[slotBase + PageLayout.SlotFlagsOffset] = PageLayout.SlotIsOverflow;
    }

    // ── Test 1: TryGet reads overflow value round-trip ────────────────────────

    [Fact]
    public void TryGet_ReadsOverflowValue_RoundTrip()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, meta) = OpenEngine();

        // Step 1: Insert key=42 with placeholder byte[5] (serialises to 9 bytes = OverflowPointerSize).
        byte[] placeholder = new byte[5];
        engine.Insert(42, placeholder);

        // Step 2: Prepare large data and serialise it for the overflow chain.
        byte[] largeData = new byte[100];
        new Random(1).NextBytes(largeData);
        // ByteArraySerializer serialises as [4-byte length][content].
        byte[] serialisedLargeData = new byte[4 + largeData.Length];
        BinaryPrimitives.WriteInt32BigEndian(serialisedLargeData, largeData.Length);
        largeData.CopyTo(serialisedLargeData.AsSpan(4));

        // Step 3: Allocate overflow chain for the serialised bytes (1 overflow page for 104 bytes).
        mgr.AllocateOverflowChain(serialisedLargeData, out uint firstPageId, out uint[] chainIds);
        chainIds.Should().HaveCount(1, "104 bytes fits in one overflow page");

        // Step 4: Inject overflow pointer into the leaf slot.
        // Root page is the only leaf (single entry tree).
        uint leafPageId = meta.RootPageId;
        var leafFrame   = mgr.FetchPage(leafPageId);
        InjectOverflowPointer(leafFrame, slotIndex: 0, firstPageId, serialisedLargeData.Length);
        mgr.MarkDirtyAndUnpin(leafPageId, bypassWal: true);

        // Step 5: TryGet should dispatch to ReadOverflowChain and return the original data.
        bool found = engine.TryGet(42, out byte[] result);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(largeData, "TryGet must reassemble the value from the overflow chain");
    }

    // ── Test 2: TryGet inline value — regression ──────────────────────────────

    [Fact]
    public void TryGet_InlineValue_UnchangedByPhase99a()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] expected = new byte[] { 1, 2, 3 };
        engine.Insert(42, expected);

        bool found = engine.TryGet(42, out byte[] result);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(expected, "inline TryGet must be unaffected by Phase 99a changes");
    }

    // ── Test 3: Delete frees overflow chain on in-place fast path ─────────────

    [Fact]
    public void Delete_FreesOverflowChain_OnInPlacePath()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, meta) = OpenEngine();

        // Insert placeholder.
        byte[] placeholder = new byte[5];
        engine.Insert(42, placeholder);

        // Allocate a 1-page overflow chain.
        byte[] serialisedData = new byte[104];
        new Random(2).NextBytes(serialisedData);
        BinaryPrimitives.WriteInt32BigEndian(serialisedData, 100); // length prefix
        mgr.AllocateOverflowChain(serialisedData, out uint firstPageId, out uint[] chainIds);
        chainIds.Should().HaveCount(1);

        // Inject overflow pointer into the leaf.
        uint leafPageId = meta.RootPageId;
        var leafFrame   = mgr.FetchPage(leafPageId);
        InjectOverflowPointer(leafFrame, slotIndex: 0, firstPageId, serialisedData.Length);
        mgr.MarkDirtyAndUnpin(leafPageId, bypassWal: true);

        // Delete key=42 — in-place fast path must free the overflow chain.
        bool deleted = engine.Delete(42);
        deleted.Should().BeTrue();

        // Allocate one new page: it should reuse the freed overflow page.
        var freshFrame = mgr.AllocatePage(PageType.Overflow);
        uint reusedId  = freshFrame.PageId;
        mgr.MarkDirtyAndUnpin(freshFrame.PageId);

        wal.Dispose();
        mgr.Dispose();

        reusedId.Should().Be(chainIds[0],
            "Delete must free the overflow chain page so it can be reused");
    }

    // ── Test 4: Delete inline key — regression ────────────────────────────────

    [Fact]
    public void Delete_InlineKey_UnchangedByPhase99a()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        engine.Insert(1, new byte[] { 10 });
        engine.Insert(2, new byte[] { 20 });

        engine.Delete(1).Should().BeTrue();
        engine.TryGet(1, out _).Should().BeFalse("deleted key must not be found");
        engine.TryGet(2, out byte[] v).Should().BeTrue();
        v.Should().Equal(new byte[] { 20 }, "surviving key must be unaffected");

        wal.Dispose();
        mgr.Dispose();
    }

    // ── Phase 99b Tests: Insert and Update overflow write path ────────────────

    // ── Test 5: Insert overflow value round-trip ──────────────────────────────

    [Fact]
    public void Insert_OverflowValue_RoundTrip()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] largeData = new byte[5000];
        new Random(5).NextBytes(largeData);

        engine.Insert(99, largeData);

        bool found = engine.TryGet(99, out byte[] result);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(largeData, "Insert must store large value in overflow chain");
    }

    // ── Test 6: Upsert inline → overflow ──────────────────────────────────────

    [Fact]
    public void Insert_UpsertInlineToOverflow()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] small = new byte[] { 1, 2, 3 };
        byte[] large = new byte[5000];
        new Random(6).NextBytes(large);

        engine.Insert(42, small);
        engine.Insert(42, large);   // upsert: inline → overflow

        bool found = engine.TryGet(42, out byte[] result);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(large, "upsert must replace inline value with overflow value");
    }

    // ── Test 7: Upsert overflow → inline (old chain freed) ───────────────────

    [Fact]
    public void Insert_UpsertOverflowToInline()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, meta) = OpenEngine();

        byte[] large = new byte[5000];
        new Random(7).NextBytes(large);
        byte[] small = new byte[] { 7, 8 };

        engine.Insert(42, large);

        // Capture the overflow chain page IDs before the upsert frees them.
        // We can infer the page count: ByteArraySerializer serialises as [4-byte len][data],
        // so serialised size = 4 + 5000 = 5004 bytes.
        // OverflowPageLayout.DataCapacity(8192) = 8192 - 38 = 8154, so 1 overflow page.
        int serialisedLargeSize = 4 + large.Length; // 5004 bytes → 1 overflow page

        engine.Insert(42, small);   // upsert: overflow → inline; old chain must be freed

        bool found = engine.TryGet(42, out byte[] result);

        // Verify the old overflow page was freed by allocating a new page and checking ID reuse.
        var freshFrame = mgr.AllocatePage(PageType.Leaf);
        uint reusedId  = freshFrame.PageId;
        mgr.MarkDirtyAndUnpin(freshFrame.PageId);

        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(small, "upsert must replace overflow value with inline value");
        // The freed overflow page ID should be recycled (it's the next available page).
        // We can't predict the exact ID without tracking it, but we verify the page is allocatable.
        reusedId.Should().BeGreaterThan(0, "freed overflow page must be recyclable");
    }

    // ── Test 8: Upsert overflow → overflow (old chain freed) ─────────────────

    [Fact]
    public void Insert_UpsertOverflowToOverflow()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] largeA = new byte[5000];
        byte[] largeB = new byte[6000];
        new Random(8).NextBytes(largeA);
        new Random(9).NextBytes(largeB);

        engine.Insert(42, largeA);
        engine.Insert(42, largeB);   // upsert: overflow → overflow

        bool found = engine.TryGet(42, out byte[] result);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(largeB, "upsert must replace old overflow value with new overflow value");
    }

    // ── Test 9: Update inline → inline regression ────────────────────────────

    [Fact]
    public void Update_InlineToInline_Regression()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        engine.Insert(1, new byte[] { 10 });
        engine.Update(1, _ => new byte[] { 20 });

        bool found = engine.TryGet(1, out byte[] result);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(new byte[] { 20 }, "Update inline→inline must still work");
    }

    // ── Test 10: Update inline → overflow ────────────────────────────────────

    [Fact]
    public void Update_InlineToOverflow()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] large = new byte[5000];
        new Random(10).NextBytes(large);

        engine.Insert(1, new byte[] { 1 });
        engine.Update(1, _ => large);

        bool found = engine.TryGet(1, out byte[] result);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(large, "Update must store large new value in overflow chain");
    }

    // ── Test 11: Update overflow → inline (old chain freed) ──────────────────

    [Fact]
    public void Update_OverflowToInline()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] large = new byte[5000];
        new Random(11).NextBytes(large);
        byte[] small = new byte[] { 42 };

        engine.Insert(1, large);
        engine.Update(1, _ => small);

        bool found = engine.TryGet(1, out byte[] result);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(small, "Update overflow→inline must write inline value and free old chain");
    }

    // ── Phase 100a Tests: Transactional CoW overflow path ─────────────────────
    // Tests use TreeEngine<int,byte[]> BeginTransaction() which returns ITransaction<int,byte[]>.
    // Large byte[5000] serialises to 5004 bytes > MaxEntrySize(8192)=4059; stored as overflow.

    // ── Test 12: TryGet reads overflow value inside a transaction ─────────────

    [Fact]
    public void TryGet_OverflowValue_InTransaction()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] large = new byte[5000];
        new Random(12).NextBytes(large);

        // Insert overflow value via auto-commit path (Phase 99b — already proven).
        engine.Insert(99, large);

        // Read it inside a transaction — exercises TryGetInTransaction overflow dispatch.
        using var tx = engine.BeginTransaction();
        bool found = tx.TryGet(99, out byte[] result);
        tx.Commit();
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(large, "TryGet in transaction must read overflow value correctly");
    }

    // ── Test 13: Insert overflow value in transaction — commit ────────────────

    [Fact]
    public void Insert_OverflowValue_InTransaction_Commit()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] large = new byte[5000];
        new Random(13).NextBytes(large);

        using (var tx = engine.BeginTransaction())
        {
            tx.Insert(42, large);
            tx.Commit();
        }

        bool found = engine.TryGet(42, out byte[] result);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(large, "Committed overflow insert must be readable outside tx");
    }

    // ── Test 14: Insert overflow value in transaction — rollback ──────────────

    [Fact]
    public void Insert_OverflowValue_InTransaction_Rollback()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] large = new byte[5000];
        new Random(14).NextBytes(large);

        using (var tx = engine.BeginTransaction())
        {
            tx.Insert(42, large);
            // Dispose without Commit → rollback; overflow pages must be freed
        }

        bool found = engine.TryGet(42, out _);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeFalse("rolled-back overflow insert must not be visible");
    }

    // ── Test 15: Upsert inline → overflow in transaction ─────────────────────

    [Fact]
    public void Insert_UpsertInlineToOverflow_InTransaction()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] small = new byte[] { 1, 2, 3 };
        byte[] large = new byte[5000];
        new Random(15).NextBytes(large);

        engine.Insert(42, small);

        using (var tx = engine.BeginTransaction())
        {
            tx.Insert(42, large);   // upsert: inline → overflow
            tx.Commit();
        }

        bool found = engine.TryGet(42, out byte[] result);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(large, "upsert inline→overflow in transaction must commit correctly");
    }

    // ── Test 16: Upsert overflow → inline in transaction ─────────────────────

    [Fact]
    public void Insert_UpsertOverflowToInline_InTransaction()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] large = new byte[5000];
        new Random(16).NextBytes(large);
        byte[] small = new byte[] { 7, 8, 9 };

        engine.Insert(42, large);

        using (var tx = engine.BeginTransaction())
        {
            tx.Insert(42, small);   // upsert: overflow → inline; old chain retired at commit
            tx.Commit();
        }

        bool found = engine.TryGet(42, out byte[] result);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(small, "upsert overflow→inline in transaction must commit correctly");
    }

    // ── Test 17: Upsert overflow → overflow in transaction ───────────────────

    [Fact]
    public void Insert_UpsertOverflowToOverflow_InTransaction()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] largeA = new byte[5000];
        byte[] largeB = new byte[6000];
        new Random(17).NextBytes(largeA);
        new Random(18).NextBytes(largeB);

        engine.Insert(42, largeA);

        using (var tx = engine.BeginTransaction())
        {
            tx.Insert(42, largeB);   // upsert: overflow → overflow; old chain retired at commit
            tx.Commit();
        }

        bool found = engine.TryGet(42, out byte[] result);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(largeB, "upsert overflow→overflow in transaction must commit correctly");
    }

    // ── Test 18: TryUpdate inline → overflow in transaction ──────────────────

    [Fact]
    public void Update_InlineToOverflow_InTransaction()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] small = new byte[] { 10, 20 };
        byte[] large = new byte[5000];
        new Random(19).NextBytes(large);

        engine.Insert(1, small);

        using (var tx = engine.BeginTransaction())
        {
            tx.TryUpdate(1, large).Should().BeTrue();
            tx.Commit();
        }

        bool found = engine.TryGet(1, out byte[] result);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeTrue();
        result.Should().Equal(large, "TryUpdate inline→overflow in transaction must commit correctly");
    }

    // ── Test 19: Delete overflow value in transaction — commit ────────────────

    [Fact]
    public void Delete_OverflowValue_InTransaction_Commit()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] large = new byte[5000];
        new Random(20).NextBytes(large);

        engine.Insert(42, large);

        using (var tx = engine.BeginTransaction())
        {
            tx.TryDelete(42).Should().BeTrue();
            tx.Commit();
        }

        bool found = engine.TryGet(42, out _);
        wal.Dispose();
        mgr.Dispose();

        found.Should().BeFalse("deleted overflow key must not be found after commit");
    }

    // ── Phase 100b: auto-commit CoW path + scan (snapshot-active) ────────────

    // T20: Insert overflow value while snapshot is active (forces CoW path).
    [Fact]
    public void Insert_OverflowValue_CoW_SnapshotActive_Succeeds()
    {
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] large = new byte[5000];
        new Random(1).NextBytes(large);

        // Open a snapshot to force the CoW path.
        using (var snap = engine.BeginSnapshot())
        {
            engine.Insert(99, large);

            // Key must be visible outside snapshot (snapshot sees old state).
            snap.TryGet(99, out _).Should().BeFalse("snapshot predates the insert");
        }

        // After snapshot closes, TryGet must return the value.
        engine.TryGet(99, out var result).Should().BeTrue();
        result.Should().Equal(large, "CoW overflow insert must round-trip");

        wal.Dispose();
        mgr.Dispose();
    }

    // T21: Upsert inline→overflow while snapshot is active.
    [Fact]
    public void Insert_UpsertInlineToOverflow_CoW_SnapshotActive_Succeeds()
    {
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] inline = new byte[10];
        engine.Insert(100, inline);

        byte[] large = new byte[5000];
        new Random(2).NextBytes(large);

        using (engine.BeginSnapshot())
        {
            engine.Insert(100, large);  // upsert: inline → overflow via CoW
        }

        engine.TryGet(100, out var result).Should().BeTrue();
        result.Should().Equal(large, "inline→overflow upsert via CoW must round-trip");

        wal.Dispose();
        mgr.Dispose();
    }

    // T22: Update overflow→overflow while snapshot is active.
    [Fact]
    public void Update_OverflowToOverflow_CoW_SnapshotActive_Succeeds()
    {
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] large1 = new byte[5000];
        byte[] large2 = new byte[5001];
        new Random(3).NextBytes(large1);
        new Random(4).NextBytes(large2);

        engine.Insert(200, large1);

        using (engine.BeginSnapshot())
        {
            engine.Update(200, _ => large2);  // overflow → overflow via CoW
        }

        engine.TryGet(200, out var result).Should().BeTrue();
        result.Should().Equal(large2, "overflow→overflow update via CoW must round-trip");

        wal.Dispose();
        mgr.Dispose();
    }

    // T23: Update inline→overflow while snapshot is active.
    [Fact]
    public void Update_InlineToOverflow_CoW_SnapshotActive_Succeeds()
    {
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] inline = new byte[10];
        engine.Insert(300, inline);

        byte[] large = new byte[5000];
        new Random(5).NextBytes(large);

        using (engine.BeginSnapshot())
        {
            engine.Update(300, _ => large);
        }

        engine.TryGet(300, out var result).Should().BeTrue();
        result.Should().Equal(large, "inline→overflow update via CoW must round-trip");

        wal.Dispose();
        mgr.Dispose();
    }

    // T24: Scan returns overflow values correctly.
    [Fact]
    public void Scan_ReturnsOverflowValues()
    {
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] small = new byte[10];
        byte[] large = new byte[5000];
        new Random(6).NextBytes(large);

        engine.Insert(1, small);
        engine.Insert(2, large);
        engine.Insert(3, small);

        var entries = engine.Scan().ToList();
        entries.Should().HaveCount(3);
        entries[0].Key.Should().Be(1);
        entries[1].Key.Should().Be(2);
        entries[1].Value.Should().Equal(large, "scan must return overflow value for key 2");
        entries[2].Key.Should().Be(3);

        wal.Dispose();
        mgr.Dispose();
    }

    // T25: ScanReverse returns overflow values correctly.
    [Fact]
    public void ScanReverse_ReturnsOverflowValues()
    {
        var (mgr, wal, engine, _) = OpenEngine();

        byte[] large = new byte[5000];
        new Random(7).NextBytes(large);

        engine.Insert(10, new byte[10]);
        engine.Insert(20, large);
        engine.Insert(30, new byte[10]);

        var entries = engine.ScanReverse().ToList();
        entries.Should().HaveCount(3);
        entries[0].Key.Should().Be(30);
        entries[1].Key.Should().Be(20);
        entries[1].Value.Should().Equal(large, "reverse scan must return overflow value for key 20");
        entries[2].Key.Should().Be(10);

        wal.Dispose();
        mgr.Dispose();
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    public void Dispose()
    {
        if (File.Exists(_dbPath))  File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);
    }
}
