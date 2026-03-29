using System;
using System.Buffers.Binary;
using System.IO;
using System.IO.Hashing;
using Xunit;
using FluentAssertions;
using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;

namespace BPlusTree.UnitTests.Storage;

/// <summary>
/// Phase 96 — Slot Format v2 (6-byte slot: 4-byte packed + Flags + reserved).
/// Verifies: slot roundtrip, shift correctness, capacity, and format version validation.
/// </summary>
public class SlotFormatV2Tests
{
    // ── Slot roundtrip ────────────────────────────────────────────────────────

    [Fact]
    public void SlotEntry_RoundTrip_FlagsZero()
    {
        var buffer = new byte[PageLayout.DefaultPageSize];
        var page   = new NodePage(buffer);
        page.Initialize(1, PageType.Leaf);

        page.SetSlot(0, cellOffset: 500, cellLength: 12, flags: 0);
        var (off, len, flags) = page.GetSlot(0);

        off.Should().Be(500);
        len.Should().Be(12);
        flags.Should().Be(0);
    }

    [Fact]
    public void SlotEntry_RoundTrip_FlagsNonZero()
    {
        var buffer = new byte[PageLayout.DefaultPageSize];
        var page   = new NodePage(buffer);
        page.Initialize(1, PageType.Leaf);

        page.SetSlot(0, cellOffset: 1024, cellLength: 9, flags: PageLayout.SlotIsOverflow);
        var (off, len, flags) = page.GetSlot(0);

        off.Should().Be(1024);
        len.Should().Be(9);
        flags.Should().Be(PageLayout.SlotIsOverflow);
    }

    // ── Shift correctness ────────────────────────────────────────────────────

    [Fact]
    public void InsertSlot_ShiftsFlags()
    {
        var buffer = new byte[PageLayout.DefaultPageSize];
        var page   = new NodePage(buffer);
        page.Initialize(1, PageType.Leaf);

        // Manually place 2 slots (bypassing AllocateCell — we test shift logic only)
        page.InsertSlot(0, cellOffset: 100, cellLength: 10, flags: 0x00);
        page.InsertSlot(1, cellOffset: 200, cellLength: 20, flags: 0x01);

        // Insert in the middle — slot 1 should shift to slot 2
        page.InsertSlot(1, cellOffset: 150, cellLength: 15, flags: 0x02);

        page.SlotCount.Should().Be(3);
        var (off0, len0, f0) = page.GetSlot(0);
        var (off1, len1, f1) = page.GetSlot(1);
        var (off2, len2, f2) = page.GetSlot(2);

        off0.Should().Be(100); len0.Should().Be(10); f0.Should().Be(0x00);
        off1.Should().Be(150); len1.Should().Be(15); f1.Should().Be(0x02);
        off2.Should().Be(200); len2.Should().Be(20); f2.Should().Be(0x01);
    }

    [Fact]
    public void RemoveSlot_ShiftsFlags()
    {
        var buffer = new byte[PageLayout.DefaultPageSize];
        var page   = new NodePage(buffer);
        page.Initialize(1, PageType.Leaf);

        page.InsertSlot(0, cellOffset: 100, cellLength: 10, flags: 0x00);
        page.InsertSlot(1, cellOffset: 200, cellLength: 20, flags: 0x01);
        page.InsertSlot(2, cellOffset: 300, cellLength: 30, flags: 0x02);

        // Remove middle slot
        page.RemoveSlot(1);

        page.SlotCount.Should().Be(2);
        var (off0, len0, f0) = page.GetSlot(0);
        var (off1, len1, f1) = page.GetSlot(1);

        off0.Should().Be(100); len0.Should().Be(10); f0.Should().Be(0x00);
        off1.Should().Be(300); len1.Should().Be(30); f1.Should().Be(0x02);
    }

    // ── Capacity ─────────────────────────────────────────────────────────────

    [Fact]
    public void LeafCapacity_AtPageSize8192_Is581()
    {
        var frame = new Frame(PageLayout.DefaultPageSize);
        frame.PageId = 1;
        var leaf = new LeafNode<int, int>(frame, Int32Serializer.Instance, Int32Serializer.Instance);
        leaf.Initialize();

        int count = 0;
        while (leaf.TryInsert(count, count)) count++;

        // 6-byte slot + 4-byte int key + 4-byte int value = 14 bytes/entry
        // (8192 - 48) / 14 = 8144 / 14 = 581
        count.Should().Be(581, "int/int leaf capacity at PageSize=8192 with 6-byte slot");
    }

    [Fact]
    public void LeafCapacity_AtPageSize4096_Is289()
    {
        var frame = new Frame(4096);
        frame.PageId = 1;
        var leaf = new LeafNode<int, int>(frame, Int32Serializer.Instance, Int32Serializer.Instance);
        leaf.Initialize();

        int count = 0;
        while (leaf.TryInsert(count, count)) count++;

        // (4096 - 48) / 14 = 4048 / 14 = 289
        count.Should().Be(289, "int/int leaf capacity at PageSize=4096 with 6-byte slot");
    }

    // ── Format version validation ─────────────────────────────────────────────

    [Fact]
    public void FormatVersion_Open_RejectsV1()
    {
        var dbPath  = Path.GetTempFileName();
        var walPath = Path.GetTempFileName();
        try
        {
            // Create a valid v2 file first
            var opts = new BPlusTreeOptions
            {
                DataFilePath       = dbPath,
                WalFilePath        = walPath,
                PageSize           = 4096,
                BufferPoolCapacity = 16,
            };
            File.Delete(dbPath);
            var mgr = PageManager.Open(opts);
            mgr.Dispose();

            // Corrupt: overwrite FormatVersion byte with 1 and recompute checksum so
            // the page passes checksum verification but fails format version check.
            var bytes = File.ReadAllBytes(dbPath);
            bytes[PageLayout.FormatVersionOffset] = 1;
            // Recompute CRC32 over first page (same algorithm as PageChecksum.Stamp)
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(PageLayout.ChecksumOffset), 0);
            uint crc = Crc32.HashToUInt32(bytes.AsSpan(0, opts.PageSize));
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(PageLayout.ChecksumOffset), crc);
            File.WriteAllBytes(dbPath, bytes);

            // Re-open must throw
            var act = () => PageManager.Open(opts);
            act.Should().Throw<InvalidDataException>()
               .WithMessage("*version 1*");
        }
        finally
        {
            if (File.Exists(dbPath))  File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public void FormatVersion_Open_AcceptsV2()
    {
        var dbPath  = Path.GetTempFileName();
        var walPath = Path.GetTempFileName();
        try
        {
            File.Delete(dbPath);
            var opts = new BPlusTreeOptions
            {
                DataFilePath       = dbPath,
                WalFilePath        = walPath,
                PageSize           = 4096,
                BufferPoolCapacity = 16,
            };

            // Create
            var mgr1 = PageManager.Open(opts);
            mgr1.Dispose();

            // Re-open — must not throw
            var act = () =>
            {
                var mgr2 = PageManager.Open(opts);
                mgr2.Dispose();
            };
            act.Should().NotThrow();
        }
        finally
        {
            if (File.Exists(dbPath))  File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }
}
