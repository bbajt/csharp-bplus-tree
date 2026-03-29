using System;
using System.IO;
using Xunit;
using FluentAssertions;
using BPlusTree.Core.Api;
using BPlusTree.Core.Storage;

namespace BPlusTree.UnitTests.Storage;

/// <summary>
/// Phase 97 — Overflow page I/O layer.
/// Verifies: AllocateOverflowChain / ReadOverflowChain / FreeOverflowChain roundtrips
/// at various value sizes. No crash tests (WAL safety is Phase 98).
/// </summary>
public class OverflowChainTests
{
    // ── Helpers ───────────────────────────────────────────────────────────────

    private static PageManager OpenTemp(out string dbPath, out string walPath)
    {
        dbPath  = Path.GetTempFileName();
        walPath = Path.GetTempFileName();
        File.Delete(dbPath);   // PageManager.Open creates it fresh when absent
        return PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath       = dbPath,
            WalFilePath        = walPath,
            PageSize           = 8192,
            BufferPoolCapacity = 256,
        });
    }

    private static byte[] MakeValue(int length)
    {
        var v = new byte[length];
        for (int i = 0; i < length; i++)
            v[i] = (byte)(i & 0xFF);
        return v;
    }

    private static void Cleanup(string dbPath, string walPath)
    {
        if (File.Exists(dbPath))  File.Delete(dbPath);
        if (File.Exists(walPath)) File.Delete(walPath);
    }

    // ── Round-trip tests ──────────────────────────────────────────────────────

    [Fact]
    public void OverflowChain_RoundTrip_1Byte()
    {
        var mgr = OpenTemp(out var db, out var wal);
        try
        {
            var value = MakeValue(1);
            mgr.AllocateOverflowChain(value, out uint firstId, out _);
            var result = mgr.ReadOverflowChain(firstId, value.Length);
            result.Should().Equal(value);
        }
        finally { mgr.Dispose(); Cleanup(db, wal); }
    }

    [Fact]
    public void OverflowChain_RoundTrip_4KB()
    {
        // 4096 bytes fits in one overflow page (DataCapacity(8192) = 8154)
        var mgr = OpenTemp(out var db, out var wal);
        try
        {
            var value = MakeValue(4096);
            mgr.AllocateOverflowChain(value, out uint firstId, out var ids);
            ids.Should().HaveCount(1);
            var result = mgr.ReadOverflowChain(firstId, value.Length);
            result.Should().Equal(value);
        }
        finally { mgr.Dispose(); Cleanup(db, wal); }
    }

    [Fact]
    public void OverflowChain_RoundTrip_8KB()
    {
        // 8192 bytes requires 2 overflow pages at DataCapacity(8192) = 8154
        var mgr = OpenTemp(out var db, out var wal);
        try
        {
            var value = MakeValue(8192);
            mgr.AllocateOverflowChain(value, out uint firstId, out var ids);
            ids.Should().HaveCount(2);
            var result = mgr.ReadOverflowChain(firstId, value.Length);
            result.Should().Equal(value);
        }
        finally { mgr.Dispose(); Cleanup(db, wal); }
    }

    [Fact]
    public void OverflowChain_RoundTrip_100KB()
    {
        // 102,400 bytes → ceil(102400 / 8154) = 13 overflow pages
        var mgr = OpenTemp(out var db, out var wal);
        try
        {
            var value = MakeValue(102_400);
            mgr.AllocateOverflowChain(value, out uint firstId, out var ids);
            ids.Should().HaveCount(13);
            var result = mgr.ReadOverflowChain(firstId, value.Length);
            result.Should().Equal(value);
        }
        finally { mgr.Dispose(); Cleanup(db, wal); }
    }

    [Fact]
    public void OverflowChain_RoundTrip_1MB()
    {
        // 1,048,576 bytes → ceil(1048576 / 8154) = 129 overflow pages
        var mgr = OpenTemp(out var db, out var wal);
        try
        {
            var value = MakeValue(1_048_576);
            mgr.AllocateOverflowChain(value, out uint firstId, out var ids);
            ids.Should().HaveCount(129);
            var result = mgr.ReadOverflowChain(firstId, value.Length);
            result.Should().Equal(value);
        }
        finally { mgr.Dispose(); Cleanup(db, wal); }
    }

    // ── Free releases pages back to freelist ──────────────────────────────────

    [Fact]
    public void OverflowChain_Free_ReleasesPages()
    {
        // Allocate a 2-page chain (8192 bytes), free it, then allocate 2 single pages.
        // The new pages should reuse the freed IDs (freelist is LIFO stack).
        var mgr = OpenTemp(out var db, out var wal);
        try
        {
            var value = MakeValue(8192); // 2 overflow pages
            mgr.AllocateOverflowChain(value, out _, out var ids);
            ids.Should().HaveCount(2);

            // Record the IDs that were freed
            var freed0 = ids[0];
            var freed1 = ids[1];

            mgr.FreeOverflowChain(ids[0]);

            // Allocate 2 new pages; freelist should hand back the freed IDs
            var f1 = mgr.AllocatePage(PageType.Overflow);
            var reused1 = f1.PageId;
            mgr.MarkDirtyAndUnpin(f1.PageId);

            var f2 = mgr.AllocatePage(PageType.Overflow);
            var reused2 = f2.PageId;
            mgr.MarkDirtyAndUnpin(f2.PageId);

            new[] { reused1, reused2 }.Should().BeEquivalentTo(new[] { freed0, freed1 });
        }
        finally { mgr.Dispose(); Cleanup(db, wal); }
    }

    // ── Span overload tests (Phase 103) ───────────────────────────────────────

    [Fact]
    public void OverflowChain_SpanRead_MatchesByteArrayRead()
    {
        // Span overload must produce identical bytes to the byte[] overload.
        var mgr = OpenTemp(out var db, out var wal);
        try
        {
            var value = MakeValue(102_400); // 100 KB — 13 overflow pages
            mgr.AllocateOverflowChain(value, out uint firstId, out _);

            byte[] fromArray = mgr.ReadOverflowChain(firstId, value.Length);

            var buf = new byte[value.Length];
            mgr.ReadOverflowChain(firstId, value.Length, buf.AsSpan());

            buf.Should().Equal(fromArray);
        }
        finally { mgr.Dispose(); Cleanup(db, wal); }
    }

    [Fact]
    public void OverflowChain_SpanRead_ArrayPoolPattern_Correct()
    {
        // Verifies the ArrayPool pattern used in TreeEngine produces correct bytes.
        var mgr = OpenTemp(out var db, out var wal);
        try
        {
            var value = MakeValue(1_048_576); // 1 MB — 129 overflow pages
            mgr.AllocateOverflowChain(value, out uint firstId, out _);

            byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(value.Length);
            byte[] copy;
            try
            {
                mgr.ReadOverflowChain(firstId, value.Length, rented.AsSpan(0, value.Length));
                copy = rented[..value.Length].ToArray();
            }
            finally { System.Buffers.ArrayPool<byte>.Shared.Return(rented); }

            copy.Should().Equal(value);
        }
        finally { mgr.Dispose(); Cleanup(db, wal); }
    }
}
