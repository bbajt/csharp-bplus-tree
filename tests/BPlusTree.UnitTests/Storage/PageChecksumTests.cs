using System.Buffers.Binary;
using System.IO;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Storage;

public sealed class PageChecksumTests : IDisposable
{
    private const int PageSize = 4096;
    private readonly string _path = Path.GetTempFileName();

    // ── Test 1 ────────────────────────────────────────────────────────────────

    [Fact]
    public void Stamp_ThenVerify_NoException()
    {
        var page = new byte[PageSize];
        // Write some non-zero content to make the CRC non-trivial.
        for (int i = 0; i < PageSize; i++) page[i] = (byte)(i & 0xFF);

        PageChecksum.Stamp(page);

        // Checksum field must be non-zero after stamping non-zero content.
        uint stored = BinaryPrimitives.ReadUInt32LittleEndian(
            page.AsSpan(PageLayout.ChecksumOffset, sizeof(uint)));
        stored.Should().NotBe(0u);

        // Verify must not throw.
        var act = () => PageChecksum.Verify(page, pageId: 99);
        act.Should().NotThrow();
    }

    // ── Test 2 ────────────────────────────────────────────────────────────────

    [Fact]
    public void Verify_CorruptedByte_ThrowsPageChecksumException()
    {
        var page = new byte[PageSize];
        for (int i = 0; i < PageSize; i++) page[i] = (byte)(i & 0xFF);
        PageChecksum.Stamp(page);

        // Corrupt a payload byte (well away from the checksum field).
        page[500] ^= 0xFF;

        var act = () => PageChecksum.Verify(page, pageId: 7);
        var ex  = act.Should().Throw<PageChecksumException>().Which;
        ex.PageId.Should().Be(7u);
        ex.Stored.Should().NotBe(ex.Computed);
        // Buffer must be left unchanged (checksum field restored).
        BinaryPrimitives.ReadUInt32LittleEndian(
            page.AsSpan(PageLayout.ChecksumOffset, sizeof(uint)))
            .Should().Be(ex.Stored);
    }

    // ── Test 3 ────────────────────────────────────────────────────────────────

    [Fact]
    public void Verify_ZeroChecksum_NoException()
    {
        // All-zero page: checksum field is 0 → backward-compat skip.
        var page = new byte[PageSize];
        var act1 = () => PageChecksum.Verify(page, pageId: 0);
        act1.Should().NotThrow();

        // Mutate a non-checksum byte — stored checksum is still 0 → still skip.
        page[1000] = 0xAB;
        var act2 = () => PageChecksum.Verify(page, pageId: 0);
        act2.Should().NotThrow();
    }

    // ── Test 4 ────────────────────────────────────────────────────────────────

    [Fact]
    public void FetchPage_ColdPath_VerifiesChecksum()
    {
        // capacity=1: every Pin of a different page evicts the current one,
        // guaranteeing that the next Pin is always a cold-path disk read.
        // Two pages on disk: 0 (written with real content) and 1 (eviction target).
        using var storage = StorageFile.Open(_path, PageSize, createNew: true);
        storage.AllocatePage(); // page 0 — zeroed
        storage.AllocatePage(); // page 1 — zeroed
        var pool = new BufferPool(storage, capacity: 1);

        // Write non-zero content to page 0 and flush to disk — stamps checksum.
        var f0 = pool.Pin(0);
        f0.Data[100] = 0xDE;
        f0.Data[101] = 0xAD;
        pool.Unpin(0, isDirty: true);
        pool.FlushPage(0);

        // Evict page 0 by loading page 1 (second-chance clock: 2 sweeps over 1 frame).
        pool.Pin(1);
        pool.Unpin(1);

        // ── Cold-path reload — checksum must pass ─────────────────────────────
        var act = () => { var f = pool.Pin(0); pool.Unpin(0); };
        act.Should().NotThrow<PageChecksumException>();

        // ── Corrupt page 0 on disk via a second StorageFile handle ────────────
        using (var corrupt = StorageFile.Open(_path, PageSize, createNew: false))
        {
            var raw = corrupt.ReadPage(0); // fresh byte[] from disk
            raw[200] ^= 0xFF;             // flip byte outside checksum field (offset 28-31)
            corrupt.WritePage(0, raw);
        }

        // Evict page 0 again (currently in pool after the clean Pin above).
        pool.Pin(1);
        pool.Unpin(1);

        // ── Cold-path reload of corrupted page — must throw ───────────────────
        var actCorrupt = () => pool.Pin(0);
        actCorrupt.Should().Throw<PageChecksumException>()
            .Which.PageId.Should().Be(0u);
    }

    public void Dispose()
    {
        if (File.Exists(_path)) File.Delete(_path);
    }
}
