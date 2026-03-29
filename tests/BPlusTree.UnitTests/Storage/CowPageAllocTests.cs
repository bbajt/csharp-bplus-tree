using System.Buffers.Binary;
using BPlusTree.Core.Api;
using BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Storage;

/// <summary>
/// Tests for PageManager.AllocatePageCow added in M+3.
/// Verifies content copy, self-page-ID fixup, source immutability, and frame state.
/// </summary>
public class CowPageAllocTests : IDisposable
{
    private readonly string _dbPath = Path.GetTempFileName();

    private PageManager Open()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        return PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _dbPath + ".wal",
            PageSize            = 4096,
            BufferPoolCapacity  = 64,
            CheckpointThreshold = 256,
        });
    }

    public void Dispose()
    {
        static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch (IOException) { }
        }
        TryDelete(_dbPath);
        TryDelete(_dbPath + ".wal");
    }

    // ── Test 1: Content copied and self-page-ID updated ───────────────────────

    [Fact]
    public void AllocatePageCow_DataMatchesSource_AndUpdatesPageIdInData()
    {
        var mgr = Open();

        // Allocate source page and write a recognisable pattern at a known offset.
        var src = mgr.AllocatePage(PageType.Leaf);
        uint srcId = src.PageId;
        const byte pattern = 0xAB;
        src.Data.AsSpan(48, 16).Fill(pattern);
        mgr.MarkDirtyAndUnpin(srcId);

        // CoW-allocate a shadow copy.
        var newFrame = mgr.AllocatePageCow(srcId);

        // Different physical page.
        newFrame.PageId.Should().NotBe(srcId);

        // Content copied correctly.
        newFrame.Data.AsSpan(48, 16).ToArray().Should().AllBeEquivalentTo(pattern,
            "CoW copy must replicate source page content");

        // Self-page-ID field updated to new page ID (big-endian).
        uint storedId = BinaryPrimitives.ReadUInt32BigEndian(
            newFrame.Data.AsSpan(PageLayout.PageIdOffset, sizeof(uint)));
        storedId.Should().Be(newFrame.PageId,
            "Data[PageIdOffset] must reflect the new page ID, not the source's");

        mgr.MarkDirtyAndUnpin(newFrame.PageId);
        mgr.Dispose();
    }

    // ── Test 2: Source page content unchanged ────────────────────────────────

    [Fact]
    public void AllocatePageCow_SourcePageContentUnchanged()
    {
        var mgr = Open();

        // Allocate source page and write pattern.
        var src = mgr.AllocatePage(PageType.Leaf);
        uint srcId = src.PageId;
        const byte pattern = 0xCD;
        src.Data.AsSpan(48, 16).Fill(pattern);
        mgr.MarkDirtyAndUnpin(srcId);

        // CoW-allocate.
        var newFrame = mgr.AllocatePageCow(srcId);

        // Fetch source and verify the pattern bytes are unchanged.
        var srcAgain = mgr.FetchPage(srcId);
        srcAgain.Data.AsSpan(48, 16).ToArray().Should().AllBeEquivalentTo(pattern,
            "source page content must not be modified by CoW allocation");

        // Source frame's PageId property must still be srcId.
        srcAgain.PageId.Should().Be(srcId,
            "source frame's PageId must not be modified by CoW allocation");

        mgr.Unpin(srcId);
        mgr.MarkDirtyAndUnpin(newFrame.PageId);
        mgr.Dispose();
    }

    // ── Test 3: Returned frame is dirty and pinned ────────────────────────────

    [Fact]
    public void AllocatePageCow_NewFrameIsDirtyAndPinned()
    {
        var mgr = Open();

        // Allocate source page.
        var src = mgr.AllocatePage(PageType.Leaf);
        uint srcId = src.PageId;
        mgr.MarkDirtyAndUnpin(srcId);

        // CoW-allocate.
        var newFrame = mgr.AllocatePageCow(srcId);

        newFrame.IsDirty.Should().BeTrue("CoW frame must be dirty (needs to be written to disk)");
        newFrame.PinCount.Should().BeGreaterThanOrEqualTo(1, "CoW frame must be pinned (caller holds it)");

        mgr.MarkDirtyAndUnpin(newFrame.PageId);
        mgr.Dispose();
    }
}
