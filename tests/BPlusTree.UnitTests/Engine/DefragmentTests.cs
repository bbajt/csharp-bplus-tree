using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

public class DefragmentTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal) OpenRaw()
    {
        var wal = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = PageSize,
            BufferPoolCapacity  = 64,
            CheckpointThreshold = 32,
        }, wal);
        return (mgr, wal);
    }

    // Insert `count` cells of `cellSize` bytes each, appending to the slot array.
    // Cell i is filled with byte value (i + 1). Returns stored byte arrays.
    private static byte[][] InsertCells(Frame frame, int count, int cellSize = 16)
    {
        var stored = new byte[count][];
        for (int i = 0; i < count; i++)
        {
            var page = new NodePage(frame.Data);
            var data = new byte[cellSize];
            Array.Fill(data, (byte)(i + 1));
            ushort cellOffset = (ushort)(page.FreeSpaceOffset + page.FreeSpaceSize - cellSize);
            var span = page.AllocateCell(cellSize);
            data.CopyTo(span);
            page.InsertSlot(page.SlotCount, cellOffset, (ushort)cellSize);
            stored[i] = data;
        }
        return stored;
    }

    [Fact]
    public void FragmentedBytes_FreshPage_IsZero()
    {
        var (mgr, wal) = OpenRaw();
        using (mgr)
        using (wal)
        {
            var frame = mgr.AllocatePage(PageType.Leaf);
            try
            {
                var page = new NodePage(frame.Data);
                page.Initialize(frame.PageId, PageType.Leaf);

                PageRewriter.FragmentedBytes(frame).Should().Be(0);
            }
            finally { mgr.Unpin(frame.PageId); }
        }
    }

    [Fact]
    public void FragmentedBytes_AfterDeletes_IsPositive()
    {
        var (mgr, wal) = OpenRaw();
        using (mgr)
        using (wal)
        {
            var frame = mgr.AllocatePage(PageType.Leaf);
            try
            {
                var page = new NodePage(frame.Data);
                page.Initialize(frame.PageId, PageType.Leaf);
                InsertCells(frame, 5);

                // Remove 2 slots — slot entries are reclaimed but cell bytes remain (fragmentation).
                new NodePage(frame.Data).RemoveSlot(0);
                new NodePage(frame.Data).RemoveSlot(0);

                PageRewriter.FragmentedBytes(frame).Should().BeGreaterThan(0);
            }
            finally { mgr.Unpin(frame.PageId); }
        }
    }

    [Fact]
    public void Defragment_FreeSpaceIncreasedByFragmentedBytes()
    {
        var (mgr, wal) = OpenRaw();
        using (mgr)
        using (wal)
        {
            var frame = mgr.AllocatePage(PageType.Leaf);
            try
            {
                var page = new NodePage(frame.Data);
                page.Initialize(frame.PageId, PageType.Leaf);
                InsertCells(frame, 5);
                new NodePage(frame.Data).RemoveSlot(0);
                new NodePage(frame.Data).RemoveSlot(0);

                int fragBefore = PageRewriter.FragmentedBytes(frame);
                int freeBefore = new NodePage(frame.Data).FreeSpaceSize;

                PageRewriter.Defragment(frame, wal, 1);

                int freeAfter = new NodePage(frame.Data).FreeSpaceSize;
                int fragAfter = PageRewriter.FragmentedBytes(frame);

                freeAfter.Should().Be(freeBefore + fragBefore);
                fragAfter.Should().Be(0);
            }
            finally { mgr.Unpin(frame.PageId); }
        }
    }

    [Fact]
    public void Defragment_SlotCountUnchanged()
    {
        var (mgr, wal) = OpenRaw();
        using (mgr)
        using (wal)
        {
            var frame = mgr.AllocatePage(PageType.Leaf);
            try
            {
                var page = new NodePage(frame.Data);
                page.Initialize(frame.PageId, PageType.Leaf);
                InsertCells(frame, 5);
                new NodePage(frame.Data).RemoveSlot(0);
                new NodePage(frame.Data).RemoveSlot(0);

                int slotsBefore = new NodePage(frame.Data).SlotCount;

                PageRewriter.Defragment(frame, wal, 1);

                ((int)new NodePage(frame.Data).SlotCount).Should().Be(slotsBefore);
            }
            finally { mgr.Unpin(frame.PageId); }
        }
    }

    [Fact]
    public void Defragment_AllCellDataPreserved_ByteForByte()
    {
        var (mgr, wal) = OpenRaw();
        using (mgr)
        using (wal)
        {
            var frame = mgr.AllocatePage(PageType.Leaf);
            try
            {
                const int n = 5;
                const int k = 2;

                var page = new NodePage(frame.Data);
                page.Initialize(frame.PageId, PageType.Leaf);
                var stored = InsertCells(frame, n);

                // Remove k slots from the front; cell bytes stay, creating dead space.
                for (int i = 0; i < k; i++)
                    new NodePage(frame.Data).RemoveSlot(0);

                PageRewriter.Defragment(frame, wal, 1);

                // Remaining n-k slots must contain byte-for-byte identical data.
                int remaining = n - k;
                for (int i = 0; i < remaining; i++)
                {
                    var view = new NodePage(frame.Data);
                    var (offset, length, _) = view.GetSlot(i);
                    var actual = frame.Data.AsSpan(offset, length).ToArray();
                    actual.Should().Equal(stored[k + i]);
                }
            }
            finally { mgr.Unpin(frame.PageId); }
        }
    }

    [Fact]
    public void Defragment_WalRecordWritten()
    {
        var (mgr, wal) = OpenRaw();
        using (mgr)
        using (wal)
        {
            var frame = mgr.AllocatePage(PageType.Leaf);
            try
            {
                var page = new NodePage(frame.Data);
                page.Initialize(frame.PageId, PageType.Leaf);
                InsertCells(frame, 5);
                new NodePage(frame.Data).RemoveSlot(0);

                wal.Flush();
                long walBefore = new FileInfo(_walPath).Length;

                PageRewriter.Defragment(frame, wal, 1);
                wal.Flush();

                long walAfter = new FileInfo(_walPath).Length;
                walAfter.Should().BeGreaterThan(walBefore);
            }
            finally { mgr.Unpin(frame.PageId); }
        }
    }

    [Fact]
    public void Defragment_Noop_WhenNoFragmentation()
    {
        var (mgr, wal) = OpenRaw();
        using (mgr)
        using (wal)
        {
            var frame = mgr.AllocatePage(PageType.Leaf);
            try
            {
                var page = new NodePage(frame.Data);
                page.Initialize(frame.PageId, PageType.Leaf);
                InsertCells(frame, 3);

                wal.Flush();
                long walBefore = new FileInfo(_walPath).Length;
                int freeBefore = new NodePage(frame.Data).FreeSpaceSize;

                PageRewriter.Defragment(frame, wal, 1);
                wal.Flush();

                ((int)new NodePage(frame.Data).FreeSpaceSize).Should().Be(freeBefore);
                new FileInfo(_walPath).Length.Should().Be(walBefore);
            }
            finally { mgr.Unpin(frame.PageId); }
        }
    }

    [Fact]
    public void Defragment_PagePassesNodePageValidation()
    {
        var (mgr, wal) = OpenRaw();
        using (mgr)
        using (wal)
        {
            var frame = mgr.AllocatePage(PageType.Leaf);
            try
            {
                var page = new NodePage(frame.Data);
                page.Initialize(frame.PageId, PageType.Leaf);
                InsertCells(frame, 5);
                new NodePage(frame.Data).RemoveSlot(0);
                new NodePage(frame.Data).RemoveSlot(0);

                PageRewriter.Defragment(frame, wal, 1);

                // Validate structural invariants.
                var view = new NodePage(frame.Data);
                view.Magic.Should().Be(PageLayout.MagicNumber);
                view.FreeSpaceOffset.Should().Be(
                    (ushort)(PageLayout.FirstSlotOffset + view.SlotCount * PageLayout.SlotEntrySize));

                int totalLiveBytes = 0;
                for (int i = 0; i < view.SlotCount; i++)
                {
                    var (_, len, _) = view.GetSlot(i);
                    totalLiveBytes += len;
                }
                view.FreeSpaceSize.Should().Be(
                    (ushort)(PageSize - view.FreeSpaceOffset - totalLiveBytes));

                PageRewriter.FragmentedBytes(frame).Should().Be(0);
            }
            finally { mgr.Unpin(frame.PageId); }
        }
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); File.Delete(_walPath); } catch { }
    }
}
