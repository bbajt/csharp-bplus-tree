using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

public class PageRewriterTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal) Open()
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

    public void Dispose()
    {
        try
        {
            File.Delete(_dbPath);
            File.Delete(_walPath);
        }
        catch { }
    }

    [Fact]
    public void Rewriter_FragmentedBytes_FreshPage_IsZero()
    {
        var (mgr, wal) = Open();
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
    public void Rewriter_Defragment_IncreasesFreeSpace()
    {
        var (mgr, wal) = Open();
        using (mgr)
        using (wal)
        {
            var frame = mgr.AllocatePage(PageType.Leaf);
            try
            {
                var page = new NodePage(frame.Data);
                page.Initialize(frame.PageId, PageType.Leaf);

                // Insert 4 cells of 16 bytes each, then remove 2 to create fragmentation.
                for (int i = 0; i < 4; i++)
                {
                    var view = new NodePage(frame.Data);
                    var data = new byte[16];
                    Array.Fill(data, (byte)(i + 1));
                    ushort cellOffset = (ushort)(view.FreeSpaceOffset + view.FreeSpaceSize - 16);
                    var span = view.AllocateCell(16);
                    data.CopyTo(span);
                    view.InsertSlot(view.SlotCount, cellOffset, 16);
                }
                new NodePage(frame.Data).RemoveSlot(0);
                new NodePage(frame.Data).RemoveSlot(0);

                int freeBefore = new NodePage(frame.Data).FreeSpaceSize;
                int fragBefore = PageRewriter.FragmentedBytes(frame);
                fragBefore.Should().BeGreaterThan(0);

                PageRewriter.Defragment(frame, wal, 1);

                ((int)new NodePage(frame.Data).FreeSpaceSize).Should().Be(freeBefore + fragBefore);
            }
            finally { mgr.Unpin(frame.PageId); }
        }
    }

    [Fact]
    public void Rewriter_Defragment_Noop_WhenNoFragmentation()
    {
        var (mgr, wal) = Open();
        using (mgr)
        using (wal)
        {
            var frame = mgr.AllocatePage(PageType.Leaf);
            try
            {
                var page = new NodePage(frame.Data);
                page.Initialize(frame.PageId, PageType.Leaf);

                int freeBefore = new NodePage(frame.Data).FreeSpaceSize;

                PageRewriter.Defragment(frame, wal, 1);

                ((int)new NodePage(frame.Data).FreeSpaceSize).Should().Be(freeBefore);
                PageRewriter.FragmentedBytes(frame).Should().Be(0);
            }
            finally { mgr.Unpin(frame.PageId); }
        }
    }
}
