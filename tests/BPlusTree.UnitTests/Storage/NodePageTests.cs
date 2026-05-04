using Xunit;
using ByTech.BPlusTree.Core.Storage;

namespace ByTech.BPlusTree.Core.Tests.Storage;

public class NodePageTests
{
    private const int PageSize = 8192;

    [Fact]
    public void NodePage_CanBeCreated()
    {
        var buffer = new byte[PageSize];
        var page = new NodePage(buffer);
        // Just verify it can be created without throwing
        Assert.True(true);
    }

    [Fact]
    public void NodePage_Initialize_SetsMagicNumber()
    {
        var buffer = new byte[PageSize];
        var page = new NodePage(buffer);
        page.Initialize(1, PageType.Leaf);
        Assert.Equal(PageLayout.MagicNumber, page.Magic);
    }

    [Fact]
    public void NodePage_Initialize_SetsPageType()
    {
        var buffer = new byte[PageSize];
        var page = new NodePage(buffer);
        page.Initialize(1, PageType.Leaf);
        Assert.Equal(PageType.Leaf, page.PageType);
    }

    [Fact]
    public void NodePage_Initialize_SetsPageId()
    {
        var buffer = new byte[PageSize];
        var page = new NodePage(buffer);
        page.Initialize(42, PageType.Leaf);
        Assert.Equal(42u, page.PageId);
    }

    [Fact]
    public void NodePage_Initialize_SetsSlotCount()
    {
        var buffer = new byte[PageSize];
        var page = new NodePage(buffer);
        page.Initialize(1, PageType.Leaf);
        Assert.Equal(0, page.SlotCount);
    }

    [Fact]
    public void NodePage_Initialize_SetsFreeSpaceOffset()
    {
        var buffer = new byte[PageSize];
        var page = new NodePage(buffer);
        page.Initialize(1, PageType.Leaf);
        Assert.Equal((ushort)PageLayout.FirstSlotOffset, page.FreeSpaceOffset);
    }

    [Fact]
    public void NodePage_Initialize_SetsFreeSpaceSize()
    {
        var buffer = new byte[PageSize];
        var page = new NodePage(buffer);
        page.Initialize(1, PageType.Leaf);
        Assert.Equal((ushort)(PageSize - PageLayout.FirstSlotOffset), page.FreeSpaceSize);
    }

    [Fact]
    public void NodePage_Initialize_SetsParentPageId()
    {
        var buffer = new byte[PageSize];
        var page = new NodePage(buffer);
        page.Initialize(1, PageType.Leaf);
        Assert.Equal(PageLayout.NullPageId, page.ParentPageId);
    }

    [Fact]
    public void NodePage_Initialize_Leaf_SetsNullSiblingPointers()
    {
        var buffer = new byte[PageSize];
        var page = new NodePage(buffer);
        page.Initialize(1, PageType.Leaf);
        Assert.Equal(PageLayout.NullPageId, page.PrevLeafPageId);
        Assert.Equal(PageLayout.NullPageId, page.NextLeafPageId);
    }

    [Fact]
    public void NodePage_HasFreeSpace_SmallRequest_ReturnsTrue()
    {
        var buffer = new byte[PageSize];
        var page = new NodePage(buffer);
        page.Initialize(1, PageType.Leaf);
        Assert.True(page.HasFreeSpace(100));
    }

    [Fact]
    public void NodePage_AllocateCell_ReturnsCorrectLength()
    {
        var buffer = new byte[PageSize];
        var page = new NodePage(buffer);
        page.Initialize(1, PageType.Leaf);
        var cell = page.AllocateCell(100);
        Assert.Equal(100, cell.Length);
    }
}