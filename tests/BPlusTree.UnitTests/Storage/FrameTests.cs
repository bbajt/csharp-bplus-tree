using System;
using Xunit;
using BPlusTree.Core.Storage;

namespace BPlusTree.UnitTests.Storage;

public class FrameTests
{
    private const int PageSize = 8192;

    [Fact]
    public void Frame_Ctor_InitializesCorrectly()
    {
        var frame = new Frame(PageSize);

        Assert.Equal(PageSize, frame.Data.Length);
        Assert.False(frame.IsPinned);
        Assert.Equal(PageLayout.NullPageId, frame.PageId);
        Assert.False(frame.IsDirty);
        Assert.Equal(0ul, frame.PageLsn);
        Assert.False(frame.ReferenceBit);
    }

    [Fact]
    public void Frame_Ctor_ThrowsForInvalidPageSize()
    {
        Assert.Throws<ArgumentException>(() => new Frame(0));
        Assert.Throws<ArgumentException>(() => new Frame(-1));
    }

    [Fact]
    public void Frame_Pin_IncrementsPinCount()
    {
        var frame = new Frame(PageSize);

        frame.Pin();
        Assert.True(frame.IsPinned);

        frame.Pin();
        Assert.True(frame.IsPinned);
    }

    [Fact]
    public void Frame_Unpin_DecrementsPinCount()
    {
        var frame = new Frame(PageSize);

        frame.Pin();
        frame.Pin();
        Assert.True(frame.IsPinned);

        frame.Unpin();
        Assert.True(frame.IsPinned);

        frame.Unpin();
        Assert.False(frame.IsPinned);
    }

    [Fact]
    public void Frame_IsPinned_ReflectsPinCount()
    {
        var frame = new Frame(PageSize);

        Assert.False(frame.IsPinned);

        frame.Pin();
        Assert.True(frame.IsPinned);

        frame.Unpin();
        Assert.False(frame.IsPinned);
    }

    [Fact]
    public void Frame_Reset_ClearsAllFields()
    {
        var frame = new Frame(PageSize);

        // Set some values
        frame.PageId = 42;
        frame.IsDirty = true;
        frame.PageLsn = 12345;
        frame.ReferenceBit = true;
        frame.Pin();

        frame.Reset();

        Assert.Equal(PageLayout.NullPageId, frame.PageId);
        Assert.False(frame.IsDirty);
        Assert.Equal(0ul, frame.PageLsn);
        Assert.False(frame.ReferenceBit);
        Assert.False(frame.IsPinned);

        // Verify data buffer is cleared
        foreach (var byteValue in frame.Data)
        {
            Assert.Equal(0, byteValue);
        }
    }

    [Fact]
    public void Frame_Data_LengthMatchesPageSize()
    {
        var frame = new Frame(PageSize);
        Assert.Equal(PageSize, frame.Data.Length);
    }
}