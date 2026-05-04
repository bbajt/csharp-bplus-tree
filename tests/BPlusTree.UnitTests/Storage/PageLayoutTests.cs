using Xunit;
using FluentAssertions;
using ByTech.BPlusTree.Core.Storage;

namespace ByTech.BPlusTree.Core.Tests.Storage;

public class PageLayoutTests
{
    [Fact]
    public void MagicNumber_IsCorrectValue()
    {
        PageLayout.MagicNumber.Should().Be(0xB17EEF00);
    }

    [Fact]
    public void FormatVersion_IsTwo()
    {
        PageLayout.FormatVersion.Should().Be(2);
    }

    [Fact]
    public void CommonHeaderSize_Is32()
    {
        PageLayout.CommonHeaderSize.Should().Be(32);
    }

    [Fact]
    public void FirstSlotOffset_Is48()
    {
        PageLayout.FirstSlotOffset.Should().Be(48);
    }

    [Fact]
    public void SlotEntrySize_Is6()
    {
        PageLayout.SlotEntrySize.Should().Be(6);
    }

    [Fact]
    public void NullPageId_IsMaxUInt()
    {
        PageLayout.NullPageId.Should().Be(uint.MaxValue);
    }

    [Fact]
    public void NullLsn_IsZero()
    {
        PageLayout.NullLsn.Should().Be(0UL);
    }

    [Fact]
    public void DefaultPageSize_Is8192()
    {
        PageLayout.DefaultPageSize.Should().Be(8192);
    }
}