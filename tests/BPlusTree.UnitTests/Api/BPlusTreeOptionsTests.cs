using Xunit;
using FluentAssertions;
using ByTech.BPlusTree.Core.Api;

namespace ByTech.BPlusTree.Core.Tests.Api;

public class BPlusTreeOptionsTests
{
    private static BPlusTreeOptions ValidOptions() => new()
    {
        DataFilePath       = "test.db",
        WalFilePath        = "test.wal",
        PageSize           = 8192,
        BufferPoolCapacity = 64,
        CheckpointThreshold = 16,
        FillFactor         = 0.70,
    };

    [Fact] public void ValidOptions_DoesNotThrow()
        => ValidOptions().Invoking(o => o.Validate()).Should().NotThrow();

    [Theory]
    [InlineData(4096)]
    [InlineData(8192)]
    [InlineData(16384)]
    [InlineData(65536)]
    public void PageSize_PowerOfTwo_InRange_IsValid(int size)
    {
        var o = ValidOptions(); o.PageSize = size;
        o.Invoking(x => x.Validate()).Should().NotThrow();
    }

    [Theory]
    [InlineData(1024)]   // too small
    [InlineData(3000)]   // not power of two
    [InlineData(9000)]   // not power of two
    [InlineData(131072)] // too large
    public void PageSize_Invalid_Throws(int size)
    {
        var o = ValidOptions(); o.PageSize = size;
        o.Invoking(x => x.Validate()).Should().Throw<ArgumentException>();
    }

    [Fact] public void EmptyDataFilePath_Throws()
    {
        var o = ValidOptions(); o.DataFilePath = "";
        o.Invoking(x => x.Validate()).Should().Throw<ArgumentException>();
    }

    [Fact] public void EmptyWalFilePath_Throws()
    {
        var o = ValidOptions(); o.WalFilePath = "";
        o.Invoking(x => x.Validate()).Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineData(0.49)]
    [InlineData(0.96)]
    [InlineData(-1.0)]
    public void FillFactor_OutOfRange_Throws(double fill)
    {
        var o = ValidOptions(); o.FillFactor = fill;
        o.Invoking(x => x.Validate()).Should().Throw<ArgumentException>();
    }

    [Fact] public void BufferPoolCapacity_LessThan16_Throws()
    {
        var o = ValidOptions(); o.BufferPoolCapacity = 15;
        o.Invoking(x => x.Validate()).Should().Throw<ArgumentException>();
    }

    [Fact] public void CheckpointThreshold_GreaterThanOrEqualBufferCapacity_IsOversized()
    {
        var o = ValidOptions();
        o.BufferPoolCapacity  = 32;
        o.CheckpointThreshold = 32;  // oversized: threshold ≥ capacity
        // No longer a hard error — downgraded to advisory in R-A.
        o.Invoking(x => x.Validate()).Should().NotThrow();
        o.IsCheckpointThresholdOversized.Should().BeTrue();
    }
}