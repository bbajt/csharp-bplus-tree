using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Api;

/// <summary>
/// Tests for BPlusTreeOptions.Validate() being called from BPlusTree.Open() (Phase R-A).
/// </summary>
public class OptionsValidationTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
    }

    [Fact]
    public void Open_WithInvalidFlushIntervalMs_ThrowsArgumentException()
    {
        var options = new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 4096,
            BufferPoolCapacity  = 256,
            CheckpointThreshold = 64,
            FlushIntervalMs     = 0,   // invalid: must be ≥ 1
        };

        var act = () => BPlusTree<int, int>.Open(options,
            Int32Serializer.Instance, Int32Serializer.Instance);

        act.Should().Throw<ArgumentException>()
            .WithMessage("*FlushIntervalMs*");
    }

    [Fact]
    public void Open_WithOversizedCheckpointThreshold_EmitsOnWarning()
    {
        // CheckpointThreshold ≥ BufferPoolCapacity — advisory, not an error
        var warnings = new List<string>();
        var options = new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 4096,
            BufferPoolCapacity  = 64,
            CheckpointThreshold = 512,   // oversized
            OnWarning           = w => warnings.Add(w),
        };
        options.IsCheckpointThresholdOversized.Should().BeTrue("precondition");

        using var store = BPlusTree<int, int>.Open(options,
            Int32Serializer.Instance, Int32Serializer.Instance);

        warnings.Should().Contain(w => w.Contains("CheckpointThreshold"),
            "an oversized threshold must emit an OnWarning callback");
    }

    private BPlusTreeOptions ValidOptions() => new BPlusTreeOptions
    {
        DataFilePath        = _dbPath,
        WalFilePath         = _walPath,
        PageSize            = 4096,
        BufferPoolCapacity  = 256,
        CheckpointThreshold = 64,
    };

    // ── T-C: EvictionWaitTimeoutMs validation ────────────────────────────────

    [Fact]
    public void Validate_EvictionWaitTimeoutMs_NegativeTwo_Throws()
    {
        var opts = ValidOptions();
        opts.EvictionWaitTimeoutMs = -2;
        opts.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("*EvictionWaitTimeoutMs*");
    }

    [Fact]
    public void Validate_EvictionWaitTimeoutMs_MinusOne_DoesNotThrow()
    {
        var opts = ValidOptions();
        opts.EvictionWaitTimeoutMs = -1;   // -1 = infinite wait; valid sentinel
        opts.Invoking(o => o.Validate()).Should().NotThrow();
    }

    // ── T-D: NaN watermark guard ──────────────────────────────────────────────

    [Fact]
    public void Validate_EvictionHighWatermark_NaN_Throws()
    {
        var opts = ValidOptions();
        opts.EvictionHighWatermark = double.NaN;
        opts.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("*EvictionHighWatermark*");
    }

    [Fact]
    public void Validate_EvictionLowWatermark_NaN_Throws()
    {
        var opts = ValidOptions();
        opts.EvictionLowWatermark = double.NaN;
        opts.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("*EvictionLowWatermark*");
    }

    [Fact]
    public void Validate_FillFactor_NaN_Throws()
    {
        var opts = ValidOptions();
        opts.FillFactor = double.NaN;
        opts.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("*FillFactor*");
    }
}
