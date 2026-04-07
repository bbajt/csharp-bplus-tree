using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Api;

/// <summary>
/// Tests for BPlusTreeOptions.OnWarning callback.
/// </summary>
public class TreeWarningTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
    }

    [Fact]
    public void Open_WalBufferOverflow_FiresOnWarning()
    {
        // CheckpointThreshold × PageSize = 2000 × 8192 = 16 MB > WalBufferSize 8 MB → overflow
        var warnings = new List<string>();
        var options = new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 8192,
            CheckpointThreshold = 2000,
            BufferPoolCapacity  = 4096,
            WalBufferSize       = 8 * 1024 * 1024,
            OnWarning           = w => warnings.Add(w),
        };
        options.WillOverflowWalBuffer.Should().BeTrue("precondition: options must overflow WAL buffer");

        using var store = BPlusTree<int, int>.Open(options, Int32Serializer.Instance, Int32Serializer.Instance);

        warnings.Should().Contain(w => w.Contains("CheckpointThreshold (2000)"));
    }

    [Fact]
    public void Open_NormalOptions_NoWarningFired()
    {
        var warnings = new List<string>();
        var options = new BPlusTreeOptions
        {
            DataFilePath = _dbPath,
            WalFilePath  = _walPath,
            OnWarning    = w => warnings.Add(w),
        };
        options.WillOverflowWalBuffer.Should().BeFalse("precondition: normal options must not overflow");

        using var store = BPlusTree<int, int>.Open(options, Int32Serializer.Instance, Int32Serializer.Instance);

        warnings.Should().BeEmpty("normal options must not fire any warning");
    }
}
