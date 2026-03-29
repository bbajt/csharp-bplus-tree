using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Api;

/// <summary>
/// Tests for BPlusTree.TreeWarning static event (Phase P-G).
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
    public void Open_WalBufferOverflow_FiresTreeWarning()
    {
        // CheckpointThreshold × PageSize = 2000 × 8192 = 16 MB > WalBufferSize 8 MB → overflow
        var options = new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 8192,
            CheckpointThreshold = 2000,
            BufferPoolCapacity  = 4096,
            WalBufferSize       = 8 * 1024 * 1024,
        };
        options.WillOverflowWalBuffer.Should().BeTrue("precondition: options must overflow WAL buffer");

        var warnings = new List<string>();
        Action<string> handler = w => warnings.Add(w);
        BPlusTree<int, int>.TreeWarning += handler;
        try
        {
            using var store = BPlusTree<int, int>.Open(options, Int32Serializer.Instance, Int32Serializer.Instance);
        }
        finally
        {
            BPlusTree<int, int>.TreeWarning -= handler;
        }

        // With parallel test execution the static event may capture warnings from
        // concurrent tests too.  Assert that at least one warning is specifically
        // for the overflow config we provided (CheckpointThreshold=2000, PageSize=8192).
        warnings.Should().Contain(w => w.Contains("CheckpointThreshold (2000)"));
    }

    [Fact]
    public void Open_NormalOptions_NoTreeWarningFired()
    {
        // Default WalBufferSize = 8 MB, CheckpointThreshold = 256, PageSize = 8192
        // 256 × 8192 = 2 MB < 8 MB → no overflow
        var options = new BPlusTreeOptions
        {
            DataFilePath = _dbPath,
            WalFilePath  = _walPath,
        };
        options.WillOverflowWalBuffer.Should().BeFalse("precondition: normal options must not overflow");

        var warnings = new List<string>();
        Action<string> handler = w => warnings.Add(w);
        BPlusTree<int, int>.TreeWarning += handler;
        try
        {
            using var store = BPlusTree<int, int>.Open(options, Int32Serializer.Instance, Int32Serializer.Instance);
        }
        finally
        {
            BPlusTree<int, int>.TreeWarning -= handler;
        }

        // Parallel tests may fire the event with their own overflow/oversized configs.
        // Assert that no warning fired mentioning OUR specific pool size (BufferPoolCapacity=2048),
        // which would only appear if our options triggered a warning.
        warnings.Should().NotContain(w => w.Contains("BufferPoolCapacity (2048)"),
            "normal options (CheckpointThreshold=256 < BufferPoolCapacity=2048) must not fire any warning");
    }
}
