using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Api;

public class CompactionEventTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 8192,
            BufferPoolCapacity  = 512,
            CheckpointThreshold = 64,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
        try { if (File.Exists(_dbPath + ".compact")) File.Delete(_dbPath + ".compact"); } catch { }
    }

    [Fact]
    public void Compact_Fires_CompactionStarted_Event()
    {
        using var tree = Open();
        for (int i = 0; i < 50; i++) tree.Put(i, i);

        var firedPaths = new List<string>();
        Action<string> handler = path => firedPaths.Add(path);
        BPlusTree<int, int>.CompactionStarted += handler;
        try
        {
            tree.Compact();
        }
        finally
        {
            BPlusTree<int, int>.CompactionStarted -= handler;
        }

        // At least one CompactionStarted fired for our specific data file.
        firedPaths.Should().Contain(_dbPath,
            "CompactionStarted must fire with the data file path before compaction begins");
    }

    [Fact]
    public void Compact_Fires_CompactionCompleted_WithResult()
    {
        using var tree = Open();
        for (int i = 0; i < 50; i++) tree.Put(i, i);
        for (int i = 0; i < 25; i++) tree.Delete(i);

        CompactionResult? captured = null;
        Action<string, CompactionResult> handler = (path, result) =>
        {
            if (path == _dbPath) captured = result;
        };
        BPlusTree<int, int>.CompactionCompleted += handler;
        try
        {
            tree.Compact();
        }
        finally
        {
            BPlusTree<int, int>.CompactionCompleted -= handler;
        }

        captured.Should().NotBeNull("CompactionCompleted must fire for our data file");
        captured!.Value.Duration.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero,
            "duration must be non-negative");
        captured!.Value.PagesFreed.Should().BeGreaterThanOrEqualTo(0,
            "pages freed must be non-negative");
    }
}
