using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Api;

public class CompactionEventTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open(
        Action<string>? onStarted = null,
        Action<string, CompactionResult>? onCompleted = null)
        => BPlusTree<int, int>.Open(
            new BPlusTreeOptions
            {
                DataFilePath           = _dbPath,
                WalFilePath            = _walPath,
                PageSize               = 8192,
                BufferPoolCapacity     = 512,
                CheckpointThreshold    = 64,
                OnCompactionStarted    = onStarted,
                OnCompactionCompleted  = onCompleted,
            },
            Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
        try { if (File.Exists(_dbPath + ".compact")) File.Delete(_dbPath + ".compact"); } catch { }
    }

    [Fact]
    public void Compact_Fires_OnCompactionStarted()
    {
        var firedPaths = new List<string>();
        using var tree = Open(onStarted: path => firedPaths.Add(path));
        for (int i = 0; i < 50; i++) tree.Put(i, i);

        tree.Compact();

        firedPaths.Should().Contain(_dbPath,
            "OnCompactionStarted must fire with the data file path before compaction begins");
    }

    [Fact]
    public void Compact_Fires_OnCompactionCompleted_WithResult()
    {
        CompactionResult? captured = null;
        using var tree = Open(onCompleted: (path, result) =>
        {
            if (path == _dbPath) captured = result;
        });
        for (int i = 0; i < 50; i++) tree.Put(i, i);
        for (int i = 0; i < 25; i++) tree.Delete(i);

        tree.Compact();

        captured.Should().NotBeNull("OnCompactionCompleted must fire for our data file");
        captured!.Value.Duration.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero,
            "duration must be non-negative");
        captured!.Value.PagesFreed.Should().BeGreaterThanOrEqualTo(0,
            "pages freed must be non-negative");
    }
}
