using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

public class TreeMetadataTests : IDisposable
{
    private const int PageSize = 4096;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private PageManager OpenManager() => PageManager.Open(new BPlusTreeOptions
    {
        DataFilePath = _dbPath, WalFilePath = _walPath,
        PageSize = PageSize, BufferPoolCapacity = 64, CheckpointThreshold = 16,
    });

    [Fact]
    public void Load_AfterOpen_DoesNotThrow()
    {
        using var mgr = OpenManager();
        var meta = new TreeMetadata(mgr);
        meta.Invoking(m => m.Load()).Should().NotThrow();
    }

    [Fact]
    public void SetRoot_Then_Flush_Then_Reload_Persists()
    {
        using var mgr = OpenManager();
        var meta = new TreeMetadata(mgr);
        meta.Load();
        meta.SetRoot(rootPageId: 7, treeHeight: 2);
        meta.Flush();
        mgr.FlushPage((uint)PageLayout.MetaPageId);

        // Reload
        var meta2 = new TreeMetadata(mgr);
        meta2.Load();
        meta2.RootPageId.Should().Be(7u);
        meta2.TreeHeight.Should().Be(2u);
    }

    [Fact]
    public void IncrementRecordCount_UpdatesCount()
    {
        using var mgr = OpenManager();
        var meta = new TreeMetadata(mgr);
        meta.Load();
        meta.IncrementRecordCount();
        meta.IncrementRecordCount();
        meta.TotalRecordCount.Should().Be(2UL);
    }

    public void Dispose() { try { File.Delete(_dbPath); File.Delete(_walPath); } catch { } }
}
