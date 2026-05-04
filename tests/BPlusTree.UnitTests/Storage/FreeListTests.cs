using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Storage;

/// <summary>
/// Tests for FreeList durability: freed pages must be marked dirty
/// so their chain-link bytes are flushed before the next checkpoint.
/// </summary>
public class FreeListTests : IDisposable
{
    private readonly string _dbPath = Path.GetTempFileName();

    private PageManager Open()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        return PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _dbPath + ".wal",
            PageSize            = 4096,
            BufferPoolCapacity  = 64,
            CheckpointThreshold = 256,
        });
    }

    public void Dispose()
    {
        static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch (IOException) { }
        }
        TryDelete(_dbPath);
        TryDelete(_dbPath + ".wal");
    }

    [Fact]
    public void FreeList_Deallocate_MarksPageDirty()
    {
        var mgr = Open();

        // Allocate a page so we have something to free.
        var frame = mgr.AllocatePage(PageType.Leaf);
        uint pageId = frame.PageId;
        mgr.MarkDirtyAndUnpin(pageId);

        // Zero dirty count before the free.
        mgr.CheckpointFlush();
        mgr.BufferPool.DirtyCount.Should().Be(0);

        // Freeing the page must result in at least one dirty frame
        // (the freed page's chain-link bytes must be flushed at checkpoint).
        mgr.FreePage(pageId);

        mgr.BufferPool.DirtyCount.Should().BeGreaterThan(0,
            "freed page chain-link bytes must be marked dirty so they survive a crash");

        mgr.Dispose();
    }

    [Fact]
    public void PageManager_Dispose_CalledTwice_DoesNotThrow()
    {
        var mgr = Open();

        // First Dispose flushes and closes resources.
        // Second Dispose must be a silent no-op — not throw or re-flush.
        var act = () => { mgr.Dispose(); mgr.Dispose(); };
        act.Should().NotThrow("double-Dispose must be safe regardless of I/O exceptions during first call");
    }
}
