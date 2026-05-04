using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for the epoch-gated freelist infrastructure added in M+2.
/// Exercises RetirePage / SweepRetiredPages in isolation via direct construction
/// of TransactionCoordinator + PageManager (no TreeEngine involved).
///
/// Observing reclamation: after a swept page is passed to PageManager.FreePage,
/// the next AllocatePage call draws from the freelist and returns that same pageId.
/// </summary>
public class EpochGatedFreelistTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal, TransactionCoordinator coordinator) Open()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        var wal  = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 4096,
            BufferPoolCapacity  = 64,
            CheckpointThreshold = 256,
        }, wal);
        var coordinator = new TransactionCoordinator();
        coordinator.SetPageManager(mgr);
        return (mgr, wal, coordinator);
    }

    public void Dispose()
    {
        static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch (IOException) { }
        }
        TryDelete(_dbPath);
        TryDelete(_walPath);
    }

    // ── Test 1: No active readers — RetirePage frees immediately ─────────────

    [Fact]
    public void RetirePage_NoActiveReaders_FreeImmediately()
    {
        var (mgr, wal, coordinator) = Open();

        // Allocate a page and immediately unpin it (no references held).
        var frame1 = mgr.AllocatePage(PageType.Leaf);
        uint pageId = frame1.PageId;
        mgr.MarkDirtyAndUnpin(pageId);

        // No active readers — RetirePage should sweep and free immediately.
        coordinator.RetirePage(pageId);

        // The next allocation must return the same pageId (drawn from freelist).
        var frame2 = mgr.AllocatePage(PageType.Leaf);
        frame2.PageId.Should().Be(pageId, "retired page must be reclaimed immediately when no readers are active");
        mgr.MarkDirtyAndUnpin(frame2.PageId);

        mgr.Dispose();
        wal.Dispose();
    }

    // ── Test 2: Active reader holds page; exit triggers sweep ─────────────────

    [Fact]
    public void RetirePage_ActiveReader_HoldsUntilReaderExits()
    {
        var (mgr, wal, coordinator) = Open();

        // Allocate and unpin a page to retire.
        var frame1 = mgr.AllocatePage(PageType.Leaf);
        uint pageId = frame1.PageId;
        mgr.MarkDirtyAndUnpin(pageId);

        // Enter a reader snapshot — this pins the epoch.
        ulong epoch = coordinator.EnterReadEpoch();

        // Retire the page. Reader is active → page must NOT be freed yet.
        coordinator.RetirePage(pageId);

        // Allocate a probe page — must be a new page, not the retired one.
        var probe = mgr.AllocatePage(PageType.Leaf);
        probe.PageId.Should().NotBe(pageId, "retired page must not be reclaimed while a reader holds an epoch ≤ retireEpoch");
        mgr.MarkDirtyAndUnpin(probe.PageId);

        // Exit the reader — triggers sweep → page freed.
        coordinator.ExitReadEpoch(epoch);

        // Next allocation must return the retired page.
        var frame2 = mgr.AllocatePage(PageType.Leaf);
        frame2.PageId.Should().Be(pageId, "retired page must be reclaimed after the blocking reader exits");
        mgr.MarkDirtyAndUnpin(frame2.PageId);

        mgr.Dispose();
        wal.Dispose();
    }

    // ── Test 3: Multiple readers; freed only after oldest exits ───────────────

    [Fact]
    public void RetirePage_MultipleReaders_FreeOnlyAfterOldestExits()
    {
        var (mgr, wal, coordinator) = Open();

        // Allocate and unpin a page to retire.
        var frame1 = mgr.AllocatePage(PageType.Leaf);
        uint pageId = frame1.PageId;
        mgr.MarkDirtyAndUnpin(pageId);

        // Two concurrent readers.
        ulong e1 = coordinator.EnterReadEpoch();
        ulong e2 = coordinator.EnterReadEpoch();

        // Retire the page — both readers active → not freed.
        coordinator.RetirePage(pageId);

        // Exit the newer reader only — e1 is still active → not freed.
        coordinator.ExitReadEpoch(e2);

        // Allocate a probe — must NOT be pageId yet.
        var probe = mgr.AllocatePage(PageType.Leaf);
        probe.PageId.Should().NotBe(pageId, "retired page must not be reclaimed while e1 (the oldest reader) is still active");
        mgr.MarkDirtyAndUnpin(probe.PageId);

        // Exit the oldest reader — no readers remain → sweep → page freed.
        coordinator.ExitReadEpoch(e1);

        // Next allocation must return the retired page.
        var frame2 = mgr.AllocatePage(PageType.Leaf);
        frame2.PageId.Should().Be(pageId, "retired page must be reclaimed after all blocking readers exit");
        mgr.MarkDirtyAndUnpin(frame2.PageId);

        mgr.Dispose();
        wal.Dispose();
    }
}
