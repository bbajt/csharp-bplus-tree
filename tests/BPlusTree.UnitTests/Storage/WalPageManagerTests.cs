using System;
using System.IO;
using Xunit;
using FluentAssertions;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;

namespace ByTech.BPlusTree.Core.Tests.Storage;

public class WalPageManagerTests : IDisposable
{
    private const int PageSize = 4096;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    /// <summary>
    /// Opens a fresh PageManager wired to a WalWriter.
    /// Deletes the pre-created empty temp DB file so PageManager.Open treats it as new.
    /// </summary>
    private (PageManager mgr, WalWriter wal) OpenWithWal()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        var opts = new BPlusTreeOptions
        {
            DataFilePath       = _dbPath,
            WalFilePath        = _walPath,
            PageSize           = PageSize,
            BufferPoolCapacity = 64,
            CheckpointThreshold = 16,
        };
        var wal = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr = PageManager.Open(opts, wal);
        return (mgr, wal);
    }

    [Fact]
    public void MarkDirtyAndUnpin_WithoutWalLsn_AutoLogsWalRecord()
    {
        // Phase 20a: when WAL is present and no walLsn is supplied, the manager
        // writes the WAL record automatically (WAL-before-page invariant upheld transparently).
        var (mgr, wal) = OpenWithWal();
        using var _ = wal;
        var frame = mgr.AllocatePage(PageType.Leaf);
        long lsnBefore = (long)wal.CurrentLsn.Value;
        mgr.Invoking(m => m.MarkDirtyAndUnpin(frame.PageId))
           .Should().NotThrow();
        wal.CurrentLsn.Value.Should().BeGreaterThan((ulong)lsnBefore, "WAL record must have been appended");
        mgr.Dispose();
    }

    [Fact]
    public void MarkDirtyAndUnpin_WithValidWalLsn_Succeeds()
    {
        var (mgr, wal) = OpenWithWal();
        using var _ = wal;
        var frame = mgr.AllocatePage(PageType.Leaf);
        var lsn = wal.Append(WalRecordType.UpdatePage, 1, frame.PageId,
            LogSequenceNumber.None, frame.Data);
        wal.Flush();
        mgr.Invoking(m => m.MarkDirtyAndUnpin(frame.PageId, lsn)).Should().NotThrow();
        mgr.Dispose();
    }

    [Fact]
    public void Recovery_ReappliesAfterImages_ForStaleLsnPages()
    {
        var opts = new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = PageSize,
            BufferPoolCapacity  = 64,
            CheckpointThreshold = 16,
        };

        uint targetPageId;
        byte[] expectedData;

        // Session 1: write known data to a page and WAL, then simulate crash
        {
            if (File.Exists(_dbPath)) File.Delete(_dbPath);
            var wal = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
            var mgr = PageManager.Open(opts, wal);
            var frame = mgr.AllocatePage(PageType.Leaf);
            targetPageId = frame.PageId;
            expectedData = new byte[PageSize];
            expectedData[42] = 0xAB;
            Array.Copy(expectedData, frame.Data, PageSize);
            var lsn = wal.Append(WalRecordType.UpdatePage, 1, targetPageId,
                LogSequenceNumber.None, expectedData);
            wal.Flush();
            // Simulate crash: dirty page NOT flushed to data file (never called MarkDirtyAndUnpin)
            wal.Dispose();
            mgr.Dispose();
        }

        // Session 2: reopen and recover — WAL must replay the after-image
        {
            var wal2 = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
            var mgr2 = PageManager.Open(opts, wal2);
            mgr2.Recover();
            var frame2 = mgr2.FetchPage(targetPageId);
            frame2.Data[42].Should().Be(0xAB);
            mgr2.Unpin(targetPageId);
            wal2.Dispose();
            mgr2.Dispose();
        }
    }

    [Fact]
    public void Recovery_Idempotent_RunTwiceNoError()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);

        var opts = new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = PageSize,
            BufferPoolCapacity  = 64,
            CheckpointThreshold = 16,
        };
        var wal = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr = PageManager.Open(opts, wal);
        mgr.Recover();
        mgr.Invoking(m => m.Recover()).Should().NotThrow();
        wal.Dispose();
        mgr.Dispose();
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath);  } catch { }
        try { File.Delete(_walPath); } catch { }
    }
}
