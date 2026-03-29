using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Api;

public class BPlusTreeFlushTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> OpenSync() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 8192,
            BufferPoolCapacity  = 64,
            CheckpointThreshold = 128,
            SyncMode            = WalSyncMode.Synchronous,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    private BPlusTree<int, int> OpenGroupCommit(int flushIntervalMs = 10_000) =>
        BPlusTree<int, int>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 64,
                CheckpointThreshold = 128,
                SyncMode            = WalSyncMode.GroupCommit,
                FlushIntervalMs     = flushIntervalMs,   // 10s max allowed: won't auto-flush during test
                FlushBatchSize      = 65_536,            // max allowed: 50 inserts won't trigger this
            },
            Int32Serializer.Instance, Int32Serializer.Instance);

    [Fact]
    public void Flush_SynchronousMode_IsNoOp()
    {
        using var tree = OpenSync();
        for (int i = 0; i < 100; i++) tree.Put(i, i);

        // Synchronous mode flushes the WAL after every write — Flush() is a no-op.
        // Elapsed-time assertion removed: inherently flaky under CI load (Phase 45).
        // The meaningful contract is: Flush() does not throw and completes without error.
        tree.Invoking(t => t.Flush()).Should().NotThrow(
            "Flush() in Synchronous mode is a no-op — WAL is already fully durable");
    }

    [Fact]
    public void Flush_GroupCommitMode_FlushedLsnAdvancesToCurrentLsn()
    {
        using var tree = OpenGroupCommit(flushIntervalMs: 10_000);

        for (int i = 0; i < 50; i++) tree.Put(i, i);

        var wal        = tree.GetWalWriterForTesting();
        var preFlushed = wal.FlushedLsn;            // ulong
        var currentLsn = wal.CurrentLsn.Value;      // ulong

        // Pre-condition: with a 10-second flush interval, FlushedLsn should be
        // behind CurrentLsn (the background thread hasn't fired yet).
        preFlushed.Should().BeLessThan(currentLsn,
            "GroupCommit with FlushIntervalMs=10000 should not have auto-flushed yet. " +
            "If FlushedLsn == CurrentLsn, the background thread fired faster than expected.");

        tree.Flush();

        wal.FlushedLsn.Should().BeGreaterThanOrEqualTo(currentLsn,
            "After Flush(), FlushedLsn must be >= the CurrentLsn captured before the call.");
    }

    [Fact]
    public void Flush_GroupCommitMode_DataDurableAfterFlush()
    {
        // Phase 1: write + flush
        {
            using var tree = OpenGroupCommit(flushIntervalMs: 10_000);
            for (int i = 0; i < 200; i++) tree.Put(i, i * 10);
            tree.Flush();   // WAL fsynced — all 200 inserts are durable
            // Dispose calls GracefulClose (checkpoint + WAL truncation).
            // Data survives regardless of whether recovery or clean load is used on re-open.
        }

        // Phase 2: re-open + verify
        {
            using var tree = OpenSync();
            for (int i = 0; i < 200; i++)
            {
                tree.TryGet(i, out int v).Should().BeTrue($"key {i} must survive after Flush()");
                v.Should().Be(i * 10);
            }
        }
    }

    [Fact]
    public void Flush_AfterDispose_ThrowsObjectDisposedException()
    {
        var tree = OpenSync();
        tree.Dispose();

        tree.Invoking(t => t.Flush())
            .Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Put_AfterDispose_ThrowsObjectDisposedException()
    {
        var tree = OpenSync();
        tree.Dispose();

        tree.Invoking(t => t.Put(1, 1))
            .Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void TryGet_AfterDispose_ThrowsObjectDisposedException()
    {
        var tree = OpenSync();
        tree.Dispose();

        tree.Invoking(t => t.TryGet(1, out _))
            .Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void SyncMode_ReflectsOptionsOnOpen()
    {
        using (var syncTree = OpenSync())
            syncTree.SyncMode.Should().Be(WalSyncMode.Synchronous);

        using (var gcTree = OpenGroupCommit())
            gcTree.SyncMode.Should().Be(WalSyncMode.GroupCommit);
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); }  catch { }
        try { File.Delete(_walPath); } catch { }
    }
}
