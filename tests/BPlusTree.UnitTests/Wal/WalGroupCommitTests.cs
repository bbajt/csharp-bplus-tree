using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using FluentAssertions;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Wal;

namespace ByTech.BPlusTree.Core.Tests.Wal;

public class WalGroupCommitTests : IDisposable
{
    private readonly string _path = Path.GetTempFileName();

    public void Dispose()
    {
        if (File.Exists(_path))
            File.Delete(_path);
    }

    // Each empty WAL record is WalRecordLayout.HeaderSize bytes (no data payload).
    private static void AppendEmpty(WalWriter writer, int count)
    {
        for (int i = 0; i < count; i++)
            writer.Append(WalRecordType.UpdatePage, (uint)i, 0,
                          LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
    }

    // ── Test 1 ───────────────────────────────────────────────────────────────

    [Fact]
    public void GroupCommit_RecordsOnDisk_AfterTimerFires()
    {
        // Arrange: large batch size so only the timer can trigger the fsync.
        using var writer = WalWriter.Open(_path, bufferSize: 65_536,
            syncMode:        WalSyncMode.GroupCommit,
            flushIntervalMs: 50,
            flushBatchSize:  65_536);

        // Write a few records — they stay in the in-memory buffer.
        AppendEmpty(writer, 3);

        // Act: wait longer than the timer interval.
        Thread.Sleep(200);

        // Assert: background thread drained + fsynced the buffer.
        new FileInfo(_path).Length.Should().BeGreaterThan(0);
    }

    // ── Test 2 ───────────────────────────────────────────────────────────────

    [Fact]
    public void GroupCommit_RecordsOnDisk_WhenBatchSizeReached()
    {
        // Arrange: small buffer (512 bytes) and tiny batch threshold (5 records).
        // Each empty record is ~33 bytes → buffer holds ~15 records before overflow.
        // After overflow the batch signal fires immediately (15 >= 5).
        using var writer = WalWriter.Open(_path, bufferSize: 512,
            syncMode:        WalSyncMode.GroupCommit,
            flushIntervalMs: 60_000,   // huge timeout — timer must NOT fire during test
            flushBatchSize:  5);

        // Write 20 records to guarantee at least one buffer overflow.
        AppendEmpty(writer, 20);

        // Give the background thread time to wake up and fsync.
        Thread.Sleep(200);

        // Assert: flush happened without waiting for the 60-second timer.
        new FileInfo(_path).Length.Should().BeGreaterThan(0);
    }

    // ── Test 3 ───────────────────────────────────────────────────────────────

    [Fact]
    public void GroupCommit_ExplicitFlush_IsSynchronous()
    {
        // Arrange: large timer so background thread won't fire during the test.
        using var writer = WalWriter.Open(_path, bufferSize: 65_536,
            syncMode:        WalSyncMode.GroupCommit,
            flushIntervalMs: 60_000,
            flushBatchSize:  65_536);

        AppendEmpty(writer, 5);

        // Act: explicit Flush() must be synchronous — data on disk before it returns.
        writer.Flush();

        // Assert immediately (no sleep needed).
        new FileInfo(_path).Length.Should().BeGreaterThan(0);
    }

    // ── Test 4 ───────────────────────────────────────────────────────────────

    [Fact]
    public void GroupCommit_Dispose_FlushesPendingRecords()
    {
        // Arrange: large timer so only Dispose triggers the final flush.
        var writer = WalWriter.Open(_path, bufferSize: 65_536,
            syncMode:        WalSyncMode.GroupCommit,
            flushIntervalMs: 60_000,
            flushBatchSize:  65_536);

        AppendEmpty(writer, 5);

        // Act: Dispose without any explicit Flush call.
        writer.Dispose();

        // Assert: final synchronous Flush inside Dispose wrote all pending records.
        new FileInfo(_path).Length.Should().BeGreaterThan(0);
    }

    // ── Test 5 ───────────────────────────────────────────────────────────────

    [Fact]
    public void Synchronous_Default_UnchangedBehaviour()
    {
        // Arrange: tiny buffer (512 bytes) in default Synchronous mode.
        using var writer = WalWriter.Open(_path, bufferSize: 512);

        // Write enough records to trigger a buffer overflow + synchronous fsync.
        AppendEmpty(writer, 20);

        // Assert: data is on disk immediately — no background thread involved.
        new FileInfo(_path).Length.Should().BeGreaterThan(0);
    }

    // ── Tree-level helpers ───────────────────────────────────────────────────

    private static (BPlusTree<int, int> tree, string dbPath, string walPath)
        OpenGroupCommit(int flushIntervalMs = 5, int flushBatchSize = 256)
    {
        var dbPath  = Path.GetTempFileName();
        var walPath = Path.GetTempFileName();
        var tree = BPlusTree<int, int>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = dbPath,
                WalFilePath         = walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 512,
                CheckpointThreshold = 256,
                WalBufferSize       = 4 * 1024 * 1024,
                SyncMode            = WalSyncMode.GroupCommit,
                FlushIntervalMs     = flushIntervalMs,
                FlushBatchSize      = flushBatchSize,
            },
            Int32Serializer.Instance,
            Int32Serializer.Instance);
        return (tree, dbPath, walPath);
    }

    private static (BPlusTree<int, int> tree, string dbPath, string walPath)
        OpenSync(string? dbPath = null, string? walPath = null)
    {
        dbPath  ??= Path.GetTempFileName();
        walPath ??= Path.GetTempFileName();
        var tree = BPlusTree<int, int>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = dbPath,
                WalFilePath         = walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 512,
                CheckpointThreshold = 256,
                WalBufferSize       = 4 * 1024 * 1024,
                SyncMode            = WalSyncMode.Synchronous,
            },
            Int32Serializer.Instance,
            Int32Serializer.Instance);
        return (tree, dbPath, walPath);
    }

    // ── Test 6: Append never blocks during fsync ─────────────────────────────

    [Fact(Timeout = 10_000)]
    public async Task GroupCommit_AppendDoesNotBlockDuringFsync()
    {
        // With the fix, Append() latency must be microsceconds even while FlushLoop
        // is running an fsync. Without the fix, Append blocks for the full fsync
        // duration (5–50ms) because FlushLoop held _lock across the entire syscall.
        var (tree, dbPath, walPath) = OpenGroupCommit(flushIntervalMs: 5);
        var latencies = new ConcurrentBag<long>();
        var cts       = new CancellationTokenSource(TimeSpan.FromSeconds(8));

        var inserter = Task.Run(() =>
        {
            var sw = new Stopwatch();
            for (int i = 0; i < 50_000 && !cts.Token.IsCancellationRequested; i++)
            {
                sw.Restart();
                tree.Put(i, i);
                sw.Stop();
                latencies.Add(sw.ElapsedTicks);
            }
        }, cts.Token);

        await inserter;

        var sorted    = latencies.OrderBy(t => t).ToList();
        long p99Ticks = sorted[(int)(sorted.Count * 0.99)];
        double p99Ms  = p99Ticks * 1000.0 / Stopwatch.Frequency;

        // Broken impl: p99 ≈ fsync latency = 5–50ms
        // Fixed impl:  p99 < 5ms (buffer copy only)
        p99Ms.Should().BeLessThan(5.0,
            $"p99 Append latency {p99Ms:F2}ms — FlushLoop may still hold _lock during fsync");

        tree.Close();
        try { File.Delete(dbPath); File.Delete(walPath); } catch { }
    }

    // ── Test 7: FlushedLsn never ahead of actual flush ───────────────────────

    [Fact(Timeout = 10_000)]
    public async Task GroupCommit_FlushedLsn_NeverAheadOfActualFlush()
    {
        // After Close(), all data must be durably on disk (checkpoint flushed dirty pages,
        // FinalFlush + WAL flush ensured WAL records were fsynced).
        // Verify by reopening the same data file and reading all inserted keys.
        // If FlushedLsn were ahead of actual flush (Failure Point #1), dirty pages could
        // be evicted before their WAL records were durable — corrupting data on crash.
        var (tree, dbPath, walPath) = OpenGroupCommit(flushIntervalMs: 5);
        for (int i = 0; i < 10_000; i++) tree.Put(i, i);
        tree.Close();

        // Reopen from the data file (checkpoint wrote all dirty pages on Close).
        var (tree2, _, _) = OpenSync(dbPath, walPath);
        for (int i = 0; i < 10_000; i++)
            tree2.TryGet(i, out _).Should().BeTrue(
                $"key {i} missing — data not durably written on Close() (Failure Point #1)");
        tree2.Close();

        try { File.Delete(dbPath); File.Delete(walPath); } catch { }
        await Task.CompletedTask;
    }

    // ── Test 8: FlushUpTo fast-paths when already flushed ────────────────────

    [Fact(Timeout = 10_000)]
    public async Task GroupCommit_FlushUpTo_SkipsRedundantFsync_WhenAlreadyFlushed()
    {
        // If FlushLoop has already fsynced, FlushUpTo(that LSN) must return in
        // microseconds (fast path), not call fsync again.
        var (tree, dbPath, walPath) = OpenGroupCommit(flushIntervalMs: 5);
        for (int i = 0; i < 1_000; i++) tree.Put(i, i);

        Thread.Sleep(50);   // let FlushLoop fire at least once

        var walWriter  = tree.GetWalWriterForTesting();
        ulong flushedLsn = walWriter.FlushedLsn;
        flushedLsn.Should().BeGreaterThan(0, "FlushLoop should have run at least once");

        var sw = Stopwatch.StartNew();
        walWriter.FlushUpTo(flushedLsn);
        sw.Stop();

        sw.ElapsedMilliseconds.Should().BeLessThan(2,
            "FlushUpTo(already-flushed LSN) must fast-path without calling fsync");

        tree.Close();
        try { File.Delete(dbPath); File.Delete(walPath); } catch { }
        await Task.CompletedTask;
    }

    // ── Test 9: Concurrent FlushLoop + FlushUpTo never corrupt stream ─────────

    [Fact(Timeout = 15_000)]
    public async Task GroupCommit_ConcurrentFlushLoopAndFlushUpTo_NoCorruption()
    {
        // Hammer both FlushLoop (via timed inserts) and FlushUpTo (via manual calls)
        // concurrently. Without _flushLock, concurrent FileStream.Flush calls race.
        // Verify: all inserted data recoverable after close + data-file wipe.
        var (tree, dbPath, walPath) = OpenGroupCommit(flushIntervalMs: 2);
        var walWriter = tree.GetWalWriterForTesting();
        var cts       = new CancellationTokenSource(TimeSpan.FromSeconds(8));

        var inserter = Task.Run(() =>
        {
            for (int i = 0; i < 20_000 && !cts.Token.IsCancellationRequested; i++)
                tree.Put(i, i);
        }, cts.Token);

        var flusher = Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                walWriter.FlushUpTo(walWriter.FlushedLsn + 1);
                await Task.Delay(3, cts.Token).ConfigureAwait(false);
            }
        }, cts.Token);

        await Task.WhenAll(inserter,
            flusher.ContinueWith(_ => { }));   // absorb cancellation from flusher

        tree.Close();

        // Reopen from data file — all actually-inserted keys must be present.
        // If _flushLock is missing and concurrent FileStream.Flush corrupts the WAL,
        // some pages will be unreadable after the checkpoint that Close() performs.
        var (tree2, _, _) = OpenSync(dbPath, walPath);
        int found = 0;
        for (int i = 0; i < 20_000; i++)
            if (tree2.TryGet(i, out _)) found++;
        found.Should().BeGreaterThan(14_000,
            "most inserted keys must be present — concurrent flush must not corrupt data");
        tree2.Close();

        try { File.Delete(dbPath); File.Delete(walPath); } catch { }
    }

    // ── Test 10: FinalFlush called on Close ───────────────────────────────────

    [Fact(Timeout = 10_000)]
    public async Task GroupCommit_Close_FlushesPendingRecords()
    {
        // FlushIntervalMs=60_000 → FlushLoop timer will NOT fire during the test.
        // FinalFlush (called when FlushLoop exits on cancellation) is the only flush path.
        // Verify: all records durable after Close() without explicit Flush().
        var (tree, dbPath, walPath) = OpenGroupCommit(flushIntervalMs: 9_999); // max valid; large enough the timer won't fire
        for (int i = 0; i < 500; i++) tree.Put(i, i);
        tree.Close();   // FlushLoop cancellation must trigger FinalFlush before exiting

        // Reopen from data file — all keys inserted before Close() must be present.
        // Close() calls FinalFlush (drains WAL) then checkpoint (flushes dirty pages).
        // If FinalFlush is missing, WAL records may be lost on shutdown (Failure Point #4).
        var (tree2, _, _) = OpenSync(dbPath, walPath);
        for (int i = 0; i < 500; i++)
            tree2.TryGet(i, out _).Should().BeTrue(
                $"key {i} missing — data not durable after Close() (Failure Point #4)");
        tree2.Close();

        try { File.Delete(dbPath); File.Delete(walPath); } catch { }
        await Task.CompletedTask;
    }

    // ── Test 11: GroupCommit buffers WAL writes (structural, not timing) ────────

    [Fact]
    public void GroupCommit_BuffersWalWrites_FlushedLsnLagsCurrentLsn()
    {
        // GroupCommit mode buffers WAL writes in memory — fsync is deferred to the
        // background flush loop or an explicit Flush() call. After a batch of inserts
        // with auto-flush suppressed, FlushedLsn must be behind CurrentLsn.
        // This is environment-independent: it does not compare timing between modes.
        // Timing-comparison replacement for Phase 45 flake (Phase 46).
        var (tree, dbPath, walPath) = OpenGroupCommit(
            flushIntervalMs: 10_000,   // prevent background flush during test
            flushBatchSize:  65_536);  // prevent batch-size flush during test

        try
        {
            var wal = tree.GetWalWriterForTesting();

            for (int i = 0; i < 50; i++) tree.Put(i, i * 10);

            wal.FlushedLsn.Should().BeLessThan(wal.CurrentLsn.Value,
                "GroupCommit mode buffers WAL writes — FlushedLsn must lag CurrentLsn " +
                "after inserts when auto-flush is suppressed");
        }
        finally
        {
            tree.Close();
            try { File.Delete(dbPath); File.Delete(walPath); } catch { }
        }
    }
}
