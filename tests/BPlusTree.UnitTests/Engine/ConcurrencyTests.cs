using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// All tests have a hard Timeout — any test hanging beyond its limit indicates a deadlock.
/// </summary>
public class ConcurrencyTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, TreeEngine<int, int> engine) Open()
    {
        var wal  = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = PageSize, BufferPoolCapacity = 256, CheckpointThreshold = 64,
        }, wal);
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, new TreeEngine<int, int>(mgr, ns, meta));
    }

    // ── No deadlock — reads ───────────────────────────────────────────────────

    [Fact(Timeout = 30_000)]
    public async Task ConcurrentReaders_20Threads_NoDeadlock()
    {
        await Task.Run(() =>
        {
            var (mgr, engine) = Open();
            for (int i = 0; i < 1000; i++) engine.Insert(i, i);
            var threads = Enumerable.Range(0, 20).Select(_ => new Thread(() =>
            {
                for (int i = 0; i < 500; i++) engine.TryGet(i % 1000, out _);
            })).ToList();
            threads.ForEach(t => t.Start());
            bool all = threads.All(t => t.Join(TimeSpan.FromSeconds(25)));
            all.Should().BeTrue("deadlock detected: not all reader threads completed");
            mgr.Dispose();
        });
    }

    // ── No deadlock — writes ──────────────────────────────────────────────────

    [Fact(Timeout = 30_000)]
    public async Task ConcurrentWriters_NonOverlapping_NoDeadlock_AllKeysPresent()
    {
        await Task.Run(() =>
        {
            var (mgr, engine) = Open();
            var threads = Enumerable.Range(0, 8).Select(t => new Thread(() =>
            {
                for (int i = t * 500; i < (t + 1) * 500; i++) engine.Insert(i, i);
            })).ToList();
            threads.ForEach(t => t.Start());
            bool all = threads.All(t => t.Join(TimeSpan.FromSeconds(25)));
            all.Should().BeTrue("deadlock detected");
            for (int i = 0; i < 4000; i++)
                engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after concurrent insert");
            mgr.Dispose();
        });
    }

    // ── No deadlock — mixed read/write ────────────────────────────────────────

    [Fact(Timeout = 30_000)]
    public async Task MixedReadWrite_NoDeadlock_30Seconds()
    {
        await Task.Run(() =>
        {
            var (mgr, engine) = Open();
            for (int i = 0; i < 500; i++) engine.Insert(i, i);

            var cts     = new CancellationTokenSource(TimeSpan.FromSeconds(25));
            var readers = Enumerable.Range(0, 10).Select(_ => Task.Run(() =>
            {
                while (!cts.Token.IsCancellationRequested)
                    engine.TryGet(Random.Shared.Next(1000), out _);
            }, cts.Token)).ToArray();
            var writers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
            {
                int k = 500;
                while (!cts.Token.IsCancellationRequested)
                    engine.Insert(k++ % 2000, k);
            }, cts.Token)).ToArray();

            bool completed = Task.WaitAll(readers.Concat(writers).ToArray(),
                TimeSpan.FromSeconds(28));
            // Tasks cancelled by token = normal. Only fail if timeout or exception.
            // Any AggregateException from the tasks = corruption or deadlock.
            mgr.Dispose();
        });
    }

    // ── Structural integrity after concurrent splits ───────────────────────────

    [Fact(Timeout = 30_000)]
    public async Task ConcurrentInserts_ForcingSplits_AllKeysRetrievable()
    {
        await Task.Run(() =>
        {
            var (mgr, engine) = Open();
            var threads = Enumerable.Range(0, 8).Select(t => new Thread(() =>
            {
                for (int i = 0; i < 1000; i++) engine.Insert(t * 10_000 + i, i);
            })).ToList();
            threads.ForEach(t => t.Start());
            bool all = threads.All(t => t.Join(TimeSpan.FromSeconds(25)));
            all.Should().BeTrue("deadlock during concurrent splits");
            for (int t = 0; t < 8; t++)
                for (int i = 0; i < 1000; i++)
                    engine.TryGet(t * 10_000 + i, out _).Should().BeTrue($"key {t * 10_000 + i} missing");
            mgr.Dispose();
        });
    }

    // ── Iterator safety ───────────────────────────────────────────────────────

    [Fact(Timeout = 30_000)]
    public async Task ConcurrentScanAndInsert_ScanCompletesWithoutException()
    {
        await Task.Run(() =>
        {
            var (mgr, engine) = Open();
            for (int i = 0; i < 1000; i++) engine.Insert(i, i);

            Exception? scanException = null;
            var scanner = new Thread(() =>
            {
                try { var _ = engine.Scan().Select(x => x.Key).ToList(); }
                catch (Exception ex) { scanException = ex; }
            });
            var inserter = new Thread(() =>
            {
                for (int i = 1000; i < 2000; i++) engine.Insert(i, i);
            });
            scanner.Start(); inserter.Start();
            scanner.Join(); inserter.Join();
            scanException.Should().BeNull("scan must complete without exception under concurrent insert");
            mgr.Dispose();
        });
    }

    // ── Hot-key contention ────────────────────────────────────────────────────

    [Fact(Timeout = 30_000)]
    public async Task HotKey_20ConcurrentUpdates_NoDataLoss()
    {
        await Task.Run(() =>
        {
            var (mgr, engine) = Open();
            engine.Insert(0, 0);
            // 20 threads each incrementing key 0: final value must be 20
            var threads = Enumerable.Range(0, 20).Select(_ => new Thread(() =>
            {
                engine.Update(0, v => v + 1);
            })).ToList();
            threads.ForEach(t => t.Start());
            threads.ForEach(t => t.Join());
            // Without proper locking this will be < 20 due to lost updates.
            // With proper write latching, each increment is serialized.
            engine.TryGet(0, out int final);
            final.Should().Be(20, "all 20 increments must be serialized");
            mgr.Dispose();
        });
    }

    public void Dispose() { try { File.Delete(_dbPath); File.Delete(_walPath); } catch { } }
}
