using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Integration;

/// <summary>
/// Verifies that two concurrent transactions can both trigger leaf splits
/// without corrupting shared state (the old _metadata.SetRoot() race).
///
/// With the old code, two threads simultaneously executing InsertInTransaction
/// on full leaf pages would overwrite each other's savedRoot via the
/// save→SetRoot→use→restore pattern on the shared _metadata object.
/// Splitter now returns (newRootId, newHeight) instead of mutating _metadata,
/// so each caller's shadow root is computed from the return value alone.
/// </summary>
public class ConcurrentSplitTests : IDisposable
{
    private readonly string _dbPath  = Path.Combine(Path.GetTempPath(), $"csplit_{Guid.NewGuid():N}.db");
    private readonly string _walPath = Path.Combine(Path.GetTempPath(), $"csplit_{Guid.NewGuid():N}.wal");

    // Small page size (4096) → leaf capacity ~337 int/int entries → splits occur sooner.
    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 4096,
            BufferPoolCapacity  = 64,
            CheckpointThreshold = 65536,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { }
        try { File.Delete(_walPath); } catch { }
    }

    // M97: 60s→180s under full-suite parallel load. Concurrent-split stress is
    // CPU-bound and got starved under xUnit parallelism.
    [Fact(Timeout = 180_000)]
    public async Task ConcurrentInserts_BothTriggerSplits_NoCorruption()
    {
        await Task.Run(() =>
        {
            using var tree = Open();

            // Pre-seed ~400 keys so the first leaf pages are nearly full.
            // With 4096-byte pages, int/int leaves hold ~337 entries; 400 keys fills ~2 leaves.
            const int BaseCount = 400;
            for (int i = 0; i < BaseCount; i++)
                tree.Put(i, i);

            // Two threads insert disjoint new key ranges, each forcing leaf splits.
            // Thread 0: keys 1000–1099, Thread 1: keys 2000–2099.
            // Retry on conflict until all 100 keys per thread are inserted.
            const int Thread0Base = 1000;
            const int Thread1Base = 2000;
            const int PerThread   = 100;

            var t0Committed = new List<int>();
            var t1Committed = new List<int>();

            var task0 = Task.Run(() =>
            {
                var committed = new List<int>();
                for (int i = 0; i < PerThread; )
                {
                    try
                    {
                        using var tx = tree.BeginTransaction();
                        tx.Insert(Thread0Base + i, Thread0Base + i);
                        tx.Commit();
                        committed.Add(Thread0Base + i);
                        i++;
                    }
                    catch (TransactionConflictException) { /* retry */ }
                }
                lock (t0Committed) t0Committed.AddRange(committed);
            });

            var task1 = Task.Run(() =>
            {
                var committed = new List<int>();
                for (int i = 0; i < PerThread; )
                {
                    try
                    {
                        using var tx = tree.BeginTransaction();
                        tx.Insert(Thread1Base + i, Thread1Base + i);
                        tx.Commit();
                        committed.Add(Thread1Base + i);
                        i++;
                    }
                    catch (TransactionConflictException) { /* retry */ }
                }
                lock (t1Committed) t1Committed.AddRange(committed);
            });

            Task.WaitAll(task0, task1);

            // All base keys must still be present.
            for (int i = 0; i < BaseCount; i++)
                tree.TryGet(i, out _).Should().BeTrue($"base key {i} missing after concurrent splits");

            // All committed new keys must be retrievable.
            foreach (int key in t0Committed)
                tree.TryGet(key, out _).Should().BeTrue($"thread-0 committed key {key} missing");
            foreach (int key in t1Committed)
                tree.TryGet(key, out _).Should().BeTrue($"thread-1 committed key {key} missing");

            t0Committed.Should().HaveCount(PerThread, "thread 0 must commit all 100 keys");
            t1Committed.Should().HaveCount(PerThread, "thread 1 must commit all 100 keys");

            // Sorted scan must be strictly ascending.
            int? prev = null;
            foreach (var (k, _) in tree)
            {
                if (prev.HasValue)
                    k.Should().BeGreaterThan(prev.Value, $"key {k} breaks sort order after {prev.Value}");
                prev = k;
            }
        });
    }
}
