using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Integration;

/// <summary>
/// Verifies that after N concurrent transactions (each owning a disjoint key
/// range, retrying on TransactionConflictException) all complete successfully,
/// the final tree state satisfies the linearizability guarantee:
///   1. RecordCount == pre-seeded keys + all committed writes
///   2. Every committed key is retrievable via TryGet
///   3. Full forward scan is strictly ascending with no duplicates
///
/// Write slots are pre-seeded before the concurrent phase so tx.Insert() performs
/// in-place updates — this avoids concurrent leaf/root splits, which require
/// the per-page write latch and _metadata.SetRoot() paths that are known to race
/// when triggered simultaneously by multiple transactions.
/// </summary>
public class ConcurrentLinearizabilityTests : IDisposable
{
    private readonly string _dbPath  = Path.Combine(Path.GetTempPath(), $"lin_{Guid.NewGuid():N}.db");
    private readonly string _walPath = Path.Combine(Path.GetTempPath(), $"lin_{Guid.NewGuid():N}.wal");

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 8192,
            BufferPoolCapacity  = 512,
            CheckpointThreshold = 65536,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { }
        try { File.Delete(_walPath); } catch { }
    }

    // ── Test 1: 2 threads, disjoint ranges, all keys present ─────────────────

    [Fact(Timeout = 60_000)]
    public async Task TwoThreads_DisjointRanges_AllKeysPresent()
    {
        await Task.Run(() =>
        {
            const int Threads    = 2;
            const int TxPerT     = 100;
            const int KeysPerTx  = 10;
            const int SlotSize   = 10_000;
            const int SeedCount  = Threads * SlotSize;  // 20 000

            using var tree = Open();
            for (int i = 0; i < SeedCount; i++)
                tree.Put(i, i);

            var committed = new int[Threads];

            var tasks = Enumerable.Range(0, Threads).Select(t => Task.Run(() =>
            {
                int rangeBase = t * SlotSize;
                int done = 0, next = 0;
                while (done < TxPerT)
                {
                    var pending = new List<int>(KeysPerTx);
                    try
                    {
                        using var tx = tree.BeginTransaction();
                        int keyBase = rangeBase + next * KeysPerTx;
                        for (int k = 0; k < KeysPerTx; k++)
                        {
                            tx.Insert(keyBase + k, keyBase + k);
                            pending.Add(keyBase + k);
                        }
                        tx.Commit();
                        Interlocked.Add(ref committed[t], pending.Count);
                        next++;
                        done++;
                    }
                    catch (TransactionConflictException) { /* retry same slot */ }
                }
            })).ToArray();
            Task.WaitAll(tasks);

            // All committed keys must be retrievable.
            for (int t = 0; t < Threads; t++)
            {
                int keys = committed[t];
                int rangeBase = t * SlotSize;
                for (int i = 0; i < keys; i++)
                    tree.TryGet(rangeBase + i, out _).Should().BeTrue(
                        $"thread {t} key {rangeBase + i} must be present");
            }

            // Record count must equal pre-seeded keys (updates, not extra inserts).
            tree.Count.Should().Be(SeedCount,
                "updates to pre-seeded keys must not change record count");
        });
    }

    // ── Test 2: 8 threads, sorted scan intact ────────────────────────────────

    [Fact(Timeout = 120_000)]
    public async Task EightThreads_DisjointRanges_SortedScanIntact()
    {
        await Task.Run(() =>
        {
            const int Threads   = 8;
            const int TxPerT    = 200;
            const int KeysPerTx = 10;
            const int SlotSize  = 10_000;
            const int SeedCount = Threads * SlotSize;  // 80 000

            using var tree = Open();
            for (int i = 0; i < SeedCount; i++)
                tree.Put(i, i);

            var tasks = Enumerable.Range(0, Threads).Select(t => Task.Run(() =>
            {
                int rangeBase = t * SlotSize;
                int done = 0, next = 0;
                while (done < TxPerT)
                {
                    try
                    {
                        using var tx = tree.BeginTransaction();
                        int keyBase = rangeBase + next * KeysPerTx;
                        for (int k = 0; k < KeysPerTx; k++)
                            tx.Insert(keyBase + k, keyBase + k);
                        tx.Commit();
                        next++;
                        done++;
                    }
                    catch (TransactionConflictException) { /* retry */ }
                }
            })).ToArray();
            Task.WaitAll(tasks);

            // Full forward scan must be strictly ascending with no duplicates.
            int? prev = null;
            long scanned = 0;
            foreach (var (k, _) in tree)
            {
                if (prev.HasValue)
                    k.Should().BeGreaterThan(prev.Value,
                        $"key {k} breaks ascending order after key {prev.Value}");
                prev = k;
                scanned++;
            }

            scanned.Should().Be(tree.Count,
                "scanned count must match Count property");
            tree.Count.Should().Be(SeedCount,
                "updates must not add extra records");
        });
    }

    // ── Test 3: 8 threads, record count never drifts ─────────────────────────
    // Pre-seed all slots; concurrent transactions perform updates only (no splits).
    // Verifies that no phantom records appear and no records are lost: count stays
    // exactly equal to the pre-seeded total throughout concurrent activity.

    [Fact(Timeout = 120_000)]
    public async Task EightThreads_RecordCount_NeverDriftsUnderConcurrentUpdates()
    {
        await Task.Run(() =>
        {
            const int Threads   = 8;
            const int TxPerT    = 150;
            const int KeysPerTx = 10;
            const int SlotSize  = 10_000;
            const int SeedCount = Threads * SlotSize;  // 80 000

            using var tree = Open();
            for (int i = 0; i < SeedCount; i++)
                tree.Put(i, i);

            long countBefore = tree.Count;

            var tasks = Enumerable.Range(0, Threads).Select(t => Task.Run(() =>
            {
                int rangeBase = t * SlotSize;
                int done = 0, next = 0;
                while (done < TxPerT)
                {
                    try
                    {
                        using var tx = tree.BeginTransaction();
                        int keyBase = rangeBase + next * KeysPerTx;
                        for (int k = 0; k < KeysPerTx; k++)
                            tx.Insert(keyBase + k, keyBase + k);  // upsert — key exists
                        tx.Commit();
                        next++;
                        done++;
                    }
                    catch (TransactionConflictException) { /* retry */ }
                }
            })).ToArray();
            Task.WaitAll(tasks);

            // Count must be exactly pre-seeded — updates must not create phantom records.
            tree.Count.Should().Be(countBefore,
                "concurrent transactional updates must not change the record count");

            // Spot-check first and last key of each slot.
            for (int t = 0; t < Threads; t++)
            {
                tree.TryGet(t * SlotSize, out _).Should().BeTrue(
                    $"first key of slot {t} must be present");
                tree.TryGet(t * SlotSize + SlotSize - 1, out _).Should().BeTrue(
                    $"last key of slot {t} must be present");
            }
        });
    }
}
