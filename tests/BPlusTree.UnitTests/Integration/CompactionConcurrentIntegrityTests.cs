using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Integration;

/// <summary>
/// Verifies data integrity when Compact() runs concurrently with active writers.
///
/// Compact() uses a two-phase online protocol:
///   Phase A (compaction lock released): walks the live leaf chain, writes .compact
///           file, accumulates a _deltaMap for writes that arrive during Phase A.
///   Phase B (writer lock held): applies _deltaMap to the .compact file, renames
///           it over the original.
///
/// Any key committed before or during Phase A (captured in delta) or after Phase B
/// (directly to the new file) must be present after compaction. This test drives
/// concurrent writers throughout Compact() and asserts full data integrity afterward.
/// </summary>
public class CompactionConcurrentIntegrityTests : IDisposable
{
    private readonly string _dbPath  = Path.Combine(Path.GetTempPath(), $"cmp_{Guid.NewGuid():N}.db");
    private readonly string _walPath = Path.Combine(Path.GetTempPath(), $"cmp_{Guid.NewGuid():N}.wal");

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

    // ── Test 1: all original surviving keys present after compaction ──────────

    [Fact(Timeout = 120_000)]
    public async Task Compact_WhileConcurrentWriters_AllOriginalKeysSurvive()
    {
        await Task.Run(() =>
        {
            const int Seed      = 50_000;
            const int Delete    = 40_000;  // delete most keys to make compaction meaningful
            const int Threads   = 4;
            const int TxPerT    = 30;
            const int KeysPerTx = 10;
            const int SlotSize  = 10_000;

            using var tree = Open();

            // Seed and delete to create fragmentation.
            for (int i = 0; i < Seed; i++)
                tree.Put(i, i);
            for (int i = 0; i < Delete; i++)
                tree.Delete(i);
            // Surviving original keys: Delete .. Seed-1 (10 000 keys: 40 000–49 999)

            // Pre-seed writer slots (beyond Seed) so concurrent tx.Insert() are updates.
            for (int t = 0; t < Threads; t++)
                for (int i = 0; i < SlotSize; i++)
                    tree.Put(Seed + t * SlotSize + i, i);

            var committedKeys = Enumerable.Range(0, Threads)
                .Select(_ => new List<int>()).ToArray();

            var cts = new CancellationTokenSource();

            var writerTasks = Enumerable.Range(0, Threads).Select(t => Task.Run(() =>
            {
                int rangeBase = Seed + t * SlotSize;
                int done = 0, next = 0;
                while (done < TxPerT && !cts.IsCancellationRequested)
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
                        lock (committedKeys[t]) committedKeys[t].AddRange(pending);
                        next++;
                        done++;
                    }
                    catch (TransactionConflictException) { /* retry */ }
                    catch (CompactionInProgressException) { Thread.Sleep(1); }
                }
            })).ToArray();

            // Compact concurrently with the writers.
            var compactTask = Task.Run(() => tree.Compact());

            Task.WaitAll(writerTasks.Append(compactTask).ToArray());

            // All surviving original keys must still be present.
            for (int i = Delete; i < Seed; i++)
                tree.TryGet(i, out _).Should().BeTrue(
                    $"original key {i} missing after compaction");

            // All committed writer keys must be present.
            foreach (var keys in committedKeys)
                foreach (int key in keys)
                    tree.TryGet(key, out _).Should().BeTrue(
                        $"committed key {key} missing after compaction");
        });
    }

    // ── Test 2: full scan sorted and complete after compaction ────────────────

    [Fact(Timeout = 120_000)]
    public async Task Compact_ThenFullScan_SortedAndComplete()
    {
        await Task.Run(() =>
        {
            const int Seed      = 50_000;
            const int Delete    = 40_000;
            const int Threads   = 4;
            const int TxPerT    = 30;
            const int KeysPerTx = 10;
            const int SlotSize  = 10_000;

            using var tree = Open();

            for (int i = 0; i < Seed; i++) tree.Put(i, i);
            for (int i = 0; i < Delete; i++) tree.Delete(i);

            for (int t = 0; t < Threads; t++)
                for (int i = 0; i < SlotSize; i++)
                    tree.Put(Seed + t * SlotSize + i, i);

            var writerTasks = Enumerable.Range(0, Threads).Select(t => Task.Run(() =>
            {
                int rangeBase = Seed + t * SlotSize;
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
                    catch (CompactionInProgressException) { Thread.Sleep(1); }
                }
            })).ToArray();

            var compactTask = Task.Run(() => tree.Compact());
            Task.WaitAll(writerTasks.Append(compactTask).ToArray());

            // Full forward scan must be strictly ascending.
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
                "scanned count must match Count after compaction");
        });
    }

    // ── Test 3: compacted file smaller after heavy deletes ───────────────────

    [Fact(Timeout = 60_000)]
    public async Task Compact_AfterHeavyDeletes_FileSizeReduced()
    {
        await Task.Run(() =>
        {
            const int Seed   = 50_000;
            const int Delete = 40_000;  // delete 80% of keys

            using var tree = Open();
            for (int i = 0; i < Seed; i++)   tree.Put(i, i);
            for (int i = 0; i < Delete; i++) tree.Delete(i);

            tree.Flush();  // flush so file size reflects the deletes
            long sizeBefore = new FileInfo(_dbPath).Length;

            tree.Compact();

            long sizeAfter = new FileInfo(_dbPath).Length;
            sizeAfter.Should().BeLessThan(sizeBefore * 50 / 100,
                "compacting after 80% delete must reduce file to less than 50% of original size");
        });
    }
}
