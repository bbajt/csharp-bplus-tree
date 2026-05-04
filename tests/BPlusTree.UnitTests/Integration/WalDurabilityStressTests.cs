using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Integration;

/// <summary>
/// Verifies the WAL durability guarantee under concurrent write load:
/// after all concurrent transactions commit and the data file is corrupted
/// (simulating a crash where dirty pages were never flushed), WAL replay on the
/// next Open() must restore every committed transaction exactly.
///
/// Crash simulation: after Flush() ensures WAL is on disk, overwrite page 0 of
/// the data file with zeros (same technique as RecoveryTests.Scenario1). The WAL
/// file is untouched. On the next Open(), RecoverFromWal() runs automatically
/// inside BPlusTree.Open() via the TreeEngine constructor and replays all records.
/// </summary>
public class WalDurabilityStressTests : IDisposable
{
    private const int PageSize = 8192;

    private readonly string _dbPath  = Path.Combine(Path.GetTempPath(), $"wal_{Guid.NewGuid():N}.db");
    private readonly string _walPath = Path.Combine(Path.GetTempPath(), $"wal_{Guid.NewGuid():N}.wal");

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = PageSize,
            BufferPoolCapacity  = 512,
            CheckpointThreshold = 65536,  // high threshold — no auto-checkpoint during test
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    /// <summary>
    /// Simulate crash: corrupt the first page of the data file.
    /// WAL remains intact; recovery must replay from WAL on next Open().
    /// </summary>
    private void CorruptDataFile()
    {
        using var fs = new FileStream(_dbPath, FileMode.Open, FileAccess.Write, FileShare.None);
        fs.Write(new byte[PageSize], 0, PageSize);
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { }
        try { File.Delete(_walPath); } catch { }
    }

    // ── Test 1: 4 concurrent writers, all committed keys survive crash ────────

    [Fact(Timeout = 120_000)]
    public async Task FourConcurrentWriters_AllCommittedKeysSurviveCrash()
    {
        await Task.Run(() =>
        {
            const int Threads   = 4;
            const int TxPerT    = 50;
            const int KeysPerTx = 10;
            const int SlotSize  = 10_000;
            const int SeedCount = Threads * SlotSize;  // 40 000

            // Phase 1: write and commit concurrently, tracking every committed key.
            var committedKeys = Enumerable.Range(0, Threads)
                .Select(_ => new List<int>()).ToArray();

            {
                using var tree = Open();

                // Pre-seed all slots so concurrent tx.Insert() are updates (no split races).
                for (int i = 0; i < SeedCount; i++)
                    tree.Put(i, i);

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
                            lock (committedKeys[t]) committedKeys[t].AddRange(pending);
                            next++;
                            done++;
                        }
                        catch (TransactionConflictException) { /* retry same slot */ }
                    }
                })).ToArray();
                Task.WaitAll(tasks);

                tree.Flush();      // guarantee WAL is on disk
                tree.Dispose();
                CorruptDataFile(); // simulate crash: data pages lost, WAL intact
            }

            // Phase 2: reopen — WAL replay restores all committed transactions.
            {
                using var tree = Open();

                foreach (var keys in committedKeys)
                    foreach (int key in keys)
                        tree.TryGet(key, out _).Should().BeTrue(
                            $"committed key {key} must survive crash+recovery");

                // Pre-seeded keys that were NOT touched must also survive.
                tree.TryGet(0, out _).Should().BeTrue("first pre-seeded key must survive recovery");
                tree.TryGet(SeedCount - 1, out _).Should().BeTrue("last pre-seeded key must survive recovery");
            }
        });
    }

    // ── Test 2: large single transaction survives crash ───────────────────────

    [Fact(Timeout = 60_000)]
    public async Task LargeTransaction_AllKeysSurviveCrash()
    {
        await Task.Run(() =>
        {
            const int Count = 5_000;

            // Commit one large transaction (many WAL records).
            {
                using var tree = Open();
                using var tx   = tree.BeginTransaction();
                for (int i = 0; i < Count; i++)
                    tx.Insert(i, i);
                tx.Commit();

                tree.Flush();
                tree.Dispose();
                CorruptDataFile();
            }

            // Recovery: all 5 000 keys must be present.
            {
                using var tree = Open();

                tree.Count.Should().Be(Count,
                    "record count must be exact after large-tx recovery");

                for (int i = 0; i < Count; i++)
                    tree.TryGet(i, out _).Should().BeTrue(
                        $"key {i} must survive large-transaction crash+recovery");
            }
        });
    }
}
