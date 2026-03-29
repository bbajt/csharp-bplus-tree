using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Integration;

/// <summary>
/// Verifies snapshot point-in-time isolation under concurrent write pressure.
///
/// A snapshot opened before a batch of concurrent writes must never reflect any
/// write committed after the snapshot was opened. The CoW/shadow-paging mechanism
/// ensures the snapshot's page graph is immutable: while _snapshotCount > 0, all
/// writes allocate shadow copies rather than modifying pages in-place. The snapshot
/// holds a pinned root page ID; all reads traverse from that unchanged root.
///
/// After the snapshot is closed, the live tree must reflect all committed writes.
/// </summary>
public class SnapshotIsolationStressTests : IDisposable
{
    private readonly string _dbPath  = Path.Combine(Path.GetTempPath(), $"snap_{Guid.NewGuid():N}.db");
    private readonly string _walPath = Path.Combine(Path.GetTempPath(), $"snap_{Guid.NewGuid():N}.wal");

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

    // ── Test 1: snapshot does not see concurrently inserted keys ─────────────

    [Fact(Timeout = 120_000)]
    public async Task Snapshot_DoesNotSeeKeysConcurrentlyInserted()
    {
        await Task.Run(() =>
        {
            const int BaseCount = 10_000;
            const int Threads   = 4;
            const int PerThread = 10_000;
            const int SlotSize  = 10_000;

            using var tree = Open();

            // Insert base set: keys 0 – BaseCount-1.
            for (int i = 0; i < BaseCount; i++)
                tree.Put(i, i);

            // Open snapshot at base set boundary.
            using var snap = tree.BeginSnapshot();

            // Concurrently insert keys beyond base set (auto-commit puts).
            var tasks = Enumerable.Range(0, Threads).Select(t => Task.Run(() =>
            {
                int rangeBase = BaseCount + t * SlotSize;
                for (int i = 0; i < PerThread; i++)
                    tree.Put(rangeBase + i, rangeBase + i);
            })).ToArray();
            Task.WaitAll(tasks);

            // Snapshot must not see any post-open key.
            snap.TryGet(BaseCount, out _).Should().BeFalse(
                "snapshot must not see the first key inserted after it was opened");
            snap.TryGet(BaseCount + (Threads - 1) * SlotSize + PerThread - 1, out _).Should().BeFalse(
                "snapshot must not see the last key inserted after it was opened");

            // Snapshot must still see all base-set keys.
            snap.TryGet(0, out _).Should().BeTrue(
                "snapshot must see the first base-set key");
            snap.TryGet(BaseCount - 1, out _).Should().BeTrue(
                "snapshot must see the last base-set key");

            // Snapshot count must equal the base set.
            snap.Count.Should().Be(BaseCount,
                "snapshot Count must equal the base set size");

            // Live tree must see all keys.
            tree.Count.Should().Be(BaseCount + (long)Threads * PerThread,
                "live tree must see all keys after snapshot is closed");
        });
    }

    // ── Test 2: snapshot Count stable under write storm ──────────────────────

    [Fact(Timeout = 120_000)]
    public async Task Snapshot_CountStableUnderConcurrentWrites()
    {
        await Task.Run(() =>
        {
            const int BaseCount       = 5_000;
            const int Threads         = 4;
            const int WritesPerThread = 2_000;
            const int SlotSize        = 10_000;

            using var tree = Open();
            for (int i = 0; i < BaseCount; i++)
                tree.Put(i, i);

            using var snap = tree.BeginSnapshot();

            // Writers run in the background inserting new keys.
            var cts = new CancellationTokenSource();
            var writeTasks = Enumerable.Range(0, Threads).Select(t => Task.Run(() =>
            {
                int rangeBase = BaseCount + t * SlotSize;
                for (int i = 0; i < WritesPerThread && !cts.IsCancellationRequested; i++)
                    tree.Put(rangeBase + i, i);
            })).ToArray();

            // Sample the snapshot count several times while writers are active.
            for (int sample = 0; sample < 5; sample++)
            {
                snap.Count.Should().Be(BaseCount,
                    $"snapshot Count must be stable at {BaseCount} on sample {sample}");
                Thread.Sleep(5);
            }

            cts.Cancel();
            Task.WaitAll(writeTasks);

            // Final check: snapshot still frozen at base count.
            snap.Count.Should().Be(BaseCount,
                "snapshot Count must be unchanged after all writes complete");
        });
    }

    // ── Test 3: multiple simultaneous snapshots with independent views ────────

    [Fact(Timeout = 60_000)]
    public async Task MultipleSnapshots_IndependentPointInTimeViews()
    {
        await Task.Run(() =>
        {
            using var tree = Open();

            // Snapshot A: sees keys 0 – 999.
            for (int i = 0; i < 1_000; i++) tree.Put(i, i);
            using var snapA = tree.BeginSnapshot();

            // Snapshot B: sees keys 0 – 1 999.
            for (int i = 1_000; i < 2_000; i++) tree.Put(i, i);
            using var snapB = tree.BeginSnapshot();

            // Snapshot C: sees keys 0 – 2 999.
            for (int i = 2_000; i < 3_000; i++) tree.Put(i, i);
            using var snapC = tree.BeginSnapshot();

            // Insert more keys after all snapshots — none should see them.
            for (int i = 3_000; i < 4_000; i++) tree.Put(i, i);

            // ── Snapshot A: sees 0–999 only ───────────────────────────────────
            snapA.TryGet(999,   out _).Should().BeTrue("A must see last key in its range");
            snapA.TryGet(1_000, out _).Should().BeFalse("A must not see key added after it opened");
            snapA.Count.Should().Be(1_000);

            // ── Snapshot B: sees 0–1 999 only ────────────────────────────────
            snapB.TryGet(1_999, out _).Should().BeTrue("B must see last key in its range");
            snapB.TryGet(2_000, out _).Should().BeFalse("B must not see key added after it opened");
            snapB.Count.Should().Be(2_000);

            // ── Snapshot C: sees 0–2 999 only ────────────────────────────────
            snapC.TryGet(2_999, out _).Should().BeTrue("C must see last key in its range");
            snapC.TryGet(3_000, out _).Should().BeFalse("C must not see key added after it opened");
            snapC.Count.Should().Be(3_000);

            // ── Live tree sees all 4 000 ─────────────────────────────────────
            tree.TryGet(3_999, out _).Should().BeTrue("live tree must see all keys");
            tree.Count.Should().Be(4_000);
        });
    }
}
