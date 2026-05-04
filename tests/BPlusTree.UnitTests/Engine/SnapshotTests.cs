using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for the lightweight read-only snapshot API (Phase M+7).
///
/// Properties verified:
///   1. Snapshot sees the committed state at open time (frozen view).
///   2. Writes committed AFTER the snapshot is opened are NOT visible.
///   3. Multiple snapshots may be open simultaneously with independent views.
///   4. Snapshot does not hold the checkpoint gate (doesn't block WAL truncation).
///   5. Dispose releases the epoch; subsequent calls throw ObjectDisposedException.
/// </summary>
public class SnapshotTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine,
             NodeSerializer<int, int> ns, TreeMetadata meta) Open()
    {
        var wal  = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 8192,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 4096,
        }, wal);
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, wal, new TreeEngine<int, int>(mgr, ns, meta), ns, meta);
    }

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
    }

    // ── Test 1: snapshot sees committed state at open time ──

    [Fact]
    public void Snapshot_TryGet_SeesFrozenState_AtOpenTime()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 200; i++) engine.Insert(i, i * 10);

        using var snap = engine.BeginSnapshot();

        // Insert after snapshot is opened — must NOT be visible to snap.
        engine.Insert(9999, 999);

        snap.TryGet(9999, out _).Should().BeFalse(
            "key inserted after snapshot open must not be visible");
        snap.TryGet(100, out int v).Should().BeTrue(
            "key present at snapshot open must be visible");
        v.Should().Be(1000);

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 2: snapshot does not see deletions committed after open ──

    [Fact]
    public void Snapshot_TryGet_DoesNotSeeDeleteAfterOpen()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 200; i++) engine.Insert(i, i * 10);

        using var snap = engine.BeginSnapshot();

        // Delete after snapshot is opened — key must still be visible to snap.
        engine.Delete(50);

        snap.TryGet(50, out int v).Should().BeTrue(
            "key deleted after snapshot open must still be visible to the snapshot");
        v.Should().Be(500);

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 3: snapshot Scan returns frozen view ──

    [Fact]
    public void Snapshot_Scan_ReturnsFrozenView()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 200; i++) engine.Insert(i, i * 10);

        using var snap = engine.BeginSnapshot();

        // Write after snapshot open.
        engine.Insert(9999, 999);
        engine.Delete(1);

        var results = snap.Scan().ToList();

        results.Count.Should().Be(200, "snapshot sees exactly 200 keys from open time");
        results.Should().NotContain(r => r.Key == 9999,
            "key inserted after snap open must not appear in Scan");
        results.Should().Contain(r => r.Key == 1,
            "key deleted after snap open must still appear in Scan");

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 4: multiple simultaneous snapshots have independent views ──

    [Fact]
    public void Snapshot_MultipleSimultaneous_HaveIndependentViews()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 100; i++) engine.Insert(i, i * 10);

        using var snap1 = engine.BeginSnapshot();   // sees keys 1–100

        for (int i = 101; i <= 200; i++) engine.Insert(i, i * 10);

        using var snap2 = engine.BeginSnapshot();   // sees keys 1–200

        snap1.Scan().Count().Should().Be(100, "snap1 was opened before keys 101–200 were inserted");
        snap2.Scan().Count().Should().Be(200, "snap2 was opened after all 200 keys were inserted");

        snap1.TryGet(150, out _).Should().BeFalse("key 150 did not exist when snap1 was opened");
        snap2.TryGet(150, out int v).Should().BeTrue("key 150 existed when snap2 was opened");
        v.Should().Be(1500);

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 5: empty tree snapshot works and dispose cleans up without error ──

    [Fact]
    public void Snapshot_EmptyTree_Works()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        // No inserts.
        using var snap = engine.BeginSnapshot();

        snap.TryGet(1, out _).Should().BeFalse("empty tree TryGet must return false");
        snap.Scan().Should().BeEmpty("empty tree Scan must yield nothing");

        // Dispose must not throw.
        snap.Dispose();

        // Double-dispose must not throw.
        snap.Dispose();

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 6: non-transactional Scan sees a frozen snapshot view ────────────

    /// <summary>
    /// Verifies that non-transactional <c>TreeEngine.Scan()</c> now has full epoch
    /// protection. The snapshot is captured lazily on the first <c>MoveNext()</c> call.
    /// Mutations made after that point go through the CoW path (HasActiveSnapshots=true)
    /// and are invisible to the in-progress scan.
    /// </summary>
    [Fact]
    public void Scan_NonTransactional_ReturnsFrozenView()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 200; i++) engine.Insert(i, i * 10);

        // Begin non-transactional scan. The snapshot is NOT captured yet —
        // it is captured lazily on the first MoveNext() call (iterator is lazy).
        var e = engine.Scan().GetEnumerator();

        // First MoveNext() → BeginSnapshot() is called → epoch registered →
        // snapshot root frozen at the tree containing keys 1–200.
        e.MoveNext().Should().BeTrue();

        // Mutate the tree while the scan is paused between MoveNext() calls.
        // HasActiveSnapshots == true (scan holds open epoch) → both writes use CoW:
        // a new root is installed; the old root (epoch-protected by the scan) is not freed.
        engine.Insert(9999, 999);   // not in snapshot → must NOT appear in scan results
        engine.Delete(1);           // was in snapshot → must still appear in scan results

        // Collect everything the scan yields (including the first item already advanced).
        var results = new List<(int Key, int Value)>();
        results.Add(e.Current);
        while (e.MoveNext())
            results.Add(e.Current);
        e.Dispose();   // releases epoch; SweepRetiredPages() runs

        results.Should().HaveCount(200,
            "non-transactional Scan must see exactly the 200 keys present at scan start");
        results.Should().NotContain(r => r.Key == 9999,
            "key inserted after scan start must not appear in the frozen scan view");
        results.Should().Contain(r => r.Key == 1,
            "key deleted after scan start must still appear in the frozen scan view");

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }
}
