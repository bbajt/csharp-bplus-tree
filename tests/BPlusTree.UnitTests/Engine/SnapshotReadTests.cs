using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for snapshot-aware TryGet and Scan on ITransaction (Phase M+6).
///
/// Properties verified:
///   1. Read-your-own-writes: in-transaction inserts/deletes are visible via tx.TryGet / tx.Scan.
///   2. Snapshot isolation: uncommitted writes from another transaction are NOT visible.
/// </summary>
public class SnapshotReadTests : IDisposable
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

    // ── Test 1: read-your-own-writes — TryGet sees in-transaction insert ──

    [Fact]
    public void TxRead_TryGet_SeesSelf_Insert()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 200; i++) engine.Insert(i, i * 10);

        using var tx = engine.BeginTransaction();
        tx.Insert(9999, 777);

        tx.TryGet(9999, out int v).Should().BeTrue("in-transaction insert must be visible via tx.TryGet");
        v.Should().Be(777);

        // Pre-seeded key must also be readable
        tx.TryGet(100, out int v2).Should().BeTrue("pre-seeded key must be visible via tx.TryGet");
        v2.Should().Be(1000);

        tx.Commit();
        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 2: read-your-own-writes — TryGet does not see deleted key ──

    [Fact]
    public void TxRead_TryGet_SeesSelf_Delete()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 200; i++) engine.Insert(i, i * 10);

        using var tx = engine.BeginTransaction();
        tx.TryDelete(100).Should().BeTrue();

        tx.TryGet(100, out _).Should().BeFalse("deleted key must not be visible within transaction");
        tx.TryGet(101, out int v).Should().BeTrue("adjacent key must still be visible");
        v.Should().Be(1010);

        tx.Commit();
        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 3: snapshot isolation — tx1 does not see tx2's uncommitted insert ──

    [Fact]
    public void TxRead_TryGet_SnapshotIsolation()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 200; i++) engine.Insert(i, i * 10);

        // Two transactions open on the same thread (per-op lock model allows this).
        var tx1 = engine.BeginTransaction();
        var tx2 = engine.BeginTransaction();

        // tx2 inserts a key but does NOT commit.
        tx2.Insert(9999, 777);

        // tx1's snapshot predates tx2's shadow tree — must not see the key.
        tx1.TryGet(9999, out _).Should().BeFalse(
            "tx1 must not see tx2's uncommitted insert (snapshot isolation)");

        tx2.Dispose(); // rollback
        tx1.Dispose();

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 4: read-your-own-writes — Scan sees in-transaction insert ──

    [Fact]
    public void TxRead_Scan_SeesSelf_Insert()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 200; i++) engine.Insert(i, i * 10);

        using var tx = engine.BeginTransaction();
        tx.Insert(9999, 777);

        var results = tx.Scan().ToList();

        results.Should().Contain((9999, 777), "in-transaction insert must appear in tx.Scan");
        results.Should().Contain((100, 1000), "pre-seeded key must appear in tx.Scan");
        results.Count.Should().Be(201, "201 keys total (200 pre-seeded + 1 inserted in tx)");

        tx.Commit();
        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 5: read-your-own-writes — Scan does not see deleted key ──

    [Fact]
    public void TxRead_Scan_SeesSelf_Delete()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 200; i++) engine.Insert(i, i * 10);

        using var tx = engine.BeginTransaction();
        tx.TryDelete(50).Should().BeTrue();

        var results = tx.Scan().ToList();

        results.Should().NotContain(r => r.Key == 50, "deleted key must not appear in tx.Scan");
        results.Count.Should().Be(199, "199 keys remain after in-transaction delete");

        tx.Commit();
        engine.Close(); wal.Dispose(); mgr.Dispose();
    }
}
