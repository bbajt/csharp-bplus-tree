using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for the CoW transaction write path (Phase M+5).
/// Verifies that InsertInTransaction and TryDeleteInTransaction:
///   - accumulate shadow pages without touching the live tree until Commit(),
///   - install the new root atomically at Commit(),
///   - discard shadow pages on Dispose() (rollback) leaving the live tree unchanged.
/// Uses PageSize=8192 with 200 records so the tree fits in a single leaf (no splits/merges).
/// </summary>
public class CowTransactionTests : IDisposable
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

    // ── Test 1: CoW insert commit — new root installed, key visible, tree valid ──

    [Fact]
    public void Tx_CoW_Insert_Commit_TreeValid()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 200; i++) engine.Insert(i, i * 10);

        uint oldRoot = meta.RootPageId;

        using (var tx = engine.BeginTransaction())
        {
            tx.Insert(9999, 999);
            tx.Commit();
        }

        engine.TryGet(9999, out int v).Should().BeTrue("committed insert must be visible");
        v.Should().Be(999);
        meta.RootPageId.Should().NotBe(oldRoot,
            "CoW commit installs a new shadow root");
        new TreeValidator<int, int>(mgr, ns, meta).Validate().IsValid
            .Should().BeTrue("tree must be structurally valid after CoW insert commit");

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 2: CoW insert rollback — root unchanged, key not visible, tree valid ──

    [Fact]
    public void Tx_CoW_Insert_Rollback_NothingVisible()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 200; i++) engine.Insert(i, i * 10);

        uint oldRoot = meta.RootPageId;

        var tx = engine.BeginTransaction();
        tx.Insert(9999, 999);
        tx.Dispose(); // rollback — no Commit()

        engine.TryGet(9999, out _).Should().BeFalse("rolled-back insert must not be visible");
        meta.RootPageId.Should().Be(oldRoot, "rollback must leave live root unchanged");
        new TreeValidator<int, int>(mgr, ns, meta).Validate().IsValid
            .Should().BeTrue("tree must be structurally valid after CoW insert rollback");

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 3: CoW delete commit — key gone, new root installed, tree valid ──

    [Fact]
    public void Tx_CoW_Delete_Commit_KeyGone()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 200; i++) engine.Insert(i, i * 10);

        uint oldRoot = meta.RootPageId;

        using (var tx = engine.BeginTransaction())
        {
            tx.TryDelete(100).Should().BeTrue("key 100 must exist before delete");
            tx.Commit();
        }

        engine.TryGet(100, out _).Should().BeFalse("committed delete must remove the key");
        meta.RootPageId.Should().NotBe(oldRoot,
            "CoW commit installs a new shadow root");
        new TreeValidator<int, int>(mgr, ns, meta).Validate().IsValid
            .Should().BeTrue("tree must be structurally valid after CoW delete commit");

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }

    // ── Test 4: CoW delete rollback — key still present, root unchanged, tree valid ──

    [Fact]
    public void Tx_CoW_Delete_Rollback_KeyStillPresent()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();

        for (int i = 1; i <= 200; i++) engine.Insert(i, i * 10);

        uint oldRoot = meta.RootPageId;

        var tx = engine.BeginTransaction();
        tx.TryDelete(100);
        tx.Dispose(); // rollback — no Commit()

        engine.TryGet(100, out int v).Should().BeTrue("rolled-back delete must leave key present");
        v.Should().Be(1000, "value must be the original value after rollback");
        meta.RootPageId.Should().Be(oldRoot, "rollback must leave live root unchanged");
        new TreeValidator<int, int>(mgr, ns, meta).Validate().IsValid
            .Should().BeTrue("tree must be structurally valid after CoW delete rollback");

        engine.Close(); wal.Dispose(); mgr.Dispose();
    }
}
