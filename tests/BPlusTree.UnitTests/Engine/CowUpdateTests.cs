using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for Update() CoW write path added in M+4a.
/// </summary>
public class CowUpdateTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine, TreeMetadata meta) Open()
    {
        if (File.Exists(_dbPath))  File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);

        var wal  = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 4096,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 4096,
        }, wal);
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, wal, new TreeEngine<int, int>(mgr, ns, meta), meta);
    }

    public void Dispose()
    {
        static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch (IOException) { }
        }
        TryDelete(_dbPath);
        TryDelete(_walPath);
    }

    // ── Test 1: Value updated correctly ───────────────────────────────────────

    [Fact]
    public void Update_CoW_ValueUpdated()
    {
        var (mgr, wal, engine, meta) = Open();

        for (int i = 1; i <= 200; i++)
            engine.Insert(i, i * 10);

        bool updated = engine.Update(100, v => v + 999);

        updated.Should().BeTrue();
        engine.TryGet(100, out int v100).Should().BeTrue();
        v100.Should().Be(100 * 10 + 999);

        // Spot-check other keys unchanged.
        engine.TryGet(1, out int v1);     v1.Should().Be(10);
        engine.TryGet(50, out int v50);   v50.Should().Be(500);
        engine.TryGet(150, out int v150); v150.Should().Be(1500);
        engine.TryGet(200, out int v200); v200.Should().Be(2000);

        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 2: Shadow root installed after Update ─────────────────────────────

    [Fact]
    public void Update_CoW_RootChanged()
    {
        var (mgr, wal, engine, meta) = Open();

        for (int i = 1; i <= 200; i++)
            engine.Insert(i, i * 10);

        uint oldRoot = meta.RootPageId;

        // Open a snapshot to force the CoW path (in-place is skipped when a snapshot is open).
        using var snapshot = engine.BeginSnapshot();
        engine.Update(100, v => v + 1);

        meta.RootPageId.Should().NotBe(oldRoot, "a new shadow root must be installed by CoW Update");

        var validator = new TreeValidator<int, int>(mgr,
            new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance),
            meta);
        validator.Validate().IsValid.Should().BeTrue("tree must be structurally valid after CoW Update");

        wal.Dispose();
        mgr.Dispose();
    }

    // ── Test 3: Key not found returns false ────────────────────────────────────

    [Fact]
    public void Update_CoW_KeyNotFound_ReturnsFalse()
    {
        var (mgr, wal, engine, meta) = Open();

        for (int i = 1; i <= 100; i++)
            engine.Insert(i, i * 10);

        uint oldRoot = meta.RootPageId;

        bool updated = engine.Update(9999, v => v + 1);

        updated.Should().BeFalse();
        meta.RootPageId.Should().Be(oldRoot, "root must be unchanged when key not found");

        for (int i = 1; i <= 100; i++)
        {
            engine.TryGet(i, out int v).Should().BeTrue();
            v.Should().Be(i * 10);
        }

        wal.Dispose();
        mgr.Dispose();
    }
}
