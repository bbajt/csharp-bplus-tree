using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for Phase 17b: internal node borrow and merge.
/// Requires height-3 trees so that internal nodes actually underflow.
/// Use large key counts to force multi-level trees.
/// </summary>
public class MergerInternalTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, TreeEngine<int, int> engine, TreeMetadata meta) Open()
    {
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = PageSize, BufferPoolCapacity = 256, CheckpointThreshold = 128,
        });
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, new TreeEngine<int, int>(mgr, ns, meta), meta);
    }

    // ── Borrow from right internal ────────────────────────────────────────────

    [Fact]
    public void BorrowFromRightInternal_SeparatorRotatesCorrectly()
    {
        // Insert enough to create height-3 tree.
        // Delete from left subtree until its internal node underflows.
        // Verify: all remaining keys still retrievable.
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 5000; i++) engine.Insert(i, i);
        for (int i = 0; i < 3000; i++) engine.Delete(i);
        for (int i = 3000; i < 5000; i++)
            engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after internal right borrow");
        mgr.Dispose();
    }

    [Fact]
    public void BorrowFromRightInternal_AllChildPointersValid()
    {
        // After borrow: every child pointer in both internal nodes must reference a real page.
        // Scan the tree — any bad pointer causes an exception or a wrong key.
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 5000; i++) engine.Insert(i, i);
        for (int i = 0; i < 2500; i++) engine.Delete(i);
        // Full scan to exercise all child pointers
        engine.Scan().Select(x => x.Key).Should().BeInAscendingOrder();
        mgr.Dispose();
    }

    // ── Borrow from left internal ─────────────────────────────────────────────

    [Fact]
    public void BorrowFromLeftInternal_SeparatorRotatesCorrectly()
    {
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 5000; i++) engine.Insert(i, i);
        for (int i = 2500; i < 5000; i++) engine.Delete(i);
        for (int i = 0; i < 2500; i++)
            engine.TryGet(i, out _).Should().BeTrue($"key {i} missing after internal left borrow");
        mgr.Dispose();
    }

    // ── Internal merge ────────────────────────────────────────────────────────

    [Fact]
    public void MergeInternals_PullsSeparatorFromParent()
    {
        // After merge: the separator that divided the two internal nodes must be
        // inside the surviving internal node (not in the parent).
        // Verify: full scan still returns all keys in sorted order.
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 5000; i++) engine.Insert(i, i);
        for (int i = 0; i < 4500; i++) engine.Delete(i);
        var keys = engine.Scan().Select(x => x.Key).ToList();
        keys.Should().BeInAscendingOrder();
        keys.Should().HaveCount(500);
        mgr.Dispose();
    }

    [Fact]
    public void MergeInternals_ChildPointersAllPresent()
    {
        // After internal merge, no child pointer must be lost.
        // Every key that was reachable before the merge must still be reachable.
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 5000; i++) engine.Insert(i, i);
        for (int i = 1000; i < 4000; i++) engine.Delete(i);
        for (int i = 0;    i < 1000; i++) engine.TryGet(i, out _).Should().BeTrue();
        for (int i = 4000; i < 5000; i++) engine.TryGet(i, out _).Should().BeTrue();
        mgr.Dispose();
    }

    [Fact]
    public void MergeInternals_RecursiveRebalance_PropagatesUpward()
    {
        // An internal merge may cause the grandparent to underflow.
        // Chain of merges must propagate without stopping mid-tree.
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 10_000; i++) engine.Insert(i, i);
        for (int i = 0; i < 9_500; i++) engine.Delete(i);
        var keys = engine.Scan().Select(x => x.Key).ToList();
        keys.Should().HaveCount(500);
        keys.Should().BeInAscendingOrder();
        mgr.Dispose();
    }

    [Fact]
    public void TreeValidator_PassesAfterInternalMerge()
    {
        var (mgr, engine, _) = Open();
        for (int i = 0; i < 5000; i++) engine.Insert(i, i);
        for (int i = 0; i < 4000; i++) engine.Delete(i);
        // Full structural validation
        var ns        = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta      = new TreeMetadata(mgr);
        meta.Load();
        var validator = new TreeValidator<int, int>(mgr, ns, meta);
        validator.Validate().IsValid.Should().BeTrue();
        mgr.Dispose();
    }

    public void Dispose() { try { File.Delete(_dbPath); File.Delete(_walPath); } catch { } }
}
