using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for SSI Phase 1: read-set tracking in transactional reads (Phase 88).
///
/// Properties verified:
///   1. TryGet (key present) → ReadSet contains the leaf page that was visited.
///   2. TryGet (key absent)  → ReadSet contains the leaf page that was visited (miss read recorded).
///   3. TryGet on empty tree → ReadSet is empty (NullPageId early-exit; no leaf visited).
///   4. Scan over multiple leaves → ReadSet.Count matches number of distinct leaves traversed.
///   5. CountRange over multiple leaves → ReadSet.Count > 0.
/// </summary>
public class ReadSetTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath       = _dbPath,
            WalFilePath        = _walPath,
            PageSize           = 4096,
            BufferPoolCapacity = 64,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { File.Delete(_dbPath); }  catch (IOException) { }
        try { File.Delete(_walPath); } catch (IOException) { }
    }

    // ── Test 1: TryGet found → ReadSet has 1 leaf ─────────────────────────────

    [Fact]
    public void ReadSet_TryGet_Found_RecordsOneLeafPage()
    {
        using var tree = Open();
        tree.Put(42, 100);

        using var tx = (Transaction<int, int>)tree.BeginTransaction();
        bool found = tx.TryGet(42, out _);

        found.Should().BeTrue();
        tx.ReadSet.Count.Should().Be(1, "one leaf was visited");
    }

    // ── Test 2: TryGet miss → ReadSet still has 1 leaf (miss reads recorded) ──

    [Fact]
    public void ReadSet_TryGet_NotFound_RecordsOneLeafPage()
    {
        using var tree = Open();
        tree.Put(1, 10);   // ensure tree has a root/leaf

        using var tx = (Transaction<int, int>)tree.BeginTransaction();
        bool found = tx.TryGet(999, out _);

        found.Should().BeFalse();
        tx.ReadSet.Count.Should().Be(1, "the leaf was still visited even though the key is absent");
    }

    // ── Test 3: TryGet on empty tree → ReadSet is empty ──────────────────────

    [Fact]
    public void ReadSet_TryGet_EmptyTree_RecordsNothing()
    {
        using var tree = Open();

        using var tx = (Transaction<int, int>)tree.BeginTransaction();
        bool found = tx.TryGet(1, out _);

        found.Should().BeFalse();
        tx.ReadSet.Count.Should().Be(0, "empty tree returns early before visiting any leaf");
    }

    // ── Test 4: Scan over multiple leaves → ReadSet covers all leaf pages ─────

    [Fact]
    public void ReadSet_Scan_MultipleLeaves_RecordsAllLeaves()
    {
        using var tree = Open();

        // Insert enough keys to fill several leaves (4096-byte pages, ~50 int-int entries per leaf)
        const int keyCount = 500;
        for (int i = 0; i < keyCount; i++)
            tree.Put(i, i);

        using var tx = (Transaction<int, int>)tree.BeginTransaction();
        var results = tx.Scan().ToList();

        results.Count.Should().Be(keyCount);
        tx.ReadSet.Count.Should().BeGreaterThan(1,
            "500 entries span multiple leaf pages; scan must record each one");
    }

    // ── Test 5: CountRange over multiple leaves → ReadSet has leaf page IDs ───

    [Fact]
    public void ReadSet_CountRange_RecordsVisitedLeaves()
    {
        using var tree = Open();

        const int keyCount = 500;
        for (int i = 0; i < keyCount; i++)
            tree.Put(i, i);

        using var tx = (Transaction<int, int>)tree.BeginTransaction();
        long count = tx.CountRange(0, keyCount - 1);

        count.Should().Be(keyCount);
        tx.ReadSet.Count.Should().BeGreaterThan(0,
            "CountRange traverses leaf pages and must record them in the read-set");
    }
}
