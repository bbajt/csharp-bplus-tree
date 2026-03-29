using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Tests for TryGetFirst / TryGetLast — O(log n) min/max accessors.
/// Uses the public BPlusTree&lt;TKey,TValue&gt; API (Phase 77).
///
/// Properties verified:
///   1. TryGetFirst on empty tree returns false.
///   2. TryGetLast on empty tree returns false.
///   3. Multi-key tree: TryGetFirst returns min, TryGetLast returns max; values correct.
///   4. tx.TryGetFirst sees own uncommitted insert (read-your-own-writes); rollback restores.
///   5. BeginSnapshot freezes the max; concurrent insert of larger key not visible.
/// </summary>
public class FirstLastTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 4096,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 4096,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
    }

    // ── Test 1: TryGetFirst on empty tree ──────────────────────────────────────

    [Fact]
    public void TryGetFirst_EmptyTree_ReturnsFalse()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        bool found = tree.TryGetFirst(out int key, out int value);

        found.Should().BeFalse("empty tree has no first entry");
        key.Should().Be(default(int));
        value.Should().Be(default(int));
    }

    // ── Test 2: TryGetLast on empty tree ───────────────────────────────────────

    [Fact]
    public void TryGetLast_EmptyTree_ReturnsFalse()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        bool found = tree.TryGetLast(out int key, out int value);

        found.Should().BeFalse("empty tree has no last entry");
        key.Should().Be(default(int));
        value.Should().Be(default(int));
    }

    // ── Test 3: multi-key tree — min and max ───────────────────────────────────

    [Fact]
    public void TryGetFirst_And_TryGetLast_ReturnMinMax()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        // Insert keys 5, 1, 10, 3, 7 out of order
        tree.Put(5, 50);
        tree.Put(1, 10);
        tree.Put(10, 100);
        tree.Put(3, 30);
        tree.Put(7, 70);

        tree.TryGetFirst(out int firstKey, out int firstValue).Should().BeTrue();
        firstKey.Should().Be(1, "min key is 1");
        firstValue.Should().Be(10, "value for key 1 is 10");

        tree.TryGetLast(out int lastKey, out int lastValue).Should().BeTrue();
        lastKey.Should().Be(10, "max key is 10");
        lastValue.Should().Be(100, "value for key 10 is 100");
    }

    // ── Test 4: transaction read-your-own-writes ───────────────────────────────

    [Fact]
    public void TryGetFirst_InTransaction_ReadYourOwnWrites()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        // Seed 10–20
        for (int i = 10; i <= 20; i++)
            tree.Put(i, i * 10);

        using var tx = tree.BeginTransaction();

        // Insert key smaller than current minimum
        tx.Insert(-1, -10);

        tx.TryGetFirst(out int key, out int value).Should().BeTrue();
        key.Should().Be(-1, "own uncommitted insert −1 must be the new minimum");
        value.Should().Be(-10);

        tx.TryGetLast(out int lastKey, out _).Should().BeTrue();
        lastKey.Should().Be(20, "max is still 20 — no larger key inserted");

        // Rollback — live tree must be unchanged
        tx.Dispose();

        tree.TryGetFirst(out int liveFirst, out _).Should().BeTrue();
        liveFirst.Should().Be(10, "rollback must restore minimum to 10");
    }

    // ── Test 5: snapshot frozen view ──────────────────────────────────────────

    [Fact]
    public void TryGetLast_InSnapshot_FrozenView()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = Open();

        // Seed 1–50
        for (int i = 1; i <= 50; i++)
            tree.Put(i, i);

        using var snap = tree.BeginSnapshot();

        // Concurrent insert of a larger key after snapshot is open
        tree.Put(9999, 9999);

        snap.TryGetLast(out int snapLast, out _).Should().BeTrue();
        snapLast.Should().Be(50, "snapshot was taken before key 9999 was inserted");

        snap.TryGetFirst(out int snapFirst, out _).Should().BeTrue();
        snapFirst.Should().Be(1, "snapshot minimum is unchanged");

        // Live tree reflects the new insert
        tree.TryGetLast(out int liveLast, out _).Should().BeTrue();
        liveLast.Should().Be(9999, "live tree has new maximum 9999");
    }
}
