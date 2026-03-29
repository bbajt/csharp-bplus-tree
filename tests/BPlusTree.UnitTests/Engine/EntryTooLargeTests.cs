using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Tests for the entry-size guard (Phase 83) and the isLeafFull fix for variable-length keys.
///
/// Properties verified:
///   1. Key exceeding MaxKeySize throws BPlusTreeEntryTooLargeException.
///   2. Key+value exceeding MaxEntrySize throws BPlusTreeEntryTooLargeException.
///   3. Entry at exactly MaxKeySize succeeds (boundary).
///   4. 500 string entries force multiple splits with no silent data loss (isLeafFull fix).
///   5. Same 500-entry split test via BeginTransaction.
///   6. Oversized entry in transaction throws BPlusTreeEntryTooLargeException.
/// </summary>
public class EntryTooLargeTests : IDisposable
{
    private const int PageSize = 4096;

    private readonly string _dbPath  = Path.Combine(Path.GetTempPath(), $"p83_{Guid.NewGuid():N}.db");
    private readonly string _walPath = Path.Combine(Path.GetTempPath(), $"p83_{Guid.NewGuid():N}.wal");

    private BPlusTree<string, string> OpenString() => BPlusTree<string, string>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = PageSize,
            BufferPoolCapacity  = 256,
            CheckpointThreshold = 8192,
        },
        StringSerializer.Instance, StringSerializer.Instance);

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
    }

    // ── Test 1: key too large ─────────────────────────────────────────────────

    [Fact]
    public void Key_TooLarge_Throws()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = OpenString();

        int maxKey = PageLayout.MaxKeySize(PageSize);  // 512 for pageSize=4096

        // Key that serializes to maxKey+1 bytes:
        // StringSerializer uses 2-byte prefix + UTF-8 bytes → need (maxKey+1-2) = maxKey-1 ASCII chars.
        string tooLongKey = new('x', maxKey - 1);  // 2 + (maxKey-1) = maxKey+1 bytes serialized

        var act = () => tree.Put(tooLongKey, "value");
        act.Should().Throw<BPlusTreeEntryTooLargeException>()
           .Which.ActualSize.Should().BeGreaterThan(maxKey);
    }

    // ── Test 2: value too large for inline → stored via overflow (Phase 99b) ───
    // Pre-Phase 99b: BPlusTreeEntryTooLargeException was thrown.
    // Post-Phase 99b: overflow chain is used transparently; Put succeeds.

    [Fact]
    public void Value_TooLargeForInline_StoresViaOverflow()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = OpenString();

        int maxEntry = PageLayout.MaxEntrySize(PageSize);

        // Short key (9 bytes serialized: 2 prefix + 7 chars) + value that pushes total > maxEntry.
        string shortKey  = "testkey";                               // 2 + 7 = 9 bytes
        string hugeValue = new('v', maxEntry - 9 - 2 + 1);        // 2 + len bytes; total = maxEntry+1

        // Must succeed — large value is stored via overflow chain.
        var act = () => tree.Put(shortKey, hugeValue);
        act.Should().NotThrow();

        tree.TryGet(shortKey, out var result).Should().BeTrue();
        result.Should().Be(hugeValue, "overflow round-trip must return the original large value");
    }

    // ── Test 3: key at exactly MaxKeySize succeeds ────────────────────────────

    [Fact]
    public void Entry_AtMaxKeySize_Succeeds()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = OpenString();

        int maxKey = PageLayout.MaxKeySize(PageSize);  // 512

        // Key serializing to exactly maxKey bytes: 2 prefix + (maxKey-2) chars.
        string exactKey = new('x', maxKey - 2);  // 2 + (maxKey-2) = maxKey bytes serialized

        var act = () => tree.Put(exactKey, "ok");
        act.Should().NotThrow();

        tree.TryGet(exactKey, out var v).Should().BeTrue();
        v.Should().Be("ok");
    }

    // ── Test 4: 500 string entries force splits, all retrievable ─────────────
    // This is the primary regression test for the isLeafFull fix.
    // Before the fix: entries near leaf-full boundaries are silently dropped.
    // After the fix: isLeafFull correctly triggers splits; all 500 entries survive.

    [Fact]
    public void IsLeafFull_VariableLength_SplitsCorrectly()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = OpenString();

        const int N = 500;
        var expected = new List<string>(N);

        for (int i = 0; i < N; i++)
        {
            string k = $"key-{i:D4}";   // 11 bytes serialized (2 + 9)
            string v = $"val-{i:D4}";   // 11 bytes serialized
            tree.Put(k, v);
            expected.Add(k);
        }

        expected.Sort(StringComparer.Ordinal);

        tree.Count.Should().Be(N, "all 500 entries must be present — no silent drops");

        // Scan must return all entries in ascending key order.
        var scanned = tree.Scan().Select(p => p.Key).ToList();
        scanned.Should().HaveCount(N);
        scanned.Should().Equal(expected);

        // Spot-check a few individual keys.
        tree.TryGet("key-0000", out var v0).Should().BeTrue(); v0.Should().Be("val-0000");
        tree.TryGet("key-0499", out var v1).Should().BeTrue(); v1.Should().Be("val-0499");
        tree.TryGet("key-0250", out var v2).Should().BeTrue(); v2.Should().Be("val-0250");
    }

    // ── Test 5: same split test via transaction ───────────────────────────────

    [Fact]
    public void IsLeafFull_Transaction_SplitsCorrectly()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = OpenString();

        const int N = 500;
        var expected = new List<string>(N);

        using (var scope = tree.BeginScope())
        {
            for (int i = 0; i < N; i++)
            {
                string k = $"tx-{i:D4}";
                string v = $"tv-{i:D4}";
                scope.Insert(k, v);
                expected.Add(k);
            }
            scope.Complete();
        }

        expected.Sort(StringComparer.Ordinal);

        tree.Count.Should().Be(N);
        var scanned = tree.Scan().Select(p => p.Key).ToList();
        scanned.Should().HaveCount(N);
        scanned.Should().Equal(expected);
    }

    // ── Test 6: oversized entry in transaction → stored via overflow (Phase 100a) ─
    // Pre-Phase 100a: BPlusTreeEntryTooLargeException was thrown.
    // Post-Phase 100a: overflow chain used transparently; Insert succeeds.

    [Fact]
    public void EntryTooLarge_Transaction_StoresViaOverflow()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        using var tree = OpenString();

        // Pre-populate so the transaction has a non-empty shadow tree.
        tree.Put("seed", "data");

        int maxEntry = PageLayout.MaxEntrySize(PageSize);
        string shortKey  = "k";
        string hugeValue = new('v', maxEntry);  // 2 + maxEntry bytes > maxEntry total

        using (var tx = tree.BeginTransaction())
        {
            var act = () => tx.Insert(shortKey, hugeValue);
            act.Should().NotThrow("large value in transaction must be stored via overflow");
            tx.Commit();
        }

        tree.TryGet(shortKey, out var result).Should().BeTrue();
        result.Should().Be(hugeValue, "overflow round-trip in transaction must return original value");
    }
}
