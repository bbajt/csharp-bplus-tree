using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// M93 regression: <c>Splitter&lt;TKey,TValue&gt;.SplitLeafNode</c> previously threw
/// <see cref="ArgumentOutOfRangeException"/> when a full leaf contained exactly one
/// entry. The classic split formula <c>leftCount = (n + 1) / 2</c> leaves the right
/// leaf empty for <c>n == 1</c>, and the subsequent copy-up
/// <c>rightLeaf.GetKey(0)</c> walks off the end of an empty slot array.
/// <para>
/// This case is supposed to be unreachable under the MaxEntrySize invariant (entries
/// exceeding <c>(pageSize - header) / 2</c> get routed to overflow chains), but a
/// real production repro in the Bedrock System LSU reached the crash path regardless
/// (tracked in MILESTONE-93). The defensive fix in <c>SplitLeafNode</c> moves the
/// single entry to the right leaf so the copy-up invariant holds.
/// </para>
/// <para>
/// These tests exercise the defensive fix by simulating heavy insert patterns that,
/// with the M93 code path changes, give the split machinery a chance to hit the edge
/// case. Before the fix the first <c>Put</c> would throw AOORE somewhere in the run.
/// After the fix the whole sequence round-trips cleanly.
/// </para>
/// </summary>
public class SplitN1Tests : IDisposable
{
    private const int PageSize = 4096;

    private readonly string _dbPath  = Path.Combine(Path.GetTempPath(), $"m93_{Guid.NewGuid():N}.db");
    private readonly string _walPath = Path.Combine(Path.GetTempPath(), $"m93_{Guid.NewGuid():N}.wal");

    private BPlusTree<string, string> Open() => BPlusTree<string, string>.Open(
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

    /// <summary>
    /// Insert a sequence of large-but-inline values (each close to MaxEntrySize).
    /// Older code would throw ArgumentOutOfRangeException when a leaf holding a
    /// single such entry was split; with the M93 fix every Put succeeds and all
    /// keys round-trip correctly.
    /// </summary>
    [Fact]
    public void LargeInlineEntries_CascadingSplits_DoNotThrow()
    {
        using var tree = Open();

        int maxEntry = PageLayout.MaxEntrySize(PageSize); // 2018 for 4096
        // Key: 2-byte prefix + key chars. Value: 2-byte prefix + value chars.
        // Target total (key + value) just under maxEntry so it stays inline.
        // (key="K-NN" = 6 bytes serialized; value size = maxEntry - 6 - small slack.)
        const int keyWidth   = 4;   // "K-NN"
        const int keyBytes   = 2 + keyWidth;
        int       valueBytes = maxEntry - keyBytes - 4; // slack for varying serialization
        int       valueChars = valueBytes - 2;          // minus 2-byte prefix

        string MakeValue(int i) => $"V{i:D2}".PadRight(valueChars, 'x');

        const int N = 16;
        for (int i = 0; i < N; i++)
        {
            var act = () => tree.Put($"K-{i:D2}", MakeValue(i));
            act.Should().NotThrow($"round {i}: large-inline Put must not trigger the n=1 split AOORE");
        }

        tree.Count.Should().Be(N, "all inserted keys must be present — no silent drops from the split edge case");

        for (int i = 0; i < N; i++)
        {
            tree.TryGet($"K-{i:D2}", out var v).Should().BeTrue($"K-{i:D2} must round-trip");
            v.Should().Be(MakeValue(i));
        }
    }

    /// <summary>
    /// M93 Phase 3c regression: large inline entries inserted, then fully deleted,
    /// then re-inserted. Before the page-layer fix the second wave of inserts sees
    /// stale free-space accounting — <c>NodePage.RemoveSlot</c> does not reclaim the
    /// dead cell bytes, so <c>HasFreeSpace</c> reports <c>false</c> for an entry
    /// close to <c>MaxEntrySize</c> even though the leaf's <c>SlotCount</c> is 0.
    /// The caller's <c>isLeafFull</c> check then disagrees with <c>LeafNode.Count</c>,
    /// <c>Splitter.SplitLeafNode</c> runs on an empty leaf, and the defensive guard
    /// in <c>SplitLeafNode</c> (added in M93 Phase 3b) throws
    /// <see cref="InvalidOperationException"/>.
    /// </summary>
    [Fact]
    public void DeleteAllThenInsert_DoesNotThrow()
    {
        using var tree = Open();

        int maxEntry   = PageLayout.MaxEntrySize(PageSize);
        const int keyWidth   = 4;
        const int keyBytes   = 2 + keyWidth;
        int       valueBytes = maxEntry - keyBytes - 4;
        int       valueChars = valueBytes - 2;

        string MakeValue(int i) => $"V{i:D2}".PadRight(valueChars, 'x');

        const int N = 8;

        // Wave 1: fill
        for (int i = 0; i < N; i++)
            tree.Put($"K-{i:D2}", MakeValue(i));
        tree.Count.Should().Be(N);

        // Wave 2: remove everything
        for (int i = 0; i < N; i++)
            tree.Delete($"K-{i:D2}").Should().BeTrue($"Delete K-{i:D2} must succeed");
        tree.Count.Should().Be(0, "tree is logically empty after removing every key");

        // Wave 3: insert again — each new entry is close to page capacity, so the
        // first one to land on a page whose free-space accounting still reflects
        // the dead wave-1 cells will fail HasSpaceFor and trigger the split path
        // on an empty leaf. With the page-layer fix, the page reclaims its space
        // when SlotCount hits 0 (or defragments on demand) and inserts succeed.
        for (int i = 0; i < N; i++)
        {
            int i1 = i;
            var act = () => tree.Put($"K2-{i1:D2}", MakeValue(i1));
            act.Should().NotThrow($"wave-3 round {i}: re-insert must not split an empty leaf");
        }

        tree.Count.Should().Be(N);
        for (int i = 0; i < N; i++)
        {
            tree.TryGet($"K2-{i:D2}", out var v).Should().BeTrue($"K2-{i:D2} must round-trip after delete+insert cycle");
            v.Should().Be(MakeValue(i));
        }
    }

    /// <summary>
    /// M94-0' regression: partial-fragmentation case. Insert N entries filling a
    /// single leaf page, delete every other one, then insert a new entry that
    /// would fit after defragmentation but does not fit under the current
    /// <c>FreeSpaceSize</c> (which includes dead cell bytes from the deletes).
    /// <para>
    /// Pre-fix: <c>TreeEngine.Insert</c>'s <c>isLeafFull</c> check returns
    /// <c>true</c> because <c>PageRewriter.Defragment</c> is never called from
    /// the insert path. The tree pointlessly splits the root, growing tree
    /// height from 1 to 2 even though the incoming entry would have fit post-defrag.
    /// </para>
    /// <para>
    /// Post-fix: the insert path defragments the shadow leaf when
    /// <c>FragmentedBytes &gt; 0</c> and <c>!HasSpaceFor</c>, then rechecks.
    /// With the dead bytes reclaimed the entry fits and the in-place/direct
    /// shadow-insert branch is taken — no split, tree height stays at 1.
    /// </para>
    /// </summary>
    [Fact]
    public void PartialDeletePartialInsert_DoesNotSplitWhenDefragFits()
    {
        using var tree = Open();

        // Calibrate: target ~4 entries per page on a 4 KB page so pre-defrag
        // FreeSpaceSize after 4 live + 2 dead entries is insufficient for a
        // 5th, but post-defrag it fits.
        int maxEntry   = PageLayout.MaxEntrySize(PageSize); // 2018 @ 4096
        int target     = maxEntry / 2 - 40;                 // ~975 bytes per entry → 4/page
        const int keyWidth   = 4;
        const int keyBytes   = 2 + keyWidth;
        int       valueChars = target - keyBytes - 2;

        string MakeValue(int i) => $"V{i:D2}".PadRight(valueChars, 'y');

        const int N = 4;
        for (int i = 0; i < N; i++)
            tree.Put($"P-{i:D2}", MakeValue(i));

        uint heightAfterFill = tree.GetStatistics().TreeHeight;
        heightAfterFill.Should().Be(1u, "all N entries fit on the root leaf (single-leaf tree)");

        // Delete every other key — accumulates dead cell bytes but leaves the
        // page logically half-populated.
        tree.Delete("P-01").Should().BeTrue();
        tree.Delete("P-03").Should().BeTrue();
        tree.Count.Should().Be(N - 2);

        // Insert a new entry of the same large size. Pre-fix this triggers a
        // root split because HasFreeSpace reports false despite the 2*~975
        // bytes of reclaimable dead space. Post-fix the in-place defrag
        // reclaims those bytes and the entry lands on the same root leaf.
        tree.Put("P-NEW", MakeValue(99));
        tree.Count.Should().Be(N - 1);

        uint heightAfterReinsert = tree.GetStatistics().TreeHeight;
        heightAfterReinsert.Should().Be(1u,
            "with PageRewriter.Defragment wired into the insert path the tree must not split " +
            "when the incoming entry would fit after reclaiming fragmented cells");

        // All live keys round-trip.
        foreach (var key in new[] { "P-00", "P-02", "P-NEW" })
        {
            tree.TryGet(key, out var v).Should().BeTrue($"{key} must round-trip after partial-defrag insert");
            v.Should().NotBeNullOrEmpty();
        }
    }
}
