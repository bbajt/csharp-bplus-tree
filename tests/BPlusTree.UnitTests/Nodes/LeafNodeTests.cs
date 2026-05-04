using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Nodes;

public class LeafNodeTests
{
    private const int PageSize = 8192;

    private static LeafNode<int, int> NewLeaf()
    {
        var frame = new Frame(PageSize);
        frame.PageId = 1;
        var leaf = new LeafNode<int, int>(frame, Int32Serializer.Instance, Int32Serializer.Instance);
        leaf.Initialize();
        return leaf;
    }

    [Fact]
    public void NewLeaf_IsEmpty()
        => NewLeaf().IsEmpty.Should().BeTrue();

    [Fact]
    public void TryInsert_ThenTryGet_ReturnsValue()
    {
        var leaf = NewLeaf();
        leaf.TryInsert(42, 100);
        leaf.TryGet(42, out int val).Should().BeTrue();
        val.Should().Be(100);
    }

    [Fact]
    public void TryGet_MissingKey_ReturnsFalse()
        => NewLeaf().TryGet(99, out _).Should().BeFalse();

    [Fact]
    public void TryInsert_MultipleKeys_AllRetrievable()
    {
        var leaf = NewLeaf();
        for (int i = 0; i < 10; i++) leaf.TryInsert(i, i * 10);
        for (int i = 0; i < 10; i++)
        {
            leaf.TryGet(i, out int v).Should().BeTrue();
            v.Should().Be(i * 10);
        }
    }

    [Fact]
    public void TryInsert_UpdatesExistingKey()
    {
        var leaf = NewLeaf();
        leaf.TryInsert(5, 50);
        leaf.TryInsert(5, 99); // overwrite
        leaf.TryGet(5, out int v);
        v.Should().Be(99);
    }

    [Fact]
    public void Remove_ExistingKey_ReturnsTrue()
    {
        var leaf = NewLeaf();
        leaf.TryInsert(7, 70);
        leaf.Remove(7).Should().BeTrue();
        leaf.TryGet(7, out _).Should().BeFalse();
    }

    [Fact]
    public void Remove_MissingKey_ReturnsFalse()
        => NewLeaf().Remove(99).Should().BeFalse();

    [Fact]
    public void GetKey_ReturnsKeyAtSlot()
    {
        var leaf = NewLeaf();
        leaf.TryInsert(10, 1);
        leaf.TryInsert(20, 2);
        leaf.GetKey(0).Should().Be(10);
        leaf.GetKey(1).Should().Be(20);
    }

    [Fact]
    public void Keys_StoredInSortedOrder()
    {
        var leaf = NewLeaf();
        leaf.TryInsert(30, 3);
        leaf.TryInsert(10, 1);
        leaf.TryInsert(20, 2);
        leaf.GetKey(0).Should().Be(10);
        leaf.GetKey(1).Should().Be(20);
        leaf.GetKey(2).Should().Be(30);
    }

    [Fact]
    public void SiblingPointers_RoundTrip()
    {
        var leaf = NewLeaf();
        leaf.NextLeafPageId = 55u;
        leaf.PrevLeafPageId = 33u;
        leaf.NextLeafPageId.Should().Be(55u);
        leaf.PrevLeafPageId.Should().Be(33u);
    }

    [Fact]
    public void TryInsert_ReturnsFalse_WhenPageFull()
    {
        var leaf = NewLeaf();
        // Fill until insert fails (keys: 4 bytes, values: 4 bytes, slot: 4 bytes = 12 per entry)
        // 8192 - 48 header = 8144 available / 12 per entry ≈ 678 entries
        bool inserted = true;
        int key = 0;
        while (inserted) inserted = leaf.TryInsert(key++, key);
        inserted.Should().BeFalse();
    }
}
