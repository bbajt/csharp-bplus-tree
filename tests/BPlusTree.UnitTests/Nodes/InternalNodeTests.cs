using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Nodes;

public class InternalNodeTests
{
    private const int PageSize = 8192;

    private static InternalNode<int> NewInternal(uint leftmost = 10)
    {
        var frame = new Frame(PageSize) { PageId = 99 };
        var node = new InternalNode<int>(frame, Int32Serializer.Instance);
        node.Initialize(leftmost);
        return node;
    }

    [Fact]
    public void Initialize_KeyCount_IsZero()
        => NewInternal().KeyCount.Should().Be(0);

    [Fact]
    public void Initialize_LeftmostChildId_IsSet()
        => NewInternal(leftmost: 42).LeftmostChildId.Should().Be(42u);

    [Fact]
    public void TryAppend_IncreasesKeyCount()
    {
        var n = NewInternal();
        n.TryAppend(100, 11);
        n.KeyCount.Should().Be(1);
    }

    [Fact]
    public void GetKey_AfterAppend_ReturnsCorrectKey()
    {
        var n = NewInternal(leftmost: 10);
        n.TryAppend(100, 20);
        n.GetKey(0).Should().Be(100);
    }

    [Fact]
    public void GetChildId_Index0_IsRightChildOfFirstSeparator()
    {
        var n = NewInternal(leftmost: 10);
        n.TryAppend(100, 20);
        n.GetChildId(0).Should().Be(20u);
    }

    [Fact]
    public void FindChildId_BelowFirstKey_ReturnsLeftmost()
    {
        var n = NewInternal(leftmost: 10);
        n.TryAppend(100, 20);
        n.TryAppend(200, 30);
        n.FindChildId(50).Should().Be(10u);
    }

    [Fact]
    public void FindChildId_AboveLastKey_ReturnsLastChild()
    {
        var n = NewInternal(leftmost: 10);
        n.TryAppend(100, 20);
        n.TryAppend(200, 30);
        n.FindChildId(250).Should().Be(30u);
    }

    [Fact]
    public void FindChildId_EqualToKey_ReturnsRightChild()
    {
        var n = NewInternal(leftmost: 10);
        n.TryAppend(100, 20);
        n.FindChildId(100).Should().Be(20u);
    }

    [Fact]
    public void TryInsertSeparator_MaintainsSortedOrder()
    {
        var n = NewInternal(leftmost: 10);
        n.TryAppend(300, 40);
        n.TryInsertSeparator(100, 20);  // insert before 300
        n.TryInsertSeparator(200, 30);  // insert between 100 and 300
        n.GetKey(0).Should().Be(100);
        n.GetKey(1).Should().Be(200);
        n.GetKey(2).Should().Be(300);
    }

    [Fact]
    public void RemoveSeparator_ShiftsRemainingKeys()
    {
        var n = NewInternal(leftmost: 10);
        n.TryAppend(100, 20);
        n.TryAppend(200, 30);
        n.RemoveSeparator(0);
        n.KeyCount.Should().Be(1);
        n.GetKey(0).Should().Be(200);
    }
}
