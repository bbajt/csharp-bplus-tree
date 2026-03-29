using Xunit;
using FluentAssertions;
using BPlusTree.Core.Wal;

namespace BPlusTree.UnitTests.Wal;

public class LogSequenceNumberTests
{
    [Fact] public void None_HasValue0()          => LogSequenceNumber.None.Value.Should().Be(0UL);
    [Fact] public void None_IsNotValid()         => LogSequenceNumber.None.IsValid.Should().BeFalse();
    [Fact] public void NonZero_IsValid()         => new LogSequenceNumber(1).IsValid.Should().BeTrue();

    [Fact] public void Equality_SameValue_IsTrue()
        => new LogSequenceNumber(42).Should().Be(new LogSequenceNumber(42));

    [Fact] public void Equality_DifferentValue_IsFalse()
        => new LogSequenceNumber(1).Should().NotBe(new LogSequenceNumber(2));

    [Fact] public void LessThan_ReturnsCorrectOrder()
        => (new LogSequenceNumber(5) < new LogSequenceNumber(10)).Should().BeTrue();

    [Fact] public void GreaterThan_ReturnsCorrectOrder()
        => (new LogSequenceNumber(10) > new LogSequenceNumber(5)).Should().BeTrue();

    [Fact] public void CompareTo_LowerValue_ReturnsNegative()
        => new LogSequenceNumber(1).CompareTo(new LogSequenceNumber(2)).Should().BeNegative();

    [Fact] public void CompareTo_Equal_ReturnsZero()
        => new LogSequenceNumber(7).CompareTo(new LogSequenceNumber(7)).Should().Be(0);

    [Fact] public void ToString_ContainsValue()
        => new LogSequenceNumber(99).ToString().Should().Contain("99");

    [Fact] public void GetHashCode_EqualLSNs_SameHashCode()
        => new LogSequenceNumber(5).GetHashCode().Should().Be(new LogSequenceNumber(5).GetHashCode());
}