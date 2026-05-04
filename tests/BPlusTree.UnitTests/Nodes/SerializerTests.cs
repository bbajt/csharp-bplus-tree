using System;
using Xunit;
using FluentAssertions;
using ByTech.BPlusTree.Core.Nodes;

namespace ByTech.BPlusTree.Core.Tests.Nodes;

public class SerializerTests
{
    // ── Int32 ─────────────────────────────────────────────────────────────────
    [Theory]
    [InlineData(0), InlineData(1), InlineData(-1), InlineData(int.MaxValue), InlineData(int.MinValue)]
    public void Int32_RoundTrip(int value)
    {
        var buf = new byte[4];
        Int32Serializer.Instance.Serialize(value, buf);
        Int32Serializer.Instance.Deserialize(buf).Should().Be(value);
    }

    [Fact]
    public void Int32_PreservesSortOrder_PositiveIntegers()
    {
        var buf1 = new byte[4]; Int32Serializer.Instance.Serialize(10, buf1);
        var buf2 = new byte[4]; Int32Serializer.Instance.Serialize(20, buf2);
        buf1.AsSpan().SequenceCompareTo(buf2.AsSpan()).Should().BeNegative();
    }

    [Fact]
    public void Int32_PreservesSortOrder_NegativeBeforePositive()
    {
        var buf1 = new byte[4]; Int32Serializer.Instance.Serialize(-1, buf1);
        var buf2 = new byte[4]; Int32Serializer.Instance.Serialize(0,  buf2);
        buf1.AsSpan().SequenceCompareTo(buf2.AsSpan()).Should().BeNegative();
    }

    [Fact]
    public void Int32_StoredBigEndian()
    {
        var buf = new byte[4];
        // Sign-bit flip: 0x01020304 ^ 0x80000000 = 0x81020304 — MSB comes first (big-endian).
        Int32Serializer.Instance.Serialize(0x01020304, buf);
        buf[0].Should().Be(0x81); // high byte of 0x81020304 (sign bit flipped)
        buf[3].Should().Be(0x04);
    }

    // ── Int64 ─────────────────────────────────────────────────────────────────
    [Theory]
    [InlineData(0L), InlineData(long.MaxValue), InlineData(long.MinValue)]
    public void Int64_RoundTrip(long value)
    {
        var buf = new byte[8];
        Int64Serializer.Instance.Serialize(value, buf);
        Int64Serializer.Instance.Deserialize(buf).Should().Be(value);
    }

    // ── Guid ──────────────────────────────────────────────────────────────────
    [Fact]
    public void Guid_RoundTrip()
    {
        var g = Guid.NewGuid();
        var buf = new byte[16];
        GuidSerializer.Instance.Serialize(g, buf);
        GuidSerializer.Instance.Deserialize(buf).Should().Be(g);
    }

    // ── String ────────────────────────────────────────────────────────────────
    [Theory]
    [InlineData(""), InlineData("hello"), InlineData("BPlusTree 🌳")]
    public void String_RoundTrip(string value)
    {
        var buf = new byte[512];
        int written = StringSerializer.Instance.Serialize(value, buf);
        StringSerializer.Instance.Deserialize(buf[..written]).Should().Be(value);
    }

    [Fact]
    public void String_PreservesSortOrder()
    {
        var buf1 = new byte[64]; int w1 = StringSerializer.Instance.Serialize("apple", buf1);
        var buf2 = new byte[64]; int w2 = StringSerializer.Instance.Serialize("banana", buf2);
        buf1[..w1].AsSpan().SequenceCompareTo(buf2[..w2].AsSpan()).Should().BeNegative();
    }

    // ── ByteArray ─────────────────────────────────────────────────────────────
    [Fact]
    public void ByteArray_RoundTrip()
    {
        var data = new byte[] { 1, 2, 3, 255 };
        var buf = new byte[64];
        int w = ByteArraySerializer.Instance.Serialize(data, buf);
        ByteArraySerializer.Instance.Deserialize(buf[..w]).Should().Equal(data);
    }
}
