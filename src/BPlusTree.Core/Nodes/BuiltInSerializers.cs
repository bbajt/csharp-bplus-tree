using BPlusTree.Core.Api;
using System.Buffers.Binary;
using System.Text;

namespace BPlusTree.Core.Nodes;

/// <summary>Int32 big-endian key serializer. Sort order: natural integer order.</summary>
/// <remarks>
/// Sign bit is flipped before writing so that two's-complement negative values
/// sort before positive values under unsigned lexicographic byte comparison.
/// </remarks>
public sealed class Int32Serializer : IKeySerializer<int>, IValueSerializer<int>
{
    public static readonly Int32Serializer Instance = new();
    public int FixedSize => 4;

    /// <inheritdoc />
    public int Serialize(int key, Span<byte> dst)
    {
        // Flip sign bit so negative integers sort before positive under byte comparison.
        BinaryPrimitives.WriteUInt32BigEndian(dst, unchecked((uint)key) ^ 0x80000000u);
        return 4;
    }

    /// <inheritdoc />
    public int Deserialize(ReadOnlySpan<byte> src)
    {
        uint sortable = BinaryPrimitives.ReadUInt32BigEndian(src);
        return unchecked((int)(sortable ^ 0x80000000u));
    }

    /// <inheritdoc />
    public int Compare(int x, int y) => x.CompareTo(y);
}

/// <summary>Int64 big-endian key serializer.</summary>
public sealed class Int64Serializer : IKeySerializer<long>, IValueSerializer<long>
{
    public static readonly Int64Serializer Instance = new();
    public int  FixedSize => 8;

    /// <inheritdoc />
    public int Serialize(long key, Span<byte> dst)
    {
        // Flip sign bit so negative longs sort before positive under byte comparison.
        BinaryPrimitives.WriteUInt64BigEndian(dst, unchecked((ulong)key) ^ 0x8000000000000000UL);
        return 8;
    }

    /// <inheritdoc />
    public long Deserialize(ReadOnlySpan<byte> src)
    {
        ulong sortable = BinaryPrimitives.ReadUInt64BigEndian(src);
        return unchecked((long)(sortable ^ 0x8000000000000000UL));
    }

    /// <inheritdoc />
    public int  Compare(long x, long y) => x.CompareTo(y);
}

/// <summary>Guid big-endian key serializer. Bytes in RFC 4122 network order.</summary>
/// <remarks>
/// .NET's TryWriteBytes uses mixed-endian (little-endian for first three fields).
/// We write all fields in big-endian (network) byte order for consistent storage.
/// </remarks>
public sealed class GuidSerializer : IKeySerializer<Guid>, IValueSerializer<Guid>
{
    public static readonly GuidSerializer Instance = new();
    public int  FixedSize => 16;

    /// <inheritdoc />
    public int Serialize(Guid key, Span<byte> dst)
    {
        // Read .NET's mixed-endian layout and rewrite in RFC 4122 network (big-endian) order.
        Span<byte> tmp = stackalloc byte[16];
        key.TryWriteBytes(tmp);

        // tmp[0..3] = data1 in little-endian; write as big-endian uint32
        BinaryPrimitives.WriteUInt32BigEndian(dst,      BinaryPrimitives.ReadUInt32LittleEndian(tmp));
        // tmp[4..5] = data2 in little-endian; write as big-endian uint16
        BinaryPrimitives.WriteUInt16BigEndian(dst[4..], BinaryPrimitives.ReadUInt16LittleEndian(tmp[4..]));
        // tmp[6..7] = data3 in little-endian; write as big-endian uint16
        BinaryPrimitives.WriteUInt16BigEndian(dst[6..], BinaryPrimitives.ReadUInt16LittleEndian(tmp[6..]));
        // tmp[8..15] = data4 bytes, already in network order
        tmp[8..].CopyTo(dst[8..]);

        return 16;
    }

    /// <inheritdoc />
    public Guid Deserialize(ReadOnlySpan<byte> src)
    {
        // Reverse the big-endian → little-endian conversion for the first three fields.
        Span<byte> tmp = stackalloc byte[16];

        BinaryPrimitives.WriteUInt32LittleEndian(tmp,      BinaryPrimitives.ReadUInt32BigEndian(src));
        BinaryPrimitives.WriteUInt16LittleEndian(tmp[4..], BinaryPrimitives.ReadUInt16BigEndian(src[4..]));
        BinaryPrimitives.WriteUInt16LittleEndian(tmp[6..], BinaryPrimitives.ReadUInt16BigEndian(src[6..]));
        src[8..16].CopyTo(tmp[8..]);

        return new Guid(tmp);
    }

    /// <inheritdoc />
    public int  Compare(Guid x, Guid y) => x.CompareTo(y);
}

/// <summary>
/// Length-prefixed UTF-8 string serializer (2-byte big-endian length prefix).
/// Sort order: lexicographic UTF-8 byte order (matches string.Ordinal for ASCII).
/// FixedSize = -1 (variable).
/// Maximum serialized size: 65537 bytes (2-byte prefix + 65535 UTF-8 bytes).
/// Strings exceeding 65535 UTF-8 bytes throw <see cref="BPlusTreeEntryTooLargeException"/>.
/// </summary>
public sealed class StringSerializer : IKeySerializer<string>, IValueSerializer<string>
{
    public static readonly StringSerializer Instance = new();
    public int    FixedSize => -1;

    /// <inheritdoc />
    public int Serialize(string key, Span<byte> dst)
    {
        int byteCount = Encoding.UTF8.GetByteCount(key);
        if (byteCount > ushort.MaxValue)
            throw new BPlusTreeEntryTooLargeException(
                $"String value is too large ({byteCount} UTF-8 bytes). " +
                $"StringSerializer supports up to {ushort.MaxValue} bytes.",
                actualSize: byteCount, maxSize: ushort.MaxValue);
        BinaryPrimitives.WriteUInt16BigEndian(dst, (ushort)byteCount);
        Encoding.UTF8.GetBytes(key, dst[2..]);
        return 2 + byteCount;
    }

    /// <inheritdoc />
    public string Deserialize(ReadOnlySpan<byte> src)
    {
        int length = BinaryPrimitives.ReadUInt16BigEndian(src);
        return Encoding.UTF8.GetString(src.Slice(2, length));
    }

    /// <inheritdoc />
    public int    Compare(string x, string y) => string.CompareOrdinal(x, y);
    /// <inheritdoc />
    public int    MeasureSize(string key) => 2 + Encoding.UTF8.GetByteCount(key);
    /// <inheritdoc />
    public int    GetSerializedSize(ReadOnlySpan<byte> data)
        => 2 + BinaryPrimitives.ReadUInt16BigEndian(data);
}

/// <summary>
/// Length-prefixed byte array serializer (4-byte big-endian length prefix).
/// FixedSize = -1.
/// </summary>
public sealed class ByteArraySerializer : IKeySerializer<byte[]>, IValueSerializer<byte[]>
{
    public static readonly ByteArraySerializer Instance = new();
    public int    FixedSize => -1;

    /// <inheritdoc />
    public int Serialize(byte[] key, Span<byte> dst)
    {
        BinaryPrimitives.WriteInt32BigEndian(dst, key.Length);
        key.CopyTo(dst[4..]);
        return 4 + key.Length;
    }

    /// <inheritdoc />
    public byte[] Deserialize(ReadOnlySpan<byte> src)
    {
        int length = BinaryPrimitives.ReadInt32BigEndian(src);
        return src.Slice(4, length).ToArray();
    }

    /// <inheritdoc />
    public int    Compare(byte[] x, byte[] y) => x.AsSpan().SequenceCompareTo(y.AsSpan());
    /// <inheritdoc />
    public int    MeasureSize(byte[] key) => 4 + key.Length;
    /// <inheritdoc />
    public int    GetSerializedSize(ReadOnlySpan<byte> data)
        => 4 + BinaryPrimitives.ReadInt32BigEndian(data);
}

/// <summary>
/// DateTime serializer using UTC ticks in big-endian order (8 bytes).
/// Sort order: chronological (earlier dates sort first).
/// Stores <see cref="DateTime.ToUniversalTime()"/>.Ticks to ensure consistent ordering
/// regardless of the source <see cref="DateTimeKind"/>.
/// </summary>
public sealed class DateTimeSerializer : IKeySerializer<DateTime>, IValueSerializer<DateTime>
{
    public static readonly DateTimeSerializer Instance = new();
    public int FixedSize => 8;

    /// <inheritdoc />
    public int Serialize(DateTime key, Span<byte> dst)
    {
        BinaryPrimitives.WriteInt64BigEndian(dst, key.ToUniversalTime().Ticks);
        return 8;
    }

    /// <inheritdoc />
    public DateTime Deserialize(ReadOnlySpan<byte> src)
    {
        long ticks = BinaryPrimitives.ReadInt64BigEndian(src);
        return new DateTime(ticks, DateTimeKind.Utc);
    }

    /// <inheritdoc />
    /// <remarks>Normalizes both operands to UTC before comparison.</remarks>
    public int Compare(DateTime x, DateTime y) => x.ToUniversalTime().Ticks.CompareTo(y.ToUniversalTime().Ticks);
}
