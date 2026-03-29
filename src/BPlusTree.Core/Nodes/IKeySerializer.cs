namespace BPlusTree.Core.Nodes;

/// <summary>
/// Serializes a key type to/from a fixed or variable-length big-endian byte span.
/// Implementations must preserve sort order: if a &lt; b then Serialize(a) &lt; Serialize(b)
/// under lexicographic byte comparison.
/// </summary>
public interface IKeySerializer<T>
{
    /// <summary>Fixed serialized size in bytes, or -1 if variable.</summary>
    int FixedSize { get; }

    /// <summary>Serialize key into destination. Returns bytes written.</summary>
    int Serialize(T key, Span<byte> destination);

    /// <summary>Deserialize key from source. Returns bytes consumed.</summary>
    T Deserialize(ReadOnlySpan<byte> source);

    /// <summary>Compare two serialized keys. Returns negative/zero/positive.</summary>
    int Compare(T x, T y);

    /// <summary>
    /// Returns the number of bytes required to serialize <paramref name="key"/>.
    /// Fixed-size serializers return <see cref="FixedSize"/>. Variable-size serializers
    /// override this to compute the exact byte count without writing to a buffer
    /// (e.g., <c>Encoding.UTF8.GetByteCount</c>).
    /// </summary>
    int MeasureSize(T key) => FixedSize;

    /// <summary>
    /// Returns the number of bytes the serialized key at <paramref name="data"/> occupies.
    /// Fixed-size serializers return <see cref="FixedSize"/>. Variable-size serializers
    /// override this to read their length prefix from the page bytes.
    /// </summary>
    int GetSerializedSize(ReadOnlySpan<byte> data) => FixedSize;
}
