namespace BPlusTree.Core.Nodes;

/// <summary>
/// Serializes a value type to/from bytes. Sort order is NOT required.
/// </summary>
public interface IValueSerializer<T>
{
    int FixedSize { get; }
    int Serialize(T value, Span<byte> destination);
    T   Deserialize(ReadOnlySpan<byte> source);

    /// <summary>
    /// Returns the number of bytes required to serialize <paramref name="value"/>.
    /// Fixed-size serializers return <see cref="FixedSize"/>. Variable-size serializers override.
    /// </summary>
    int MeasureSize(T value) => FixedSize;

    /// <summary>
    /// Returns the number of bytes the serialized value at <paramref name="data"/> occupies.
    /// Fixed-size serializers return <see cref="FixedSize"/>. Variable-size serializers
    /// override this to read their length prefix from the page bytes.
    /// </summary>
    int GetSerializedSize(ReadOnlySpan<byte> data) => FixedSize;
}
