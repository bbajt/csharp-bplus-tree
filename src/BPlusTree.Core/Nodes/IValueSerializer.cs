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
}
