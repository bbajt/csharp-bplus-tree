using System.Buffers.Binary;
using System.Text;
using ByTech.BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.EventLog;

/// <summary>
/// Variable-length value serializer for <see cref="Event"/>.
/// Layout: Category (2-byte length + UTF-8) | Payload (2-byte length + UTF-8).
/// </summary>
public sealed class EventSerializer : IValueSerializer<Event>
{
    public static readonly EventSerializer Instance = new();

    public int FixedSize => -1;

    public int MeasureSize(Event e)
        => 2 + Encoding.UTF8.GetByteCount(e.Category)
         + 2 + Encoding.UTF8.GetByteCount(e.Payload);

    public int Serialize(Event e, Span<byte> dst)
    {
        int pos = 0;
        pos += WriteString(e.Category, dst[pos..]);
        pos += WriteString(e.Payload,  dst[pos..]);
        return pos;
    }

    public Event Deserialize(ReadOnlySpan<byte> src)
    {
        int pos = 0;
        string category = ReadString(src[pos..], out int n1); pos += n1;
        string payload  = ReadString(src[pos..], out int n2); pos += n2;
        return new Event { Category = category, Payload = payload };
    }

    private static int WriteString(string s, Span<byte> dst)
    {
        int byteCount = Encoding.UTF8.GetByteCount(s);
        BinaryPrimitives.WriteUInt16BigEndian(dst, (ushort)byteCount);
        Encoding.UTF8.GetBytes(s, dst[2..]);
        return 2 + byteCount;
    }

    private static string ReadString(ReadOnlySpan<byte> src, out int bytesConsumed)
    {
        int len = BinaryPrimitives.ReadUInt16BigEndian(src);
        bytesConsumed = 2 + len;
        return Encoding.UTF8.GetString(src.Slice(2, len));
    }
}
