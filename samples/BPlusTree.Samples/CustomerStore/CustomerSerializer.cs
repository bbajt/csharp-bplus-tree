using System.Buffers.Binary;
using System.Text;
using BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.CustomerStore;

/// <summary>
/// Variable-length value serializer for <see cref="Customer"/>.
/// Layout: Age (4 bytes) | FirstName (2-byte length + UTF-8) | LastName | Email.
/// </summary>
public sealed class CustomerSerializer : IValueSerializer<Customer>
{
    public static readonly CustomerSerializer Instance = new();

    public int FixedSize => -1; // variable length

    public int MeasureSize(Customer c)
        => 4
         + 2 + Encoding.UTF8.GetByteCount(c.FirstName)
         + 2 + Encoding.UTF8.GetByteCount(c.LastName)
         + 2 + Encoding.UTF8.GetByteCount(c.Email);

    public int Serialize(Customer c, Span<byte> dst)
    {
        int pos = 0;

        BinaryPrimitives.WriteInt32BigEndian(dst[pos..], c.Age);
        pos += 4;

        pos += WriteString(c.FirstName, dst[pos..]);
        pos += WriteString(c.LastName,  dst[pos..]);
        pos += WriteString(c.Email,     dst[pos..]);

        return pos;
    }

    public Customer Deserialize(ReadOnlySpan<byte> src)
    {
        int pos = 0;

        int age = BinaryPrimitives.ReadInt32BigEndian(src[pos..]);
        pos += 4;

        string firstName = ReadString(src[pos..], out int n1); pos += n1;
        string lastName  = ReadString(src[pos..], out int n2); pos += n2;
        string email     = ReadString(src[pos..], out int n3); pos += n3;

        return new Customer
        {
            Age       = age,
            FirstName = firstName,
            LastName  = lastName,
            Email     = email,
        };
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
