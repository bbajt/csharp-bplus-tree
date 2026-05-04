using System.Buffers.Binary;
using ByTech.BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.Transactions;

/// <summary>
/// Fixed-size value serializer for <see cref="OrderItem"/>.
/// Layout: ProductId (4) | Quantity (4) | PriceCents (4) = 12 bytes total.
/// </summary>
public sealed class OrderItemSerializer : IValueSerializer<OrderItem>
{
    public static readonly OrderItemSerializer Instance = new();

    public int FixedSize => 12;

    public int Serialize(OrderItem item, Span<byte> dst)
    {
        BinaryPrimitives.WriteInt32BigEndian(dst,      item.ProductId);
        BinaryPrimitives.WriteInt32BigEndian(dst[4..], item.Quantity);
        BinaryPrimitives.WriteInt32BigEndian(dst[8..], item.PriceCents);
        return 12;
    }

    public OrderItem Deserialize(ReadOnlySpan<byte> src)
        => new()
        {
            ProductId  = BinaryPrimitives.ReadInt32BigEndian(src),
            Quantity   = BinaryPrimitives.ReadInt32BigEndian(src[4..]),
            PriceCents = BinaryPrimitives.ReadInt32BigEndian(src[8..]),
        };
}
