namespace BPlusTree.Samples.Transactions;

/// <summary>
/// A line item in an order. Key = order-line ID (int).
/// PriceCents avoids floating-point for money values.
/// </summary>
public sealed class OrderItem
{
    public int ProductId  { get; init; }
    public int Quantity   { get; init; }
    public int PriceCents { get; init; }

    public decimal PriceDecimal => PriceCents / 100m;

    public override string ToString()
        => $"Product {ProductId} × {Quantity} @ ${PriceDecimal:F2}";
}
