using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.Transactions;

/// <summary>
/// Order processing: multiple OrderItem records must be written atomically.
/// Demonstrates BeginScope / Complete (commit) and rollback-on-exception.
///
/// Scenario:
///   PlaceOrder succeeds — all line items committed together.
///   PlaceOrderWithFailure simulates a mid-order stock-check failure —
///   the partial writes are rolled back and the store stays consistent.
/// </summary>
public static class TransactionsDemo
{
    private static readonly string DbPath  = Path.Combine(Path.GetTempPath(), "sample04.db");
    private static readonly string WalPath = Path.Combine(Path.GetTempPath(), "sample04.wal");

    public static void Run()
    {
        SampleHelpers.TryDelete(DbPath);
        SampleHelpers.TryDelete(WalPath);

        using var store = Open();

        int nextLineItemId = 1;

        // ── Successful order ──────────────────────────────────────────────────
        Console.WriteLine("Placing order #1 (3 items, should succeed)...");
        PlaceOrder(store, ref nextLineItemId, new[]
        {
            new OrderItem { ProductId = 101, Quantity = 2, PriceCents = 1999 },
            new OrderItem { ProductId = 205, Quantity = 1, PriceCents = 4999 },
            new OrderItem { ProductId = 310, Quantity = 5, PriceCents =  499 },
        });
        Console.WriteLine($"  Line items in store after order #1: {store.Count}");

        // ── Failed order — rollback ───────────────────────────────────────────
        Console.WriteLine("\nPlacing order #2 (fails mid-way — rollback expected)...");
        try
        {
            PlaceOrderWithFailure(store, ref nextLineItemId, new[]
            {
                new OrderItem { ProductId = 400, Quantity = 1, PriceCents = 9999 },
                new OrderItem { ProductId = 401, Quantity = 3, PriceCents = 2999 },
                // Third item triggers a simulated stock-check failure.
                new OrderItem { ProductId = 999, Quantity = 1, PriceCents =    1 },
            });
        }
        catch (InvalidOperationException ex)
        {
            Console.WriteLine($"  Order #2 failed as expected: {ex.Message}");
        }

        Console.WriteLine($"  Line items in store after failed order: {store.Count}");
        // Must still be 3 — rollback removed the partial inserts from order #2.

        // ── Verify contents ───────────────────────────────────────────────────
        Console.WriteLine("\nAll committed line items:");
        foreach (var (id, item) in store.Scan())
            Console.WriteLine($"  id={id}  {item}");

        // ── Async commit variant ──────────────────────────────────────────────
        Console.WriteLine("\nPlacing order #3 via async commit...");
        nextLineItemId = PlaceOrderAsync(store, nextLineItemId, new[]
        {
            new OrderItem { ProductId = 500, Quantity = 2, PriceCents = 3499 },
        }).GetAwaiter().GetResult();
        Console.WriteLine($"  Line items after order #3: {store.Count}");

        SampleHelpers.TryDelete(DbPath);
        SampleHelpers.TryDelete(WalPath);
    }

    private static void PlaceOrder(
        BPlusTree<int, OrderItem> store,
        ref int nextId,
        OrderItem[] items)
    {
        using var scope = store.BeginScope();
        foreach (var item in items)
            scope.Insert(nextId++, item);
        scope.Complete(); // commit — all items or none
    }

    private static void PlaceOrderWithFailure(
        BPlusTree<int, OrderItem> store,
        ref int nextId,
        OrderItem[] items)
    {
        // nextId must not advance on rollback — capture before the scope.
        int savedNextId = nextId;
        try
        {
            using var scope = store.BeginScope();
            foreach (var item in items)
            {
                // Simulate a stock-check that rejects product 999.
                if (item.ProductId == 999)
                    throw new InvalidOperationException($"Product {item.ProductId} is out of stock.");

                scope.Insert(nextId++, item);
            }
            scope.Complete();
        }
        catch
        {
            nextId = savedNextId; // restore counter — scope rolled back automatically
            throw;
        }
    }

    private static async Task<int> PlaceOrderAsync(
        BPlusTree<int, OrderItem> store,
        int nextId,
        OrderItem[] items)
    {
        await using var scope = store.BeginScope();
        foreach (var item in items)
            scope.Insert(nextId++, item);
        scope.Complete();
        // DisposeAsync commits via CommitAsync (non-blocking fsync).
        return nextId;
    }

    private static BPlusTree<int, OrderItem> Open()
        => BPlusTree<int, OrderItem>.Open(
            new BPlusTreeOptions { DataFilePath = DbPath, WalFilePath = WalPath },
            Int32Serializer.Instance,
            OrderItemSerializer.Instance);
}
