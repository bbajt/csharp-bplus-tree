using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.RangeQueries;

/// <summary>
/// Tour of read-side range primitives: <c>Scan</c>, <c>ScanReverse</c>,
/// <c>TryGetFirst</c> / <c>TryGetLast</c>, <c>CountRange</c>. Common idioms —
/// top-N newest, paginated descending, sparse range count — fall out of
/// these directly without scanning the whole tree.
/// </summary>
public static class RangeQueriesDemo
{
    public static void Run()
    {
        string dbPath  = Path.Combine(Path.GetTempPath(), "sample10.db");
        string walPath = Path.Combine(Path.GetTempPath(), "sample10.wal");
        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);

        using var store = BPlusTree<long, string>.Open(
            new BPlusTreeOptions { DataFilePath = dbPath, WalFilePath = walPath },
            Int64Serializer.Instance, StringSerializer.Instance);

        // Seed timestamped events. Keys are unix-ms (sortable as long).
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var seed = Enumerable.Range(0, 100)
                             .Select(i => ((long)(now - (100 - i) * 1000), $"event-{i:D3}"));
        store.PutRange(seed);

        // First / last key (cheap — skips down to leftmost / rightmost leaf).
        store.TryGetFirst(out long firstKey, out string? firstVal);
        store.TryGetLast (out long lastKey,  out string? lastVal);
        Console.WriteLine($"First : {firstKey}  →  {firstVal}");
        Console.WriteLine($"Last  : {lastKey}   →  {lastVal}");

        // Range count (no value materialization).
        long midpoint = now - 50_000;
        long firstHalf  = store.CountRange(firstKey, midpoint);
        long secondHalf = store.CountRange(midpoint, lastKey);
        Console.WriteLine($"CountRange first half  ({firstKey}..{midpoint}) : {firstHalf}");
        Console.WriteLine($"CountRange second half ({midpoint}..{lastKey})  : {secondHalf}");

        // Top-5 newest — ScanReverse from the end.
        Console.WriteLine("\nTop 5 newest events (ScanReverse + Take):");
        foreach (var (k, v) in store.ScanReverse().Take(5))
            Console.WriteLine($"  {k}  →  {v}");

        // Paginated descending: page of 10 strictly older than a cursor.
        long cursor = lastKey;
        Console.WriteLine($"\nPage of 10 descending below cursor {cursor}:");
        int n = 0;
        foreach (var (k, v) in store.ScanReverse(endKey: cursor))
        {
            if (k >= cursor) continue;  // exclusive upper bound idiom
            Console.WriteLine($"  {k}  →  {v}");
            if (++n == 10) break;
        }

        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);
    }
}
