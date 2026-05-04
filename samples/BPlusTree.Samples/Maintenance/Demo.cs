using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.Maintenance;

/// <summary>
/// Operator-side maintenance APIs: <c>Compact</c> (online, reclaims free
/// pages), <c>GetStatistics</c> (live observability — record count, page
/// count, buffer-pool hit rate, WAL size), and <c>Validate</c> (full
/// structural invariant check).
/// </summary>
public static class MaintenanceDemo
{
    public static void Run()
    {
        string dbPath  = Path.Combine(Path.GetTempPath(), "sample12.db");
        string walPath = Path.Combine(Path.GetTempPath(), "sample12.wal");
        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);

        using var store = BPlusTree<int, string>.Open(
            new BPlusTreeOptions { DataFilePath = dbPath, WalFilePath = walPath },
            Int32Serializer.Instance, StringSerializer.Instance);

        // Insert 10k, then delete 70% to create reclaimable free pages.
        Console.WriteLine("Seeding 10,000 records, then deleting 70%...");
        store.PutRange(Enumerable.Range(0, 10_000).Select(i => (i, $"value-{i:D5}")));
        for (int i = 0; i < 10_000; i++)
            if (i % 10 < 7) store.Delete(i);

        // Statistics BEFORE compact.
        var before = store.GetStatistics();
        PrintStats("Before Compact()", before);

        // Online compaction. Concurrent readers/writers stay live throughout.
        Console.WriteLine("\nRunning Compact()...");
        var result = store.Compact();
        Console.WriteLine($"  Bytes saved : {result.BytesSaved:N0}");
        Console.WriteLine($"  Pages freed : {result.PagesFreed:N0}");
        Console.WriteLine($"  Duration    : {result.Duration.TotalMilliseconds:F0} ms");

        var after = store.GetStatistics();
        PrintStats("\nAfter Compact()", after);

        // Structural validation — checks every invariant (sortedness,
        // leaf chain, height, latch consistency, freelist soundness).
        Console.WriteLine("\nRunning Validate()...");
        var validation = store.Validate();
        Console.WriteLine(validation.IsValid
            ? "  IsValid: true — every invariant holds"
            : $"  IsValid: false — {validation.Errors.Count} error(s):\n    " + string.Join("\n    ", validation.Errors));

        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);
    }

    private static void PrintStats(string label, TreeStatistics s)
    {
        Console.WriteLine(label);
        Console.WriteLine($"  Records          : {s.TotalRecords:N0}");
        Console.WriteLine($"  Pages (total)    : {s.TotalPages:N0}");
        Console.WriteLine($"  Pages (free)     : {s.FreePages:N0}");
        Console.WriteLine($"  Tree height      : {s.TreeHeight}");
        Console.WriteLine($"  WAL bytes        : {s.WalSizeBytes:N0}");
        Console.WriteLine($"  BufferPool hits  : {s.BufferPoolHits:N0} / misses {s.BufferPoolMisses:N0}");
        Console.WriteLine($"  Pool occupancy   : {s.BufferPoolOccupancyFraction:P1}");
    }
}
