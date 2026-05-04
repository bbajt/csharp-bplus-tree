using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.SimpleKvStore;

/// <summary>
/// Simplest possible usage: int keys, string values, auto-commit writes.
/// Reopens the store to prove data survives across instances.
/// </summary>
public static class SimpleKvStoreDemo
{
    public static void Run()
    {
        string dbPath  = Path.Combine(Path.GetTempPath(), "sample01.db");
        string walPath = Path.Combine(Path.GetTempPath(), "sample01.wal");

        // Clean slate for a repeatable demo.
        File.Delete(dbPath);
        File.Delete(walPath);

        // ── First session: write data ─────────────────────────────────────────
        Console.WriteLine("Opening store (first session)...");
        using (var store = Open(dbPath, walPath))
        {
            store.Put(1, "apple");
            store.Put(2, "banana");
            store.Put(3, "cherry");
            store.Put(4, "date");
            store.Put(5, "elderberry");

            // Overwrite an existing key.
            store.Put(3, "CHERRY (updated)");

            // Conditional insert — no-op when key exists.
            bool inserted = store.TryInsert(1, "should not appear");
            Console.WriteLine($"TryInsert on existing key returned: {inserted}");  // false

            // Delete one entry.
            store.Delete(4);

            Console.WriteLine($"Records in store: {store.Count}");  // 4

            // Scan ascending.
            Console.WriteLine("All entries (ascending):");
            foreach (var (key, value) in store)
                Console.WriteLine($"  {key} → {value}");
        }

        // ── Second session: reopen and verify persistence ─────────────────────
        Console.WriteLine("\nReopening store (second session)...");
        using (var store = Open(dbPath, walPath))
        {
            Console.WriteLine($"Records after reopen: {store.Count}");  // 4

            if (store.TryGet(3, out string? cherry))
                Console.WriteLine($"Key 3 → \"{cherry}\"");  // CHERRY (updated)

            if (store.TryGet(4, out _))
                Console.WriteLine("Key 4 still present (unexpected)");
            else
                Console.WriteLine("Key 4 correctly absent after delete");

            // Range scan.
            Console.WriteLine("Range scan [1, 3]:");
            foreach (var (key, value) in store.Scan(startKey: 1, endKey: 3))
                Console.WriteLine($"  {key} → {value}");

            // Min / max keys.
            store.TryGetFirst(out int minKey, out string? minVal);
            store.TryGetLast(out int maxKey,  out string? maxVal);
            Console.WriteLine($"Min: {minKey}={minVal}  Max: {maxKey}={maxVal}");

            // Statistics.
            var stats = store.GetStatistics();
            Console.WriteLine("\nTree statistics:");
            Console.WriteLine($"  Records   : {stats.TotalRecords:N0}");
            Console.WriteLine($"  Pages     : {stats.TotalPages}");
            Console.WriteLine($"  Height    : {stats.TreeHeight}");
            Console.WriteLine($"  WAL bytes : {stats.WalSizeBytes:N0}");
        }

        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);
    }

    private static BPlusTree<int, string> Open(string dbPath, string walPath)
        => BPlusTree<int, string>.Open(
            new BPlusTreeOptions { DataFilePath = dbPath, WalFilePath = walPath },
            Int32Serializer.Instance,
            StringSerializer.Instance);
}
