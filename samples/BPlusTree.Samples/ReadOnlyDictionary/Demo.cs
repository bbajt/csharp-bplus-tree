using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.ReadOnlyDictionary;

/// <summary>
/// The tree (and its snapshots) implement <see cref="IReadOnlyDictionary{TKey,TValue}"/>,
/// so any code that accepts that contract — LINQ, third-party libraries,
/// reporting layers — works directly without an intermediate copy.
/// </summary>
public static class ReadOnlyDictionaryDemo
{
    public static void Run()
    {
        string dbPath  = Path.Combine(Path.GetTempPath(), "sample13.db");
        string walPath = Path.Combine(Path.GetTempPath(), "sample13.wal");
        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);

        using var store = BPlusTree<string, int>.Open(
            new BPlusTreeOptions { DataFilePath = dbPath, WalFilePath = walPath },
            StringSerializer.Instance, Int32Serializer.Instance);

        store.PutRange(new[]
        {
            new KeyValuePair<string, int>("apple",   12),
            new KeyValuePair<string, int>("banana",   5),
            new KeyValuePair<string, int>("cherry",  47),
            new KeyValuePair<string, int>("date",     2),
            new KeyValuePair<string, int>("fig",     19),
        });

        // Pass the live tree directly to a method that expects the read-only
        // dictionary contract — no copy, no adapter.
        int totalLive = SumValues(store);
        Console.WriteLine($"Sum of all values (live tree, IReadOnlyDictionary): {totalLive}");

        // LINQ over the tree via the IReadOnlyDictionary surface (KVP).
        IReadOnlyDictionary<string, int> dict = store;
        var topTwo = dict.OrderByDescending(kvp => kvp.Value).Take(2);
        Console.WriteLine("\nTop 2 entries by value (LINQ over IReadOnlyDictionary):");
        foreach (var kvp in topTwo)
            Console.WriteLine($"  {kvp.Key,-8} → {kvp.Value}");

        // Snapshots ALSO implement IReadOnlyDictionary — point-in-time view.
        using var snap = store.BeginSnapshot();
        store.Put("apple", 999);  // mutate live tree after snapshot

        IReadOnlyDictionary<string, int> snapDict = snap;
        int snapApple = snapDict["apple"];
        int liveApple = dict["apple"];
        Console.WriteLine($"\nSnapshot dictionary access  : snap[\"apple\"] = {snapApple}  (frozen)");
        Console.WriteLine($"Live tree dictionary access : store[\"apple\"] = {liveApple}  (mutated)");

        // ContainsKey via the IReadOnlyDictionary surface.
        Console.WriteLine($"\nContainsKey(\"banana\") on live tree: {dict.ContainsKey("banana")}");
        Console.WriteLine($"ContainsKey(\"xyz\")    on live tree: {dict.ContainsKey("xyz")}");

        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);
    }

    /// <summary>Trivially consumes the IReadOnlyDictionary contract.</summary>
    private static int SumValues(IReadOnlyDictionary<string, int> dict)
    {
        int total = 0;
        foreach (var kvp in dict) total += kvp.Value;
        return total;
    }
}
