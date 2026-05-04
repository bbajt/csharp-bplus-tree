using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Engine;
using ByTech.BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.Snapshot;

/// <summary>
/// Demonstrates snapshot isolation: a frozen read-only view of the tree
/// that remains unchanged while the live tree is modified concurrently.
///
/// Scenario:
///   1. Populate the live tree with customers 1–5.
///   2. Open a snapshot (sees customers 1–5).
///   3. Add customer 6, delete customer 1 on the live tree.
///   4. Show the snapshot still sees the original 5 customers.
///   5. Open a second snapshot — it sees the updated state (6 customers, 1 deleted).
///   6. Compare live tree vs both snapshots.
/// </summary>
public static class SnapshotDemo
{
    public static void Run()
    {
        string dbPath  = Path.Combine(Path.GetTempPath(), "sample05.db");
        string walPath = Path.Combine(Path.GetTempPath(), "sample05.wal");
        File.Delete(dbPath);
        File.Delete(walPath);

        var options = new BPlusTreeOptions { DataFilePath = dbPath, WalFilePath = walPath };
        options.Validate();

        using var store = BPlusTree<int, string>.Open(
            options, Int32Serializer.Instance, StringSerializer.Instance);

        // ── Populate ──────────────────────────────────────────────────────────
        for (int i = 1; i <= 5; i++)
            store.Put(i, $"customer-{i}");

        Console.WriteLine($"Live tree before snapshot: {store.Count} entries");

        // ── Open snapshot A — frozen at this point ────────────────────────────
        using ISnapshot<int, string> snapA = store.BeginSnapshot();
        Console.WriteLine($"Snapshot A opened (sees {snapA.Count} entries)");

        // ── Mutate the live tree ──────────────────────────────────────────────
        store.Put(6, "customer-6");   // new entry
        store.Delete(1);              // remove existing entry

        Console.WriteLine($"\nLive tree after mutations: {store.Count} entries");

        // ── Open snapshot B — sees the post-mutation state ────────────────────
        using ISnapshot<int, string> snapB = store.BeginSnapshot();
        Console.WriteLine($"Snapshot B opened (sees {snapB.Count} entries)");

        // ── Compare all three views ───────────────────────────────────────────
        Console.WriteLine("\nSnapshot A (frozen — original 5 customers):");
        foreach (var (k, v) in snapA.Scan())
            Console.WriteLine($"  {k} → {v}");

        Console.WriteLine("\nSnapshot B (post-mutation — 5 entries, key 1 gone, key 6 added):");
        foreach (var (k, v) in snapB.Scan())
            Console.WriteLine($"  {k} → {v}");

        Console.WriteLine("\nLive tree (same as snapshot B at this moment):");
        foreach (var (k, v) in store.Scan())
            Console.WriteLine($"  {k} → {v}");

        // ── Prove snapshot A doesn't see key 6 ───────────────────────────────
        bool snapASees6 = snapA.TryGet(6, out _);
        bool snapASees1 = snapA.TryGet(1, out _);
        Console.WriteLine($"\nSnapshot A sees key 6 (added after snapshot): {snapASees6}");  // false
        Console.WriteLine($"Snapshot A sees key 1 (deleted after snapshot): {snapASees1}");  // true

        // Snapshots are IDisposable — using statements handle cleanup above.
        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);
    }
}
