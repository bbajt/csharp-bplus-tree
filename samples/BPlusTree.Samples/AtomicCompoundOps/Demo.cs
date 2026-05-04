using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.AtomicCompoundOps;

/// <summary>
/// ConcurrentDictionary-style atomic operations exposed by the tree:
/// <c>GetOrAdd</c>, <c>AddOrUpdate</c>, <c>TryUpdate(factory)</c>,
/// <c>TryGetAndDelete</c>. Each fuses a read+write under one latch so the
/// caller never has to coordinate them.
/// </summary>
public static class AtomicCompoundOpsDemo
{
    public static void Run()
    {
        string dbPath  = Path.Combine(Path.GetTempPath(), "sample08.db");
        string walPath = Path.Combine(Path.GetTempPath(), "sample08.wal");
        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);

        using var store = BPlusTree<string, int>.Open(
            new BPlusTreeOptions { DataFilePath = dbPath, WalFilePath = walPath },
            StringSerializer.Instance, Int32Serializer.Instance);

        // ── GetOrAdd: insert if absent, return existing otherwise ─────────────
        int v1 = store.GetOrAdd("hits/home", 1);
        int v2 = store.GetOrAdd("hits/home", 999);  // ignored — already exists
        Console.WriteLine($"GetOrAdd first call  : {v1}  (inserted)");
        Console.WriteLine($"GetOrAdd second call : {v2}  (existing kept — argument ignored)");

        // ── AddOrUpdate: increment-or-init pattern in one call ────────────────
        for (int i = 0; i < 5; i++)
            store.AddOrUpdate("hits/about", addValue: 1, updateValueFactory: (_, prev) => prev + 1);
        store.TryGet("hits/about", out int aboutHits);
        Console.WriteLine($"AddOrUpdate 5x       : hits/about = {aboutHits}");

        // ── TryUpdate(factory): atomic read-modify-write under latch ──────────
        store.Put("balance", 100);
        bool ok = store.TryUpdate("balance", current => current - 30);
        store.TryGet("balance", out int balance);
        Console.WriteLine($"TryUpdate(factory)   : ok={ok}, balance={balance}");

        bool missing = store.TryUpdate("nonexistent", current => current + 1);
        Console.WriteLine($"TryUpdate missing key: ok={missing}  (false — key absent, factory not called)");

        // ── TryGetAndDelete: pop-style consume ────────────────────────────────
        store.Put("inbox/msg-1", 42);
        bool popped = store.TryGetAndDelete("inbox/msg-1", out int popValue);
        bool present = store.TryGet("inbox/msg-1", out _);
        Console.WriteLine($"TryGetAndDelete      : popped={popped}, value={popValue}, stillThere={present}");

        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);
    }
}
