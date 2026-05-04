using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.CrashRecovery;

/// <summary>
/// WAL-based durability + atomicity across process restarts:
///
///  1. Auto-commit writes are durable on Synchronous WAL — every commit fsync's.
///  2. A transaction disposed without <c>Commit</c> aborts; its writes never appear.
///  3. Reopening the same files after a clean shutdown surfaces every committed
///     record (and ONLY committed records). This is the contract a fresh process
///     would observe after a crash followed by orderly restart.
/// </summary>
public static class CrashRecoveryDemo
{
    public static void Run()
    {
        string dbPath  = Path.Combine(Path.GetTempPath(), "sample11.db");
        string walPath = Path.Combine(Path.GetTempPath(), "sample11.wal");
        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);

        // ── Session 1: 100 auto-commit writes (each fsync'd) ──────────────────
        Console.WriteLine("Session 1: 100 auto-commit writes (Synchronous WAL)...");
        using (var store = Open(dbPath, walPath))
        {
            for (int i = 0; i < 100; i++)
                store.Put(i, $"committed-{i:D3}");
        }
        Console.WriteLine($"  After dispose: DB={new FileInfo(dbPath).Length:N0} bytes, " +
                          $"WAL={new FileInfo(walPath).Length:N0} bytes");

        // ── Session 2: open + start a tx, abandon WITHOUT Commit ──────────────
        Console.WriteLine("\nSession 2: opening transaction, writing 50 records, NOT committing...");
        using (var store = Open(dbPath, walPath))
        {
            using (var tx = store.BeginTransaction())
            {
                for (int i = 100; i < 150; i++)
                    tx.Insert(i, $"uncommitted-{i:D3}");
                // tx.Dispose() runs WITHOUT Commit() → abort, writes discarded.
            }
            Console.WriteLine($"  Records visible after abort: {store.Count}  (back to committed-only)");
        }

        // ── Session 3: reopen — only committed data must be present ───────────
        Console.WriteLine("\nSession 3: reopening — verify atomicity + durability...");
        using (var store = Open(dbPath, walPath))
        {
            int total = (int)store.Count;
            bool firstCommitted   = store.TryGet(  0, out string? a) && a == "committed-000";
            bool lastCommitted    = store.TryGet( 99, out string? b) && b == "committed-099";
            bool noUncommittedLow = !store.TryGet(100, out _);
            bool noUncommittedHi  = !store.TryGet(149, out _);

            Console.WriteLine($"  Records in store         : {total}        (expected 100)");
            Console.WriteLine($"  committed-000 present    : {firstCommitted}");
            Console.WriteLine($"  committed-099 present    : {lastCommitted}");
            Console.WriteLine($"  uncommitted-100 absent   : {noUncommittedLow}");
            Console.WriteLine($"  uncommitted-149 absent   : {noUncommittedHi}");

            Console.WriteLine(total == 100 && firstCommitted && lastCommitted && noUncommittedLow && noUncommittedHi
                ? "\nOK: only committed transactions survived across restart."
                : "\nERROR: durability/atomicity contract violated.");
        }

        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);
    }

    private static BPlusTree<int, string> Open(string dbPath, string walPath)
        => BPlusTree<int, string>.Open(
            new BPlusTreeOptions
            {
                DataFilePath = dbPath,
                WalFilePath  = walPath,
                SyncMode     = WalSyncMode.Synchronous,
            },
            Int32Serializer.Instance,
            StringSerializer.Instance);
}
