using System.Diagnostics;
using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.GroupCommitThroughput;

/// <summary>
/// Compares write throughput between WalSyncMode.Synchronous and WalSyncMode.GroupCommit.
///
/// Synchronous: fsync is called at every checkpoint boundary — maximum durability,
///              every committed write is on disk before the call returns.
///
/// GroupCommit: a background thread fsyncs every FlushIntervalMs milliseconds.
///              Offers higher throughput at the cost of a small durability window
///              (~FlushIntervalMs ms of writes could be lost on a hard crash).
///              Suitable for bulk loads, analytics, or "best-effort" stores.
/// </summary>
public static class GroupCommitDemo
{
    private const int RecordCount = 50_000;

    public static void Run()
    {
        Console.WriteLine($"Inserting {RecordCount:N0} records in each mode...\n");

        long syncMs  = MeasureInserts(WalSyncMode.Synchronous, "sample06-sync");
        long groupMs = MeasureInserts(WalSyncMode.GroupCommit, "sample06-group");

        Console.WriteLine($"Synchronous : {syncMs,6} ms  ({RecordCount * 1000L / Math.Max(syncMs, 1):N0} inserts/sec)");
        Console.WriteLine($"GroupCommit : {groupMs,6} ms  ({RecordCount * 1000L / Math.Max(groupMs, 1):N0} inserts/sec)");

        double speedup = syncMs > 0 ? (double)syncMs / groupMs : 0;
        Console.WriteLine($"\nGroupCommit is ~{speedup:F1}× faster for this workload.");
        Console.WriteLine("Trade-off: up to FlushIntervalMs (5 ms default) of writes");
        Console.WriteLine("           could be lost on a hard crash in GroupCommit mode.");
    }

    private static long MeasureInserts(WalSyncMode mode, string filePrefix)
    {
        string dbPath  = Path.Combine(Path.GetTempPath(), filePrefix + ".db");
        string walPath = Path.Combine(Path.GetTempPath(), filePrefix + ".wal");
        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);

        var options = new BPlusTreeOptions
        {
            DataFilePath   = dbPath,
            WalFilePath    = walPath,
            SyncMode       = mode,
            FlushIntervalMs = 5,    // relevant only for GroupCommit
            FlushBatchSize  = 256,  // relevant only for GroupCommit
        };
        options.Validate();

        var sw = Stopwatch.StartNew();
        using (var store = BPlusTree<int, string>.Open(
                   options, Int32Serializer.Instance, StringSerializer.Instance))
        {
            for (int i = 0; i < RecordCount; i++)
                store.Put(i, $"value-{i}");
        }
        sw.Stop();

        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);
        return sw.ElapsedMilliseconds;
    }
}
