using System.Diagnostics;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.BulkLoad;

/// <summary>
/// Compares <c>PutRange</c> (one transaction, atomic — all-or-nothing) against
/// a loop of <c>Put</c> (one auto-commit per record, individually visible).
/// Use <c>PutRange</c> when atomicity matters: the tree never reflects a
/// partial bulk insert. Use <c>Put</c> when interleaved partial visibility
/// is fine. Throughput differs by workload — measure both for your shape.
/// </summary>
public static class BulkLoadDemo
{
    public static void Run()
    {
        const int recordCount = 50_000;
        var data = Enumerable.Range(0, recordCount)
                             .Select(i => (Key: i, Value: $"value-{i:D8}"))
                             .ToList();

        TimeSpan loopTime  = MeasureLoopOfPut(data);
        TimeSpan rangeTime = MeasurePutRange(data);

        double loopPerSec  = recordCount / loopTime.TotalSeconds;
        double rangePerSec = recordCount / rangeTime.TotalSeconds;

        Console.WriteLine($"Records                     : {recordCount:N0}");
        Console.WriteLine($"Loop of Put (per-row commit): {loopTime.TotalMilliseconds,7:F0} ms  ({loopPerSec,10:N0} ops/sec)");
        Console.WriteLine($"PutRange (single tx, atomic): {rangeTime.TotalMilliseconds,7:F0} ms  ({rangePerSec,10:N0} ops/sec)");
        Console.WriteLine();
        Console.WriteLine("Atomicity contracts:");
        Console.WriteLine("  Loop of Put : after each row, the row is queryable. A crash mid-loop");
        Console.WriteLine("                leaves a prefix of the input in the tree.");
        Console.WriteLine("  PutRange    : nothing is queryable until Commit completes. A crash");
        Console.WriteLine("                mid-PutRange leaves the tree exactly as it was before.");
    }

    private static TimeSpan MeasureLoopOfPut(List<(int Key, string Value)> data)
    {
        string db  = Path.Combine(Path.GetTempPath(), "sample09a.db");
        string wal = Path.Combine(Path.GetTempPath(), "sample09a.wal");
        SampleHelpers.TryDelete(db); SampleHelpers.TryDelete(wal);

        var sw = Stopwatch.StartNew();
        using (var store = Open(db, wal))
        {
            foreach (var (k, v) in data)
                store.Put(k, v);
        }
        sw.Stop();

        SampleHelpers.TryDelete(db); SampleHelpers.TryDelete(wal);
        return sw.Elapsed;
    }

    private static TimeSpan MeasurePutRange(List<(int Key, string Value)> data)
    {
        string db  = Path.Combine(Path.GetTempPath(), "sample09b.db");
        string wal = Path.Combine(Path.GetTempPath(), "sample09b.wal");
        SampleHelpers.TryDelete(db); SampleHelpers.TryDelete(wal);

        var sw = Stopwatch.StartNew();
        using (var store = Open(db, wal))
        {
            store.PutRange(data);
        }
        sw.Stop();

        SampleHelpers.TryDelete(db); SampleHelpers.TryDelete(wal);
        return sw.Elapsed;
    }

    private static BPlusTree<int, string> Open(string db, string wal)
        => BPlusTree<int, string>.Open(
            new BPlusTreeOptions { DataFilePath = db, WalFilePath = wal },
            Int32Serializer.Instance, StringSerializer.Instance);
}
