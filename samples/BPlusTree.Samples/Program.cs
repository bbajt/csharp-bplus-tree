using BPlusTree.Samples.SimpleKvStore;
using BPlusTree.Samples.CustomerStore;
using BPlusTree.Samples.EventLog;
using BPlusTree.Samples.Transactions;
using BPlusTree.Samples.Snapshot;
using BPlusTree.Samples.GroupCommitThroughput;
using BPlusTree.Samples.CompareAndSwap;
using BPlusTree.Samples.AtomicCompoundOps;
using BPlusTree.Samples.BulkLoad;
using BPlusTree.Samples.RangeQueries;
using BPlusTree.Samples.CrashRecovery;
using BPlusTree.Samples.Maintenance;
using BPlusTree.Samples.ReadOnlyDictionary;

RunDemo("01 SimpleKvStore",         SimpleKvStoreDemo.Run);
RunDemo("02 CustomerStore",         CustomerStoreDemo.Run);
RunDemo("03 EventLog",              EventLogDemo.Run);
RunDemo("04 Transactions",          TransactionsDemo.Run);
RunDemo("05 Snapshot",              SnapshotDemo.Run);
RunDemo("06 GroupCommitThroughput", GroupCommitDemo.Run);
RunDemo("07 CompareAndSwap",        CompareAndSwapDemo.Run);
RunDemo("08 AtomicCompoundOps",     AtomicCompoundOpsDemo.Run);
RunDemo("09 BulkLoad",              BulkLoadDemo.Run);
RunDemo("10 RangeQueries",          RangeQueriesDemo.Run);
RunDemo("11 CrashRecovery",         CrashRecoveryDemo.Run);
RunDemo("12 Maintenance",           MaintenanceDemo.Run);
RunDemo("13 ReadOnlyDictionary",    ReadOnlyDictionaryDemo.Run);

static void RunDemo(string name, Action run)
{
    Console.WriteLine();
    Console.WriteLine($"{'=',-60}");
    Console.WriteLine($"  {name}");
    Console.WriteLine($"{'=',-60}");
    try
    {
        run();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ERROR] {ex.GetType().Name}: {ex.Message}");
    }
}
