using BPlusTree.Samples.SimpleKvStore;
using BPlusTree.Samples.CustomerStore;
using BPlusTree.Samples.EventLog;
using BPlusTree.Samples.Transactions;
using BPlusTree.Samples.Snapshot;
using BPlusTree.Samples.GroupCommitThroughput;

RunDemo("01 SimpleKvStore",         SimpleKvStoreDemo.Run);
RunDemo("02 CustomerStore",         CustomerStoreDemo.Run);
RunDemo("03 EventLog",              EventLogDemo.Run);
RunDemo("04 Transactions",          TransactionsDemo.Run);
RunDemo("05 Snapshot",              SnapshotDemo.Run);
RunDemo("06 GroupCommitThroughput", GroupCommitDemo.Run);

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
