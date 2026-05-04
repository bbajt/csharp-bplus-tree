using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.CompareAndSwap;

/// <summary>
/// Demonstrates <see cref="BPlusTree{TKey,TValue}.TryCompareAndSwap"/> as a
/// lock-free optimistic-update primitive. Multiple threads race to advance a
/// shared counter; only the thread whose <c>expected</c> matches current state
/// wins each round — losers retry.
/// </summary>
public static class CompareAndSwapDemo
{
    public static void Run()
    {
        string dbPath  = Path.Combine(Path.GetTempPath(), "sample07.db");
        string walPath = Path.Combine(Path.GetTempPath(), "sample07.wal");
        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);

        using var store = BPlusTree<int, int>.Open(
            new BPlusTreeOptions { DataFilePath = dbPath, WalFilePath = walPath },
            Int32Serializer.Instance, Int32Serializer.Instance);

        const int counterKey = 0;
        store.Put(counterKey, 0);

        const int threadCount = 8;
        const int incrementsPerThread = 1000;
        long totalAttempts = 0;

        var threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++)
        {
            threads[t] = new Thread(() =>
            {
                long localAttempts = 0;
                for (int i = 0; i < incrementsPerThread; i++)
                {
                    while (true)
                    {
                        localAttempts++;
                        store.TryGet(counterKey, out int current);
                        if (store.TryCompareAndSwap(counterKey, current, current + 1))
                            break;
                        // Lost the race — another thread updated; retry with fresh read.
                    }
                }
                Interlocked.Add(ref totalAttempts, localAttempts);
            });
            threads[t].Start();
        }
        foreach (var th in threads) th.Join();

        store.TryGet(counterKey, out int finalValue);
        int expected = threadCount * incrementsPerThread;

        Console.WriteLine($"Threads          : {threadCount}");
        Console.WriteLine($"Increments/thread: {incrementsPerThread:N0}");
        Console.WriteLine($"Final counter    : {finalValue:N0}  (expected {expected:N0})");
        Console.WriteLine($"Total CAS calls  : {totalAttempts:N0}  (retry rate {(totalAttempts - expected) * 100.0 / expected:F1}%)");
        Console.WriteLine(finalValue == expected
            ? "OK: no lost updates — CAS preserved linearizability."
            : "ERROR: counter diverged from expected.");

        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);
    }
}
