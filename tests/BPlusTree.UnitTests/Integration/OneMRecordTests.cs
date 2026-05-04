using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Integration;

public class OneMRecordTests : IDisposable
{
    private const int N = 1_000_000;
    private readonly string _dbPath  = Path.Combine(Path.GetTempPath(), $"1m_{Guid.NewGuid():N}.db");
    private readonly string _walPath = Path.Combine(Path.GetTempPath(), $"1m_{Guid.NewGuid():N}.wal");

    [Fact(Timeout = 120_000)]  // 2 minute timeout
    public async Task Insert_1M_Sequential_AllRetrievable()
    {
        await Task.Run(() =>
        {
            using var tree = Open();
            for (int i = 0; i < N; i++) tree.Put(i, i);
            // Spot-check 1000 random keys
            var rng = new Random(42);
            for (int t = 0; t < 1000; t++)
            {
                int k = rng.Next(N);
                tree.TryGet(k, out int v).Should().BeTrue($"key {k} not found");
                v.Should().Be(k);
            }
        });
    }

    [Fact(Timeout = 120_000)]
    public async Task Insert_1M_TreeHeight_AtMost4()
    {
        await Task.Run(() =>
        {
            using var tree = Open();
            for (int i = 0; i < N; i++) tree.Put(i, i);
            tree.GetStatistics().TreeHeight.Should().BeLessOrEqualTo(4u);
        });
    }

    [Fact(Timeout = 300_000)]  // 5 minute timeout (scan is slower)
    public async Task Scan_1M_Returns_AllRecords_InOrder()
    {
        await Task.Run(() =>
        {
            using var tree = Open();
            for (int i = 0; i < N; i++) tree.Put(i, i);
            long count = 0; int prev = -1;
            foreach (var (k, _) in tree.Scan())
            {
                k.Should().BeGreaterThan(prev);
                prev = k; count++;
            }
            count.Should().Be(N);
        });
    }

    [Fact(Timeout = 300_000)]
    public async Task Mixed_1M_Ops_MatchReferenceDictionary()
    {
        await Task.Run(() =>
        {
            using var tree = Open();
            var reference = new Dictionary<int, int>();
            var rng = new Random(123);
            for (int i = 0; i < N; i++)
            {
                int op = i % 3;
                int k = rng.Next(N / 2), v = rng.Next();
                if (op == 0) { tree.Put(k, v); reference[k] = v; }
                else if (op == 1 && reference.ContainsKey(k)) { tree.Delete(k); reference.Remove(k); }
                else { tree.TryGet(k, out _); }
            }
            // Validate a sample
            foreach (var (k, expected) in reference.Take(1000))
            {
                tree.TryGet(k, out int actual).Should().BeTrue();
                actual.Should().Be(expected);
            }
        });
    }

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions { DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = 8192, BufferPoolCapacity = 1024, CheckpointThreshold = 256 },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose() { try { File.Delete(_dbPath); File.Delete(_walPath); } catch { } }
}
