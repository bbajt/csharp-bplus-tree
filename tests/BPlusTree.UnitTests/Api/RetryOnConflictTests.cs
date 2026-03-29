using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Api;

/// <summary>
/// Tests for BPlusTree auto-commit retry loop with CancellationToken support (Phase P-D).
/// </summary>
public class RetryOnConflictTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = 4096,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 64,
        },
        Int32Serializer.Instance, Int32Serializer.Instance);

    public void Dispose()
    {
        try { if (File.Exists(_dbPath))  File.Delete(_dbPath);  } catch (IOException) { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch (IOException) { }
    }

    [Fact]
    public void Put_AlreadyCancelledToken_ThrowsOperationCancelled()
    {
        using var store = Open();
        store.Put(1, 100); // seed one entry so a second concurrent writer can conflict

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // The tree is idle here (no concurrent transaction), so Put will succeed
        // without ever hitting the retry path.  To verify cancellation we open
        // a transaction that holds a write lock on key 1, then attempt Put from
        // the outside thread with an already-cancelled token.
        using var tx = store.BeginTransaction();
        tx.TryUpdate(1, 999); // acquires write lock on the page

        var act = () => store.Put(1, 200, cts.Token);
        act.Should().Throw<OperationCanceledException>();
    }

    [Fact]
    public void Put_NoCancellation_SucceedsNormally()
    {
        using var store = Open();
        var cts = new CancellationTokenSource();

        bool inserted = store.Put(42, 1, cts.Token);

        inserted.Should().BeTrue();
        store.TryGet(42, out var v).Should().BeTrue();
        v.Should().Be(1);
    }

    [Fact]
    public void Delete_AlreadyCancelledToken_ThrowsOperationCancelled()
    {
        using var store = Open();
        store.Put(5, 50);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        using var tx = store.BeginTransaction();
        tx.TryUpdate(5, 999); // hold write lock

        var act = () => store.Delete(5, cts.Token);
        act.Should().Throw<OperationCanceledException>();
    }
}
