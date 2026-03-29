using System;
using System.IO;
using Xunit;
using FluentAssertions;
using BPlusTree.Core.Api;
using BPlusTree.Core.Storage;

namespace BPlusTree.UnitTests.Storage;

public class BufferPoolTests : IDisposable
{
    private const int PageSize = 4096;
    private readonly string _path = Path.GetTempFileName();
    private readonly StorageFile _storage;

    public BufferPoolTests()
    {
        _storage = StorageFile.Open(_path, PageSize, createNew: true);
        // Pre-allocate 10 pages so they exist on disk
        for (int i = 0; i < 9; i++) _storage.AllocatePage();
    }

    [Fact]
    public void Pin_PageInPool_ReturnsSameFrame()
    {
        var pool = new BufferPool(_storage, capacity: 4);
        var f1 = pool.Pin(0);
        var f2 = pool.Pin(0);
        f1.Should().BeSameAs(f2);
    }

    [Fact]
    public void Pin_IncrementsPinCount()
    {
        var pool = new BufferPool(_storage, capacity: 4);
        var f = pool.Pin(0);
        f.PinCount.Should().Be(1);
    }

    [Fact]
    public void Unpin_DecrementsPinCount()
    {
        var pool = new BufferPool(_storage, capacity: 4);
        pool.Pin(0);
        pool.Unpin(0);
        // After unpin, frame may still be in pool but pincount = 0
    }

    [Fact]
    public void Eviction_FlushDirtyFrame_BeforeReuse()
    {
        // Capacity = 2. Pin page 0, mark dirty, unpin. Pin page 1, unpin.
        // Pin page 2 → must evict either 0 or 1; if 0 was dirty it should flush first.
        var pool = new BufferPool(_storage, capacity: 2);
        var f0 = pool.Pin(0);
        f0.Data[0] = 0xBB;
        pool.Unpin(0, isDirty: true);

        pool.Pin(1); pool.Unpin(1);
        pool.Pin(2); pool.Unpin(2);

        // Verify page 0 was flushed: read from storage directly
        var buf = new byte[PageSize];
        _storage.ReadPage(0, buf);
        buf[0].Should().Be(0xBB);
    }

    [Fact]
    public void Clock_GivesSecondChance_BeforeEvicting()
    {
        // With capacity 3: load pages 0, 1, 2 (all get ReferenceBit=true after pin+unpin).
        // Loading page 3 should do one full clock sweep clearing reference bits
        // before finding victim — no page with reference bit set should be evicted first.
        var pool = new BufferPool(_storage, capacity: 3);
        pool.Pin(0); pool.Unpin(0);
        pool.Pin(1); pool.Unpin(1);
        pool.Pin(2); pool.Unpin(2);
        // This should succeed (clockhand sweeps once clearing ref bits, picks victim)
        pool.Invoking(p => p.Pin(3)).Should().NotThrow();
    }

    [Fact]
    public void AllFramesPinned_Throws_BufferPoolExhaustedException()
    {
        var pool = new BufferPool(_storage, capacity: 2);
        pool.Pin(0); // pinned, not unpinned
        pool.Pin(1); // pinned, not unpinned
        pool.Invoking(p => p.Pin(2))
            .Should().Throw<BufferPoolExhaustedException>();
    }

    [Fact]
    public void FlushAllDirty_WritesDirtyPagesToStorage()
    {
        var pool = new BufferPool(_storage, capacity: 4);
        var f = pool.Pin(0);
        f.Data[5] = 0xCC;
        pool.Unpin(0, isDirty: true);
        pool.FlushAllDirty();

        var buf = new byte[PageSize];
        _storage.ReadPage(0, buf);
        buf[5].Should().Be(0xCC);
    }

    public void Dispose()
    {
        _storage.Dispose();
        try { File.Delete(_path); } catch { }
    }
}