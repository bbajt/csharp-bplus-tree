using System.Collections.Concurrent;

namespace ByTech.BPlusTree.Core.Engine;

/// <summary>
/// Per-page ReaderWriterLockSlim latches. Latches are created on first access and
/// retained until the page is freed (Remove) or the manager is disposed.
///
/// Latch lifecycle:
///   AcquireReadLatch(pageId)  — acquires read  lock; returns IDisposable that releases on Dispose()
///   AcquireWriteLatch(pageId) — acquires write lock; returns IDisposable that releases on Dispose()
///   Remove(pageId)            — called when a page is freed; disposes and removes its latch
///   ReleaseAll()              — releases all held latches (called on emergency dispose)
///
/// Thread safety: ConcurrentDictionary for latch map; ReaderWriterLockSlim for page-level locking.
/// Timeout: all AcquireXxxLatch calls must pass a timeout to detect deadlocks in tests.
///          Default timeout: 30 seconds. Throw TimeoutException if not acquired in time.
/// </summary>
internal sealed class PageLatchManager : IDisposable
{
    private static readonly TimeSpan DefaultLatchTimeout = TimeSpan.FromSeconds(30);

    private readonly ConcurrentDictionary<uint, ReaderWriterLockSlim> _latches = new();
    private readonly TimeSpan _timeout;

    public PageLatchManager(TimeSpan? timeout = null)
        => _timeout = timeout ?? DefaultLatchTimeout;

    private ReaderWriterLockSlim GetOrCreate(uint pageId)
        => _latches.GetOrAdd(pageId, _ => new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion));

    /// <summary>
    /// Acquires a read latch on pageId. Multiple readers may hold read latches simultaneously.
    /// Blocks until acquired or timeout expires (throws TimeoutException on timeout).
    /// Returns a ReadLatchHandle struct that releases the read latch on Dispose().
    /// internal (not public): returning the concrete struct (not IDisposable) avoids boxing
    /// at call sites — the primary allocation-reduction goal of Phase 34.
    /// </summary>
    internal ReadLatchHandle AcquireReadLatch(uint pageId)
    {
        var rwl = GetOrCreate(pageId);
        if (!rwl.TryEnterReadLock(_timeout))
            throw new TimeoutException($"Timeout acquiring read latch on page {pageId}.");
        return new ReadLatchHandle(rwl);
    }

    /// <summary>
    /// Acquires a write latch on pageId. Exclusive — blocks all readers and other writers.
    /// Returns a WriteLatchHandle struct that releases the write latch on Dispose().
    /// internal (not public): returning the concrete struct (not IDisposable) avoids boxing
    /// at call sites that store the handle in Span&lt;WriteLatchHandle&gt; — the primary
    /// allocation-reduction goal of Phase 33. InternalsVisibleTo exposes it to unit tests.
    /// </summary>
    internal WriteLatchHandle AcquireWriteLatch(uint pageId)
    {
        var rwl = GetOrCreate(pageId);
        if (!rwl.TryEnterWriteLock(_timeout))
            throw new TimeoutException($"Timeout acquiring write latch on page {pageId}.");
        return new WriteLatchHandle(rwl);
    }

    /// <summary>
    /// Removes the latch for <paramref name="pageId"/> from the manager.
    /// Acquires an exclusive write lock (blocking new readers) to ensure no reader
    /// is currently active, then disposes and removes the latch.
    /// Call when a page is freed so subsequent accesses to the same page ID get a fresh latch.
    /// </summary>
    public void Remove(uint pageId)
    {
        if (_latches.TryRemove(pageId, out var rwl))
        {
            // Wait for any current holders to release before disposing
            if (rwl.TryEnterWriteLock(_timeout))
                rwl.ExitWriteLock();
            rwl.Dispose();
        }
    }

    /// <summary>Release all latches held by this manager.</summary>
    public void ReleaseAll()
    {
        foreach (var key in _latches.Keys.ToArray())
        {
            if (_latches.TryRemove(key, out var rwl))
                rwl.Dispose();
        }
    }

    /// <inheritdoc />
    public void Dispose() => ReleaseAll();

    // ReadLatchHandle  is defined in ReadLatchHandle.cs  (top-level internal struct). (Phase 34)
    // WriteLatchHandle is defined in WriteLatchHandle.cs (top-level internal struct). (Phase 33)
}
