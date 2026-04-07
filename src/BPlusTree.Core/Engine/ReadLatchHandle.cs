using System.Threading;

namespace BPlusTree.Core.Engine;

/// <summary>
/// Lightweight struct that wraps a ReaderWriterLockSlim read-lock release.
/// Returned by PageLatchManager.AcquireReadLatch and LatchCoupling.CrabReadDown.
///
/// Stored in local variables on the call stack — no heap allocation.
/// IsValid guards against operating on a default(ReadLatchHandle) sentinel
/// used at the root of a read traversal (no parent to release).
///
/// Non-readonly: _disposed is mutable. Dispose() modifies the struct in-place
/// when called on a local variable — no defensive copy.
///
/// Phase 34: mirrors WriteLatchHandle (Phase 33). Eliminates IDisposable boxing
/// on the read path — CrabReadDown returns ReadLatchHandle by value, not IDisposable.
/// </summary>
internal struct ReadLatchHandle : IDisposable
{
    private readonly ReaderWriterLockSlim _rwl;
    private bool _disposed;

    internal ReadLatchHandle(ReaderWriterLockSlim rwl)
        => (_rwl, _disposed) = (rwl, false);

    /// <summary>
    /// True when this handle wraps a live lock acquisition.
    /// False for default(ReadLatchHandle) — the sentinel used at the tree root.
    /// </summary>
    internal bool IsValid => _rwl != null;

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _rwl.ExitReadLock();
    }
}
