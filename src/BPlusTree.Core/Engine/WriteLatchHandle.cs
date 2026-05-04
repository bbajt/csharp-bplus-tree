namespace ByTech.BPlusTree.Core.Engine;

/// <summary>
/// Write-latch handle returned by PageLatchManager.AcquireWriteLatch.
///
/// Design: non-readonly struct (not class, not readonly struct).
///
/// Why struct: eliminates per-latch heap allocation when stored in Span&lt;WriteLatchHandle&gt;
/// on the stack (Phase 33). A class would allocate one object per B+ tree level per insert.
///
/// Why NOT readonly: the _disposed field must be mutable for the idempotency guard.
/// A readonly struct would prevent _disposed from being set in Dispose(), causing the
/// compiler to silently make defensive copies — which would break the double-dispose guard
/// entirely. Non-readonly + Span&lt;WriteLatchHandle&gt; is the safe combination: callers always
/// hold WriteLatchHandle as a Span element and call Dispose() directly on the element
/// (not through IDisposable), so no boxing occurs and no defensive copy is made.
///
/// Why _disposed guard: ExitWriteLock() throws SynchronizationLockException on double-call.
/// The guard is cheap (one bool field) and prevents accidental double-dispose from
/// error-handling paths where ReleaseAll might be called after a partial Dispose.
/// </summary>
internal struct WriteLatchHandle : IDisposable
{
    private readonly ReaderWriterLockSlim _rwl;
    private bool _disposed;

    public WriteLatchHandle(ReaderWriterLockSlim rwl) => _rwl = rwl;

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_disposed) { _disposed = true; _rwl.ExitWriteLock(); }
    }
}
