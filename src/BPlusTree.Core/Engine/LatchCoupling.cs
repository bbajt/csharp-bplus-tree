namespace BPlusTree.Core.Engine;

/// <summary>
/// Latch coupling (crab-walking) for safe concurrent B+ tree traversal.
///
/// Read path (TryGet, Scan):
///   Acquire read latch on child → release read latch on parent → continue down.
///   At leaf: hold read latch while reading, then release.
///
/// Write path (Insert, Delete):
///   Acquire write latch on child.
///   If child is "safe" (won't split or merge): release ALL ancestor write latches.
///   If not safe: hold all ancestor write latches (structural change may propagate up).
///   At leaf: perform mutation, then release all held write latches bottom-up.
///
/// "Safe for insert" = node.FreeSlots > 0 (will not split).
/// "Safe for delete" = node.KeyCount > node.MinKeys (will not underflow or merge).
///
/// The latch coupling implementation is stateless — callers manage the ancestor stack.
///
/// Phase 33: ancestor latches are stored in Span&lt;WriteLatchHandle&gt; on the caller's stack
/// (stackalloc) to eliminate per-traversal heap allocations. ReleaseAll and CrabWriteDown
/// accept the span + a ref count instead of IList&lt;IDisposable&gt;.
/// </summary>
internal sealed class LatchCoupling
{
    /// <summary>
    /// Maximum B+ tree height for any practical key count and page size.
    /// With 8 KB pages, 8-byte keys: branching factor ~500.
    ///   N = 10^18 → height ≤ 7. 20 levels provides a 2× safety margin.
    /// stackalloc cost: 20 × sizeof(WriteLatchHandle) ≈ 160 bytes of stack. (Phase 33)
    /// </summary>
    internal const int MaxTreeHeight = 20;

    private readonly PageLatchManager _latches;

    public LatchCoupling(PageLatchManager latches) => _latches = latches;

    /// <summary>
    /// Acquire a read latch on childPageId and release the read latch on parentLatch.
    /// Pass default(ReadLatchHandle) at root (IsValid = false → no parent to release).
    /// Returns the new child read latch by value — no heap allocation at any step.
    /// </summary>
    internal ReadLatchHandle CrabReadDown(uint childPageId, ReadLatchHandle parentLatch)
    {
        var childLatch = _latches.AcquireReadLatch(childPageId);
        if (parentLatch.IsValid) parentLatch.Dispose();
        return childLatch;
    }

    /// <summary>
    /// Acquire a write latch on childPageId.
    /// If isSafe: release all latches in ancestorLatches (top to bottom) and reset count to 0.
    /// Returns the child write latch (caller adds it to ancestors).
    /// ancestors and count are the stack-allocated latch buffer and its live entry count.
    /// </summary>
    internal WriteLatchHandle CrabWriteDown(
        uint childPageId,
        bool isSafe,
        Span<WriteLatchHandle> ancestorLatches,
        ref int count)
    {
        var childLatch = _latches.AcquireWriteLatch(childPageId);
        if (isSafe)
            ReleaseAll(ancestorLatches, ref count);
        return childLatch;
    }

    /// <summary>
    /// Release all live latches in the span in reverse order (bottom-up), then set count to 0.
    /// Called after a leaf operation completes or on split retry.
    /// </summary>
    internal static void ReleaseAll(Span<WriteLatchHandle> latches, ref int count)
    {
        for (int i = count - 1; i >= 0; i--)
            latches[i].Dispose();
        count = 0;
    }
}
