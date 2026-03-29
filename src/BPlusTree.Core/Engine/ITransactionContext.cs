using BPlusTree.Core.Wal;

namespace BPlusTree.Core.Engine;

/// <summary>
/// Non-generic internal interface for transaction context. Allows non-generic types
/// (TreeMetadata, Splitter, Merger) to interact with an active transaction without
/// coupling to the generic Transaction&lt;TKey, TValue&gt; type.
/// </summary>
internal interface ITransactionContext
{
    uint              TransactionId { get; }
    LogSequenceNumber LastLsn       { get; }

    void UpdateLastLsn(LogSequenceNumber lsn);

    /// <summary>
    /// Captures the before-image of a page. Idempotent — first call wins.
    /// Must be called while the frame is pinned and BEFORE any mutation to frame.Data.
    /// </summary>
    void CaptureBeforeImage(uint pageId, ReadOnlySpan<byte> pageData);

    /// <summary>Returns the captured before-image bytes for the given page.</summary>
    byte[] GetBeforeImage(uint pageId);

    /// <summary>
    /// Records a newly allocated page. On rollback, this page is freed.
    /// Call immediately after AllocatePage, before writing any data to the frame.
    /// </summary>
    void TrackAllocatedPage(uint pageId);

    /// <summary>
    /// Defers a FreePage call until Commit(). On rollback, the page is NOT freed —
    /// the absorbing page's before-image restores all pointers to it.
    /// </summary>
    void DeferFreePage(uint pageId);
}
