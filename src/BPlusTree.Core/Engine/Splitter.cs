using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;

namespace BPlusTree.Core.Engine;

/// <summary>
/// Handles all three B+ tree split cases:
///   1. Leaf split:    full leaf → split into two leaves; promote median key to parent.
///   2. Root split:    if root is a leaf and full → split leaf; create new internal root.
///   3. Internal split: full internal node → split into two internal nodes; promote median upward.
///
/// Split invariants:
///   - Left sibling keeps floor((n+1)/2) entries; right sibling gets the rest.
///   - For leaf splits: promoted key = first key of the RIGHT sibling (copy-up).
///   - For internal splits: promoted key = middle key (push-up, removed from both children).
///   - Leaf sibling pointers (prev/next) must be correct after split.
///   - FillFactor is NOT applied during splits (applied only on initial bulk-load).
///
/// Root updates: when a root split occurs, <see cref="SplitLeaf"/> and
/// <see cref="SplitInternal"/> return the new root page ID and height rather than
/// mutating a shared <c>TreeMetadata</c> instance. This keeps <c>Splitter</c>
/// free of shared-state side-effects and makes concurrent transactional splits safe:
/// each caller receives its own new-root value and updates its per-transaction
/// shadow root without touching the live metadata.
/// </summary>
internal sealed class Splitter<TKey, TValue>
    where TKey : notnull
{
    private readonly PageManager                  _pageManager;
    private readonly NodeSerializer<TKey, TValue> _nodeSerializer;
    private readonly ITransactionContext?         _tx;

    internal Splitter(
        PageManager pageManager,
        NodeSerializer<TKey, TValue> nodeSerializer,
        ITransactionContext? tx = null)
    {
        _pageManager    = pageManager;
        _nodeSerializer = nodeSerializer;
        _tx             = tx;
    }

    /// <summary>
    /// Called by TreeEngine.Insert when the target leaf is full.
    /// Splits the leaf at <paramref name="leafPageId"/>, promotes the median key upward.
    /// If the parent is full: recursively splits the parent.
    /// If there is no parent (root == leaf): calls SplitRoot instead.
    ///
    /// <paramref name="path"/> = list of (pageId, slotIndex) from root to leaf,
    ///   used to navigate back up and insert the promoted separator.
    /// <paramref name="currentHeight"/> = the caller's current effective tree height
    ///   (shadow height for transactional callers; live height for auto-commit callers).
    ///
    /// Returns <c>(newRootId, newHeight)</c> if a root split occurred;
    /// returns <c>(0, 0)</c> if the root was not changed.
    /// After this returns the caller re-descends to insert the original key.
    /// </summary>
    public (uint newRootId, uint newHeight) SplitLeaf(
        uint leafPageId,
        ReadOnlySpan<(uint pageId, int childIndex)> path,
        int  pathCount,
        uint currentHeight)
    {
        if (pathCount == 0)
            return SplitRoot(leafPageId, currentHeight);

        var (promotedKey, rightLeafId) = SplitLeafNode(leafPageId);
        return InsertSeparatorUp(leafPageId, promotedKey, rightLeafId, path, pathCount, currentHeight);
    }

    /// <summary>
    /// Called when an internal node overflows during key promotion.
    /// Splits the internal node, promotes its median key one level up.
    ///
    /// Returns <c>(newRootId, newHeight)</c> if a root split occurred;
    /// returns <c>(0, 0)</c> otherwise.
    /// </summary>
    public (uint newRootId, uint newHeight) SplitInternal(
        uint internalPageId,
        ReadOnlySpan<(uint pageId, int childIndex)> path,
        int  pathCount,
        uint currentHeight)
    {
        var leftFrame = _pageManager.FetchPage(internalPageId);
        _tx?.CaptureBeforeImage(internalPageId, leftFrame.Data);   // Category A — capture before mutation
        var left      = _nodeSerializer.AsInternal(leftFrame);

        int n        = left.KeyCount;
        int midIndex = n / 2;

        TKey promotedKey = left.GetKey(midIndex);

        var rightFrame = _pageManager.AllocatePage(PageType.Internal);
        _tx?.TrackAllocatedPage(rightFrame.PageId);                 // Category B — track new alloc
        var right      = _nodeSerializer.AsInternal(rightFrame);
        right.Initialize(left.GetChildId(midIndex));

        for (int i = midIndex + 1; i < n; i++)
            right.TryAppend(left.GetKey(i), left.GetChildId(i));

        for (int i = n - 1; i >= midIndex; i--)
            left.RemoveSeparator(i);

        if (_tx != null)
        {
            var lsn = _pageManager.MarkDirtyAndUnpin(internalPageId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(internalPageId));
            _tx.UpdateLastLsn(lsn);
        }
        else { _pageManager.MarkDirtyAndUnpin(internalPageId); }
        _pageManager.MarkDirtyAndUnpin(rightFrame.PageId);          // auto-commit — new page, no before-image

        return InsertSeparatorUp(internalPageId, promotedKey, rightFrame.PageId, path, pathCount, currentHeight);
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    /// <summary>
    /// Called when the root is a leaf and it is full (tree height = 1).
    /// Creates two leaf children, creates a new internal root.
    /// Returns (newRootId, newHeight) — caller is responsible for updating metadata.
    /// </summary>
    private (uint newRootId, uint newHeight) SplitRoot(uint rootLeafPageId, uint currentHeight)
    {
        var (promotedKey, rightLeafId) = SplitLeafNode(rootLeafPageId);

        // Create new internal root; leftmost child = original (left) leaf page.
        var rootFrame = _pageManager.AllocatePage(PageType.Internal);
        _tx?.TrackAllocatedPage(rootFrame.PageId);                  // Category B — track new alloc
        var root      = _nodeSerializer.AsInternal(rootFrame);
        root.Initialize(rootLeafPageId);
        root.TryAppend(promotedKey, rightLeafId);
        _pageManager.MarkDirtyAndUnpin(rootFrame.PageId);           // new page, no before-image

        return (rootFrame.PageId, currentHeight + 1);
    }

    /// <summary>
    /// Splits a full leaf page in half. The original page retains the left (lower) half.
    /// Allocates a new right leaf, fixes sibling pointers, marks both dirty.
    /// Returns (promotedKey, rightLeafPageId).
    /// </summary>
    private (TKey promotedKey, uint rightLeafId) SplitLeafNode(uint leafPageId)
    {
        var leftFrame = _pageManager.FetchPage(leafPageId);
        _tx?.CaptureBeforeImage(leafPageId, leftFrame.Data);        // Category A — capture before mutation
        var leftLeaf  = _nodeSerializer.AsLeaf(leftFrame);

        int n         = leftLeaf.Count;
        int leftCount = (n + 1) / 2;   // left retains floor((n+1)/2) entries

        var rightFrame = _pageManager.AllocatePage(PageType.Leaf);
        _tx?.TrackAllocatedPage(rightFrame.PageId);                 // Category B — track new alloc
        var rightLeaf  = _nodeSerializer.AsLeaf(rightFrame);
        rightLeaf.Initialize();

        // Copy right half from left into the new right leaf (in ascending key order).
        for (int i = leftCount; i < n; i++)
            rightLeaf.TryInsert(leftLeaf.GetKey(i), leftLeaf.GetValue(i));

        // Remove right half from left (iterate from the end to keep earlier indices stable).
        for (int i = n - 1; i >= leftCount; i--)
            leftLeaf.Remove(leftLeaf.GetKey(i));

        // Stitch sibling doubly-linked list.
        uint oldNextId = leftLeaf.NextLeafPageId;
        leftLeaf.NextLeafPageId  = rightFrame.PageId;
        rightLeaf.PrevLeafPageId = leafPageId;
        rightLeaf.NextLeafPageId = oldNextId;
        if (oldNextId != PageLayout.NullPageId)
        {
            var oldNextFrame = _pageManager.FetchPage(oldNextId);
            _tx?.CaptureBeforeImage(oldNextId, oldNextFrame.Data);  // Category A — conditional, capture before mutation
            var oldNextLeaf = _nodeSerializer.AsLeaf(oldNextFrame);
            oldNextLeaf.PrevLeafPageId = rightFrame.PageId;
            if (_tx != null)
            {
                var lsn = _pageManager.MarkDirtyAndUnpin(oldNextId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(oldNextId));
                _tx.UpdateLastLsn(lsn);
            }
            else { _pageManager.MarkDirtyAndUnpin(oldNextId); }
        }

        TKey promotedKey = rightLeaf.GetKey(0);  // copy-up: first key of right sibling
        uint rightId     = rightFrame.PageId;

        if (_tx != null)
        {
            var lsn = _pageManager.MarkDirtyAndUnpin(leafPageId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(leafPageId));
            _tx.UpdateLastLsn(lsn);
        }
        else { _pageManager.MarkDirtyAndUnpin(leafPageId); }
        _pageManager.MarkDirtyAndUnpin(rightId);                    // new page, no before-image

        return (promotedKey, rightId);
    }

    /// <summary>
    /// Propagates a separator (promotedKey, rightChildId) upward along <paramref name="path"/>.
    /// If the parent has room: inserts and returns (0, 0).
    /// If the parent is full: splits it (handling the pending separator) and recurses.
    /// If <paramref name="path"/> is empty: creates a new internal root and returns its ID.
    /// <paramref name="leftChildId"/> = the page that was just split (becomes left half; needed
    /// to wire the new root's leftmost child pointer when path is empty).
    /// </summary>
    private (uint newRootId, uint newHeight) InsertSeparatorUp(
        uint leftChildId,
        TKey promotedKey,
        uint rightChildId,
        ReadOnlySpan<(uint pageId, int childIndex)> path,
        int  pathCount,
        uint currentHeight)
    {
        if (pathCount == 0)
        {
            // The node we split WAS the root. Wrap it in a new internal root.
            var newRootFrame = _pageManager.AllocatePage(PageType.Internal);
            _tx?.TrackAllocatedPage(newRootFrame.PageId);           // Category B — track new alloc
            var newRoot      = _nodeSerializer.AsInternal(newRootFrame);
            newRoot.Initialize(leftChildId);
            newRoot.TryAppend(promotedKey, rightChildId);
            _pageManager.MarkDirtyAndUnpin(newRootFrame.PageId);    // new page, no before-image
            return (newRootFrame.PageId, currentHeight + 1);
        }

        var (parentId, _) = path[pathCount - 1];
        var parentFrame   = _pageManager.FetchPage(parentId);
        _tx?.CaptureBeforeImage(parentId, parentFrame.Data);        // Category A — capture before mutation
        var parent        = _nodeSerializer.AsInternal(parentFrame);

        if (parent.TryInsertSeparator(promotedKey, rightChildId))
        {
            if (_tx != null)
            {
                var lsn = _pageManager.MarkDirtyAndUnpin(parentId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(parentId));
                _tx.UpdateLastLsn(lsn);
            }
            else { _pageManager.MarkDirtyAndUnpin(parentId); }
            return (0, 0);
        }

        // Parent is full — split it and handle the pending separator in one pass.
        // Pass path with count - 1 (grandparent slice) — zero allocation, no LINQ. (Phase 33)
        _pageManager.Unpin(parentId);

        var (parentPromoted, rightParentId) =
            SplitInternalAndInsert(parentId, promotedKey, rightChildId);

        return InsertSeparatorUp(parentId, parentPromoted, rightParentId, path, pathCount - 1, currentHeight);
    }

    /// <summary>
    /// Splits a full internal node and simultaneously inserts the pending separator
    /// (pendingKey, pendingRightChild) into the correct half.
    /// Returns (promotedKey, rightInternalPageId).
    /// The promoted key is pushed up (not copied into either child).
    /// </summary>
    private (TKey promotedKey, uint rightPageId) SplitInternalAndInsert(
        uint internalPageId, TKey pendingKey, uint pendingRightChild)
    {
        var leftFrame = _pageManager.FetchPage(internalPageId);
        _tx?.CaptureBeforeImage(internalPageId, leftFrame.Data);    // Category A — capture before mutation
        var left      = _nodeSerializer.AsInternal(leftFrame);

        int n        = left.KeyCount;
        int midIndex = n / 2;

        TKey promotedKey = left.GetKey(midIndex);

        var rightFrame = _pageManager.AllocatePage(PageType.Internal);
        _tx?.TrackAllocatedPage(rightFrame.PageId);                 // Category B — track new alloc
        var right      = _nodeSerializer.AsInternal(rightFrame);
        // Right's leftmost child = child pointer immediately to the right of the promoted key.
        right.Initialize(left.GetChildId(midIndex));

        // Copy keys [midIndex+1 .. n-1] to the right node.
        for (int i = midIndex + 1; i < n; i++)
            right.TryAppend(left.GetKey(i), left.GetChildId(i));

        // Remove keys [midIndex .. n-1] from left (mid is promoted, not kept in either child).
        for (int i = n - 1; i >= midIndex; i--)
            left.RemoveSeparator(i);

        // Route the pending separator into the correct half based on key ordering.
        var ks = _nodeSerializer.KeySerializer;
        if (ks.Compare(pendingKey, promotedKey) < 0)
            left.TryInsertSeparator(pendingKey, pendingRightChild);
        else
            right.TryInsertSeparator(pendingKey, pendingRightChild);

        if (_tx != null)
        {
            var lsn = _pageManager.MarkDirtyAndUnpin(internalPageId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(internalPageId));
            _tx.UpdateLastLsn(lsn);
        }
        else { _pageManager.MarkDirtyAndUnpin(internalPageId); }
        _pageManager.MarkDirtyAndUnpin(rightFrame.PageId);          // new page, no before-image

        return (promotedKey, rightFrame.PageId);
    }
}
