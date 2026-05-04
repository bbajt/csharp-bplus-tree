using ByTech.BPlusTree.Core.Nodes;
using ByTech.BPlusTree.Core.Storage;

namespace ByTech.BPlusTree.Core.Engine;

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
        {
            if (!right.TryAppend(left.GetKey(i), left.GetChildId(i)))
                throw new InvalidOperationException(
                    $"Splitter internal-node split: TryAppend of separator {i} into fresh right node failed. " +
                    "Variable-size separator key total exceeds page capacity.");
        }

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
        if (n == 0)
        {
            // M93: callers should never request a split on an empty leaf. When this fires,
            // TreeEngine.Insert's isLeafFull check disagreed with LeafNode.Count — a page-
            // state consistency bug that needs its own follow-up investigation (see M94
            // carry-forward in DESIGN-DEBT). Throw explicitly so the bug surfaces rather
            // than degrading silently; the M93 Phase 2 instrumentation captures the stack.
            throw new InvalidOperationException(
                $"SplitLeafNode called on an empty leaf (page {leafPageId}). This indicates " +
                "the caller's isLeafFull check disagreed with LeafNode.Count. Tracked as " +
                "an M94+ carry-forward; the M93 n=1 fix addresses the primary symptom.");
        }

        // M93: classic split ratio leaves floor((n+1)/2) on the left, the rest on the right.
        // The B+ tree invariant (MaxEntrySize ≤ pageSize/2) promises a leaf with ≥ 2 entries
        // always splits non-empty on both sides. **But** the n == 1 case — a leaf with a
        // single entry where HasSpaceFor returned false for the incoming insert — produces
        // leftCount = 1 = n, so the right leaf receives no entries, and the copy-up at
        // `rightLeaf.GetKey(0)` throws ArgumentOutOfRangeException (tracked through an
        // entire cluster-mutation stack in M93-B repro). Defensive fix: move the single
        // entry into the right leaf so the copy-up invariant holds and the caller's
        // re-descend routes the new key into whichever half owns its comparison range.
        int leftCount = n == 1 ? 0 : (n + 1) / 2;

        var rightFrame = _pageManager.AllocatePage(PageType.Leaf);
        _tx?.TrackAllocatedPage(rightFrame.PageId);                 // Category B — track new alloc
        var rightLeaf  = _nodeSerializer.AsLeaf(rightFrame);
        rightLeaf.Initialize();

        // Copy right half from left into the new right leaf (in ascending key order).
        for (int i = leftCount; i < n; i++)
        {
            if (!rightLeaf.TryInsert(leftLeaf.GetKey(i), leftLeaf.GetValue(i)))
                throw new InvalidOperationException(
                    $"Splitter.SplitLeafNode: TryInsert of entry {i}/{n} into fresh right leaf failed. " +
                    "Variable-size value workload may have entries that don't fit even into an empty leaf, " +
                    "or count-based split point doesn't balance bytes.");
        }

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

        int n = left.KeyCount;

        // M141 P3: byte-aware split-point selection + rebuild-from-scratch.
        // The original code chose midIndex = n/2 (count-based) and then routed the
        // pending separator with TryInsertSeparator (bool ignored). With variable-
        // size separator keys (e.g. string), the receiving half could have
        // insufficient bytes — the pending separator AND its child pointer would
        // silently drop, orphaning a leaf from the tree's parent pointers while
        // the leaf chain still included it. Root cause for DEBT-BPT-M140-residual.
        //
        // Fix: gather all separators + children, choose midIndex by BYTES so each
        // half (including the pending entry on its target side) fits, then rebuild
        // both halves from scratch (Initialize + TryAppend). Initialize reclaims
        // any orphan bytes, so byte calc is exact.
        int usable = _pageManager.PageSize - PageLayout.FirstSlotOffset;
        var keys     = new TKey[n];
        var children = new uint[n + 1];
        children[0] = left.LeftmostChildId;
        for (int i = 0; i < n; i++)
        {
            keys[i]         = left.GetKey(i);
            children[i + 1] = left.GetChildId(i);
        }

        int pendingEntrySize = _nodeSerializer.KeySerializer.MeasureSize(pendingKey)
                             + 4 + PageLayout.SlotEntrySize;
        var sepSizes = new int[n];
        for (int i = 0; i < n; i++)
            sepSizes[i] = _nodeSerializer.KeySerializer.MeasureSize(keys[i])
                        + 4 + PageLayout.SlotEntrySize;

        int chosenMid = -1;
        for (int delta = 0; delta <= n / 2 && chosenMid < 0; delta++)
        {
            int[] candidates = delta == 0 ? new[] { n / 2 } : new[] { n / 2 - delta, n / 2 + delta };
            foreach (int candidate in candidates)
            {
                if (candidate < 1 || candidate > n - 1) continue;
                int leftBytes = 0;  for (int j = 0; j < candidate; j++) leftBytes += sepSizes[j];
                int rightBytes = 0; for (int j = candidate + 1; j < n; j++) rightBytes += sepSizes[j];
                if (leftBytes > usable || rightBytes > usable) continue;
                int cmp = _nodeSerializer.KeySerializer.Compare(pendingKey, keys[candidate]);
                bool fits = cmp < 0
                    ? leftBytes + pendingEntrySize <= usable
                    : cmp > 0 && rightBytes + pendingEntrySize <= usable;
                if (fits) { chosenMid = candidate; break; }
            }
        }
        if (chosenMid < 0)
            throw new InvalidOperationException(
                $"SplitInternalAndInsert: cannot find a split point that accommodates pending separator " +
                $"(pendingEntry={pendingEntrySize}B, usable={usable}B). Tree invariant violated.");

        int midIndex = chosenMid;
        TKey promotedKey = keys[midIndex];

        // Allocate right and reinit both nodes from scratch to drop orphan bytes.
        var rightFrame = _pageManager.AllocatePage(PageType.Internal);
        _tx?.TrackAllocatedPage(rightFrame.PageId);
        var right = _nodeSerializer.AsInternal(rightFrame);

        // Left = separators [0..midIndex-1] with children [0..midIndex].
        left.Initialize(children[0]);
        for (int i = 0; i < midIndex; i++)
        {
            if (!left.TryAppend(keys[i], children[i + 1]))
                throw new InvalidOperationException(
                    $"SplitInternalAndInsert: rebuild left TryAppend({i}) failed despite byte preflight.");
        }
        // Right = separators [midIndex+1..n-1] with children [midIndex+1..n], leftmost = children[midIndex+1].
        right.Initialize(children[midIndex + 1]);
        for (int i = midIndex + 1; i < n; i++)
        {
            if (!right.TryAppend(keys[i], children[i + 1]))
                throw new InvalidOperationException(
                    $"SplitInternalAndInsert: rebuild right TryAppend({i}) failed despite byte preflight.");
        }

        // Insert pending separator into its target half.
        var ks = _nodeSerializer.KeySerializer;
        if (ks.Compare(pendingKey, promotedKey) < 0)
        {
            if (!left.TryInsertSeparator(pendingKey, pendingRightChild))
                throw new InvalidOperationException(
                    "SplitInternalAndInsert: TryInsertSeparator into LEFT half failed despite byte preflight.");
        }
        else
        {
            if (!right.TryInsertSeparator(pendingKey, pendingRightChild))
                throw new InvalidOperationException(
                    "SplitInternalAndInsert: TryInsertSeparator into RIGHT half failed despite byte preflight.");
        }

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
