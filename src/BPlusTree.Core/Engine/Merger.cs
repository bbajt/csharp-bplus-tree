using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;

namespace BPlusTree.Core.Engine;

/// <summary>
/// Handles underflow after deletion: borrow from a sibling or merge with a sibling.
///
/// Underflow threshold: node has fewer than ceil(maxEntries / 2) entries.
/// Root is exempt — may have as few as 1 key (or 0 entries after all deletes).
///
/// Decision order for an underflowing node:
///   1. Can borrow from right sibling? (right sibling has > threshold entries) → BorrowFromRight
///   2. Can borrow from left  sibling? (left  sibling has > threshold entries) → BorrowFromLeft
///   3. Neither: merge with right sibling (preferred) or left sibling.
///
/// Leaf merge invariants:
///   - All entries from right leaf move into left leaf.
///   - Right leaf is freed (added to FreeList).
///   - Sibling pointers updated: left.Next = right.Next; right.Next.Prev = left.
///   - Separator key removed from parent (may trigger parent underflow — handled in 17b/17c).
///
/// CollapseRoot:
///   - Called when root internal node reaches 0 keys (its only child becomes the new root).
///   - Root page freed. Metadata.RootPageId and TreeHeight updated.
/// </summary>
public sealed class Merger<TKey, TValue>
    where TKey : notnull
{
    private readonly PageManager                  _pageManager;
    private readonly NodeSerializer<TKey, TValue> _nodeSerializer;
    private readonly TreeMetadata                 _metadata;
    private readonly ITransactionContext?         _tx;

    internal Merger(
        PageManager pageManager,
        NodeSerializer<TKey, TValue> nodeSerializer,
        TreeMetadata metadata,
        ITransactionContext? tx = null)
    {
        _pageManager    = pageManager;
        _nodeSerializer = nodeSerializer;
        _metadata       = metadata;
        _tx             = tx;
    }

    // ── Public entry points ───────────────────────────────────────────────────

    /// <summary>
    /// Called after a key is removed from a leaf and leaf.Count dropped below threshold.
    /// path = list of (pageId, childIndex) from root down to (but not including) the leaf.
    /// </summary>
    public void RebalanceLeaf(uint leafPageId, ReadOnlySpan<(uint pageId, int childIndex)> path, int pathCount)
    {
        // 1. Root leaf — exempt from underflow requirements.
        if (pathCount == 0) return;

        // 2. Get parent and determine sibling IDs before unpinning.
        var (parentId, childPos) = path[pathCount - 1];
        var parentFrame = _pageManager.FetchPage(parentId);
        var parent      = _nodeSerializer.AsInternal(parentFrame);

        bool hasRight = childPos < parent.KeyCount;
        bool hasLeft  = childPos > 0;

        uint rightId = hasRight
            ? parent.GetChildId(childPos)
            : PageLayout.NullPageId;

        uint leftId = !hasLeft ? PageLayout.NullPageId
            : childPos == 1   ? parent.LeftmostChildId
                              : parent.GetChildId(childPos - 2);

        // 3. Try borrow from right sibling.
        if (hasRight)
        {
            var rightFrame = _pageManager.FetchPage(rightId);
            int rightCount = _nodeSerializer.AsLeaf(rightFrame).Count;
            _pageManager.Unpin(rightId);

            if (rightCount > LeafThreshold())
            {
                _pageManager.Unpin(parentId);
                BorrowFromRightLeaf(leafPageId, rightId, parentId, childPos);
                return;
            }
        }

        // 4. Try borrow from left sibling.
        if (hasLeft)
        {
            var leftFrame = _pageManager.FetchPage(leftId);
            int leftCount = _nodeSerializer.AsLeaf(leftFrame).Count;
            _pageManager.Unpin(leftId);

            if (leftCount > LeafThreshold())
            {
                _pageManager.Unpin(parentId);
                BorrowFromLeftLeaf(leftId, leafPageId, parentId, childPos - 1);
                return;
            }
        }

        // 5. Neither borrow possible — merge.
        _pageManager.Unpin(parentId);

        if (hasRight)
            MergeLeaves(leafPageId, rightId, parentId, childPos);
        else
            MergeLeaves(leftId, leafPageId, parentId, childPos - 1);

        // 6. Check parent after merge.
        var parentFrame2  = _pageManager.FetchPage(parentId);
        var parent2       = _nodeSerializer.AsInternal(parentFrame2);
        int parentKeyCount = parent2.KeyCount;
        _pageManager.Unpin(parentId);

        if (parentKeyCount < InternalThreshold() && pathCount > 1)
            RebalanceInternal(parentId, path, pathCount - 1); // grandparent slice — zero allocation (Phase 33)

        // 7. Collapse root when it has 0 keys (only when parent IS the root).
        if (parentKeyCount == 0 && pathCount == 1)
            CollapseRoot();
    }

    /// <summary>
    /// Called after an internal node loses a separator (due to a child merge) and its key count
    /// dropped below threshold. Mirrors RebalanceLeaf but operates on internal nodes.
    /// path = span of (pageId, childIndex) from root down to (but not including) the underflowing node.
    /// </summary>
    public void RebalanceInternal(uint internalPageId, ReadOnlySpan<(uint pageId, int childIndex)> path, int pathCount)
    {
        // Root is exempt from underflow.
        if (pathCount == 0) return;

        var (parentId, childPos) = path[pathCount - 1];
        var parentFrame = _pageManager.FetchPage(parentId);
        var parent      = _nodeSerializer.AsInternal(parentFrame);

        bool hasRight = childPos < parent.KeyCount;
        bool hasLeft  = childPos > 0;

        uint rightId = hasRight
            ? parent.GetChildId(childPos)
            : PageLayout.NullPageId;

        uint leftId = !hasLeft ? PageLayout.NullPageId
            : childPos == 1   ? parent.LeftmostChildId
                              : parent.GetChildId(childPos - 2);

        // Try borrow from right sibling.
        if (hasRight)
        {
            var rightFrame = _pageManager.FetchPage(rightId);
            int rightCount = _nodeSerializer.AsInternal(rightFrame).KeyCount;
            _pageManager.Unpin(rightId);

            if (rightCount > InternalThreshold())
            {
                _pageManager.Unpin(parentId);
                BorrowFromRightInternal(internalPageId, rightId, parentId, childPos);
                return;
            }
        }

        // Try borrow from left sibling.
        if (hasLeft)
        {
            var leftFrame = _pageManager.FetchPage(leftId);
            int leftCount = _nodeSerializer.AsInternal(leftFrame).KeyCount;
            _pageManager.Unpin(leftId);

            if (leftCount > InternalThreshold())
            {
                _pageManager.Unpin(parentId);
                BorrowFromLeftInternal(leftId, internalPageId, parentId, childPos - 1);
                return;
            }
        }

        // Neither borrow possible — merge.
        _pageManager.Unpin(parentId);

        if (hasRight)
            MergeInternals(internalPageId, rightId, parentId, childPos);
        else
            MergeInternals(leftId, internalPageId, parentId, childPos - 1);

        // Check parent after merge.
        var parentFrame2   = _pageManager.FetchPage(parentId);
        var parent2        = _nodeSerializer.AsInternal(parentFrame2);
        int parentKeyCount = parent2.KeyCount;
        _pageManager.Unpin(parentId);

        if (parentKeyCount < InternalThreshold() && pathCount > 1)
            RebalanceInternal(parentId, path, pathCount - 1); // grandparent slice — zero allocation (Phase 33)

        if (parentKeyCount == 0 && pathCount == 1)
            CollapseRoot();
    }

    // ── Threshold helpers ─────────────────────────────────────────────────────

    /// <summary>Minimum entry count for a leaf before underflow. ceil(maxCapacity / 2).</summary>
    public int LeafThreshold()
    {
        int ks           = _nodeSerializer.KeySerializer.FixedSize;
        int vs           = _nodeSerializer.ValueSerializer.FixedSize;
        int pageDataSize = _pageManager.PageSize - PageLayout.FirstSlotOffset;
        int entrySize    = ks + vs + PageLayout.SlotEntrySize;
        int maxCapacity  = pageDataSize / entrySize;
        return (maxCapacity + 1) / 2;
    }

    // ── Leaf operations ───────────────────────────────────────────────────────

    /// <summary>
    /// Move the FIRST entry of rightLeaf to the END of leftLeaf.
    /// Update the separator key in parent[separatorIndex] to the new first key of rightLeaf.
    /// Both pages marked dirty.
    /// Compacts leftLeaf first if orphaned cells would prevent the insert.
    /// </summary>
    private void BorrowFromRightLeaf(uint leftId, uint rightId, uint parentId, int separatorIndex)
    {
        var leftFrame   = _pageManager.FetchPage(leftId);
        _tx?.CaptureBeforeImage(leftId, leftFrame.Data);        // Category A
        var leftLeaf    = _nodeSerializer.AsLeaf(leftFrame);
        var rightFrame  = _pageManager.FetchPage(rightId);
        _tx?.CaptureBeforeImage(rightId, rightFrame.Data);      // Category A
        var rightLeaf   = _nodeSerializer.AsLeaf(rightFrame);
        var parentFrame = _pageManager.FetchPage(parentId);
        _tx?.CaptureBeforeImage(parentId, parentFrame.Data);    // Category A
        var parent      = _nodeSerializer.AsInternal(parentFrame);

        // Compact left leaf if accumulated orphaned cells would cause TryInsert to fail.
        int ks = _nodeSerializer.KeySerializer.FixedSize;
        int vs = _nodeSerializer.ValueSerializer.FixedSize;
        if (!leftLeaf.HasSpaceFor(ks, vs))
            CompactLeaf(leftLeaf);

        TKey   movedKey   = rightLeaf.GetKey(0);
        TValue movedValue = rightLeaf.GetValue(0);
        rightLeaf.Remove(movedKey);
        leftLeaf.TryInsert(movedKey, movedValue);

        TKey newSeparator = rightLeaf.GetKey(0);
        parent.UpdateSeparatorKey(separatorIndex, newSeparator);

        if (_tx != null)
        {
            var lsn = _pageManager.MarkDirtyAndUnpin(leftId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(leftId));
            _tx.UpdateLastLsn(lsn);
            lsn = _pageManager.MarkDirtyAndUnpin(rightId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(rightId));
            _tx.UpdateLastLsn(lsn);
            lsn = _pageManager.MarkDirtyAndUnpin(parentId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(parentId));
            _tx.UpdateLastLsn(lsn);
        }
        else
        {
            _pageManager.MarkDirtyAndUnpin(leftId);
            _pageManager.MarkDirtyAndUnpin(rightId);
            _pageManager.MarkDirtyAndUnpin(parentId);
        }
    }

    /// <summary>
    /// Move the LAST entry of leftLeaf to the START of rightLeaf.
    /// Update the separator key in parent[separatorIndex] to the moved key.
    /// Both pages marked dirty.
    /// Compacts rightLeaf first if orphaned cells would prevent the insert.
    /// </summary>
    private void BorrowFromLeftLeaf(uint leftId, uint rightId, uint parentId, int separatorIndex)
    {
        var leftFrame   = _pageManager.FetchPage(leftId);
        _tx?.CaptureBeforeImage(leftId, leftFrame.Data);        // Category A
        var leftLeaf    = _nodeSerializer.AsLeaf(leftFrame);
        var rightFrame  = _pageManager.FetchPage(rightId);
        _tx?.CaptureBeforeImage(rightId, rightFrame.Data);      // Category A
        var rightLeaf   = _nodeSerializer.AsLeaf(rightFrame);
        var parentFrame = _pageManager.FetchPage(parentId);
        _tx?.CaptureBeforeImage(parentId, parentFrame.Data);    // Category A
        var parent      = _nodeSerializer.AsInternal(parentFrame);

        // Compact right leaf if accumulated orphaned cells would cause TryInsert to fail.
        int ks = _nodeSerializer.KeySerializer.FixedSize;
        int vs = _nodeSerializer.ValueSerializer.FixedSize;
        if (!rightLeaf.HasSpaceFor(ks, vs))
            CompactLeaf(rightLeaf);

        int    lastIdx    = leftLeaf.Count - 1;
        TKey   movedKey   = leftLeaf.GetKey(lastIdx);
        TValue movedValue = leftLeaf.GetValue(lastIdx);
        leftLeaf.Remove(movedKey);
        rightLeaf.TryInsert(movedKey, movedValue);

        parent.UpdateSeparatorKey(separatorIndex, movedKey);

        if (_tx != null)
        {
            var lsn = _pageManager.MarkDirtyAndUnpin(leftId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(leftId));
            _tx.UpdateLastLsn(lsn);
            lsn = _pageManager.MarkDirtyAndUnpin(rightId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(rightId));
            _tx.UpdateLastLsn(lsn);
            lsn = _pageManager.MarkDirtyAndUnpin(parentId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(parentId));
            _tx.UpdateLastLsn(lsn);
        }
        else
        {
            _pageManager.MarkDirtyAndUnpin(leftId);
            _pageManager.MarkDirtyAndUnpin(rightId);
            _pageManager.MarkDirtyAndUnpin(parentId);
        }
    }

    /// <summary>
    /// Reinitialize a leaf page in place, reclaiming all orphaned cell bytes.
    /// Preserves all current entries and sibling pointers.
    /// Called when FreeSpaceSize has been depleted by orphaned cells from prior borrows.
    /// </summary>
    private void CompactLeaf(LeafNode<TKey, TValue> leaf)
    {
        int lc = leaf.Count;
        var keys   = new TKey[lc];
        var values = new TValue[lc];
        for (int i = 0; i < lc; i++) { keys[i] = leaf.GetKey(i); values[i] = leaf.GetValue(i); }
        uint prevId = leaf.PrevLeafPageId;
        uint nextId = leaf.NextLeafPageId;
        leaf.Initialize();
        leaf.PrevLeafPageId = prevId;
        leaf.NextLeafPageId = nextId;
        for (int i = 0; i < lc; i++) leaf.TryInsert(keys[i], values[i]);
    }

    /// <summary>
    /// Move ALL entries from rightLeaf into leftLeaf.
    /// Update sibling pointers: leftLeaf.Next = rightLeaf.Next.
    /// If rightLeaf.Next != NullPageId: fetch that page and set its PrevLeafPageId = leftId.
    /// Free rightLeaf (FreeList).
    /// Remove separator at separatorIndex from parent.
    /// Mark leftLeaf and parent dirty.
    /// Does NOT recursively rebalance parent — caller handles that.
    /// </summary>
    private void MergeLeaves(uint leftId, uint rightId, uint parentId, int separatorIndex)
    {
        var leftFrame   = _pageManager.FetchPage(leftId);
        _tx?.CaptureBeforeImage(leftId, leftFrame.Data);        // Category A
        var leftLeaf    = _nodeSerializer.AsLeaf(leftFrame);
        var rightFrame  = _pageManager.FetchPage(rightId);
        var rightLeaf   = _nodeSerializer.AsLeaf(rightFrame);   // rightId: read only → Category C (deferred free)
        var parentFrame = _pageManager.FetchPage(parentId);
        _tx?.CaptureBeforeImage(parentId, parentFrame.Data);    // Category A
        var parent      = _nodeSerializer.AsInternal(parentFrame);

        // Collect all entries from both leaves. Left entries have lower keys than right entries.
        int lc = leftLeaf.Count, rc = rightLeaf.Count;
        var keys   = new TKey[lc + rc];
        var values = new TValue[lc + rc];
        for (int i = 0; i < lc; i++) { keys[i]      = leftLeaf.GetKey(i);  values[i]      = leftLeaf.GetValue(i); }
        for (int i = 0; i < rc; i++) { keys[lc + i]  = rightLeaf.GetKey(i); values[lc + i] = rightLeaf.GetValue(i); }

        // Preserve sibling pointers before reinitializing.
        uint prevId    = leftLeaf.PrevLeafPageId;
        uint oldNextId = rightLeaf.NextLeafPageId;

        // Reinitialize left leaf to reclaim all orphaned cell space, then reinsert everything.
        leftLeaf.Initialize();
        leftLeaf.PrevLeafPageId = prevId;
        leftLeaf.NextLeafPageId = oldNextId;
        for (int i = 0; i < keys.Length; i++)
            leftLeaf.TryInsert(keys[i], values[i]);

        // Fix forward sibling pointer.
        if (oldNextId != PageLayout.NullPageId)
        {
            var oldNextFrame = _pageManager.FetchPage(oldNextId);
            _tx?.CaptureBeforeImage(oldNextId, oldNextFrame.Data);  // Category A — conditional
            var oldNextLeaf = _nodeSerializer.AsLeaf(oldNextFrame);
            oldNextLeaf.PrevLeafPageId = leftId;
            if (_tx != null)
            {
                var lsn2 = _pageManager.MarkDirtyAndUnpin(oldNextId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(oldNextId));
                _tx.UpdateLastLsn(lsn2);
            }
            else { _pageManager.MarkDirtyAndUnpin(oldNextId); }
        }

        parent.RemoveSeparator(separatorIndex);

        if (_tx != null)
        {
            var lsn = _pageManager.MarkDirtyAndUnpin(leftId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(leftId));
            _tx.UpdateLastLsn(lsn);
            lsn = _pageManager.MarkDirtyAndUnpin(parentId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(parentId));
            _tx.UpdateLastLsn(lsn);
        }
        else
        {
            _pageManager.MarkDirtyAndUnpin(leftId);
            _pageManager.MarkDirtyAndUnpin(parentId);
        }

        _pageManager.Unpin(rightId);
        if (_tx != null) _tx.DeferFreePage(rightId);    // Category C — deferred to commit
        else             _pageManager.FreePage(rightId);
    }

    /// <summary>
    /// Called when the root internal node has 0 keys remaining (single child left).
    /// The single child becomes the new root.
    /// Old root page is freed.
    /// Metadata.RootPageId = singleChildId.
    /// Metadata.TreeHeight -= 1.
    /// Metadata.Flush().
    /// </summary>
    private void CollapseRoot()
    {
        var rootFrame     = _pageManager.FetchPage(_metadata.RootPageId);
        var root          = _nodeSerializer.AsInternal(rootFrame);
        uint singleChildId = root.LeftmostChildId;
        uint oldRootId    = _metadata.RootPageId;

        _pageManager.Unpin(oldRootId);
        if (_tx != null) _tx.DeferFreePage(oldRootId);  // Category C — deferred to commit
        else             _pageManager.FreePage(oldRootId);

        _metadata.SetRoot(singleChildId, _metadata.TreeHeight - 1);
        if (_tx != null) _metadata.Flush(_tx);
        else             _metadata.Flush();
    }

    // ── Internal operations — NOT implemented in Phase 17a ───────────────────

    /// <summary>
    /// Borrow the FIRST key+child of rightInternal and rotate it into leftInternal via parent separator.
    /// 1. Pull parent.separator[separatorIndex] into leftInternal as its last key;
    ///    leftInternal's new last child pointer = rightInternal.LeftmostChildId.
    /// 2. Promote rightInternal.GetKey(0) up to parent.separator[separatorIndex].
    /// 3. rightInternal.LeftmostChildId = rightInternal.GetChildId(0).
    /// 4. rightInternal.RemoveSeparator(0).
    /// 5. Mark all three pages dirty.
    /// </summary>
    private void BorrowFromRightInternal(uint leftId, uint rightId, uint parentId, int separatorIndex)
    {
        var leftFrame   = _pageManager.FetchPage(leftId);
        _tx?.CaptureBeforeImage(leftId, leftFrame.Data);        // Category A
        var leftNode    = _nodeSerializer.AsInternal(leftFrame);
        var rightFrame  = _pageManager.FetchPage(rightId);
        _tx?.CaptureBeforeImage(rightId, rightFrame.Data);      // Category A
        var rightNode   = _nodeSerializer.AsInternal(rightFrame);
        var parentFrame = _pageManager.FetchPage(parentId);
        _tx?.CaptureBeforeImage(parentId, parentFrame.Data);    // Category A
        var parent      = _nodeSerializer.AsInternal(parentFrame);

        TKey parentSep    = parent.GetKey(separatorIndex);
        uint rightLeftmost = rightNode.LeftmostChildId;

        // 1. Append parent separator to leftNode; its right child = rightNode.LeftmostChildId.
        leftNode.TryAppend(parentSep, rightLeftmost);

        // 2. Promote rightNode's first key to parent separator.
        TKey newSep = rightNode.GetKey(0);
        parent.UpdateSeparatorKey(separatorIndex, newSep);

        // 3. rightNode.LeftmostChildId = first child pointer of rightNode.
        rightNode.LeftmostChildId = rightNode.GetChildId(0);

        // 4. Remove the first separator from rightNode.
        rightNode.RemoveSeparator(0);

        if (_tx != null)
        {
            var lsn = _pageManager.MarkDirtyAndUnpin(leftId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(leftId));
            _tx.UpdateLastLsn(lsn);
            lsn = _pageManager.MarkDirtyAndUnpin(rightId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(rightId));
            _tx.UpdateLastLsn(lsn);
            lsn = _pageManager.MarkDirtyAndUnpin(parentId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(parentId));
            _tx.UpdateLastLsn(lsn);
        }
        else
        {
            _pageManager.MarkDirtyAndUnpin(leftId);
            _pageManager.MarkDirtyAndUnpin(rightId);
            _pageManager.MarkDirtyAndUnpin(parentId);
        }
    }

    /// <summary>
    /// Borrow the LAST key+child of leftInternal and rotate it into rightInternal via parent separator.
    /// 1. Pull parent.separator[separatorIndex] into rightInternal as its FIRST key;
    ///    rightInternal's new LeftmostChildId = leftInternal's last child pointer.
    /// 2. Promote leftInternal.GetKey(last) up to parent.separator[separatorIndex].
    /// 3. leftInternal.RemoveSeparator(last).
    /// 4. Mark all three pages dirty.
    /// </summary>
    private void BorrowFromLeftInternal(uint leftId, uint rightId, uint parentId, int separatorIndex)
    {
        var leftFrame   = _pageManager.FetchPage(leftId);
        _tx?.CaptureBeforeImage(leftId, leftFrame.Data);        // Category A
        var leftNode    = _nodeSerializer.AsInternal(leftFrame);
        var rightFrame  = _pageManager.FetchPage(rightId);
        _tx?.CaptureBeforeImage(rightId, rightFrame.Data);      // Category A
        var rightNode   = _nodeSerializer.AsInternal(rightFrame);
        var parentFrame = _pageManager.FetchPage(parentId);
        _tx?.CaptureBeforeImage(parentId, parentFrame.Data);    // Category A
        var parent      = _nodeSerializer.AsInternal(parentFrame);

        int  lastIdx          = leftNode.KeyCount - 1;
        TKey lastKeyOfLeft    = leftNode.GetKey(lastIdx);
        uint lastChildOfLeft  = leftNode.GetChildId(lastIdx);

        TKey parentSep        = parent.GetKey(separatorIndex);
        uint oldRightLeftmost = rightNode.LeftmostChildId;

        // 1. rightNode gets parent separator as its new first key.
        //    Its new LeftmostChildId = leftNode's last child; right child of new key = old rightNode leftmost.
        rightNode.LeftmostChildId = lastChildOfLeft;
        rightNode.TryInsertSeparator(parentSep, oldRightLeftmost);

        // 2. Promote leftNode's last key to parent separator.
        parent.UpdateSeparatorKey(separatorIndex, lastKeyOfLeft);

        // 3. Remove leftNode's last separator.
        leftNode.RemoveSeparator(lastIdx);

        if (_tx != null)
        {
            var lsn = _pageManager.MarkDirtyAndUnpin(leftId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(leftId));
            _tx.UpdateLastLsn(lsn);
            lsn = _pageManager.MarkDirtyAndUnpin(rightId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(rightId));
            _tx.UpdateLastLsn(lsn);
            lsn = _pageManager.MarkDirtyAndUnpin(parentId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(parentId));
            _tx.UpdateLastLsn(lsn);
        }
        else
        {
            _pageManager.MarkDirtyAndUnpin(leftId);
            _pageManager.MarkDirtyAndUnpin(rightId);
            _pageManager.MarkDirtyAndUnpin(parentId);
        }
    }

    /// <summary>
    /// Merge rightInternal into leftInternal, pulling the parent separator down.
    /// 1. Pull parent.separator[separatorIndex] into leftInternal; right child = rightInternal.LeftmostChildId.
    /// 2. Copy all keys and child pointers from rightInternal into leftInternal.
    /// 3. Free rightInternal.
    /// 4. Remove separator at separatorIndex from parent.
    /// 5. Mark leftInternal and parent dirty.
    /// Reinitializes leftInternal to reclaim any orphaned cell bytes before reinserting.
    /// </summary>
    private void MergeInternals(uint leftId, uint rightId, uint parentId, int separatorIndex)
    {
        var leftFrame   = _pageManager.FetchPage(leftId);
        _tx?.CaptureBeforeImage(leftId, leftFrame.Data);        // Category A
        var leftNode    = _nodeSerializer.AsInternal(leftFrame);
        var rightFrame  = _pageManager.FetchPage(rightId);
        var rightNode   = _nodeSerializer.AsInternal(rightFrame); // rightId: read only → Category C (deferred free)
        var parentFrame = _pageManager.FetchPage(parentId);
        _tx?.CaptureBeforeImage(parentId, parentFrame.Data);    // Category A
        var parent      = _nodeSerializer.AsInternal(parentFrame);

        // Collect all entries: leftNode entries + separator + rightNode entries.
        int lk = leftNode.KeyCount;
        int rk = rightNode.KeyCount;
        int totalKeys = lk + 1 + rk;

        var allKeys     = new TKey[totalKeys];
        var allChildren = new uint[totalKeys + 1]; // totalKeys + 1 children

        // Left entries.
        allChildren[0] = leftNode.LeftmostChildId;
        for (int i = 0; i < lk; i++)
        {
            allKeys[i]         = leftNode.GetKey(i);
            allChildren[i + 1] = leftNode.GetChildId(i);
        }

        // Parent separator pulls down; right child = rightNode.LeftmostChildId.
        allKeys[lk]         = parent.GetKey(separatorIndex);
        allChildren[lk + 1] = rightNode.LeftmostChildId;

        // Right entries.
        for (int j = 0; j < rk; j++)
        {
            allKeys[lk + 1 + j]     = rightNode.GetKey(j);
            allChildren[lk + 2 + j] = rightNode.GetChildId(j);
        }

        // Reinitialize leftNode to reclaim all orphaned cell bytes, then reinsert.
        leftNode.Initialize(allChildren[0]);
        for (int i = 0; i < totalKeys; i++)
            leftNode.TryAppend(allKeys[i], allChildren[i + 1]);

        parent.RemoveSeparator(separatorIndex);

        if (_tx != null)
        {
            var lsn = _pageManager.MarkDirtyAndUnpin(leftId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(leftId));
            _tx.UpdateLastLsn(lsn);
            lsn = _pageManager.MarkDirtyAndUnpin(parentId, _tx.TransactionId, _tx.LastLsn, _tx.GetBeforeImage(parentId));
            _tx.UpdateLastLsn(lsn);
        }
        else
        {
            _pageManager.MarkDirtyAndUnpin(leftId);
            _pageManager.MarkDirtyAndUnpin(parentId);
        }

        _pageManager.Unpin(rightId);
        if (_tx != null) _tx.DeferFreePage(rightId);    // Category C — deferred to commit
        else             _pageManager.FreePage(rightId);
    }

    // ── Private threshold helpers ─────────────────────────────────────────────

    private int InternalThreshold()
    {
        int ks           = _nodeSerializer.KeySerializer.FixedSize;
        int pageDataSize = _pageManager.PageSize - PageLayout.FirstSlotOffset;
        int entrySize    = ks + 4 + PageLayout.SlotEntrySize;  // key + child pointer (4 bytes)
        int maxCapacity  = pageDataSize / entrySize;
        return (maxCapacity + 1) / 2;
    }
}
