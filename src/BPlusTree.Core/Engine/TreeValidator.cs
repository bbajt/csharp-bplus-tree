using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;

namespace BPlusTree.Core.Engine;

/// <summary>
/// Validates the structural integrity of a B+ tree.
/// Walks the leaf chain from FirstLeafPageId and verifies all keys are in strict
/// ascending order. Additional invariants (separator alignment, sibling back-links)
/// can be added in later phases.
/// </summary>
internal sealed class TreeValidator<TKey, TValue>
    where TKey : notnull
{
    private readonly PageManager                  _pageManager;
    private readonly NodeSerializer<TKey, TValue> _nodeSerializer;
    private readonly TreeMetadata                 _metadata;

    public TreeValidator(
        PageManager pageManager,
        NodeSerializer<TKey, TValue> nodeSerializer,
        TreeMetadata metadata)
    {
        _pageManager    = pageManager;
        _nodeSerializer = nodeSerializer;
        _metadata       = metadata;
    }

    /// <summary>Run structural integrity checks on the B+ tree.</summary>
    public ValidationResult Validate()
    {
        if (_metadata.RootPageId == PageLayout.NullPageId)
            return ValidationResult.Valid;

        var errors = new List<string>();

        // ── Pass 1: leaf-chain walk — key order + record count ────────────────
        uint id              = _metadata.FirstLeafPageId;
        TKey? prev           = default;
        bool hasPrev         = false;
        long leafRecordTotal = 0;

        while (id != PageLayout.NullPageId)
        {
            var frame = _pageManager.FetchPage(id);
            var leaf  = _nodeSerializer.AsLeaf(frame);
            int count = leaf.Count;
            leafRecordTotal += count;

            for (int i = 0; i < count; i++)
            {
                TKey key = leaf.GetKey(i);
                if (hasPrev)
                {
                    int cmp = _nodeSerializer.KeySerializer.Compare(key, prev!);
                    if (cmp <= 0)
                        errors.Add($"Keys out of order at leaf {id}, slot {i}: {prev} >= {key}");
                }
                prev    = key;
                hasPrev = true;
            }

            uint next = leaf.NextLeafPageId;
            _pageManager.Unpin(id);
            id = next;
        }

        if ((ulong)leafRecordTotal != _metadata.TotalRecordCount)
            errors.Add($"Record count mismatch: leaf chain has {leafRecordTotal} records, " +
                       $"metadata reports {_metadata.TotalRecordCount}.");

        // ── Pass 2: DFS from root — child pointer validity + separator alignment
        var stack = new Stack<uint>();
        stack.Push(_metadata.RootPageId);

        while (stack.Count > 0)
        {
            uint pageId = stack.Pop();
            var  frame  = _pageManager.FetchPage(pageId);
            try
            {
                if (!NodeSerializer<TKey, TValue>.IsLeaf(frame))
                {
                    var  node     = new InternalNode<TKey>(frame, _nodeSerializer.KeySerializer);
                    int  keyCount = node.KeyCount;
                    uint leftmost = node.LeftmostChildId;

                    if (leftmost == PageLayout.NullPageId)
                    {
                        errors.Add($"Internal node {pageId}: LeftmostChildId is NullPageId.");
                        continue;   // can't traverse — skip this node's entire subtree
                    }
                    stack.Push(leftmost);

                    for (int i = 0; i < keyCount; i++)
                    {
                        uint childId = node.GetChildId(i);
                        if (childId == PageLayout.NullPageId)
                        {
                            errors.Add($"Internal node {pageId}: child[{i}] is NullPageId.");
                            continue;   // skip this child's subtree, continue loop
                        }

                        TKey separator = node.GetKey(i);

                        // Forward check: separator must not exceed first key of right child.
                        var (fOk, childFirst, fErr) = TryGetFirstKey(childId);
                        if (!fOk) { errors.Add(fErr); continue; }
                        if (_nodeSerializer.KeySerializer.Compare(separator, childFirst) > 0)
                            errors.Add($"Internal node {pageId}: separator[{i}]={separator} " +
                                       $"> first key of right child {childId} ({childFirst}). " +
                                       "Separator alignment violated (forward check).");

                        // Backward check: separator must exceed last key of left subtree.
                        uint leftChild = (i == 0) ? leftmost : node.GetChildId(i - 1);
                        var (bOk, leftLast, bErr) = TryGetLastKey(leftChild);
                        if (!bOk) { errors.Add(bErr); /* still push childId below */ }
                        else if (_nodeSerializer.KeySerializer.Compare(separator, leftLast) <= 0)
                            errors.Add($"Internal node {pageId}: separator[{i}]={separator} " +
                                       $"<= last key of left child {leftChild} ({leftLast}). " +
                                       "Separator alignment violated (backward check).");

                        stack.Push(childId);
                    }
                }
                // Leaf pages: already validated in Pass 1; no action in Pass 2.
            }
            finally
            {
                _pageManager.Unpin(pageId);
            }
        }

        return ValidationResult.WithErrors(errors);
    }

    /// <summary>
    /// Returns the first (smallest) key reachable from <paramref name="pageId"/>
    /// by always following the leftmost child pointer down to a leaf.
    /// Returns (false, default, errorMessage) if pageId is NullPageId or a leaf with zero slots is reached.
    /// Pins and unpins each page before returning — no frame is held across the call.
    /// </summary>
    internal (bool success, TKey key, string error) TryGetFirstKey(uint pageId)
    {
        if (pageId == PageLayout.NullPageId)
            return (false, default!,
                $"TryGetFirstKey: encountered NullPageId during leftmost descent — " +
                "separator alignment cannot be validated for this subtree.");

        var frame = _pageManager.FetchPage(pageId);
        try
        {
            if (NodeSerializer<TKey, TValue>.IsLeaf(frame))
            {
                var leaf = _nodeSerializer.AsLeaf(frame);
                if (leaf.Count == 0)
                    return (false, default!,
                        $"GetFirstKey: leaf page {pageId} is empty — separator alignment " +
                        "cannot be validated for this subtree.");
                return (true, leaf.GetKey(0), null!);
            }
            var  node     = new InternalNode<TKey>(frame, _nodeSerializer.KeySerializer);
            uint leftmost = node.LeftmostChildId;
            return TryGetFirstKey(leftmost);   // recurse — height-bounded
        }
        finally
        {
            _pageManager.Unpin(pageId);
        }
    }

    /// <summary>
    /// Returns the last (largest) key reachable from <paramref name="pageId"/>
    /// by always following the rightmost child pointer down to a leaf.
    /// Returns (false, default, errorMessage) if pageId is NullPageId or a leaf with zero slots is reached.
    /// Pins and unpins each page before returning — no frame is held across the call.
    /// </summary>
    internal (bool success, TKey key, string error) TryGetLastKey(uint pageId)
    {
        if (pageId == PageLayout.NullPageId)
            return (false, default!,
                $"TryGetLastKey: encountered NullPageId during rightmost descent — " +
                "separator alignment cannot be validated for this subtree.");

        var frame = _pageManager.FetchPage(pageId);
        try
        {
            if (NodeSerializer<TKey, TValue>.IsLeaf(frame))
            {
                var leaf = _nodeSerializer.AsLeaf(frame);
                if (leaf.Count == 0)
                    return (false, default!,
                        $"GetLastKey: leaf page {pageId} is empty — separator alignment " +
                        "cannot be validated for this subtree.");
                return (true, leaf.GetKey(leaf.Count - 1), null!);
            }
            var  node      = new InternalNode<TKey>(frame, _nodeSerializer.KeySerializer);
            uint rightmost = (node.KeyCount > 0)
                ? node.GetChildId(node.KeyCount - 1)
                : node.LeftmostChildId;   // defensive: valid trees never hit this
            return TryGetLastKey(rightmost);   // recurse — height-bounded
        }
        finally
        {
            _pageManager.Unpin(pageId);
        }
    }
}
