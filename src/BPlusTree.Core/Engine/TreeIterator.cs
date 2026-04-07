using System.Buffers.Binary;
using System.Diagnostics;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;

namespace BPlusTree.Core.Engine;

/// <summary>
/// Forward iterator over the B+ tree leaf chain.
/// Pins exactly one leaf page at a time.
/// Unpins the current leaf before advancing to the next (never holds two pins).
/// Dispose() is idempotent — safe to call multiple times.
///
/// State machine: BEFORE_START → READING → EXHAUSTED
///   BEFORE_START : _currentFrame == null, _slotIndex == -1
///   READING      : _currentFrame != null, _slotIndex is valid index into current leaf
///   EXHAUSTED    : _currentFrame == null, _slotIndex == -1, _currentLeafId == NullPageId
///
/// IMPORTANT: TKey? cannot be used as a null sentinel for value-type TKey with the notnull
/// constraint — at the IL level, TKey? = TKey (not Nullable<TKey>), so default(TKey?) =
/// default(TKey) = 0 for int. We use explicit boolean flags instead.
///
/// See ROADMAP.md Phase 19 pseudocode for exact MoveNext() ordering.
/// Phase 37: fields made mutable for pool reuse; MoveNext uses static LeafNode accessors.
/// </summary>
internal sealed class TreeIterator<TKey, TValue> : IEnumerator<(TKey Key, TValue Value)>
    where TKey : notnull
{
    // ── Thread-local single-slot pool (Phase 37) ───────────────────────────
    [ThreadStatic]
    private static TreeIterator<TKey, TValue>? _pool;

    // Fields are not readonly — Init() resets them for pool reuse. (Phase 37)
    private PageManager                  _pageManager;
    private NodeSerializer<TKey, TValue> _nodeSerializer;
    private IKeySerializer<TKey>         _keySerializer;
    private IValueSerializer<TValue>     _valueSerializer;
    private TKey                         _startKey;
    private TKey                         _endKey;
    private bool                         _hasStartKey;
    private bool                         _hasEndKey;
    private uint                         _firstLeafId;   // saved for Reset()

    private uint   _currentLeafId;
    private int    _slotIndex;      // -1 = BEFORE_START or EXHAUSTED
    private Frame? _currentFrame;   // null iff not in READING state

    public (TKey Key, TValue Value) Current { get; private set; }
    object System.Collections.IEnumerator.Current => Current;

    /// <summary>
    /// The page ID of the leaf currently being iterated.
    /// Valid while <see cref="MoveNext"/> returns true.
    /// Used by <see cref="TreeEngine{TKey,TValue}.ScanInTransaction"/> to record leaf reads.
    /// </summary>
    internal uint CurrentLeafId => _currentLeafId;

    public TreeIterator(
        PageManager                  pageManager,
        NodeSerializer<TKey, TValue> nodeSerializer,
        IKeySerializer<TKey>         keySerializer,
        uint                         firstLeafPageId,
        bool                         hasStartKey,
        TKey                         startKey,
        bool                         hasEndKey,
        TKey                         endKey)
    {
        _pageManager     = pageManager;
        _nodeSerializer  = nodeSerializer;
        _keySerializer   = keySerializer;
        _valueSerializer = nodeSerializer.ValueSerializer;
        _hasStartKey     = hasStartKey;
        _startKey        = startKey;
        _hasEndKey       = hasEndKey;
        _endKey          = endKey;
        _firstLeafId     = firstLeafPageId;
        _currentLeafId   = firstLeafPageId;
        _currentFrame    = null;              // State A: BEFORE_START
        _slotIndex       = -1;
        Current          = default!;
    }

    /// <summary>
    /// Rent an iterator from the thread-local pool, or allocate a new one.
    /// Must be paired with Dispose() (foreach does this automatically). (Phase 37)
    /// </summary>
    internal static TreeIterator<TKey, TValue> Rent(
        PageManager                  pageManager,
        NodeSerializer<TKey, TValue> nodeSerializer,
        IKeySerializer<TKey>         keySerializer,
        uint                         firstLeafPageId,
        bool                         hasStartKey,
        TKey                         startKey,
        bool                         hasEndKey,
        TKey                         endKey)
    {
        var it = _pool;
        if (it != null)
        {
            _pool = null;
            it.Init(pageManager, nodeSerializer, keySerializer,
                    firstLeafPageId, hasStartKey, startKey, hasEndKey, endKey);
            return it;
        }
        return new TreeIterator<TKey, TValue>(pageManager, nodeSerializer, keySerializer,
                                              firstLeafPageId, hasStartKey, startKey, hasEndKey, endKey);
    }

    /// <summary>
    /// Reset all fields for pool reuse. Called by Rent() before returning a pooled instance.
    /// (Phase 37)
    /// </summary>
    private void Init(
        PageManager                  pageManager,
        NodeSerializer<TKey, TValue> nodeSerializer,
        IKeySerializer<TKey>         keySerializer,
        uint                         firstLeafPageId,
        bool                         hasStartKey,
        TKey                         startKey,
        bool                         hasEndKey,
        TKey                         endKey)
    {
        Debug.Assert(_currentFrame == null,
            "TreeIterator.Init() called on iterator with live pinned frame. Ensure Dispose() was called.");
        _pageManager     = pageManager;
        _nodeSerializer  = nodeSerializer;
        _keySerializer   = keySerializer;
        _valueSerializer = nodeSerializer.ValueSerializer;
        _hasStartKey     = hasStartKey;
        _startKey        = startKey;
        _hasEndKey       = hasEndKey;
        _endKey          = endKey;
        _firstLeafId     = firstLeafPageId;
        _currentLeafId   = firstLeafPageId;
        _slotIndex       = -1;
        _currentFrame    = null;
        Current          = default!;
    }

    /// <summary>
    /// Advance to the next entry.
    /// Follow MoveNext() pseudocode exactly — state transitions matter.
    /// Critical: unpin old frame BEFORE fetching next; set _currentFrame = null after unpin.
    /// Phase 37: uses static LeafNode accessors — zero heap allocation per call.
    /// </summary>
    public bool MoveNext()
    {
        // ── Transition from State A (first call) ───────────────────────────────
        if (_currentFrame == null && _slotIndex == -1)
        {
            if (_currentLeafId == PageLayout.NullPageId)
                goto exhausted;

            _currentFrame = _pageManager.FetchPage(_currentLeafId);
            _slotIndex    = _hasStartKey
                ? LeafNode<TKey, TValue>.FindFirstSlotGe(_currentFrame, _startKey, _keySerializer)
                : 0;
            // Fall through to main loop below
        }

        // ── Main loop ──────────────────────────────────────────────────────────
        while (_currentFrame != null)
        {
            int count = LeafNode<TKey, TValue>.GetSlotCount(_currentFrame);

            if (_slotIndex >= count)
            {
                // Current leaf exhausted — advance to next leaf
                uint nextId = LeafNode<TKey, TValue>.GetNextLeafPageId(_currentFrame);
                _pageManager.Unpin(_currentLeafId);      // UNPIN before fetching next
                _currentFrame  = null;                   // guard: frame is no longer valid
                _currentLeafId = nextId;

                if (nextId == PageLayout.NullPageId)
                    goto exhausted;

                _currentFrame = _pageManager.FetchPage(nextId);
                _slotIndex    = 0;
                continue;
            }

            // _slotIndex is valid for this leaf
            TKey key = LeafNode<TKey, TValue>.GetKey(_currentFrame, _slotIndex, _keySerializer);

            if (_hasEndKey && _keySerializer.Compare(key, _endKey) > 0)
            {
                // Past end of requested range
                _pageManager.Unpin(_currentLeafId);
                _currentFrame  = null;
                _currentLeafId = PageLayout.NullPageId;
                goto exhausted;
            }

            // Valid entry — emit it (overflow-aware: Phase 100b)
            LeafNode<TKey, TValue>.GetRawValueAtSlot(
                _currentFrame, _slotIndex, _keySerializer,
                out ReadOnlySpan<byte> rawBytes, out byte slotFlags);
            TValue value;
            if ((slotFlags & PageLayout.SlotIsOverflow) != 0)
            {
                int  totalLen    = BinaryPrimitives.ReadInt32BigEndian(rawBytes[..4]);
                uint firstPageId = BinaryPrimitives.ReadUInt32BigEndian(rawBytes.Slice(4, 4));
                byte[] chainBytes = _pageManager.ReadOverflowChain(firstPageId, totalLen);
                value = _valueSerializer.Deserialize(chainBytes);
            }
            else
            {
                value = _valueSerializer.Deserialize(rawBytes);
            }
            Current = (key, value);
            _slotIndex++;               // advance AFTER reading, not before
            return true;
        }

        exhausted:
        _currentFrame  = null;          // already unpinned above — do NOT unpin again here
        _slotIndex     = -1;
        _currentLeafId = PageLayout.NullPageId;
        return false;
    }

    /// <summary>
    /// Reset to BEFORE_START state. Unpins current frame if in READING state.
    /// </summary>
    public void Reset()
    {
        if (_currentFrame != null)
        {
            _pageManager.Unpin(_currentLeafId);
            _currentFrame = null;
        }
        _currentLeafId = _firstLeafId;
        _slotIndex     = -1;
        Current        = default!;
    }

    /// <summary>
    /// Unpins current frame if in READING state, then returns the iterator to the
    /// thread-local pool for reuse. Idempotent — double-Dispose() is safe because
    /// _currentFrame is cleared before returning to pool. (Phase 37)
    /// </summary>
    public void Dispose()
    {
        if (_currentFrame != null)
        {
            _pageManager.Unpin(_currentLeafId);
            _currentFrame  = null;
            _currentLeafId = PageLayout.NullPageId;
        }
        _pool = this;
    }
}
