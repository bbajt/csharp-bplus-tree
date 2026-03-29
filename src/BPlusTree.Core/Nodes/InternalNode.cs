using System.Buffers.Binary;
using BPlusTree.Core.Storage;

namespace BPlusTree.Core.Nodes;

/// <summary>
/// Typed overlay over an internal (non-leaf) page.
/// Layout: slot[i] points to a cell containing [serialized key (keyLen bytes)][uint childPageId (4 bytes)].
/// An internal node with N separator keys has N+1 child pointers.
/// The leftmost child pointer is stored in the extra header (HighKeyOffsetField used as LeftmostChildId).
/// Separator key at index i is the smallest key in child subtree i+1.
/// </summary>
public struct InternalNode<TKey>
    where TKey : notnull
{
    private readonly Frame                _frame;
    private readonly IKeySerializer<TKey> _keySerializer;

    public InternalNode(Frame frame, IKeySerializer<TKey> keySerializer)
    {
        _frame         = frame;
        _keySerializer = keySerializer;
    }

    private NodePage Page => new(_frame.Data.AsSpan());

    public uint PageId   => _frame.PageId;
    public int  KeyCount => Page.SlotCount;

    /// <summary>
    /// The child pointer for keys less than GetKey(0).
    /// Stored in the internal extra header at HighKeyOffsetField (repurposed as LeftmostChild).
    /// </summary>
    public uint LeftmostChildId
    {
        get => BinaryPrimitives.ReadUInt32BigEndian(_frame.Data.AsSpan(PageLayout.HighKeyOffsetField));
        set => BinaryPrimitives.WriteUInt32BigEndian(_frame.Data.AsSpan(PageLayout.HighKeyOffsetField), value);
    }

    public void Initialize(uint leftmostChildId)
    {
        Page.Initialize(_frame.PageId, PageType.Internal);
        LeftmostChildId = leftmostChildId;
    }

    /// <summary>
    /// Get separator key at index.
    /// </summary>
    public TKey GetKey(int index)
    {
        var (offset, _, _) = Page.GetSlot(index);
        int kLen = _keySerializer.FixedSize > 0
            ? _keySerializer.FixedSize
            : _keySerializer.GetSerializedSize(_frame.Data.AsSpan(offset));
        return _keySerializer.Deserialize(_frame.Data.AsSpan(offset, kLen));
    }

    /// <summary>
    /// Get the child page id for key at index.
    /// Index 0 = child to the RIGHT of separator key 0.
    /// Leftmost child = LeftmostChildId.
    /// </summary>
    public uint GetChildId(int index)
    {
        var (offset, _, _) = Page.GetSlot(index);
        int kLen = _keySerializer.FixedSize > 0
            ? _keySerializer.FixedSize
            : _keySerializer.GetSerializedSize(_frame.Data.AsSpan(offset));
        return BinaryPrimitives.ReadUInt32BigEndian(_frame.Data.AsSpan(offset + kLen));
    }

    /// <summary>
    /// Append a separator key + right child pointer. Used during bottom-up build and split.
    /// </summary>
    public bool TryAppend(TKey key, uint rightChildId)
    {
        Span<byte> keyBuf = stackalloc byte[2048];
        int keyLen = _keySerializer.Serialize(key, keyBuf);
        int cellLen = keyLen + 4;

        var p = Page;
        if (!p.HasFreeSpace(cellLen)) return false;

        ushort cellOff = (ushort)(p.FreeSpaceOffset + p.FreeSpaceSize - cellLen);
        var cellSpan = p.AllocateCell(cellLen);
        keyBuf[..keyLen].CopyTo(cellSpan);
        BinaryPrimitives.WriteUInt32BigEndian(cellSpan[keyLen..], rightChildId);

        p.InsertSlot(p.SlotCount, cellOff, (ushort)cellLen);
        return true;
    }

    /// <summary>
    /// Binary search: return index of child to follow for the given search key.
    /// Returns LeftmostChildId if searchKey &lt; GetKey(0).
    /// Returns GetChildId(i) where i is the last key &lt;= searchKey.
    /// </summary>
    public uint FindChildId(TKey searchKey)
    {
        if (KeyCount == 0) return LeftmostChildId;

        int lo = 0, hi = KeyCount - 1;
        int result = -1;
        while (lo <= hi)
        {
            int mid = lo + (hi - lo) / 2;
            int cmp = _keySerializer.Compare(searchKey, GetKey(mid));
            if (cmp < 0)
            {
                hi = mid - 1;
            }
            else // searchKey >= key[mid]
            {
                result = mid;
                lo = mid + 1;
            }
        }

        return result < 0 ? LeftmostChildId : GetChildId(result);
    }

    /// <summary>
    /// Insert separator key + right child at the correct sorted position.
    /// Used when a child splits and promotes a key upward.
    /// </summary>
    public bool TryInsertSeparator(TKey key, uint rightChildId)
    {
        Span<byte> keyBuf = stackalloc byte[2048];
        int keyLen = _keySerializer.Serialize(key, keyBuf);
        int cellLen = keyLen + 4;

        // Binary search for insertion index
        int lo = 0, hi = KeyCount - 1;
        while (lo <= hi)
        {
            int mid = lo + (hi - lo) / 2;
            int cmp = _keySerializer.Compare(key, GetKey(mid));
            if (cmp <= 0) hi = mid - 1;
            else          lo = mid + 1;
        }
        int idx = lo;

        var p = Page;
        if (!p.HasFreeSpace(cellLen)) return false;

        ushort cellOff = (ushort)(p.FreeSpaceOffset + p.FreeSpaceSize - cellLen);
        var cellSpan = p.AllocateCell(cellLen);
        keyBuf[..keyLen].CopyTo(cellSpan);
        BinaryPrimitives.WriteUInt32BigEndian(cellSpan[keyLen..], rightChildId);

        p.InsertSlot(idx, cellOff, (ushort)cellLen);
        return true;
    }

    /// <summary>
    /// Returns the 0-based position of the child that searchKey routes to.
    /// Position 0 = LeftmostChildId. Position i (i >= 1) = GetChildId(i-1).
    /// </summary>
    public int FindChildPosition(TKey searchKey)
    {
        if (KeyCount == 0) return 0;
        int lo = 0, hi = KeyCount - 1;
        int result = -1;
        while (lo <= hi)
        {
            int mid = lo + (hi - lo) / 2;
            int cmp = _keySerializer.Compare(searchKey, GetKey(mid));
            if (cmp < 0) hi = mid - 1;
            else { result = mid; lo = mid + 1; }
        }
        return result < 0 ? 0 : result + 1;
    }

    /// <summary>
    /// Returns the child page ID at the given 0-based position.
    /// Position 0 = LeftmostChildId. Position i (i >= 1) = GetChildId(i-1).
    /// </summary>
    public uint GetChildIdByPosition(int position)
        => position == 0 ? LeftmostChildId : GetChildId(position - 1);

    /// <summary>
    /// Overwrites the child page ID at the given 0-based position.
    /// Position 0 updates <see cref="LeftmostChildId"/>; positions >= 1
    /// update the child stored in the separator slot at index (position - 1).
    /// Used by the CoW write path to redirect parent pointers to shadow copies.
    /// </summary>
    internal void SetChildIdByPosition(int position, uint newId)
    {
        if (position == 0)
            LeftmostChildId = newId;
        else
            SetChildId(position - 1, newId);
    }

    /// <summary>
    /// Overwrites the separator key at the given index in place.
    /// Only valid for fixed-size keys.
    /// </summary>
    public void UpdateSeparatorKey(int index, TKey newKey)
    {
        var (offset, _, _) = Page.GetSlot(index);
        Span<byte> keyBuf = stackalloc byte[2048];
        int keyLen = _keySerializer.Serialize(newKey, keyBuf);
        keyBuf[..keyLen].CopyTo(_frame.Data.AsSpan(offset, keyLen));
    }

    /// <summary>
    /// Remove separator key at index. Also removes the right child pointer.
    /// Used during merge operations.
    /// </summary>
    public void RemoveSeparator(int index) => Page.RemoveSlot(index);

    /// <summary>
    /// Overwrites the child page ID at slot <paramref name="index"/>.
    /// For testing only — does not update the WAL.
    /// </summary>
    internal void SetChildId(int index, uint newId)
    {
        var (offset, _, _) = Page.GetSlot(index);
        int kLen = _keySerializer.FixedSize > 0
            ? _keySerializer.FixedSize
            : _keySerializer.GetSerializedSize(_frame.Data.AsSpan(offset));
        BinaryPrimitives.WriteUInt32BigEndian(
            _frame.Data.AsSpan(offset + kLen, sizeof(uint)), newId);
    }

    /// <summary>
    /// Read-path static: returns the rightmost child pointer without key comparison.
    /// An internal node with N separator keys has N+1 children; the rightmost is the
    /// right-child of the last separator or LeftmostChildId if there are no separators.
    /// Used by <see cref="TreeEngine{TKey,TValue}.FindRightmostLeaf"/>. (Phase 76)
    /// </summary>
    internal static uint FindRightmostChildId(Frame frame, IKeySerializer<TKey> keySerializer)
    {
        var page = new NodePage(frame.Data.AsSpan());
        if (page.SlotCount == 0)
            return BinaryPrimitives.ReadUInt32BigEndian(frame.Data.AsSpan(PageLayout.HighKeyOffsetField));
        var (lastOffset, _, _) = page.GetSlot(page.SlotCount - 1);
        int keyLen = keySerializer.FixedSize > 0
            ? keySerializer.FixedSize
            : keySerializer.GetSerializedSize(frame.Data.AsSpan(lastOffset));
        return BinaryPrimitives.ReadUInt32BigEndian(frame.Data.AsSpan(lastOffset + keyLen));
    }

    /// <summary>
    /// Read-path static variant: binary search for the child pointer to follow,
    /// without allocating an InternalNode wrapper. (Phase 36)
    /// </summary>
    internal static uint FindChildId(Frame frame, TKey searchKey, IKeySerializer<TKey> keySerializer)
    {
        var page = new NodePage(frame.Data.AsSpan());

        if (page.SlotCount == 0)
            return BinaryPrimitives.ReadUInt32BigEndian(frame.Data.AsSpan(PageLayout.HighKeyOffsetField));

        int lo = 0, hi = page.SlotCount - 1;
        int result = -1;
        while (lo <= hi)
        {
            int mid = lo + (hi - lo) / 2;
            var (midOffset, _, _) = page.GetSlot(mid);
            int keyLen = keySerializer.FixedSize > 0
                ? keySerializer.FixedSize
                : keySerializer.GetSerializedSize(frame.Data.AsSpan(midOffset));
            int cmp = keySerializer.Compare(searchKey, keySerializer.Deserialize(frame.Data.AsSpan(midOffset, keyLen)));
            if (cmp < 0)
                hi = mid - 1;
            else
            {
                result = mid;
                lo = mid + 1;
            }
        }

        if (result < 0)
            return BinaryPrimitives.ReadUInt32BigEndian(frame.Data.AsSpan(PageLayout.HighKeyOffsetField));

        var (foundOffset, _, _) = page.GetSlot(result);
        int foundKeyLen = keySerializer.FixedSize > 0
            ? keySerializer.FixedSize
            : keySerializer.GetSerializedSize(frame.Data.AsSpan(foundOffset));
        return BinaryPrimitives.ReadUInt32BigEndian(frame.Data.AsSpan(foundOffset + foundKeyLen));
    }
}
