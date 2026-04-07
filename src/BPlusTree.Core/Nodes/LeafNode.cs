using BPlusTree.Core.Storage;
using System.Buffers;
using System.Buffers.Binary;

namespace BPlusTree.Core.Nodes;

/// <summary>
/// Typed overlay over a leaf page. Operates on a pinned Frame's Data buffer.
/// Uses binary search on the slot array for O(log n) key lookup.
/// Keys are stored in big-endian serialized form (sort order preserved for binary search).
/// Cell layout (per slot): [serialized key (keyLen bytes)][serialized value (valLen bytes)]
/// </summary>
internal struct LeafNode<TKey, TValue>
    where TKey : notnull
{
    private readonly Frame                    _frame;
    private readonly IKeySerializer<TKey>     _keySerializer;
    private readonly IValueSerializer<TValue> _valueSerializer;

    public LeafNode(Frame frame, IKeySerializer<TKey> keySerializer, IValueSerializer<TValue> valueSerializer)
    {
        _frame           = frame;
        _keySerializer   = keySerializer;
        _valueSerializer = valueSerializer;
    }

    private NodePage Page => new(_frame.Data.AsSpan());

    public uint PageId       => _frame.PageId;
    public int  Count        => Page.SlotCount;
    public bool IsEmpty      => Count == 0;
    public uint NextLeafPageId { get => Page.NextLeafPageId; set { var p = Page; p.NextLeafPageId = value; } }
    public uint PrevLeafPageId { get => Page.PrevLeafPageId; set { var p = Page; p.PrevLeafPageId = value; } }

    /// <summary>
    /// Initialize this leaf page (first use only). Delegates to NodePage.Initialize.
    /// </summary>
    public void Initialize() => Page.Initialize(_frame.PageId, PageType.Leaf);

    /// <summary>
    /// Try to find the value for the given key.
    /// Returns true and sets value if found; false otherwise.
    /// </summary>
    public bool TryGet(TKey key, out TValue value)
    {
        var (idx, found) = BinarySearch(key);
        if (!found)
        {
            value = default!;
            return false;
        }
        value = _valueSerializer.Deserialize(GetValueSpan(idx));
        return true;
    }

    /// <summary>
    /// Insert or update the key-value pair.
    /// If key exists: overwrite the value in-place if same serialized size; else remove and re-insert.
    /// If key does not exist: allocate a new cell, insert a new slot.
    /// Returns false if page has insufficient space for a new entry.
    /// </summary>
    public bool TryInsert(TKey key, TValue value)
    {
        // Keys are validated to <= MaxKeySize (512 B) by TreeEngine before reaching here;
        // stackalloc byte[2048] is always sufficient regardless of page size.
        Span<byte> keyBuf = stackalloc byte[2048];
        int keyLen = _keySerializer.Serialize(key, keyBuf);

        // Values may exceed 2048 bytes (up to MaxEntrySize ≈ PageSize/2).
        // Use stackalloc for the common case (small/fixed-size values, no allocation);
        // fall back to ArrayPool only when the measured size exceeds the stack threshold.
        // valBuf is declared inside the try so the stackalloc span doesn't appear to escape
        // (CS8353 fires if a stackalloc result is assigned to an outer-scope variable).
        const int ValStackThreshold = 2048;
        int valMeasured = _valueSerializer.MeasureSize(value);
        byte[]? valRented = valMeasured > ValStackThreshold
            ? ArrayPool<byte>.Shared.Rent(valMeasured)
            : null;
        try
        {
            Span<byte> valBuf = valRented != null
                ? valRented.AsSpan(0, valMeasured)
                : stackalloc byte[ValStackThreshold];
            int valLen = _valueSerializer.Serialize(value, valBuf);

            var (idx, found) = BinarySearch(key);

            if (found)
            {
                var page = Page;
                var (cellOffset, cellLen, existingFlags) = page.GetSlot(idx);
                int existingValLen = cellLen - keyLen;
                bool existingIsOverflow = (existingFlags & PageLayout.SlotIsOverflow) != 0;

                // In-place overwrite only when value size matches AND the old slot is not overflow.
                // If old slot is overflow, SlotIsOverflow would remain set after overwriting bytes —
                // TryGet would then misread the new inline value as a pointer record.
                if (!existingIsOverflow && valLen == existingValLen)
                {
                    // In-place overwrite: value size unchanged, same inline type.
                    var cell = _frame.Data.AsSpan(cellOffset, cellLen);
                    valBuf[..valLen].CopyTo(cell[keyLen..]);
                    return true;
                }

                // Value size changed or type changed (overflow→inline): remove slot then re-insert.
                // Cell bytes are orphaned (no compaction in this phase).
                page.RemoveSlot(idx);
                // idx remains the correct insertion point after slot removal.
            }

            // Insert new entry.
            var p = Page;
            if (!p.HasFreeSpace(keyLen + valLen)) return false;

            // Compute cell offset before AllocateCell (which modifies FreeSpaceSize).
            ushort cellOff = (ushort)(p.FreeSpaceOffset + p.FreeSpaceSize - keyLen - valLen);

            var cellSpan = p.AllocateCell(keyLen + valLen);
            keyBuf[..keyLen].CopyTo(cellSpan);
            valBuf[..valLen].CopyTo(cellSpan[keyLen..]);

            p.InsertSlot(idx, cellOff, (ushort)(keyLen + valLen));
            return true;
        }
        finally
        {
            if (valRented != null) ArrayPool<byte>.Shared.Return(valRented);
        }
    }

    /// <summary>
    /// Write a 9-byte overflow pointer record for <paramref name="key"/> and insert the slot
    /// with <see cref="PageLayout.SlotIsOverflow"/> set to 1.
    /// If the key already exists (inline or overflow), its existing slot is removed first.
    /// The CALLER is responsible for freeing any old overflow chain before calling this method
    /// (per WAL ordering: leaf must be updated before chain is freed).
    /// (Phase 99b)
    /// </summary>
    internal void WriteOverflowPointer(TKey key, uint firstOverflowPageId, int totalSerializedLen)
    {
        Span<byte> keyBuf = stackalloc byte[2048];
        int keyLen = _keySerializer.Serialize(key, keyBuf);

        // Pointer record: [TotalSerializedLen:4 BE][FirstOverflowPageId:4 BE][_reserved:1]
        Span<byte> ptr = stackalloc byte[PageLayout.OverflowPointerSize];
        BinaryPrimitives.WriteInt32BigEndian(ptr[..4], totalSerializedLen);
        BinaryPrimitives.WriteUInt32BigEndian(ptr.Slice(4, 4), firstOverflowPageId);
        ptr[8] = 0;

        var (idx, found) = BinarySearch(key);
        var p = Page;

        if (found)
            p.RemoveSlot(idx);   // Remove old inline or old overflow pointer slot.

        int cellSize = keyLen + PageLayout.OverflowPointerSize;
        ushort cellOff = (ushort)(p.FreeSpaceOffset + p.FreeSpaceSize - cellSize);
        var cellSpan = p.AllocateCell(cellSize);
        keyBuf[..keyLen].CopyTo(cellSpan);
        ptr.CopyTo(cellSpan[keyLen..]);
        p.InsertSlot(idx, cellOff, (ushort)cellSize, flags: PageLayout.SlotIsOverflow);
    }

    /// <summary>
    /// Remove the entry for the given key.
    /// Returns true if removed; false if key not found.
    /// </summary>
    public bool Remove(TKey key)
    {
        var (idx, found) = BinarySearch(key);
        if (!found) return false;
        Page.RemoveSlot(idx);
        return true;
    }

    /// <summary>
    /// Get the key at the given slot index (for split/merge operations).
    /// </summary>
    public TKey GetKey(int slotIndex) => _keySerializer.Deserialize(GetKeySpan(slotIndex));

    /// <summary>
    /// Get the value at the given slot index.
    /// </summary>
    public TValue GetValue(int slotIndex) => _valueSerializer.Deserialize(GetValueSpan(slotIndex));

    /// <summary>
    /// Return true if this page can accommodate a new entry of the given serialized key+value size.
    /// Considers both cell bytes and one slot descriptor.
    /// </summary>
    public bool HasSpaceFor(int keySize, int valueSize)
        => Page.HasFreeSpace(keySize + valueSize);

    // ── Binary search ─────────────────────────────────────────────────────────
    /// <summary>
    /// Binary search for key. Returns (index, true) if found; (insertionIndex, false) if not.
    /// </summary>
    private (int index, bool found) BinarySearch(TKey key)
    {
        int lo = 0, hi = Count - 1;
        while (lo <= hi)
        {
            int mid = lo + (hi - lo) / 2;
            TKey midKey = _keySerializer.Deserialize(GetKeySpan(mid));
            int cmp = _keySerializer.Compare(key, midKey);
            if (cmp == 0) return (mid, true);
            if (cmp < 0)  hi = mid - 1;
            else          lo = mid + 1;
        }
        return (lo, false);
    }

    private ReadOnlySpan<byte> GetKeySpan(int slotIndex)
    {
        var (offset, _, _) = Page.GetSlot(slotIndex);
        int keySize = _keySerializer.FixedSize > 0
            ? _keySerializer.FixedSize
            : _keySerializer.GetSerializedSize(_frame.Data.AsSpan(offset));
        return _frame.Data.AsSpan(offset, keySize);
    }

    private ReadOnlySpan<byte> GetValueSpan(int slotIndex)
    {
        var (offset, cellLen, _) = Page.GetSlot(slotIndex);
        int keySize = _keySerializer.FixedSize > 0
            ? _keySerializer.FixedSize
            : _keySerializer.GetSerializedSize(_frame.Data.AsSpan(offset));
        return _frame.Data.AsSpan(offset + keySize, cellLen - keySize);
    }

    /// <summary>
    /// Read-path static variant: binary search for key and deserialize value,
    /// without allocating a LeafNode wrapper. (Phase 36)
    /// </summary>
    internal static bool TryGet(
        Frame frame,
        TKey key,
        IKeySerializer<TKey> keySerializer,
        IValueSerializer<TValue> valueSerializer,
        out TValue value)
    {
        var page = new NodePage(frame.Data.AsSpan());

        int lo = 0, hi = page.SlotCount - 1;
        while (lo <= hi)
        {
            int mid = lo + (hi - lo) / 2;
            var (midOffset, midCellLen, _) = page.GetSlot(mid);
            int keySize = keySerializer.FixedSize > 0
                ? keySerializer.FixedSize
                : keySerializer.GetSerializedSize(frame.Data.AsSpan(midOffset));
            TKey midKey = keySerializer.Deserialize(frame.Data.AsSpan(midOffset, keySize));
            int cmp = keySerializer.Compare(key, midKey);
            if (cmp == 0)
            {
                value = valueSerializer.Deserialize(frame.Data.AsSpan(midOffset + keySize, midCellLen - keySize));
                return true;
            }
            if (cmp < 0) hi = mid - 1;
            else         lo = mid + 1;
        }
        value = default!;
        return false;
    }

    /// <summary>
    /// Read-path static variant: binary search for key, returning the raw value bytes and
    /// the slot flags byte. The returned span points into <paramref name="frame"/>.Data —
    /// caller must not use it after the frame is unpinned.
    /// Used by TreeEngine to detect SlotIsOverflow before dispatching to ReadOverflowChain.
    /// (Phase 99a)
    /// </summary>
    internal static bool TryGetRawValue(
        Frame frame,
        TKey key,
        IKeySerializer<TKey> keySerializer,
        out ReadOnlySpan<byte> rawValueBytes,
        out byte slotFlags)
    {
        var page = new NodePage(frame.Data.AsSpan());

        int lo = 0, hi = page.SlotCount - 1;
        while (lo <= hi)
        {
            int mid = lo + (hi - lo) / 2;
            var (midOffset, midCellLen, flags) = page.GetSlot(mid);
            int keySize = keySerializer.FixedSize > 0
                ? keySerializer.FixedSize
                : keySerializer.GetSerializedSize(frame.Data.AsSpan(midOffset));
            TKey midKey = keySerializer.Deserialize(frame.Data.AsSpan(midOffset, keySize));
            int cmp = keySerializer.Compare(key, midKey);
            if (cmp == 0)
            {
                rawValueBytes = frame.Data.AsSpan(midOffset + keySize, midCellLen - keySize);
                slotFlags     = flags;
                return true;
            }
            if (cmp < 0) hi = mid - 1;
            else         lo = mid + 1;
        }
        rawValueBytes = default;
        slotFlags     = 0;
        return false;
    }

    /// <summary>
    /// Read-path: number of entries in this leaf page without creating a LeafNode wrapper.
    /// (Phase 37)
    /// </summary>
    internal static int GetSlotCount(Frame frame)
    {
        var page = new NodePage(frame.Data.AsSpan());
        return page.SlotCount;
    }

    /// <summary>
    /// Read-path: deserialize the key at <paramref name="slotIndex"/>
    /// without creating a LeafNode wrapper. (Phase 37)
    /// </summary>
    internal static TKey GetKey(Frame frame, int slotIndex, IKeySerializer<TKey> keySerializer)
    {
        var page = new NodePage(frame.Data.AsSpan());
        var (cellOffset, _, _) = page.GetSlot(slotIndex);
        int keySize = keySerializer.FixedSize > 0
            ? keySerializer.FixedSize
            : keySerializer.GetSerializedSize(frame.Data.AsSpan(cellOffset));
        return keySerializer.Deserialize(frame.Data.AsSpan(cellOffset, keySize));
    }

    /// <summary>
    /// Read-path: deserialize the value at <paramref name="slotIndex"/>
    /// without creating a LeafNode wrapper. (Phase 37)
    /// </summary>
    internal static TValue GetValue(
        Frame frame,
        int slotIndex,
        IKeySerializer<TKey> keySerializer,
        IValueSerializer<TValue> valueSerializer)
    {
        var page = new NodePage(frame.Data.AsSpan());
        var (cellOffset, cellLen, _) = page.GetSlot(slotIndex);
        int keySize = keySerializer.FixedSize > 0
            ? keySerializer.FixedSize
            : keySerializer.GetSerializedSize(frame.Data.AsSpan(cellOffset));
        return valueSerializer.Deserialize(frame.Data.AsSpan(cellOffset + keySize, cellLen - keySize));
    }

    /// <summary>
    /// Read-path: return the raw value bytes and slot flags at <paramref name="slotIndex"/>
    /// without deserializing. The returned span borrows from <paramref name="frame"/>.Data —
    /// caller must not use it after the frame is unpinned.
    /// Used by TreeIterator to detect SlotIsOverflow on the scan path. (Phase 100b)
    /// </summary>
    internal static void GetRawValueAtSlot(
        Frame frame,
        int slotIndex,
        IKeySerializer<TKey> keySerializer,
        out ReadOnlySpan<byte> rawValueBytes,
        out byte slotFlags)
    {
        var page = new NodePage(frame.Data.AsSpan());
        var (cellOffset, cellLen, flags) = page.GetSlot(slotIndex);
        int keySize = keySerializer.FixedSize > 0
            ? keySerializer.FixedSize
            : keySerializer.GetSerializedSize(frame.Data.AsSpan(cellOffset));
        rawValueBytes = frame.Data.AsSpan(cellOffset + keySize, cellLen - keySize);
        slotFlags     = flags;
    }

    /// <summary>
    /// Read-path: PageId of the next leaf in the chain.
    /// Returns PageLayout.NullPageId if this is the last leaf. (Phase 37)
    /// </summary>
    internal static uint GetNextLeafPageId(Frame frame)
    {
        var page = new NodePage(frame.Data.AsSpan());
        return page.NextLeafPageId;
    }

    /// <summary>
    /// Read-path: PageId of the previous leaf in the chain.
    /// Returns PageLayout.NullPageId if this is the first leaf. (Phase 76)
    /// </summary>
    internal static uint GetPrevLeafPageId(Frame frame)
    {
        var page = new NodePage(frame.Data.AsSpan());
        return page.PrevLeafPageId;
    }

    /// <summary>
    /// Read-path: returns the last slot index where key ≤ searchKey.
    /// Returns -1 if all keys on this leaf are greater than searchKey.
    /// Mirrors <see cref="FindFirstSlotGe"/> for reverse-scan cursor positioning.
    /// (Phase 76)
    /// </summary>
    internal static int FindLastSlotLe(Frame frame, TKey searchKey, IKeySerializer<TKey> keySerializer)
    {
        int firstGe = FindFirstSlotGe(frame, searchKey, keySerializer);
        // firstGe = first index where key >= searchKey.
        // If key at firstGe == searchKey (exact match), that slot is the last ≤ searchKey.
        // If key at firstGe > searchKey (or firstGe == SlotCount), step back one.
        if (firstGe < GetSlotCount(frame))
        {
            TKey keyAtFirstGe = GetKey(frame, firstGe, keySerializer);
            if (keySerializer.Compare(keyAtFirstGe, searchKey) == 0)
                return firstGe;
        }
        return firstGe - 1;
    }

    /// <summary>
    /// Read-path: binary search for the first slot index where key ≥ searchKey.
    /// Returns SlotCount if all keys are less than searchKey.
    /// Without allocating a LeafNode wrapper. (Phase 37)
    /// </summary>
    internal static int FindFirstSlotGe(Frame frame, TKey searchKey, IKeySerializer<TKey> keySerializer)
    {
        var page = new NodePage(frame.Data.AsSpan());
        int lo = 0, hi = page.SlotCount;
        while (lo < hi)
        {
            int mid = (lo + hi) >> 1;
            TKey midKey = GetKey(frame, mid, keySerializer);
            if (keySerializer.Compare(midKey, searchKey) < 0)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo;
    }
}
