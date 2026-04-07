using System.Buffers.Binary;
using BPlusTree.Core.Storage;

namespace BPlusTree.Core.Storage;

/// <summary>
/// Zero-copy, stack-only view over a raw page buffer.
/// Must remain a ref struct — allocation on the heap is forbidden.
/// All integers stored big-endian (preserves sort order for byte-wise comparison).
/// </summary>
internal ref struct NodePage
{
    private readonly Span<byte> _data;

    public NodePage(Span<byte> pageBuffer)
    {
        if (pageBuffer.Length < PageLayout.CommonHeaderSize)
            throw new ArgumentException("Buffer too small for page header.");
        _data = pageBuffer;
    }

    // ── Page size ─────────────────────────────────────────────────────────────
    public int PageSize => _data.Length;

    // ── Common Header Accessors ───────────────────────────────────────────────
    public uint   Magic           { get => ReadU32(PageLayout.MagicOffset);     set => WriteU32(PageLayout.MagicOffset, value); }
    public PageType PageType      { get => (PageType)ReadU8(PageLayout.PageTypeOffset); set => WriteU8(PageLayout.PageTypeOffset, (byte)value); }
    public byte   FormatVersion   { get => ReadU8(PageLayout.FormatVersionOffset); set => WriteU8(PageLayout.FormatVersionOffset, value); }
    public ushort SlotCount       { get => ReadU16(PageLayout.SlotCountOffset);     set => WriteU16(PageLayout.SlotCountOffset, value); }
    public uint   PageId          { get => ReadU32(PageLayout.PageIdOffset);        set => WriteU32(PageLayout.PageIdOffset, value); }
    public uint   ParentPageId    { get => ReadU32(PageLayout.ParentPageIdOffset);   set => WriteU32(PageLayout.ParentPageIdOffset, value); }
    public ushort FreeSpaceOffset { get => ReadU16(PageLayout.FreeSpaceOffsetOffset); set => WriteU16(PageLayout.FreeSpaceOffsetOffset, value); }
    public ushort FreeSpaceSize   { get => ReadU16(PageLayout.FreeSizeOffset);   set => WriteU16(PageLayout.FreeSizeOffset, value); }
    public ulong  PageLsn         { get => ReadU64(PageLayout.PageLsnOffset);         set => WriteU64(PageLayout.PageLsnOffset, value); }

    // ── Leaf Extra Header Accessors ───────────────────────────────────────────
    public uint PrevLeafPageId { get => ReadU32(PageLayout.PrevLeafPageIdOffset); set => WriteU32(PageLayout.PrevLeafPageIdOffset, value); }
    public uint NextLeafPageId { get => ReadU32(PageLayout.NextLeafPageIdOffset); set => WriteU32(PageLayout.NextLeafPageIdOffset, value); }
    public uint LeafRecordCount{ get => ReadU32(PageLayout.LeafRecordCountOffset); set => WriteU32(PageLayout.LeafRecordCountOffset, value); }

    // ── Initialize ────────────────────────────────────────────────────────────
    /// <summary>
    /// Zero the buffer then write all header fields with their default/sentinel values.
    /// After this call the page is valid, empty, and ready for slot insertions.
    /// FreeSpaceOffset = FirstSlotOffset, FreeSpaceSize = PageSize - FirstSlotOffset.
    /// </summary>
    public void Initialize(uint pageId, PageType pageType)
    {
        _data.Clear();
        Magic          = PageLayout.MagicNumber;
        PageType       = pageType;
        FormatVersion  = PageLayout.FormatVersion;
        SlotCount      = 0;
        PageId         = pageId;
        ParentPageId   = PageLayout.NullPageId;
        FreeSpaceOffset = PageLayout.FirstSlotOffset; // slot array grows down from here
        FreeSpaceSize   = (ushort)(PageSize - PageLayout.FirstSlotOffset); // excludes header and slot array
        PageLsn        = PageLayout.NullLsn;

        if (pageType == PageType.Leaf)
        {
            PrevLeafPageId = PageLayout.NullPageId;
            NextLeafPageId = PageLayout.NullPageId;
        }
    }

    // ── Slot Array ────────────────────────────────────────────────────────────
    /// <summary>
    /// Read the slot descriptor at the given slot index.
    /// Returns (cellOffset, cellLength, flags) decoded from the 6-byte slot entry.
    /// Bits 31-16 = offset, bits 15-0 = length; byte 4 = flags.
    /// </summary>
    public (ushort cellOffset, ushort cellLength, byte flags) GetSlot(int slotIndex)
    {
        int slotBase = PageLayout.FirstSlotOffset + (slotIndex * PageLayout.SlotEntrySize);
        uint packed  = ReadU32(slotBase);
        byte flags   = _data[slotBase + PageLayout.SlotFlagsOffset];
        return ((ushort)(packed >> 16), (ushort)(packed & 0xFFFF), flags);
    }

    /// <summary>
    /// Write the slot descriptor at the given slot index.
    /// Encodes offset in bits 31-16 and length in bits 15-0; writes flags to byte 4.
    /// Does NOT modify SlotCount or FreeSpaceSize.
    /// </summary>
    public void SetSlot(int slotIndex, ushort cellOffset, ushort cellLength, byte flags = 0)
    {
        int slotBase = PageLayout.FirstSlotOffset + (slotIndex * PageLayout.SlotEntrySize);
        WriteU32(slotBase, ((uint)cellOffset << 16) | cellLength);
        _data[slotBase + PageLayout.SlotFlagsOffset] = flags;
        // byte 5 (_reserved) stays 0 — page is zeroed on Initialize
    }

    /// <summary>
    /// Insert a new slot descriptor at <paramref name="slotIndex"/>,
    /// shifting all slots at index >= slotIndex one position to the right.
    /// Increments <see cref="SlotCount"/>.
    /// Decrements <see cref="FreeSpaceSize"/> by <see cref="PageLayout.SlotEntrySize"/>.
    /// Updates <see cref="FreeSpaceOffset"/> by adding <see cref="PageLayout.SlotEntrySize"/>.
    /// Does NOT allocate a cell — caller must call <see cref="AllocateCell"/> separately.
    /// Throws <see cref="InvalidOperationException"/> if slotIndex > SlotCount.
    /// </summary>
    public void InsertSlot(int slotIndex, ushort cellOffset, ushort cellLength, byte flags = 0)
    {
        if (slotIndex > SlotCount)
            throw new InvalidOperationException("Invalid slot index for insertion.");

        // Shift existing slots to the right
        for (int i = SlotCount; i > slotIndex; i--)
        {
            var (offset, length, f) = GetSlot(i - 1);
            SetSlot(i, offset, length, f);
        }

        // Insert the new slot
        SetSlot(slotIndex, cellOffset, cellLength, flags);

        // Update metadata
        SlotCount++;
        FreeSpaceOffset += PageLayout.SlotEntrySize;
        FreeSpaceSize -= PageLayout.SlotEntrySize;
    }

    /// <summary>
    /// Remove the slot descriptor at <paramref name="slotIndex"/>,
    /// shifting all slots at index > slotIndex one position to the left.
    /// Decrements <see cref="SlotCount"/>.
    /// Increments <see cref="FreeSpaceSize"/> by <see cref="PageLayout.SlotEntrySize"/> only
    /// (cell bytes are NOT reclaimed here — that is defragmentation's job).
    /// Updates <see cref="FreeSpaceOffset"/> by subtracting <see cref="PageLayout.SlotEntrySize"/>.
    /// </summary>
    public void RemoveSlot(int slotIndex)
    {
        if (slotIndex >= SlotCount)
            throw new InvalidOperationException("Invalid slot index for removal.");

        // Shift existing slots to the left
        for (int i = slotIndex; i < SlotCount - 1; i++)
        {
            var (offset, length, f) = GetSlot(i + 1);
            SetSlot(i, offset, length, f);
        }

        // Update metadata
        SlotCount--;
        FreeSpaceOffset -= PageLayout.SlotEntrySize;
        FreeSpaceSize += PageLayout.SlotEntrySize;
    }

    // ── Cell Allocator ────────────────────────────────────────────────────────
    /// <summary>
    /// Reserve <paramref name="cellSize"/> bytes at the end of the free space region.
    /// Cells grow from the END of the page inward (toward lower offsets).
    /// The FIRST cell allocated ends at page[PageSize-1] inclusive.
    /// The SECOND cell ends at page[PageSize - firstCellSize - 1] inclusive.
    /// Returns a <see cref="Span{byte}"/> pointing directly into the page buffer.
    /// Decrements <see cref="FreeSpaceSize"/> by <paramref name="cellSize"/>
    /// PLUS <see cref="PageLayout.SlotEntrySize"/> if called as part of InsertSlot flow.
    /// NOTE: AllocateCell decrements FreeSpaceSize by cellSize only.
    ///       InsertSlot decrements FreeSpaceSize by SlotEntrySize only.
    ///       Caller must call both to properly account for both costs.
    /// Throws <see cref="InvalidOperationException"/> if insufficient space.
    /// </summary>
    public Span<byte> AllocateCell(int cellSize)
    {
        // Algorithm:
        // 1. Guard: HasFreeSpace(cellSize) must be true
        // 2. newCellOffset = (next free cell start from end) =
        //      PageSize - (sum of all previously allocated cell sizes)
        //    Simpler: track as PageSize minus all cell bytes used.
        //    But we don't store that directly. Instead:
        //    newCellOffset = (offset of the top of the cell region) - cellSize
        //    The top of the cell region is implicitly: PageSize - totalCellBytesAllocated
        //    We can compute it as: FreeSpaceOffset + FreeSpaceSize
        //    (because FreeSpace is the gap between the slot array bottom and the cell region top)
        // 3. Write nothing into the cell — caller fills it
        // 4. FreeSpaceSize -= (ushort)cellSize
        // 5. Return _data.Slice(newCellOffset, cellSize)

        if (!HasFreeSpace(cellSize))
            throw new InvalidOperationException("Insufficient space to allocate cell.");

        int newCellOffset = FreeSpaceOffset + FreeSpaceSize - cellSize;
        FreeSpaceSize -= (ushort)cellSize;
        return _data.Slice(newCellOffset, cellSize);
    }

    // ── Space Check ─────────────────────────────────────────────────────────
    /// <summary>
    /// Returns true if there is enough free space to hold a new cell of
    /// <paramref name="cellSize"/> bytes AND one additional slot descriptor.
    /// Required free: cellSize + SlotEntrySize ≤ FreeSpaceSize.
    /// </summary>
    public bool HasFreeSpace(int cellSize) => cellSize + PageLayout.SlotEntrySize <= FreeSpaceSize;

    /// <summary>
    /// Returns the raw page buffer. Used by StorageFile to write the page to disk.
    /// </summary>
    public ReadOnlySpan<byte> AsReadOnly() => _data;

    // ── Private helpers ───────────────────────────────────────────────────────
    private uint   ReadU32(int offset) => BinaryPrimitives.ReadUInt32BigEndian(_data[offset..]);
    private void   WriteU32(int offset, uint value)   => BinaryPrimitives.WriteUInt32BigEndian(_data[offset..], value);
    private ushort ReadU16(int offset) => BinaryPrimitives.ReadUInt16BigEndian(_data[offset..]);
    private void   WriteU16(int offset, ushort value) => BinaryPrimitives.WriteUInt16BigEndian(_data[offset..], value);
    private ulong  ReadU64(int offset) => BinaryPrimitives.ReadUInt64BigEndian(_data[offset..]);
    private void   WriteU64(int offset, ulong value)  => BinaryPrimitives.WriteUInt64BigEndian(_data[offset..], value);
    private byte   ReadU8 (int offset) => _data[offset];
    private void   WriteU8(int offset, byte value)    => _data[offset] = value;
}