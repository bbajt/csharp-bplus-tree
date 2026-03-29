namespace BPlusTree.Core.Storage;

/// <summary>
/// All byte offsets, sizes, and magic numbers for on-disk page layout.
/// Every value is a compile-time constant. No other file may hardcode these values.
/// </summary>
public static class PageLayout
{
    // ── Magic & Version ──────────────────────────────────────────────────────
    public const uint MagicNumber       = 0xB17EEF00;
    public const byte FormatVersion     = 2;

    // ── Common Page Header (32 bytes, all page types) ────────────────────────
    // Offset  0 : uint  Magic              (4 bytes)
    // Offset  4 : byte  PageType           (1 byte)
    // Offset  5 : byte  FormatVersion      (1 byte)
    // Offset  6 : ushort SlotCount         (2 bytes)
    // Offset  8 : uint  PageId             (4 bytes)
    // Offset 12 : uint  ParentPageId       (4 bytes)
    // Offset 16 : ushort FreeSpaceOffset   (2 bytes)
    // Offset 18 : ushort FreeSpaceSize     (2 bytes)
    // Offset 20 : ulong  PageLSN           (8 bytes)
    // Offset 28 : uint  Checksum           (4 bytes)  [reserved, always 0 in v1]
    // Total: 32 bytes
    public const int MagicOffset            = 0;
    public const int PageTypeOffset         = 4;
    public const int FormatVersionOffset    = 5;
    public const int SlotCountOffset        = 6;
    public const int PageIdOffset           = 8;
    public const int ParentPageIdOffset     = 12;
    public const int FreeSpaceOffsetOffset  = 16;
    public const int FreeSizeOffset         = 18;
    public const int PageLsnOffset          = 20;
    public const int ChecksumOffset         = 28;
    public const int CommonHeaderSize       = 32;

    // ── Leaf Extra Header (16 bytes, starts at offset 32) ────────────────────
    // Offset 32 : uint  PrevLeafPageId     (4 bytes)
    // Offset 36 : uint  NextLeafPageId     (4 bytes)
    // Offset 40 : uint  TotalRecordCount   (4 bytes)
    // Offset 44 : uint  LeafHeaderReserved (4 bytes)
    public const int PrevLeafPageIdOffset   = 32;
    public const int NextLeafPageIdOffset   = 36;
    public const int LeafRecordCountOffset  = 40;
    public const int LeafHeaderReserved     = 44;
    public const int LeafExtraHeaderSize    = 16;

    // ── Internal Extra Header (16 bytes, starts at offset 32) ────────────────
    // Offset 32 : uint  HighKeyOffset      (4 bytes)
    // Offset 36 : uint  HighKeyLength      (4 bytes)
    // Offset 40 : ulong InternalReserved   (8 bytes)
    public const int HighKeyOffsetField      = 32;
    public const int HighKeyLengthField      = 36;
    public const int InternalExtraHeaderSize = 16;

    // ── First Slot Offset ────────────────────────────────────────────────────
    // Leaf:     32 + 16 = 48
    // Internal: 32 + 16 = 48
    public const int FirstSlotOffset        = CommonHeaderSize + LeafExtraHeaderSize; // 48

    // ── Slot Entry (6 bytes per slot) ────────────────────────────────────────
    // bytes 0–3 : packed uint32 (CellOffset bits31–16, CellLength bits15–0)
    // byte  4   : Flags  (bit 0 = IsOverflow; bits 1–7 reserved, must be 0)
    // byte  5   : _reserved (must be 0)
    public const int  SlotEntrySize          = 6;
    public const int  SlotOffsetShift        = 16;
    public const uint SlotOffsetMask         = 0xFFFF0000u;
    public const uint SlotLengthMask         = 0x0000FFFFu;
    public const int  SlotFlagsOffset        = 4;    // byte offset within a slot entry
    public const byte SlotIsOverflow         = 0x01; // Flags bit 0

    // Overflow pointer record written in the leaf data area when SlotIsOverflow = 1.
    // [TotalValueLength:4 BE][FirstOverflowPageId:4 BE][_reserved:1]
    public const int  OverflowPointerSize    = 9;

    // ── Meta Page (PageId = 0) ────────────────────────────────────────────────
    // Fixed positions after CommonHeaderSize:
    // Offset 32 : uint  MetaMagic          (4 bytes)
    // Offset 36 : byte  MetaFormatVersion  (1 byte)
    // Offset 37 : byte  MetaReserved1      (3 bytes padding)
    // Offset 40 : uint  RootPageId         (4 bytes)
    // Offset 44 : uint  FirstLeafPageId    (4 bytes)
    // Offset 48 : uint  TotalPageCount     (4 bytes)
    // Offset 52 : ulong TotalRecordCount   (8 bytes)
    // Offset 60 : uint  FreeListHeadPageId (4 bytes)
    // Offset 64 : ulong LastCheckpointLsn  (8 bytes)
    // Offset 72 : uint  TreeHeight         (4 bytes)
    public const int MetaMagicOffset        = 32;
    public const int MetaFormatVerOffset    = 36;
    public const int MetaRootPageIdOffset   = 40;
    public const int MetaFirstLeafOffset    = 44;
    public const int MetaTotalPageOffset    = 48;
    public const int MetaTotalRecordOffset  = 52;
    public const int MetaFreeListHeadOffset = 60;
    public const int MetaLastCkptLsnOffset  = 64;
    public const int MetaTreeHeightOffset   = 72;
    public const int MetaPageId             = 0;

    // ── Sentinel Values ───────────────────────────────────────────────────────
    public const uint  NullPageId           = 0xFFFFFFFF;
    public const ulong NullLsn              = 0UL;

    // ── Default page size ─────────────────────────────────────────────────────
    public const int DefaultPageSize        = 8192;

    // ── Entry size limits ─────────────────────────────────────────────────────
    // MaxKeySize: key must fit in an internal-node cell (key + 4-byte child pointer).
    // Capped at 512 to guarantee internal pages hold ≥ 8 separator keys (fan-out).
    // Formula: min(512, (pageSize - FirstSlotOffset) / 4 - sizeof(uint))
    public static int MaxKeySize(int pageSize) =>
        Math.Min(512, (pageSize - FirstSlotOffset) / 4 - sizeof(uint));

    // MaxEntrySize: key+value must fit on an otherwise-empty leaf, leaving room for
    // the split invariant (both halves non-empty after every split).
    // Formula: (pageSize - FirstSlotOffset) / 2 - SlotEntrySize
    public static int MaxEntrySize(int pageSize) =>
        (pageSize - FirstSlotOffset) / 2 - SlotEntrySize;
}
