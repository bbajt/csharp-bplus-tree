namespace BPlusTree.Core.Storage;

/// <summary>
/// Byte offsets for overflow page layout.
/// Overflow pages share the 32-byte common page header with leaf/internal pages.
/// The remaining bytes store a chunk of a large value in a singly-linked chain.
/// </summary>
/// <remarks>
/// Layout (after the 32-byte common header):
///   Offset 32 : uint   NextOverflowPageId  (4 bytes; 0 = end of chain)
///   Offset 36 : ushort ChunkLength         (2 bytes; valid byte count in Data area)
///   Offset 38 : byte[] Data                (pageSize - 38 bytes)
/// </remarks>
public static class OverflowPageLayout
{
    /// <summary>Offset of the NextOverflowPageId field (4 bytes; 0 = end of chain).</summary>
    public const int NextPageIdOffset  = PageLayout.CommonHeaderSize;        // 32

    /// <summary>Offset of the ChunkLength field (2 bytes; valid bytes in Data).</summary>
    public const int ChunkLengthOffset = PageLayout.CommonHeaderSize + 4;    // 36

    /// <summary>Offset of the Data area (raw value bytes for this chunk).</summary>
    public const int DataOffset        = PageLayout.CommonHeaderSize + 4 + 2; // 38

    /// <summary>
    /// Maximum bytes of value data that fit in a single overflow page.
    /// </summary>
    public static int DataCapacity(int pageSize) => pageSize - DataOffset;
}
