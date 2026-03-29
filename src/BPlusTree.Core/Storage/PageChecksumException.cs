namespace BPlusTree.Core.Api;

/// <summary>
/// Thrown when a page's on-disk CRC32 checksum does not match the recomputed value.
/// Indicates silent storage corruption: the page bytes differ from what was written.
/// </summary>
public sealed class PageChecksumException : BPlusTreeException
{
    public uint PageId   { get; }
    public uint Stored   { get; }
    public uint Computed { get; }

    public PageChecksumException(uint pageId, uint stored, uint computed)
        : base($"Page {pageId}: checksum mismatch — stored 0x{stored:X8}, computed 0x{computed:X8}.")
    {
        PageId   = pageId;
        Stored   = stored;
        Computed = computed;
    }
}
