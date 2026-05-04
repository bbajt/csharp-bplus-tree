using System.Buffers.Binary;
using ByTech.BPlusTree.Core.Storage;

namespace ByTech.BPlusTree.Core.Engine;

/// <summary>
/// Reads and writes the meta page (PageId 0).
/// Holds cached in-memory copies of all meta fields; persists on Flush.
/// </summary>
internal sealed class TreeMetadata
{
    private readonly PageManager _pageManager;

    public uint  RootPageId         { get; private set; } = PageLayout.NullPageId;
    public uint  FirstLeafPageId    { get; private set; } = PageLayout.NullPageId;
    public uint  TotalPageCount     { get; private set; }
    public ulong TotalRecordCount   { get; private set; }
    public uint  FreeListHeadPageId { get; private set; } = PageLayout.NullPageId;
    public ulong LastCheckpointLsn  { get; private set; }
    public uint  TreeHeight         { get; private set; }

    public TreeMetadata(PageManager pageManager) => _pageManager = pageManager;

    /// <summary>Load meta fields from the meta page on disk.</summary>
    public void Load()
    {
        var frame = _pageManager.FetchPage(PageLayout.MetaPageId);
        try
        {
            var data = frame.Data.AsSpan();
            RootPageId         = BinaryPrimitives.ReadUInt32BigEndian(data[PageLayout.MetaRootPageIdOffset..]);
            FirstLeafPageId    = BinaryPrimitives.ReadUInt32BigEndian(data[PageLayout.MetaFirstLeafOffset..]);
            TotalPageCount     = BinaryPrimitives.ReadUInt32BigEndian(data[PageLayout.MetaTotalPageOffset..]);
            TotalRecordCount   = BinaryPrimitives.ReadUInt64BigEndian(data[PageLayout.MetaTotalRecordOffset..]);
            FreeListHeadPageId = BinaryPrimitives.ReadUInt32BigEndian(data[PageLayout.MetaFreeListHeadOffset..]);
            LastCheckpointLsn  = BinaryPrimitives.ReadUInt64BigEndian(data[PageLayout.MetaLastCkptLsnOffset..]);
            TreeHeight         = BinaryPrimitives.ReadUInt32BigEndian(data[PageLayout.MetaTreeHeightOffset..]);
        }
        finally
        {
            _pageManager.Unpin(PageLayout.MetaPageId);
        }
    }

    /// <summary>Persist all in-memory meta fields to the meta page and mark it dirty.</summary>
    public void Flush()
    {
        var frame = _pageManager.FetchPage(PageLayout.MetaPageId);
        var data = frame.Data.AsSpan();
        BinaryPrimitives.WriteUInt32BigEndian(data[PageLayout.MetaRootPageIdOffset..],   RootPageId);
        BinaryPrimitives.WriteUInt32BigEndian(data[PageLayout.MetaFirstLeafOffset..],    FirstLeafPageId);
        BinaryPrimitives.WriteUInt32BigEndian(data[PageLayout.MetaTotalPageOffset..],    TotalPageCount);
        BinaryPrimitives.WriteUInt64BigEndian(data[PageLayout.MetaTotalRecordOffset..],  TotalRecordCount);
        BinaryPrimitives.WriteUInt32BigEndian(data[PageLayout.MetaFreeListHeadOffset..], FreeListHeadPageId);
        BinaryPrimitives.WriteUInt64BigEndian(data[PageLayout.MetaLastCkptLsnOffset..],  LastCheckpointLsn);
        BinaryPrimitives.WriteUInt32BigEndian(data[PageLayout.MetaTreeHeightOffset..],   TreeHeight);
        _pageManager.MarkDirtyAndUnpin(PageLayout.MetaPageId);
    }

    /// <summary>
    /// Transactional overload: captures the meta page before-image before any field
    /// writes and routes the page write through the transaction's WAL record chain.
    /// Called by Splitter and Merger at metadata flush sites when _tx is non-null.
    /// CaptureBeforeImage is idempotent — first capture per page per transaction wins.
    /// </summary>
    internal void Flush(ITransactionContext tx)
    {
        var frame = _pageManager.FetchPage(PageLayout.MetaPageId);

        // Capture before-image BEFORE writing any fields.
        tx.CaptureBeforeImage(PageLayout.MetaPageId, frame.Data);

        var data = frame.Data.AsSpan();
        BinaryPrimitives.WriteUInt32BigEndian(data[PageLayout.MetaRootPageIdOffset..],   RootPageId);
        BinaryPrimitives.WriteUInt32BigEndian(data[PageLayout.MetaFirstLeafOffset..],    FirstLeafPageId);
        BinaryPrimitives.WriteUInt32BigEndian(data[PageLayout.MetaTotalPageOffset..],    TotalPageCount);
        BinaryPrimitives.WriteUInt64BigEndian(data[PageLayout.MetaTotalRecordOffset..],  TotalRecordCount);
        BinaryPrimitives.WriteUInt32BigEndian(data[PageLayout.MetaFreeListHeadOffset..], FreeListHeadPageId);
        BinaryPrimitives.WriteUInt64BigEndian(data[PageLayout.MetaLastCkptLsnOffset..],  LastCheckpointLsn);
        BinaryPrimitives.WriteUInt32BigEndian(data[PageLayout.MetaTreeHeightOffset..],   TreeHeight);

        var lsn = _pageManager.MarkDirtyAndUnpin(
            PageLayout.MetaPageId,
            tx.TransactionId,
            tx.LastLsn,
            tx.GetBeforeImage(PageLayout.MetaPageId));
        tx.UpdateLastLsn(lsn);
    }

    /// <summary>Set the root page ID and tree height.</summary>
    public void SetRoot(uint rootPageId, uint treeHeight)  { RootPageId = rootPageId; TreeHeight = treeHeight; }
    /// <summary>Set the first leaf page ID.</summary>
    public void SetFirstLeaf(uint firstLeafPageId)          => FirstLeafPageId = firstLeafPageId;
    /// <summary>Increment the total record count by one.</summary>
    public void IncrementRecordCount()                      => TotalRecordCount++;
    /// <summary>Decrement the total record count by one.</summary>
    public void DecrementRecordCount()                      => TotalRecordCount--;
    /// <summary>Set the LSN of the last completed checkpoint.</summary>
    public void SetLastCheckpointLsn(ulong lsn)             => LastCheckpointLsn = lsn;

    /// <summary>
    /// Directly sets the record count. Used post-WAL-recovery when the accumulated
    /// leaf-chain count supersedes the (possibly truncated) on-disk metadata value.
    /// Call <see cref="Flush"/> to persist.
    /// </summary>
    public void SetTotalRecordCount(ulong count) => TotalRecordCount = count;
}
