using System.Buffers;
using System.Buffers.Binary;
using BPlusTree.Core.Api;
using BPlusTree.Core.Wal;

namespace BPlusTree.Core.Storage;

/// <summary>
/// Central coordinator for all page I/O. Owns StorageFile, BufferPool, and FreeList.
/// All public methods are thread-safe.
/// </summary>
public sealed class PageManager : IDisposable
{
    private readonly StorageFile  _storage;
    private readonly BufferPool   _pool;
    private readonly FreeList     _freeList;
    private readonly BPlusTreeOptions _options;
    private readonly object       _lock = new();
    private bool _disposed;

    private WalWriter? _walWriter;

    // Pages deferred by FreePage because their frame was pinned at free-time.
    // Completed by Unpin(uint) when PinCount reaches 0.
    private readonly HashSet<uint> _deferredFrees    = new();
    private volatile int           _deferredFreeCount;

    public int  PageSize        => _storage.PageSize;
    public uint TotalPageCount  => _storage.TotalPageCount;
    public long DirtyPageCount { get; private set; }

    private PageManager(StorageFile storage, BufferPool pool, BPlusTreeOptions options)
    {
        _storage  = storage;
        _pool     = pool;
        _freeList = new FreeList(this);
        _options  = options;
    }

    /// <summary>
    /// Opens (or creates) the data file described by <paramref name="options"/>.
    /// </summary>
    /// <remarks>
    /// This method does not call <see cref="BPlusTreeOptions.Validate()"/>.
    /// Callers are responsible for validating options before calling <c>Open()</c>.
    /// <see cref="BPlusTree{TKey,TValue}.Open"/> calls <c>Validate()</c> automatically.
    /// </remarks>
    public static PageManager Open(BPlusTreeOptions options, WalWriter? wal = null)
    {
        var fileInfo  = new FileInfo(options.DataFilePath);
        var createNew = !fileInfo.Exists || fileInfo.Length == 0;
        var storage   = StorageFile.Open(options.DataFilePath, options.PageSize, createNew: createNew);
        var pool      = new BufferPool(storage, options.BufferPoolCapacity);
        var mgr       = new PageManager(storage, pool, options);
        mgr._walWriter      = wal;

        if (!createNew)
        {
            var frame = mgr.FetchPage(PageLayout.MetaPageId);
            try
            {
                // Only validate the format version when the magic number is present.
                // A zeroed/crashed page (magic = 0) is expected to be recovered via WAL;
                // rejecting it here would prevent crash recovery from working.
                // WriteInitialMetaPage uses BitConverter.GetBytes (little-endian on x64).
                uint fileMagic = BinaryPrimitives.ReadUInt32LittleEndian(frame.Data.AsSpan(PageLayout.MagicOffset));
                if (fileMagic == PageLayout.MagicNumber)
                {
                    byte fileVersion = frame.Data[PageLayout.FormatVersionOffset];
                    if (fileVersion != PageLayout.FormatVersion)
                        throw new InvalidDataException(
                            $"Unsupported data file format version {fileVersion}. " +
                            $"Expected {PageLayout.FormatVersion}. Re-import data to upgrade.");
                }

                var headPageId = BitConverter.ToUInt32(frame.Data, PageLayout.MetaFreeListHeadOffset);
                // When magic is absent the meta page is blank/crashed and will be repaired by
                // WAL recovery. Skip the chain walk to avoid following garbage pointers;
                // ReloadFreeList() called after recovery will restore Count accurately.
                mgr._freeList.LoadFromMeta(
                    fileMagic == PageLayout.MagicNumber ? headPageId : PageLayout.NullPageId);
            }
            finally
            {
                mgr.Unpin(PageLayout.MetaPageId);
            }
        }
        else
        {
            mgr.WriteInitialMetaPage();
        }

        return mgr;
    }

    /// <summary>
    /// Allocate a new page. Uses free list if available, otherwise extends file.
    /// Returns a pinned Frame ready for writing.
    /// </summary>
    public Frame AllocatePage(PageType pageType)
    {
        lock (_lock)
        {
            var pageId = _freeList.HasFreePages
                ? _freeList.Allocate()
                : _storage.AllocatePage();

            var frame = _pool.Pin(pageId);
            frame.SetPageId(pageId);
            frame.Data[PageLayout.PageTypeOffset] = (byte)pageType;
            if (_pool.OccupancyFraction > _options.EvictionHighWatermark)
                _pool.SignalEviction();
            return frame;
        }
    }

    /// <summary>
    /// Allocate a new page as a copy-on-write shadow of <paramref name="sourcePageId"/>.
    ///
    /// The source page content is copied verbatim into the new frame, then the
    /// self-page-ID field (<see cref="PageLayout.PageIdOffset"/>) in the new frame's
    /// Data is updated to the new page ID (big-endian, per NodePage convention).
    ///
    /// The source page is pinned during the copy and unpinned before return.
    /// The source page content is not modified.
    ///
    /// Returns the new frame pinned and dirty. Caller must modify the content as
    /// needed and call MarkDirtyAndUnpin when done.
    /// </summary>
    public Frame AllocatePageCow(uint sourcePageId)
    {
        lock (_lock)
        {
            // Allocate a new physical page.
            var newPageId = _freeList.HasFreePages
                ? _freeList.Allocate()
                : _storage.AllocatePage();

            var newFrame = _pool.Pin(newPageId);
            newFrame.SetPageId(newPageId);

            // Pin source to prevent eviction during the copy.
            var srcFrame = _pool.Pin(sourcePageId);
            srcFrame.Data.CopyTo(newFrame.Data, 0);
            _pool.Unpin(sourcePageId, isDirty: false);

            // Update the self-page-ID in the copied content (big-endian, NodePage convention).
            BinaryPrimitives.WriteUInt32BigEndian(
                newFrame.Data.AsSpan(PageLayout.PageIdOffset, sizeof(uint)), newPageId);

            // Mark dirty: this is a new page with modified content that must be written to disk.
            newFrame.IsDirty = true;

            if (_pool.OccupancyFraction > _options.EvictionHighWatermark)
                _pool.SignalEviction();

            return newFrame; // dirty; caller calls MarkDirtyAndUnpin after modification
        }
    }

    /// <summary>
    /// Allocate a chain of overflow pages to store <paramref name="value"/>.
    /// Pages are allocated in reverse logical order so each page's NextOverflowPageId
    /// is known at write time — no second pass is required.
    /// </summary>
    /// <param name="value">Raw serialised bytes of the large value.</param>
    /// <param name="firstPageId">Page ID of the chain head (logical page 0).</param>
    /// <param name="chainPageIds">All allocated page IDs in logical order (index 0 = head).</param>
    public void AllocateOverflowChain(
        ReadOnlySpan<byte> value,
        out uint           firstPageId,
        out uint[]         chainPageIds,
        uint               txId = 0)
    {
        int dataPerPage = OverflowPageLayout.DataCapacity(PageSize);
        int chunkCount  = value.IsEmpty ? 1 : (value.Length + dataPerPage - 1) / dataPerPage;
        var ids         = new uint[chunkCount];
        uint nextPageId = 0u; // end-of-chain sentinel

        for (int i = chunkCount - 1; i >= 0; i--)
        {
            int chunkStart = i * dataPerPage;
            int chunkLen   = Math.Min(dataPerPage, value.Length - chunkStart);

            var frame = AllocatePage(PageType.Overflow);
            ids[i] = frame.PageId;

            if (chunkLen > 0)
                value.Slice(chunkStart, chunkLen)
                     .CopyTo(frame.Data.AsSpan(OverflowPageLayout.DataOffset));

            System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(
                frame.Data.AsSpan(OverflowPageLayout.ChunkLengthOffset), (ushort)chunkLen);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(
                frame.Data.AsSpan(OverflowPageLayout.NextPageIdOffset), nextPageId);

            nextPageId = frame.PageId;
            MarkDirtyAndUnpin(frame.PageId);
        }

        firstPageId  = ids[0];
        chainPageIds = ids;
        _walWriter?.AppendAllocOverflowChain(txId, chainPageIds);
    }

    /// <summary>
    /// Free all overflow pages in the chain rooted at <paramref name="firstPageId"/>.
    /// Collects all page IDs first, writes a <see cref="Wal.WalRecordType.FreeOverflowChain"/>
    /// WAL record (Gap 2 closure), then frees the pages.
    /// The WAL record is flushed atomically with the preceding leaf UpdatePage and the
    /// following UpdateMeta by <c>_metadata.Flush()</c> — enabling the Redo Pass to
    /// re-apply the frees on crash-after-WAL-fsync-before-FreePage.
    /// </summary>
    public void FreeOverflowChain(uint firstPageId)
    {
        // Pass 1: collect all page IDs (read chain before any pages are freed).
        var ids = new System.Collections.Generic.List<uint>();
        uint pageId = firstPageId;
        while (pageId != 0)
        {
            var frame = FetchPage(pageId);
            uint nextPageId = System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(
                frame.Data.AsSpan(OverflowPageLayout.NextPageIdOffset));
            Unpin(pageId);
            ids.Add(pageId);
            pageId = nextPageId;
        }

        // Write WAL record before freeing (Gap 2). txId=0: auto-commit in-place path only.
        _walWriter?.AppendFreeOverflowChain(0, ids.ToArray());

        // Pass 2: free all collected pages.
        foreach (var id in ids)
            FreePage(id);
    }

    /// <summary>
    /// Read the full value stored in the overflow chain rooted at <paramref name="firstPageId"/>.
    /// </summary>
    /// <param name="firstPageId">Chain head page ID.</param>
    /// <param name="totalLength">Total byte length of the value (from the leaf pointer record).</param>
    /// <returns>Reconstructed value bytes.</returns>
    public byte[] ReadOverflowChain(uint firstPageId, int totalLength)
    {
        var result = new byte[totalLength];
        ReadOverflowChain(firstPageId, totalLength, result.AsSpan());
        return result;
    }

    /// <summary>
    /// Read the full value stored in the overflow chain into a caller-supplied span.
    /// No heap allocation — the caller owns the buffer (use <see cref="System.Buffers.ArrayPool{T}"/>
    /// for large values to avoid LOH pressure).
    /// </summary>
    /// <param name="firstPageId">Chain head page ID.</param>
    /// <param name="totalLength">Total byte length of the value (from the leaf pointer record).</param>
    /// <param name="destination">Must be at least <paramref name="totalLength"/> bytes.</param>
    public void ReadOverflowChain(uint firstPageId, int totalLength, Span<byte> destination)
    {
        int offset = 0;
        uint pageId = firstPageId;
        while (pageId != 0)
        {
            var frame = FetchPage(pageId);
            int chunkLen = System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(
                frame.Data.AsSpan(OverflowPageLayout.ChunkLengthOffset));
            uint nextPageId = System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(
                frame.Data.AsSpan(OverflowPageLayout.NextPageIdOffset));
            frame.Data.AsSpan(OverflowPageLayout.DataOffset, chunkLen)
                      .CopyTo(destination.Slice(offset));
            Unpin(pageId);
            offset += chunkLen;
            pageId  = nextPageId;
        }
    }

    /// <summary>Fetch an existing page into the buffer pool and pin it.</summary>
    public Frame FetchPage(uint pageId)
    {
        var frame = _pool.Pin(pageId);
        // OccupancyFraction is lock-free (volatile _occupiedCount).
        if (_pool.OccupancyFraction > _options.EvictionHighWatermark)
            _pool.SignalEviction();
        return frame;
    }

    /// <summary>
    /// Mark the frame dirty and unpin it.
    /// When a WalWriter is present and no walLsn is supplied, automatically appends
    /// an UpdatePage (or UpdateMeta for page 0) WAL record using the current page data
    /// as the after-image, satisfying the WAL-before-page invariant transparently.
    /// </summary>
    public void MarkDirtyAndUnpin(uint pageId, LogSequenceNumber? walLsn = null)
    {
        lock (_lock)
        {
            var frame = _pool.Pin(pageId);

            if (_walWriter != null && !walLsn.HasValue)
            {
                var recordType = pageId == PageLayout.MetaPageId
                    ? WalRecordType.UpdateMeta
                    : WalRecordType.UpdatePage;
                walLsn = _walWriter.Append(recordType, 0, pageId, LogSequenceNumber.None, frame.Data);
            }

            frame.PageLsn = walLsn?.Value ?? 0UL;
            _pool.Unpin(pageId, isDirty: false); // undo the extra pin
            _pool.Unpin(pageId, isDirty: true);
            DirtyPageCount++;
        }
    }

    /// <summary>
    /// Transactional overload: writes an UpdatePage/UpdateMeta WAL record whose data
    /// payload is [before-image (PageSize bytes)][after-image (PageSize bytes)].
    /// The DataLength == 2×PageSize distinguishes transactional records from auto-commit
    /// records (DataLength == PageSize) during crash recovery.
    /// Returns the LSN assigned to the WAL record so the caller can update its PrevLsn chain.
    /// Uses ArrayPool for the combined buffer — zero heap allocation on the hot path.
    /// </summary>
    internal LogSequenceNumber MarkDirtyAndUnpin(
        uint               pageId,
        uint               txId,
        LogSequenceNumber  prevLsn,
        ReadOnlySpan<byte> beforeImage)
    {
        lock (_lock)
        {
            var frame      = _pool.Pin(pageId);
            var recordType = pageId == PageLayout.MetaPageId
                ? WalRecordType.UpdateMeta
                : WalRecordType.UpdatePage;

            int    totalLen = frame.Data.Length * 2;
            byte[] combined = ArrayPool<byte>.Shared.Rent(totalLen);
            LogSequenceNumber lsn;
            try
            {
                beforeImage.CopyTo(combined.AsSpan(0, frame.Data.Length));
                frame.Data.CopyTo(combined, frame.Data.Length);
                lsn = _walWriter!.Append(recordType, txId, pageId, prevLsn,
                                   combined.AsSpan(0, totalLen));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(combined);
            }

            frame.PageLsn = lsn.Value;
            _pool.Unpin(pageId, isDirty: false); // undo the extra Pin
            _pool.Unpin(pageId, isDirty: true);
            DirtyPageCount++;
            return lsn;
        }
    }

    /// <summary>
    /// Mark the frame dirty and unpin it, bypassing or enforcing the WAL-before-page invariant.
    /// <para>
    ///   <paramref name="bypassWal"/> = <c>true</c>  — for WAL recovery writes: skips WAL logging,
    ///   uses whatever PageLsn the frame already has (recovery set it before calling this).
    /// </para>
    /// <para>
    ///   <paramref name="bypassWal"/> = <c>false</c> — enforces the invariant: throws
    ///   <see cref="InvalidOperationException"/> when a WAL is attached and no LSN was
    ///   pre-logged by the caller. Use the <c>walLsn</c> overload for normal write paths.
    /// </para>
    /// </summary>
    public void MarkDirtyAndUnpin(uint pageId, bool bypassWal)
    {
        lock (_lock)
        {
            if (!bypassWal && _walWriter != null)
                throw new InvalidOperationException(
                    "WAL-Before-Page invariant violated: write a WAL record before marking the page dirty, " +
                    "or pass walLsn to the other overload.");

            var frame = _pool.Pin(pageId);
            _pool.Unpin(pageId, isDirty: false); // undo the extra pin
            _pool.Unpin(pageId, isDirty: true);
            DirtyPageCount++;
        }
    }

    /// <summary>
    /// Extend the data file to ensure <paramref name="pageId"/> exists.
    /// Called from <see cref="CheckpointManager.RecoverFromWal"/> before fetching
    /// pages that may be absent from a wiped/truncated data file.
    /// </summary>
    internal void EnsurePageExists(uint pageId)
    {
        lock (_lock)
        {
            _storage.EnsurePageExists(pageId);
        }
    }

    /// <summary>
    /// Re-read the free-list head pointer from the meta page in the buffer pool.
    /// Called after WAL recovery to repair the free-list state that was initialised
    /// from the (possibly blank or stale) on-disk meta page during Open().
    /// </summary>
    internal void ReloadFreeList()
    {
        lock (_lock)
        {
            var frame = _pool.Pin(PageLayout.MetaPageId);
            try
            {
                var headPageId = BitConverter.ToUInt32(frame.Data, PageLayout.MetaFreeListHeadOffset);
                _freeList.LoadFromMeta(headPageId);
            }
            finally
            {
                _pool.Unpin(PageLayout.MetaPageId, isDirty: false);
            }
        }
    }

    /// <summary>Unpin a frame without marking it dirty.</summary>
    public void Unpin(uint pageId)
    {
        _pool.Unpin(pageId, false);
        if (_deferredFreeCount > 0)          // volatile read; fast exit when queue empty (~2 ns)
            TryCompleteDeferredFree(pageId);
    }

    /// <summary>Flush a specific page to disk.</summary>
    public void FlushPage(uint pageId)
    {
        lock (_lock)
        {
            _pool.FlushPage(pageId);
        }
    }

    /// <summary>
    /// Head of the in-memory free list. <see cref="PageLayout.NullPageId"/> when the
    /// free list is empty. Used by <see cref="CheckpointManager"/> to detect pages
    /// already freed so it can avoid double-freeing during WAL Redo.
    /// </summary>
    internal uint FreeListHead => _freeList.HeadPageId;

    /// <summary>Flush all dirty pages. Used during checkpoint.</summary>
    public void CheckpointFlush()
    {
        lock (_lock)
        {
            _pool.FlushAllDirty();
        }
    }

    /// <summary>Return a page to the free list.</summary>
    /// <remarks>
    /// Pin-aware: if the frame is currently pinned (PinCount &gt; 0), the free is deferred
    /// until <see cref="Unpin(uint)"/> brings PinCount to 0 via
    /// <see cref="TryCompleteDeferredFree"/>. This prevents <c>_freeList.Deallocate</c>
    /// from making the page ID reusable while a reader still holds a pin on the frame.
    /// </remarks>
    public void FreePage(uint pageId)
    {
        lock (_lock)
        {
            if (_pool.GetPinCount(pageId) > 0)
            {
                if (_deferredFrees.Add(pageId))
                    Interlocked.Increment(ref _deferredFreeCount);
                return;
            }
            _freeList.Deallocate(pageId);
            var frame = _pool.Pin(pageId);
            frame.IsDirty = true;
            _pool.Unpin(pageId, true);   // direct BufferPool call — does not retrigger TryCompleteDeferredFree
        }
    }

    /// <summary>
    /// Complete the deferred free of <paramref name="pageId"/> if it is in the deferred set
    /// and its frame's PinCount has reached 0. Called from <see cref="Unpin(uint)"/> on the
    /// page that was just unpinned. Idempotent.
    /// </summary>
    private void TryCompleteDeferredFree(uint pageId)
    {
        lock (_lock)
        {
            if (!_deferredFrees.Contains(pageId)) return;
            if (_pool.GetPinCount(pageId) > 0) return;   // still pinned by another caller
            _deferredFrees.Remove(pageId);
            Interlocked.Decrement(ref _deferredFreeCount);
            _freeList.Deallocate(pageId);
            var frame = _pool.Pin(pageId);
            frame.IsDirty = true;
            _pool.Unpin(pageId, true);   // direct BufferPool call — does not retrigger this method
        }
    }

    /// <summary>
    /// Replay WAL records forward from the last checkpoint.
    /// Re-applies UpdatePage/UpdateMeta after-images where pageLsn is stale.
    /// No-op when no WAL is attached.
    /// </summary>
    public void Recover()
    {
        if (_walWriter == null) return;

        var reader     = new WalReader(_options.WalFilePath);
        var ckptLsn    = reader.FindLastCheckpointEnd() ?? LogSequenceNumber.None;

        foreach (var record in reader.ReadForward(ckptLsn))
        {
            if (record.Type != WalRecordType.UpdatePage &&
                record.Type != WalRecordType.UpdateMeta)
                continue;

            if (record.Data.Length == 0) continue;

            lock (_lock)
            {
                var frame = _pool.Pin(record.PageId);
                if (frame.PageLsn <= record.Lsn.Value)
                {
                    record.Data.CopyTo(frame.Data);
                    frame.PageLsn = record.Lsn.Value;
                    _pool.Unpin(record.PageId, isDirty: true);
                }
                else
                {
                    _pool.Unpin(record.PageId, isDirty: false);
                }
            }
        }

        lock (_lock)
        {
            _pool.FlushAllDirty();
            _storage.Flush();
        }
    }

    /// <summary>
    /// Flush all dirty pages and close the storage file handle.
    /// Called by Compactor just before File.Move on Windows, which requires no open
    /// handles on the destination path.
    /// </summary>
    internal void PrepareForRename()
    {
        lock (_lock)
        {
            _pool.FlushAllDirty();
            _storage.CloseStream();
        }
    }

    /// <summary>
    /// Reopen the storage file (now pointing to compact content), evict all stale
    /// buffer pool frames, and reload the free-list head from the new meta page.
    /// Called by Compactor immediately after File.Move succeeds.
    /// </summary>
    internal void AfterRename()
    {
        lock (_lock)
        {
            _storage.ReopenStream();
            _pool.EvictAll();

            var frame = _pool.Pin(PageLayout.MetaPageId);
            try
            {
                var headPageId = BitConverter.ToUInt32(frame.Data, PageLayout.MetaFreeListHeadOffset);
                _freeList.LoadFromMeta(headPageId);
            }
            finally
            {
                _pool.Unpin(PageLayout.MetaPageId, isDirty: false);
            }
        }
    }

    internal StorageFile     Storage      => _storage;
    internal BufferPool      BufferPool   => _pool;
    internal FreeList        FreeList     => _freeList;
    internal WalWriter?      Wal          => _walWriter;
    internal string          WalFilePath  => _options.WalFilePath;
    internal string          DataFilePath => _options.DataFilePath;
    internal BPlusTreeOptions Options     => _options;

    private void WriteInitialMetaPage()
    {
        lock (_lock)
        {
            var frame = _pool.NewPage(0);
            try
            {
                Array.Clear(frame.Data);
                BitConverter.GetBytes(PageLayout.MagicNumber).CopyTo(frame.Data, PageLayout.MagicOffset);
                frame.Data[PageLayout.PageTypeOffset]      = (byte)PageType.Meta;
                frame.Data[PageLayout.FormatVersionOffset] = PageLayout.FormatVersion;

                // Page-pointer sentinel fields = NullPageId (empty tree, no free pages)
                BitConverter.GetBytes(PageLayout.NullPageId).CopyTo(frame.Data, PageLayout.MetaRootPageIdOffset);
                BitConverter.GetBytes(PageLayout.NullPageId).CopyTo(frame.Data, PageLayout.MetaFirstLeafOffset);
                BitConverter.GetBytes(PageLayout.NullPageId).CopyTo(frame.Data, PageLayout.MetaFreeListHeadOffset);

                frame.IsDirty = true;
                _pool.FlushPage(0);
                _storage.Flush();
            }
            finally
            {
                _pool.Unpin(0, true);
            }
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;   // set first so double-Dispose is safe even if cleanup throws
            _pool.FlushAllDirty();
            _storage.Dispose();
            _walWriter?.Dispose();
        }
    }
}
