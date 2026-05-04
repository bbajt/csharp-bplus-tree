using ByTech.BPlusTree.Core.Storage;
using System.Buffers.Binary;

namespace ByTech.BPlusTree.Core.Storage;

/// <summary>
/// Intrusive free list. Recycled page ids are stored inside the first 8 bytes of
/// freed page data: uint NextFreePageId (4 bytes) at offset 0, then uint reserved.
/// Head is stored in the meta page at PageLayout.MetaFreeListHeadOffset.
/// Thread-safe via the provided PageManager lock (callers hold the lock).
/// </summary>
internal sealed class FreeList
{
    private readonly PageManager _pageManager;

    // In-memory head of the free list. Mirrors what is written to the meta page.
    private uint _headPageId = PageLayout.NullPageId;

    // Free-page counter. Restored by walking the chain in LoadFromMeta(),
    // then kept accurate by Allocate() (decrement) and Deallocate() (increment).
    private int _freePageCount;

    public FreeList(PageManager pageManager)
        => _pageManager = pageManager;

    public bool HasFreePages => _headPageId != PageLayout.NullPageId;

    /// <summary>
    /// Number of pages currently on the free list.
    /// Accurate after open (<see cref="LoadFromMeta"/> walks the chain) and
    /// reflects all allocations and deallocations during the current session.
    /// </summary>
    public int Count => _freePageCount;

    /// <summary>
    /// Pop the head of the free list and return its pageId.
    /// Reads NextFreePageId from the freed page to update head.
    /// If list empty, calls StorageFile.AllocatePage() to grow the file.
    /// </summary>
    public uint Allocate()
    {
        if (_headPageId != PageLayout.NullPageId)
        {
            _freePageCount--;
            uint allocatedId = _headPageId;
            var frame = _pageManager.FetchPage(allocatedId);
            try
            {
                // Read the next free page ID from the first 4 bytes of the page data
                uint nextFreePageId = BinaryPrimitives.ReadUInt32LittleEndian(frame.Data.AsSpan(0, 4));

                // Update head to point to the next free page
                _headPageId = nextFreePageId;

                return allocatedId;
            }
            finally
            {
                _pageManager.Unpin(allocatedId);
            }
        }
        else
        {
            // List is empty, allocate a new page from the storage
            return _pageManager.Storage.AllocatePage();
        }
    }

    /// <summary>
    /// Push pageId onto the head of the free list.
    /// Writes current head into the freed page at offset 0, updates head to pageId.
    /// </summary>
    public void Deallocate(uint pageId)
    {
        _freePageCount++;
        // Get the frame for this page
        var frame = _pageManager.FetchPage(pageId);
        try
        {
            // Write the current head page ID to the beginning of the page data (first 4 bytes)
            BinaryPrimitives.WriteUInt32LittleEndian(frame.Data.AsSpan(0, 4), _headPageId);
            // Set the reserved field (next 4 bytes) to 0
            BinaryPrimitives.WriteUInt32LittleEndian(frame.Data.AsSpan(4, 4), 0u);

            // Update the head to point to this page
            _headPageId = pageId;
        }
        finally
        {
            _pageManager.Unpin(pageId);
        }
    }

    /// <summary>
    /// Load the free-list head from the meta page on open.
    /// Walks the full chain to restore an accurate <see cref="Count"/>.
    /// </summary>
    public void LoadFromMeta(uint headPageId)
    {
        _headPageId = headPageId;

        // Walk the chain to restore the accurate free-page count.
        // Each free page stores the next pageId in its first 4 bytes (little-endian).
        int count = 0;
        uint current = headPageId;
        while (current != PageLayout.NullPageId)
        {
            count++;
            var frame = _pageManager.FetchPage(current);
            uint next = BinaryPrimitives.ReadUInt32LittleEndian(frame.Data.AsSpan(0, 4));
            _pageManager.Unpin(current);
            current = next;
        }
        _freePageCount = count;
    }

    public uint HeadPageId => _headPageId;
}