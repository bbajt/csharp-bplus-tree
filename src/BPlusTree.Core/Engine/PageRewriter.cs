using ByTech.BPlusTree.Core.Storage;
using ByTech.BPlusTree.Core.Wal;

namespace ByTech.BPlusTree.Core.Engine;

/// <summary>
/// In-place page defragmentation. Reclaims internal fragmentation caused by
/// delete operations without changing the page's logical position in the tree.
///
/// Fragmentation arises because deletes leave dead cell bytes in the cell area
/// (the slot array entry is removed but the cell bytes are not immediately overwritten).
/// Over time, AllocateCell() can fail even though the total live cell bytes would fit.
///
/// Algorithm:
///   1. Read all live cells referenced by the current slot array.
///      A live cell is one whose offset is present in any SlotDescriptor.
///   2. Compact cells to the end of the page, packed tightly with NO gaps.
///      Cell order must match slot order (slot 0's cell is at the highest address, etc.)
///      to preserve the NodePage invariant that cells grow downward.
///   3. Rebuild the slot array: update each SlotDescriptor.CellOffset to the new address.
///   4. Update NodePage header:
///        FreeSpaceOffset = HeaderSize + SlotCount * SlotDescriptorSize
///        FreeSpaceSize   = PageSize - FreeSpaceOffset - (total live cell bytes)
///   5. Write a WAL UpdatePage record for the defragmented page.
///   6. Mark frame dirty (via WAL bypass — the UpdatePage record IS the WAL write).
///
/// Invariants that must hold after Defragment:
///   - SlotCount unchanged.
///   - All keys and values byte-for-byte identical to before defragmentation.
///   - FreeSpaceSize >= (old FreeSpaceSize + dead cell bytes).
///   - Page passes NodePage.Validate().
/// </summary>
internal static class PageRewriter
{
    /// <summary>
    /// Defragment a page in-place. frame must be pinned and write-latched by the caller.
    /// Writes one WAL UpdatePage record.
    /// No-op if FragmentedBytes(frame) == 0.
    /// </summary>
    public static void Defragment(Frame frame, WalWriter wal, uint transactionId)
    {
        if (FragmentedBytes(frame) == 0)
            return;

        var page = new NodePage(frame.Data);
        int slotCount = page.SlotCount;

        // Step 1: Read all live cells referenced by the current slot array.
        var cells = new (ushort length, byte[] data, byte flags)[slotCount];
        for (int i = 0; i < slotCount; i++)
        {
            var (offset, length, flags) = page.GetSlot(i);
            var data = new byte[length];
            frame.Data.AsSpan(offset, length).CopyTo(data);
            cells[i] = (length, data, flags);
        }

        // Step 2 & 3: Compact cells to the end of the page, slot 0 at the highest address.
        // Rebuild slot descriptors with new offsets.
        int totalLiveBytes = 0;
        for (int i = 0; i < slotCount; i++)
            totalLiveBytes += cells[i].length;

        int writePos = frame.Data.Length;
        for (int i = 0; i < slotCount; i++)
        {
            var (length, data, flags) = cells[i];
            writePos -= length;
            data.CopyTo(frame.Data, writePos);
            page.SetSlot(i, (ushort)writePos, length, flags);
        }

        // Step 4: Update NodePage header.
        int freeSpaceOffset = PageLayout.FirstSlotOffset + slotCount * PageLayout.SlotEntrySize;
        page.FreeSpaceOffset = (ushort)freeSpaceOffset;
        page.FreeSpaceSize   = (ushort)(frame.Data.Length - freeSpaceOffset - totalLiveBytes);

        // Step 5: Write a WAL UpdatePage record for the defragmented page.
        wal.Append(WalRecordType.UpdatePage, transactionId, frame.PageId, LogSequenceNumber.None, frame.Data);

        // Step 6: Mark frame dirty.
        frame.IsDirty = true;
    }

    /// <summary>
    /// Returns the total wasted bytes in the cell area (dead cells from previous deletes).
    /// FragmentedBytes = (PageSize - HeaderSize - SlotArray size - FreeSpaceSize) - (live cell bytes).
    /// Returns 0 if there is no fragmentation.
    /// </summary>
    public static int FragmentedBytes(Frame frame)
    {
        var page = new NodePage(frame.Data);

        // Total bytes occupied by the cell region (live + dead):
        //   cell region = PageSize - FreeSpaceOffset - FreeSpaceSize
        int totalAllocatedCellBytes = frame.Data.Length - page.FreeSpaceOffset - page.FreeSpaceSize;

        // Live cell bytes = sum of all current slot cell lengths.
        int liveCellBytes = 0;
        for (int i = 0; i < page.SlotCount; i++)
        {
            var (_, length, _) = page.GetSlot(i);
            liveCellBytes += length;
        }

        return Math.Max(0, totalAllocatedCellBytes - liveCellBytes);
    }
}
