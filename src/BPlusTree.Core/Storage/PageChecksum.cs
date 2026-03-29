using System.Buffers.Binary;
using System.IO.Hashing;
using BPlusTree.Core.Api;

namespace BPlusTree.Core.Storage;

/// <summary>
/// CRC32 stamp and verify helpers for page-level corruption detection.
///
/// Stamp: called immediately before every StorageFile.WritePage / WritePageBatch.
///   Zeroes the checksum field, computes CRC32 over the full page, writes the result.
///
/// Verify: called immediately after StorageFile.ReadPage in the BufferPool cold path.
///   Reads the stored checksum; if 0, skips (pre-checksum page). Otherwise zeroes
///   the field, recomputes, restores, and throws PageChecksumException on mismatch.
/// </summary>
internal static class PageChecksum
{
    /// <summary>
    /// Stamps a CRC32 checksum into <paramref name="pageData"/> at
    /// <see cref="PageLayout.ChecksumOffset"/>. Mutates the buffer in-place.
    /// Must be called with final page content (including PageLsn) already written.
    /// </summary>
    public static void Stamp(byte[] pageData)
    {
        // 1. Zero the checksum field so it is not included in the CRC input.
        BinaryPrimitives.WriteUInt32LittleEndian(
            pageData.AsSpan(PageLayout.ChecksumOffset, sizeof(uint)), 0);
        // 2. Compute CRC32 over the entire page.
        uint crc = Crc32.HashToUInt32(pageData);
        // 3. Write the result.
        BinaryPrimitives.WriteUInt32LittleEndian(
            pageData.AsSpan(PageLayout.ChecksumOffset, sizeof(uint)), crc);
    }

    /// <summary>
    /// Verifies the CRC32 checksum of <paramref name="pageData"/>.
    /// A stored checksum of 0 is silently accepted (pre-checksum page).
    /// Throws <see cref="PageChecksumException"/> on mismatch.
    /// Leaves the buffer unchanged.
    /// </summary>
    public static void Verify(byte[] pageData, uint pageId)
    {
        uint stored = BinaryPrimitives.ReadUInt32LittleEndian(
            pageData.AsSpan(PageLayout.ChecksumOffset, sizeof(uint)));

        if (stored == 0) return; // pre-checksum page — backward compat skip

        // Temporarily zero the checksum field to recompute.
        BinaryPrimitives.WriteUInt32LittleEndian(
            pageData.AsSpan(PageLayout.ChecksumOffset, sizeof(uint)), 0);
        uint computed = Crc32.HashToUInt32(pageData);
        // Restore: caller must see the buffer unchanged.
        BinaryPrimitives.WriteUInt32LittleEndian(
            pageData.AsSpan(PageLayout.ChecksumOffset, sizeof(uint)), stored);

        if (computed != stored)
            throw new PageChecksumException(pageId, stored, computed);
    }
}
