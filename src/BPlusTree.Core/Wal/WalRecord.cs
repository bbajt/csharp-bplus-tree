namespace ByTech.BPlusTree.Core.Wal;

/// <summary>
/// In-memory representation of a single WAL record decoded from disk.
/// Does NOT own the <see cref="Data"/> buffer — it is a slice of the reader's
/// read buffer. Copy if you need to retain it past the read call.
/// </summary>
internal readonly struct WalRecord
{
    /// <summary>Total on-disk byte length of this record including header and CRC.</summary>
    public int TotalRecordLength { get; init; }

    public WalRecordType Type    { get; init; }
    public LogSequenceNumber Lsn { get; init; }
    public uint TransactionId    { get; init; }
    public uint PageId           { get; init; }

    /// <summary>LSN of the previous record from the same transaction. 0 = first record.</summary>
    public LogSequenceNumber PrevLsn { get; init; }

    /// <summary>Byte length of the <see cref="Data"/> payload.</summary>
    public int DataLength { get; init; }

    /// <summary>After-image payload (for UpdatePage/UpdateMeta) or empty span.</summary>
    public ReadOnlyMemory<byte> Data { get; init; }

    /// <summary>CRC32 stored in the record (last 4 bytes on disk).</summary>
    public uint StoredCrc32 { get; init; }
}