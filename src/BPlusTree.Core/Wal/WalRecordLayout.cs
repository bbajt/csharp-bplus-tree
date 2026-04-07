namespace BPlusTree.Core.Wal;

/// <summary>
/// Byte offsets within a serialized WAL record.
/// On-disk format (all integers big-endian):
///   Offset  0 : int   TotalRecordLength  (4 bytes)
///   Offset  4 : byte  Type               (1 byte)
///   Offset  5 : ulong LSN                (8 bytes)
///   Offset 13 : uint  TransactionId      (4 bytes)
///   Offset 17 : uint  PageId             (4 bytes)
///   Offset 21 : ulong PrevLSN            (8 bytes)
///   Offset 29 : int   DataLength         (4 bytes)
///   Offset 33 : byte[] Data              (DataLength bytes)
///   Offset 33+DataLength : uint CRC32    (4 bytes)
/// Total fixed header: 33 bytes + DataLength + 4 bytes CRC
/// </summary>
internal static class WalRecordLayout
{
    public const int TotalLengthOffset  = 0;
    public const int TypeOffset         = 4;
    public const int LsnOffset          = 5;
    public const int TransactionIdOffset = 13;
    public const int PageIdOffset       = 17;
    public const int PrevLsnOffset      = 21;
    public const int DataLengthOffset   = 29;
    public const int DataOffset         = 33;
    public const int FixedHeaderSize    = 33;  // bytes before the variable-length data
    public const int CrcSize            = 4;   // CRC32 appended after data

    /// <summary>Minimum record length (fixed header + 0 data bytes + CRC).</summary>
    public const int MinRecordLength    = FixedHeaderSize + CrcSize;

    /// <summary>Compute total on-disk length for a record with dataLength payload bytes.</summary>
    public static int TotalLength(int dataLength) => FixedHeaderSize + dataLength + CrcSize;

    /// <summary>
    /// Size in bytes of the WAL file header that precedes all records.
    /// Header layout: BaseLsn (8 bytes, big-endian ulong).
    /// BaseLsn = absolute LSN of the first record in this file.
    /// After TruncateWal(), BaseLsn is set to the preserved _currentLsn so that
    /// LSNs remain monotonically increasing across truncations.
    /// </summary>
    public const int FileHeaderSize = 8;
}