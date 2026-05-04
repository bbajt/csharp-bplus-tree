using System.Buffers.Binary;
using System.IO.Hashing;

namespace ByTech.BPlusTree.Core.Wal;

/// <summary>Result of WAL structural validation.</summary>
internal readonly struct WalValidationResult
{
    public bool                    IsValid       { get; init; }
    public int                     RecordCount   { get; init; }
    public LogSequenceNumber?      FirstBadLsn   { get; init; }
    public string?                 ErrorMessage  { get; init; }
}

/// <summary>
/// Forward-scanning WAL reader. Stateless — can be called multiple times.
/// Creates a new FileStream on each open (does not keep a handle open).
/// </summary>
internal sealed class WalReader
{
    private readonly string _path;

    public WalReader(string path) => _path = path;

    /// <summary>
    /// Enumerate all valid records starting at <paramref name="fromLsn"/>.
    /// Stops at EOF or first corrupted record (does not throw on corruption).
    /// </summary>
    public IEnumerable<WalRecord> ReadForward(LogSequenceNumber fromLsn)
    {
        if (!File.Exists(_path)) yield break;

        using var stream = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

        var baseLsn    = ReadBaseLsn(stream);
        long fileOffset;
        if (fromLsn == LogSequenceNumber.None || fromLsn.Value < baseLsn)
            fileOffset = WalRecordLayout.FileHeaderSize;
        else
            fileOffset = WalRecordLayout.FileHeaderSize + (long)(fromLsn.Value - baseLsn);
        if (fileOffset >= stream.Length) yield break;
        stream.Seek(fileOffset, SeekOrigin.Begin);

        var headerBuf = new byte[WalRecordLayout.FixedHeaderSize];

        while (true)
        {
            var bytesRead = ReadExact(stream, headerBuf, WalRecordLayout.FixedHeaderSize);
            if (bytesRead == 0) yield break;
            if (bytesRead < WalRecordLayout.FixedHeaderSize) yield break;

            var totalLength = BinaryPrimitives.ReadInt32BigEndian(headerBuf.AsSpan(WalRecordLayout.TotalLengthOffset));
            if (totalLength < WalRecordLayout.MinRecordLength) yield break;

            var type          = (WalRecordType)headerBuf[WalRecordLayout.TypeOffset];
            var lsn           = BinaryPrimitives.ReadUInt64BigEndian(headerBuf.AsSpan(WalRecordLayout.LsnOffset));
            var transactionId = BinaryPrimitives.ReadUInt32BigEndian(headerBuf.AsSpan(WalRecordLayout.TransactionIdOffset));
            var pageId        = BinaryPrimitives.ReadUInt32BigEndian(headerBuf.AsSpan(WalRecordLayout.PageIdOffset));
            var prevLsn       = BinaryPrimitives.ReadUInt64BigEndian(headerBuf.AsSpan(WalRecordLayout.PrevLsnOffset));
            var dataLength    = BinaryPrimitives.ReadInt32BigEndian(headerBuf.AsSpan(WalRecordLayout.DataLengthOffset));

            if (dataLength < 0) yield break;

            // Read data + CRC as one contiguous block to verify CRC easily
            var remainder     = new byte[dataLength + WalRecordLayout.CrcSize];
            var remRead       = ReadExact(stream, remainder, remainder.Length);
            if (remRead < remainder.Length) yield break;

            // Verify CRC32: covers header + data (everything except last 4 bytes)
            var crc = new Crc32();
            crc.Append(headerBuf);
            crc.Append(remainder.AsSpan(0, dataLength));
            uint computed = BinaryPrimitives.ReadUInt32BigEndian(crc.GetCurrentHash());
            uint stored   = BinaryPrimitives.ReadUInt32BigEndian(remainder.AsSpan(dataLength));
            if (computed != stored) yield break;

            var data = new byte[dataLength];
            remainder.AsSpan(0, dataLength).CopyTo(data);

            yield return new WalRecord
            {
                TotalRecordLength = totalLength,
                Type              = type,
                Lsn               = new LogSequenceNumber(lsn),
                TransactionId     = transactionId,
                PageId            = pageId,
                PrevLsn           = new LogSequenceNumber(prevLsn),
                DataLength        = dataLength,
                Data              = data,
                StoredCrc32       = stored,
            };
        }
    }

    /// <summary>
    /// Read the single WAL record whose first byte is at <paramref name="lsn"/>.
    /// Returns null if the file does not exist, the seek position is out of range,
    /// the record is truncated, or the CRC is invalid.
    /// Used by the Undo Pass to follow PrevLsn chains backward.
    /// </summary>
    public WalRecord? ReadAt(LogSequenceNumber lsn)
    {
        if (!File.Exists(_path)) return null;

        using var stream = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

        var  baseLsn    = ReadBaseLsn(stream);
        long fileOffset = lsn.Value < baseLsn
            ? WalRecordLayout.FileHeaderSize
            : WalRecordLayout.FileHeaderSize + (long)(lsn.Value - baseLsn);
        if (fileOffset >= stream.Length) return null;
        stream.Seek(fileOffset, SeekOrigin.Begin);

        var headerBuf = new byte[WalRecordLayout.FixedHeaderSize];
        var bytesRead = ReadExact(stream, headerBuf, WalRecordLayout.FixedHeaderSize);
        if (bytesRead < WalRecordLayout.FixedHeaderSize) return null;

        var totalLength = BinaryPrimitives.ReadInt32BigEndian(headerBuf.AsSpan(WalRecordLayout.TotalLengthOffset));
        if (totalLength < WalRecordLayout.MinRecordLength) return null;

        var type          = (WalRecordType)headerBuf[WalRecordLayout.TypeOffset];
        var lsnValue      = BinaryPrimitives.ReadUInt64BigEndian(headerBuf.AsSpan(WalRecordLayout.LsnOffset));
        var transactionId = BinaryPrimitives.ReadUInt32BigEndian(headerBuf.AsSpan(WalRecordLayout.TransactionIdOffset));
        var pageId        = BinaryPrimitives.ReadUInt32BigEndian(headerBuf.AsSpan(WalRecordLayout.PageIdOffset));
        var prevLsn       = BinaryPrimitives.ReadUInt64BigEndian(headerBuf.AsSpan(WalRecordLayout.PrevLsnOffset));
        var dataLength    = BinaryPrimitives.ReadInt32BigEndian(headerBuf.AsSpan(WalRecordLayout.DataLengthOffset));

        if (dataLength < 0) return null;

        var remainder = new byte[dataLength + WalRecordLayout.CrcSize];
        var remRead   = ReadExact(stream, remainder, remainder.Length);
        if (remRead < remainder.Length) return null;

        var crc = new Crc32();
        crc.Append(headerBuf);
        crc.Append(remainder.AsSpan(0, dataLength));
        uint computed = BinaryPrimitives.ReadUInt32BigEndian(crc.GetCurrentHash());
        uint stored   = BinaryPrimitives.ReadUInt32BigEndian(remainder.AsSpan(dataLength));
        if (computed != stored) return null;

        var data = new byte[dataLength];
        remainder.AsSpan(0, dataLength).CopyTo(data);

        return new WalRecord
        {
            TotalRecordLength = totalLength,
            Type              = type,
            Lsn               = new LogSequenceNumber(lsnValue),
            TransactionId     = transactionId,
            PageId            = pageId,
            PrevLsn           = new LogSequenceNumber(prevLsn),
            DataLength        = dataLength,
            Data              = data,
            StoredCrc32       = stored,
        };
    }

    /// <summary>Scan entire WAL to find the LSN of the last CheckpointEnd record.</summary>
    public LogSequenceNumber? FindLastCheckpointEnd()
    {
        LogSequenceNumber? last = null;
        foreach (var record in ReadForward(LogSequenceNumber.None))
        {
            if (record.Type == WalRecordType.CheckpointEnd)
                last = record.Lsn;
        }
        return last;
    }

    /// <summary>Scan entire WAL to find the LSN of the last CompactionComplete record.</summary>
    public LogSequenceNumber? FindLastCompactionComplete()
    {
        LogSequenceNumber? last = null;
        foreach (var record in ReadForward(LogSequenceNumber.None))
        {
            if (record.Type == WalRecordType.CompactionComplete)
                last = record.Lsn;
        }
        return last;
    }

    /// <summary>Full-scan validation. Returns IsValid=false on first CRC mismatch or truncation.</summary>
    public WalValidationResult Validate()
    {
        if (!File.Exists(_path))
            return new WalValidationResult { IsValid = true, RecordCount = 0 };

        int count = 0;
        using var stream = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

        // Skip the 8-byte epoch header; an empty WAL (header only) is valid with 0 records.
        ReadBaseLsn(stream);
        if (stream.Length == WalRecordLayout.FileHeaderSize)
            return new WalValidationResult { IsValid = true, RecordCount = 0 };

        var headerBuf = new byte[WalRecordLayout.FixedHeaderSize];

        while (true)
        {
            var bytesRead = ReadExact(stream, headerBuf, WalRecordLayout.FixedHeaderSize);
            if (bytesRead == 0)
                return new WalValidationResult { IsValid = true, RecordCount = count };

            if (bytesRead < WalRecordLayout.FixedHeaderSize)
                return new WalValidationResult
                {
                    IsValid      = false,
                    RecordCount  = count,
                    ErrorMessage = "Truncated record header",
                };

            var totalLength = BinaryPrimitives.ReadInt32BigEndian(headerBuf.AsSpan(WalRecordLayout.TotalLengthOffset));
            var lsn         = BinaryPrimitives.ReadUInt64BigEndian(headerBuf.AsSpan(WalRecordLayout.LsnOffset));
            var dataLength  = BinaryPrimitives.ReadInt32BigEndian(headerBuf.AsSpan(WalRecordLayout.DataLengthOffset));

            if (totalLength < WalRecordLayout.MinRecordLength || dataLength < 0)
                return new WalValidationResult
                {
                    IsValid      = false,
                    RecordCount  = count,
                    FirstBadLsn  = new LogSequenceNumber(lsn),
                    ErrorMessage = "Invalid record length fields",
                };

            var remainder = new byte[dataLength + WalRecordLayout.CrcSize];
            var remRead   = ReadExact(stream, remainder, remainder.Length);
            if (remRead < remainder.Length)
                return new WalValidationResult
                {
                    IsValid      = false,
                    RecordCount  = count,
                    FirstBadLsn  = new LogSequenceNumber(lsn),
                    ErrorMessage = "Truncated record body",
                };

            var crc = new Crc32();
            crc.Append(headerBuf);
            crc.Append(remainder.AsSpan(0, dataLength));
            uint computed = BinaryPrimitives.ReadUInt32BigEndian(crc.GetCurrentHash());
            uint stored   = BinaryPrimitives.ReadUInt32BigEndian(remainder.AsSpan(dataLength));

            if (computed != stored)
                return new WalValidationResult
                {
                    IsValid      = false,
                    RecordCount  = count,
                    FirstBadLsn  = new LogSequenceNumber(lsn),
                    ErrorMessage = $"CRC mismatch at LSN {lsn}: expected {computed:X8}, got {stored:X8}",
                };

            count++;
        }
    }

    /// <summary>
    /// Reads the 8-byte BaseLsn epoch header from the start of the WAL file.
    /// Positions the stream at FileHeaderSize on return.
    /// Returns 0 if the file is smaller than the header (legacy / empty file).
    /// </summary>
    private static ulong ReadBaseLsn(Stream stream)
    {
        if (stream.Length < WalRecordLayout.FileHeaderSize)
            return 0UL;
        stream.Seek(0, SeekOrigin.Begin);
        var hdr = new byte[WalRecordLayout.FileHeaderSize];
        _ = stream.Read(hdr, 0, hdr.Length);
        return BinaryPrimitives.ReadUInt64BigEndian(hdr);
    }

    private static int ReadExact(Stream stream, byte[] buf, int count)
    {
        int total = 0;
        while (total < count)
        {
            int n = stream.Read(buf, total, count - total);
            if (n == 0) break;
            total += n;
        }
        return total;
    }
}
