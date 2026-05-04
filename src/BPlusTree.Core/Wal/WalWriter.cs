using System.Buffers.Binary;
using System.IO.Hashing;
using ByTech.BPlusTree.Core.Api;

namespace ByTech.BPlusTree.Core.Wal;

/// <summary>
/// Append-only WAL writer. Thread-safe. Buffers records in memory; Flush() writes
/// the entire buffer to disk and fsyncs. CurrentLsn = byte offset of next record.
/// </summary>
internal sealed class WalWriter : IDisposable
{
    private readonly FileStream _stream;
    private readonly byte[]     _buffer;
    private int                 _bufferPos;
    private ulong               _currentLsn;
    private readonly object     _lock      = new();
    /// <summary>
    /// Serializes concurrent _stream.Flush(flushToDisk: true) calls.
    /// Acquisition order rule: ALWAYS acquire _lock before _flushLock.
    ///                         NEVER acquire _lock while _flushLock is held.
    /// </summary>
    private readonly object     _flushLock = new();
    private bool                _disposed;

    // Reusable CRC32 instance — allocated once, Reset() before each use.
    // Safe: all Append() calls hold _lock, so _crc32 is accessed single-threaded.
    private readonly Crc32      _crc32 = new();

    // Durably-flushed LSN — updated atomically after every FlushFileBuffers call.
    // Read by EvictionWorker to verify WAL-before-page invariant.
    private ulong _flushedLsn;

    // BaseLsn = absolute LSN of the first record in the current WAL file.
    // Written in the 8-byte file header. After TruncateWal(), set to the preserved
    // _currentLsn so that file-offset = FileHeaderSize + (lsn - _baseLsn).
    private ulong _baseLsn;

    // Group commit state
    private readonly WalSyncMode          _syncMode;
    private readonly int                  _flushIntervalMs;
    private readonly int                  _flushBatchSize;
    private int                           _pendingRecords;
    private Thread?                       _flushThread;
    private CancellationTokenSource?      _flushCts;
    private readonly ManualResetEventSlim _flushSignal = new(false);

    public LogSequenceNumber CurrentLsn => new(_currentLsn);

    /// <summary>
    /// The WAL LSN up to which data has been durably flushed to disk (FlushFileBuffers returned).
    /// Monotonically non-decreasing. Safe to read from any thread without a lock.
    /// </summary>
    public ulong FlushedLsn => Volatile.Read(ref _flushedLsn);

    /// <summary>
    /// Ensures the WAL is flushed at least up to <paramref name="targetLsn"/>.
    /// If <see cref="FlushedLsn"/> is already ≥ targetLsn, returns immediately.
    /// Otherwise acquires the lock and performs a synchronous fsync.
    /// Must NOT be called while holding any page latch (deadlock risk).
    /// </summary>
    public void FlushUpTo(ulong targetLsn)
    {
        // Fast path: already flushed far enough (no lock needed)
        if (Volatile.Read(ref _flushedLsn) >= targetLsn)
            return;

        // Step 1: drain under _lock (microseconds)
        ulong lsnSnapshot;
        lock (_lock)
        {
            if (Volatile.Read(ref _flushedLsn) >= targetLsn)
                return;   // double-check: FlushLoop may have flushed while we waited
            DrainLocked();
            lsnSnapshot     = _currentLsn;
            _pendingRecords = 0;
        }

        // Step 2: fsync OUTSIDE _lock (5–50ms, non-blocking for Append)
        // _flushLock serializes concurrent fsync callers (FlushUpTo + FlushLoop).
        lock (_flushLock)
        {
            // Triple-check: FlushLoop may have completed an fsync between _lock release
            // and _flushLock acquire — avoid redundant fsync if already flushed.
            if (Volatile.Read(ref _flushedLsn) >= targetLsn)
                return;
            _stream.Flush(flushToDisk: true);
            Interlocked.Exchange(ref _flushedLsn, lsnSnapshot);
        }
    }

    private WalWriter(FileStream stream, byte[] buffer, ulong currentLsn, ulong baseLsn,
        WalSyncMode syncMode, int flushIntervalMs, int flushBatchSize)
    {
        _stream          = stream;
        _buffer          = buffer;
        _currentLsn      = currentLsn;
        _baseLsn         = baseLsn;
        _syncMode        = syncMode;
        _flushIntervalMs = flushIntervalMs;
        _flushBatchSize  = flushBatchSize;
    }

    /// <summary>
    /// Opens or creates the WAL file.
    /// New file: writes 8-byte epoch header (BaseLsn=0); _currentLsn = 0.
    /// Existing file: reads BaseLsn from header; _currentLsn = BaseLsn + (fileLength - FileHeaderSize).
    /// </summary>
    public static WalWriter Open(string path, int bufferSize,
        WalSyncMode syncMode        = WalSyncMode.Synchronous,
        int         flushIntervalMs = 5,
        int         flushBatchSize  = 256)
    {
        var stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite,
                                    FileShare.Read, bufferSize: 0, FileOptions.SequentialScan);
        ulong baseLsn;
        ulong currentLsn;

        if (stream.Length == 0)
        {
            // New file: write epoch header with BaseLsn = 0.
            var hdr = new byte[WalRecordLayout.FileHeaderSize];
            BinaryPrimitives.WriteUInt64BigEndian(hdr, 0UL);
            stream.Write(hdr, 0, hdr.Length);
            stream.Flush(flushToDisk: true);
            baseLsn    = 0;
            currentLsn = 0;
        }
        else
        {
            // Existing file: read BaseLsn from header.
            var hdr      = new byte[WalRecordLayout.FileHeaderSize];
            stream.Seek(0, SeekOrigin.Begin);
            _ = stream.Read(hdr, 0, hdr.Length);
            baseLsn    = BinaryPrimitives.ReadUInt64BigEndian(hdr);
            currentLsn = baseLsn + (ulong)(stream.Length - WalRecordLayout.FileHeaderSize);
        }

        stream.Seek(0, SeekOrigin.End);
        var writer = new WalWriter(stream, new byte[bufferSize], currentLsn, baseLsn,
                                   syncMode, flushIntervalMs, flushBatchSize);
        if (syncMode == WalSyncMode.GroupCommit)
            writer.StartFlushThread();
        return writer;
    }

    /// <summary>
    /// Serialize and buffer one WAL record.
    /// Returns the LSN assigned to this record (file offset where it will land).
    /// Automatically flushes if buffer would overflow.
    /// </summary>
    public LogSequenceNumber Append(
        WalRecordType      type,
        uint               transactionId,
        uint               pageId,
        LogSequenceNumber  prevLsn,
        ReadOnlySpan<byte> data)
    {
        var totalLength = WalRecordLayout.TotalLength(data.Length);

        lock (_lock)
        {
            if (_bufferPos + totalLength > _buffer.Length)
            {
                if (_syncMode == WalSyncMode.GroupCommit)
                {
                    DrainLocked();
                    if (_pendingRecords >= _flushBatchSize)
                        _flushSignal.Set();
                }
                else
                {
                    FlushLocked();
                }
            }

            var assignedLsn = _currentLsn;
            var pos         = _bufferPos;

            // TotalRecordLength (4 bytes, big-endian)
            BinaryPrimitives.WriteInt32BigEndian(_buffer.AsSpan(pos + WalRecordLayout.TotalLengthOffset), totalLength);
            // Type (1 byte)
            _buffer[pos + WalRecordLayout.TypeOffset] = (byte)type;
            // LSN (8 bytes, big-endian)
            BinaryPrimitives.WriteUInt64BigEndian(_buffer.AsSpan(pos + WalRecordLayout.LsnOffset), assignedLsn);
            // TransactionId (4 bytes, big-endian)
            BinaryPrimitives.WriteUInt32BigEndian(_buffer.AsSpan(pos + WalRecordLayout.TransactionIdOffset), transactionId);
            // PageId (4 bytes, big-endian)
            BinaryPrimitives.WriteUInt32BigEndian(_buffer.AsSpan(pos + WalRecordLayout.PageIdOffset), pageId);
            // PrevLsn (8 bytes, big-endian)
            BinaryPrimitives.WriteUInt64BigEndian(_buffer.AsSpan(pos + WalRecordLayout.PrevLsnOffset), prevLsn.Value);
            // DataLength (4 bytes, big-endian)
            BinaryPrimitives.WriteInt32BigEndian(_buffer.AsSpan(pos + WalRecordLayout.DataLengthOffset), data.Length);
            // Data
            if (data.Length > 0)
                data.CopyTo(_buffer.AsSpan(pos + WalRecordLayout.DataOffset));

            // CRC32 over all bytes except the last 4
            _crc32.Reset();
            _crc32.Append(_buffer.AsSpan(pos, totalLength - WalRecordLayout.CrcSize));
            Span<byte> hashBytes = stackalloc byte[4];
            _crc32.GetCurrentHash(hashBytes);
            uint crcValue = BinaryPrimitives.ReadUInt32BigEndian(hashBytes);
            BinaryPrimitives.WriteUInt32BigEndian(
                _buffer.AsSpan(pos + WalRecordLayout.DataOffset + data.Length), crcValue);

            _bufferPos    += totalLength;
            _currentLsn   += (ulong)totalLength;
            _pendingRecords++;

            return new LogSequenceNumber(assignedLsn);
        }
    }

    /// <summary>
    /// Serialize a uint[] page-ID list as big-endian bytes and append one WAL record.
    /// Used by <see cref="AppendAllocOverflowChain"/> and <see cref="AppendAllocShadowChain"/>.
    /// </summary>
    private LogSequenceNumber AppendAllocPageIds(
        WalRecordType type, uint txId, uint[] pageIds)
    {
        byte[] data = new byte[pageIds.Length * sizeof(uint)];
        for (int i = 0; i < pageIds.Length; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(
                data.AsSpan(i * 4), pageIds[i]);
        return Append(type, txId, 0, LogSequenceNumber.None, data);
    }

    /// <summary>
    /// Append an <see cref="WalRecordType.AllocOverflowChain"/> record containing all
    /// overflow page IDs in chain order. Used by the Undo Pass to free orphaned pages
    /// on crash-before-commit.
    /// </summary>
    public LogSequenceNumber AppendAllocOverflowChain(uint txId, uint[] pageIds)
        => AppendAllocPageIds(WalRecordType.AllocOverflowChain, txId, pageIds);

    /// <summary>
    /// Append an <see cref="WalRecordType.AllocShadowChain"/> record containing all
    /// CoW shadow page IDs allocated for one write path. Closes Gap 1: the Undo Pass
    /// can now free shadow pages allocated by crashed transactions.
    /// </summary>
    public LogSequenceNumber AppendAllocShadowChain(uint txId, uint[] pageIds)
        => AppendAllocPageIds(WalRecordType.AllocShadowChain, txId, pageIds);

    /// <summary>
    /// Append a <see cref="WalRecordType.FreeOverflowChain"/> record containing all
    /// overflow page IDs to free. Written before pages are returned to the free list.
    /// Enables the Redo Pass to re-apply the frees on crash-after-WAL-fsync-before-free.
    /// Closes Gap 2 for the auto-commit in-place write path.
    /// </summary>
    public LogSequenceNumber AppendFreeOverflowChain(uint txId, uint[] pageIds)
        => AppendAllocPageIds(WalRecordType.FreeOverflowChain, txId, pageIds);

    /// <summary>Write the in-memory buffer to disk and fsync. No-op if empty.</summary>
    public void Flush()
    {
        lock (_lock)
            FlushLocked();
    }

    /// <summary>
    /// Async variant of <see cref="Flush"/>. Drains the in-memory buffer to the OS
    /// under <c>_lock</c> (synchronous, microseconds), then offloads the
    /// <c>FlushFileBuffers</c> syscall to the thread pool so the caller's thread
    /// is freed during the 5–50 ms disk sync.
    /// </summary>
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ulong lsnSnapshot;
        lock (_lock)
        {
            DrainLocked();
            lsnSnapshot = _currentLsn;
        }

        return Task.Run(() =>
        {
            lock (_flushLock)
            {
                // Double-check: FlushLoop or a concurrent Flush may have already fsynced.
                if (Volatile.Read(ref _flushedLsn) >= lsnSnapshot)
                    return;
                _stream.Flush(flushToDisk: true);
                Interlocked.Exchange(ref _flushedLsn, lsnSnapshot);
            }
        }, cancellationToken);
    }

    /// <summary>
    /// Append CheckpointBegin + CheckpointEnd records around a flush.
    /// Returns the LSN of CheckpointEnd.
    /// </summary>
    public LogSequenceNumber WriteCheckpoint(IReadOnlyList<uint> dirtyPageIds)
    {
        Append(WalRecordType.CheckpointBegin, 0, 0, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
        var endLsn = Append(WalRecordType.CheckpointEnd, 0, 0, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
        Flush();
        return endLsn;
    }

    /// <summary>
    /// Discard all WAL records — truncates the WAL file to the epoch header and writes
    /// a new header with BaseLsn = current _currentLsn. _currentLsn is NOT reset:
    /// it continues monotonically so that frame.PageLsn comparisons in recovery remain
    /// valid across truncations. Called after a checkpoint when all dirty pages are on
    /// disk and pre-checkpoint records are no longer needed for recovery.
    /// </summary>
    public void TruncateWal()
    {
        lock (_lock)
        {
            // Flush any pending buffer before truncating so no partial records remain.
            FlushLocked();

            // Write new epoch header. _currentLsn is the preserved base for this epoch.
            var hdr = new byte[WalRecordLayout.FileHeaderSize];
            BinaryPrimitives.WriteUInt64BigEndian(hdr, _currentLsn);
            _baseLsn = _currentLsn;

            _stream.Seek(0, SeekOrigin.Begin);
            _stream.SetLength(WalRecordLayout.FileHeaderSize);
            _stream.Seek(0, SeekOrigin.Begin);
            _stream.Write(hdr, 0, hdr.Length);
            _stream.Flush(flushToDisk: true);
            _stream.Seek(0, SeekOrigin.End);   // position after header for new appends

            _bufferPos = 0;
            // _currentLsn is unchanged — monotonic across truncation.
            Interlocked.Exchange(ref _flushedLsn, _currentLsn);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (_syncMode == WalSyncMode.GroupCommit)
        {
            _flushCts!.Cancel();
            _flushSignal.Set();
            bool exited = _flushThread!.Join(millisecondsTimeout: 1_000);
            if (!exited)
                throw new InvalidOperationException(
                    "WalWriter flush thread did not exit within 1 second during Dispose. " +
                    "Proceeding would race with the background thread on the WAL stream.");
        }

        Flush();
        _stream.Dispose();
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /// <summary>Write buffer bytes to the OS (no fsync). Must be called under _lock.</summary>
    private void DrainLocked()
    {
        if (_bufferPos == 0) return;
        _stream.Write(_buffer, 0, _bufferPos);
        _bufferPos = 0;
    }

    private void FlushLocked()
    {
        if (_bufferPos == 0) return;
        _stream.Write(_buffer, 0, _bufferPos);
        _stream.Flush(flushToDisk: true);
        Interlocked.Exchange(ref _flushedLsn, _currentLsn);
        _bufferPos = 0;
    }

    private void StartFlushThread()
    {
        _flushCts    = new CancellationTokenSource();
        _flushThread = new Thread(FlushLoop) { IsBackground = true, Name = "WalFlushLoop" };
        _flushThread.Start();
    }

    private void FlushLoop()
    {
        // Two-step pattern (non-negotiable — see PHASE-27.MD Known Failure Points):
        //   Step 1: drain in-memory buffer to OS under _lock (microseconds)
        //   Step 2: fsync OUTSIDE _lock under _flushLock (5–50ms, non-blocking for Append)
        //
        // lsnSnapshot captured inside _lock after DrainLocked() — see Failure Point #1.
        var token = _flushCts!.Token;
        try
        {
            while (!token.IsCancellationRequested)
            {
                _flushSignal.Wait(_flushIntervalMs, token);
                _flushSignal.Reset();

                // Step 1: drain under _lock (fast — microseconds)
                ulong lsnSnapshot;
                lock (_lock)
                {
                    if (_pendingRecords == 0) continue;
                    DrainLocked();
                    lsnSnapshot     = _currentLsn;
                    _pendingRecords = 0;
                    // _lock released here — Append() can now proceed immediately
                }

                // Step 2: fsync OUTSIDE _lock (5–50ms, non-blocking for Append)
                lock (_flushLock)
                {
                    _stream.Flush(flushToDisk: true);
                    Interlocked.Exchange(ref _flushedLsn, lsnSnapshot);
                }
            }
        }
        catch (OperationCanceledException) { }

        FinalFlush();   // drain any records buffered since last flush cycle
    }

    /// <summary>
    /// Drain and fsync any pending records. Called on graceful shutdown.
    /// Safe to call after _cts is cancelled — no concurrent Append() at that point.
    /// </summary>
    private void FinalFlush()
    {
        ulong lsnSnapshot;
        lock (_lock)
        {
            if (_pendingRecords == 0) return;
            DrainLocked();
            lsnSnapshot     = _currentLsn;
            _pendingRecords = 0;
        }

        lock (_flushLock)
        {
            _stream.Flush(flushToDisk: true);
            Interlocked.Exchange(ref _flushedLsn, lsnSnapshot);
        }
    }
}
