using System.Buffers.Binary;
using System.IO;
using System.Threading;
using BPlusTree.Core.Api;
using BPlusTree.Core.Storage;

namespace BPlusTree.Core.Storage;

/// <summary>
/// Synchronous file I/O wrapper for B+ tree storage.
/// Provides thread-safe page read/write operations with proper file management.
///
/// DURABILITY MODEL:
/// Pages written by WritePage/WritePageBatch do NOT need to be fsynced individually.
/// The WAL provides the durability guarantee:
///   1. Every page modification writes a WAL record (WalWriter.Append)
///   2. WAL records are fsynced at checkpoint time (CheckpointManager)
///   3. If the process crashes between WritePage and the next checkpoint,
///      the WAL record is re-applied on recovery — the page write is idempotent.
///
/// Adding Flush(flushToDisk: true) to WritePage would issue a FlushFileBuffers
/// syscall for every evicted dirty page. With EvictionWorker writing ~999K pages
/// per 1M-insert run, this would add ~999K fsyncs on top of the ~250 checkpoint
/// fsyncs — a ~4,000x increase in fsync operations, potentially degrading throughput
/// by ~4.5× (see PHASE-29a/29b benchmarks: 14s → 64s insert time).
///
/// The only correct fsync sites are:
///   - WalWriter._stream.Flush(flushToDisk: true)  — inside FlushLoop and FlushUpTo
///   - StorageFile.Flush()                          — called by CheckpointManager only
/// </summary>
internal sealed class StorageFile : IDisposable
{
    private FileStream _fileStream;
    private readonly string _filePath;
    private readonly object _lock = new();
    private readonly int _pageSize;
    private bool _disposed = false;

    /// <summary>
    /// Gets the page size in bytes.
    /// </summary>
    public int PageSize => _pageSize;

    /// <summary>
    /// Opens or creates a storage file.
    /// </summary>
    /// <param name="filePath">The path to the storage file.</param>
    /// <param name="pageSize">The page size in bytes.</param>
    /// <param name="createNew">Whether to create a new file or open an existing one.</param>
    /// <returns>A new StorageFile instance.</returns>
    public static StorageFile Open(string filePath, int pageSize, bool createNew = false)
    {
        return new StorageFile(filePath, pageSize, createNew);
    }

    /// <summary>
    /// Initializes a new instance of the StorageFile class.
    /// </summary>
    /// <param name="filePath">The path to the storage file.</param>
    /// <param name="pageSize">The page size in bytes.</param>
    /// <param name="createNew">Whether to create a new file or open an existing one.</param>
    public StorageFile(string filePath, int pageSize, bool createNew = false)
    {
        _filePath = filePath;
        _pageSize = pageSize;
        var fileOptions = FileOptions.RandomAccess;

        // FileShare.Delete is required on Windows so that File.Move can atomically rename
        // the data file over this open handle during compaction.
        var share = FileShare.ReadWrite | FileShare.Delete;

        if (createNew)
        {
            _fileStream = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, share, bufferSize: 0, fileOptions);
        }
        else
        {
            _fileStream = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, share, bufferSize: 0, fileOptions);
        }
    }

    /// <summary>
    /// Reads a page from the file at the specified page ID.
    /// </summary>
    /// <param name="pageId">The page ID to read.</param>
    /// <returns>A span containing the page data.</returns>
    public Span<byte> ReadPage(uint pageId)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(StorageFile));

        lock (_lock)
        {
            var pageOffset = (long)pageId * _pageSize;

            // Check if page exists
            if (pageOffset >= _fileStream.Length)
                throw new PageNotFoundException($"Page {pageId} not found in file.");

            var buffer = new byte[_pageSize];
            _fileStream.Seek(pageOffset, SeekOrigin.Begin);
            var bytesRead = _fileStream.Read(buffer, 0, _pageSize);

            if (bytesRead != _pageSize)
                throw new IOException($"Failed to read complete page {pageId}.");

            return buffer;
        }
    }

    /// <summary>
    /// Reads a page from the file at the specified page ID into the provided buffer.
    /// </summary>
    /// <param name="pageId">The page ID to read.</param>
    /// <param name="buffer">The buffer to read the page data into.</param>
    public void ReadPage(uint pageId, byte[] buffer)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(StorageFile));

        if (buffer.Length != _pageSize)
            throw new ArgumentException($"Buffer must be exactly {_pageSize} bytes.", nameof(buffer));

        lock (_lock)
        {
            var pageOffset = (long)pageId * _pageSize;

            // Check if page exists
            if (pageOffset >= _fileStream.Length)
                throw new PageNotFoundException($"Page {pageId} not found in file.");

            _fileStream.Seek(pageOffset, SeekOrigin.Begin);
            var bytesRead = _fileStream.Read(buffer, 0, _pageSize);

            if (bytesRead != _pageSize)
                throw new IOException($"Failed to read complete page {pageId}.");
        }
    }

    /// <summary>
    /// Writes a page to the file at the specified page ID.
    /// </summary>
    /// <param name="pageId">The page ID to write.</param>
    /// <param name="pageData">The page data to write.</param>
    public void WritePage(uint pageId, ReadOnlySpan<byte> pageData)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(StorageFile));

        if (pageData.Length != _pageSize)
            throw new ArgumentException($"Page data must be exactly {_pageSize} bytes.", nameof(pageData));

        lock (_lock)
        {
            var pageOffset = (long)pageId * _pageSize;

            // Ensure file is large enough
            if (pageOffset >= _fileStream.Length)
            {
                // Extend file to accommodate this page
                _fileStream.SetLength(pageOffset + _pageSize);
            }

            _fileStream.Seek(pageOffset, SeekOrigin.Begin);
            _fileStream.Write(pageData);
        }
    }

    /// <summary>
    /// Write multiple frames in one sequential pass, sorted by PageId to maximise sequential I/O.
    /// Caller has already verified WAL-before-page for all frames.
    /// </summary>
    public void WritePageBatch(IReadOnlyList<Frame> frames)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(StorageFile));

        var sorted = frames.OrderBy(f => f.PageId);
        foreach (var frame in sorted)
            WritePage(frame.PageId, frame.Data);
    }

    /// <summary>
    /// Allocates a new page in the file and returns its ID.
    /// </summary>
    /// <returns>The ID of the newly allocated page.</returns>
    public uint AllocatePage()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(StorageFile));

        lock (_lock)
        {
            var pageId = (uint)(_fileStream.Length / _pageSize);
            var pageOffset = _fileStream.Length;

            // Extend file by one page size
            _fileStream.SetLength(pageOffset + _pageSize);

            return pageId;
        }
    }

    /// <summary>
    /// Flushes all pending writes to disk.
    /// </summary>
    public void Flush()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(StorageFile));

        lock (_lock)
        {
            _fileStream.Flush(true);
        }
    }

    /// <summary>
    /// Extends the file so that at least <paramref name="pageId"/> + 1 pages exist.
    /// No-op if the file already contains the page. Used during WAL recovery to
    /// materialise pages that existed at crash time but are absent from a wiped data file.
    /// </summary>
    internal void EnsurePageExists(uint pageId)
    {
        lock (_lock)
        {
            var required = (long)(pageId + 1) * _pageSize;
            if (_fileStream.Length < required)
                _fileStream.SetLength(required);
        }
    }

    /// <summary>
    /// Gets the total number of pages in the file.
    /// </summary>
    public uint TotalPageCount
    {
        get
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(StorageFile));

            lock (_lock)
            {
                return (uint)(_fileStream.Length / _pageSize);
            }
        }
    }

    /// <summary>
    /// Flushes and closes the underlying file stream without fully disposing StorageFile.
    /// Call ReopenStream() after a File.Move to restore normal operation.
    /// </summary>
    internal void CloseStream()
    {
        lock (_lock)
        {
            if (!_disposed)
            {
                _fileStream.Flush(true);
                _fileStream.Dispose();
                _disposed = true;
            }
        }
    }

    /// <summary>
    /// Reopens the file stream at the same path after CloseStream() + File.Move.
    /// </summary>
    internal void ReopenStream()
    {
        lock (_lock)
        {
            _fileStream = new FileStream(
                _filePath, FileMode.Open, FileAccess.ReadWrite,
                FileShare.ReadWrite | FileShare.Delete,
                bufferSize: 0, FileOptions.RandomAccess);
            _disposed = false;
        }
    }

    /// <summary>
    /// Disposes the StorageFile and closes the underlying file stream.
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _fileStream?.Dispose();
            _disposed = true;
        }
    }
}