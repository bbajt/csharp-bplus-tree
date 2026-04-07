using System;

namespace BPlusTree.Core.Storage;

/// <summary>
/// Represents a single frame in the buffer pool.
/// </summary>
internal sealed class Frame
{
    private readonly byte[] _data;
    private int _pinCount;
    private bool _referenceBit;
    private bool _isDirty;
    private int _isEvicting;  // 0 = normal, 1 = claimed by eviction thread

    /// <summary>
    /// Gets the page ID of this frame.
    /// </summary>
    public uint PageId { get; set; }

    /// <summary>
    /// Gets the page LSN of this frame.
    /// </summary>
    public ulong PageLsn { get; set; }

    /// <summary>
    /// Gets the data buffer for this frame.
    /// </summary>
    public byte[] Data => _data;

    /// <summary>
    /// Gets or sets the reference bit for clock algorithm.
    /// </summary>
    public bool ReferenceBit
    {
        get => _referenceBit;
        set => _referenceBit = value;
    }

    /// <summary>
    /// Gets or sets the dirty flag indicating if the frame has been modified.
    /// </summary>
    public bool IsDirty
    {
        get => _isDirty;
        set => _isDirty = value;
    }

    /// <summary>
    /// Gets the pin count of this frame.
    /// </summary>
    public int PinCount => _pinCount;

    /// <summary>
    /// Gets a value indicating whether this frame is pinned.
    /// </summary>
    public bool IsPinned => _pinCount > 0;

    /// <summary>
    /// Gets a value indicating whether this frame has been claimed by the eviction thread.
    /// </summary>
    public bool IsEvicting => Volatile.Read(ref _isEvicting) != 0;

    /// <summary>
    /// Atomically transitions the frame from normal (0) to evicting (1).
    /// Returns true if this thread won the claim; false if already evicting.
    /// </summary>
    internal bool TrySetEvicting() =>
        Interlocked.CompareExchange(ref _isEvicting, 1, 0) == 0;

    /// <summary>Clears the evicting flag. Called by ReleaseEvictedFrame after write completes.</summary>
    internal void ClearEvicting() => Volatile.Write(ref _isEvicting, 0);

    /// <summary>
    /// Atomically decrement PinCount. Returns the new value.
    /// Used by BufferPool.Unpin fast path — no lock held by caller.
    /// </summary>
    internal int AtomicDecrementPin() => Interlocked.Decrement(ref _pinCount);

    /// <summary>
    /// Atomically increment PinCount. Returns the new value.
    /// Used by BufferPool.Pin hot path — no lock held by caller.
    /// </summary>
    internal int AtomicIncrementPin() => Interlocked.Increment(ref _pinCount);

    /// <summary>
    /// Initializes a new instance of the Frame class with the specified page size.
    /// </summary>
    /// <param name="pageSize">The size of the page in bytes.</param>
    public Frame(int pageSize)
    {
        if (pageSize <= 0)
        {
            throw new ArgumentException("Page size must be positive.", nameof(pageSize));
        }

        _data = new byte[pageSize];
        _pinCount = 0;
        _referenceBit = false;
        _isDirty = false;
        PageId = PageLayout.NullPageId;
        PageLsn = 0;
    }

    /// <summary>
    /// Pins this frame.
    /// </summary>
    public void Pin()
    {
        _pinCount++;
    }

    /// <summary>
    /// Unpins this frame.
    /// </summary>
    public void Unpin()
    {
        if (_pinCount > 0)
        {
            _pinCount--;
        }
    }

    /// <summary>
    /// Resets this frame to its initial state.
    /// </summary>
    public void Reset()
    {
        _pinCount = 0;
        _referenceBit = false;
        _isDirty = false;
        ClearEvicting();
        PageId = PageLayout.NullPageId;
        PageLsn = 0;
        Array.Clear(_data);
    }

    /// <summary>
    /// Sets the page ID for this frame.
    /// </summary>
    /// <param name="pageId">The page ID to set.</param>
    internal void SetPageId(uint pageId)
    {
        PageId = pageId;
    }

    /// <summary>
    /// Sets the page LSN for this frame.
    /// </summary>
    /// <param name="pageLsn">The page LSN to set.</param>
    internal void SetPageLsn(ulong pageLsn)
    {
        PageLsn = pageLsn;
    }

}