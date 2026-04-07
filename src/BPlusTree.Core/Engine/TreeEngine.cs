using System.Diagnostics;
using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using System.Buffers.Binary;
using System.Linq;

namespace BPlusTree.Core.Engine;

/// <summary>
/// Core B+ tree traversal and mutation engine. Stateless between calls.
/// All operations go through PageManager; no direct file access.
/// Phase 14: Search only. Phase 15: Insert (no-split path).
/// Phase 21b: All four traversal paths protected by PageLatchManager + LatchCoupling.
/// </summary>
/// <summary>
/// Fixed-size inline array for write-latch ancestors on the write path.
/// Using [InlineArray] instead of stackalloc avoids CS0208 (WriteLatchHandle contains a managed
/// reference so stackalloc of it is not permitted) and CA2014 (stackalloc inside a loop).
/// Declared here (not inside Insert/Delete/Update) so it can be placed outside retry loops.
/// (Phase 33)
/// </summary>
[System.Runtime.CompilerServices.InlineArray(LatchCoupling.MaxTreeHeight)]
file struct WriteLatchBuffer { private WriteLatchHandle _element; }

/// <summary>
/// Fixed-size inline array for the ancestor page-id path on the write path.
/// (uint, int) is unmanaged; stackalloc would also work, but [InlineArray] keeps the
/// pattern uniform with WriteLatchBuffer and avoids the CA2014 loop warning. (Phase 33)
/// </summary>
[System.Runtime.CompilerServices.InlineArray(LatchCoupling.MaxTreeHeight)]
file struct PathBuffer { private (uint pageId, int childIndex) _element; }

/// <summary>
/// Fixed-size inline array for the original page IDs on the CoW write path.
/// oldPageIds[pathLen] = leafId; oldPageIds[0..pathLen-1] = ancestor page IDs.
/// Avoids heap allocation in Insert/Delete CoW paths. (M+4b)
/// </summary>
[System.Runtime.CompilerServices.InlineArray(LatchCoupling.MaxTreeHeight + 1)]
file struct OldPageIdBuffer { private uint _element; }

/// <summary>
/// Fixed-size inline array for shadow ancestor page IDs on the CoW write path.
/// Parallel to path[0..pathLen-1]. Avoids heap allocation in Insert/Delete CoW paths. (M+4b)
/// </summary>
[System.Runtime.CompilerServices.InlineArray(LatchCoupling.MaxTreeHeight)]
file struct ShadowAncestorBuffer { private uint _element; }

/// <summary>Operation type stored in the online compaction delta map (Phase 106).</summary>
internal enum DeltaOp { Insert, Delete }

/// <summary>Core B+ tree engine. Manages all read/write paths, transactions, and tree structure operations.</summary>
internal sealed class TreeEngine<TKey, TValue> : IDisposable
    where TKey : notnull
{
    private readonly PageManager                  _pageManager;
    private readonly NodeSerializer<TKey, TValue> _nodeSerializer;
    private readonly TreeMetadata                 _metadata;
    private readonly Splitter<TKey, TValue>       _splitter;
    private readonly Merger<TKey, TValue>         _merger;
    private readonly CheckpointManager?           _checkpointManager;
    private readonly PageLatchManager             _latches;
    private readonly LatchCoupling                _crab;
    private readonly TransactionCoordinator       _coordinator = new();

    // ── Online compaction delta tracking (Phase 106) ─────────────────────────
    // Non-null only while a compaction's Phase A is in progress (writer lock released,
    // leaf walk + compact build running). Guarded by the writer lock on all writes.
    private Dictionary<TKey, (DeltaOp Op, TValue? Value)>? _deltaMap;

    /// <summary>True while online compaction delta tracking is active.</summary>
    internal bool DeltaEnabled => _deltaMap != null;

    /// <summary>Enable delta tracking. Called under writer lock before releasing it for Phase A.</summary>
    internal void EnableDelta() => _deltaMap = new Dictionary<TKey, (DeltaOp, TValue?)>();

    /// <summary>
    /// Disable delta tracking and return the accumulated map.
    /// Called under writer lock at the start of Phase B (swap).
    /// </summary>
    internal Dictionary<TKey, (DeltaOp Op, TValue? Value)> DisableDelta()
    {
        var map  = _deltaMap!;
        _deltaMap = null;
        return map;
    }

    /// <summary>
    /// Record a key change into the active delta map.
    /// Called by Transaction.Commit() to hand off transactional key changes.
    /// Must be called while the writer lock is held.
    /// </summary>
    internal void RecordDelta(TKey key, TValue? value, DeltaOp op)
    {
        Debug.Assert(_coordinator == null || _coordinator.IsWriterLockHeld,
            "RecordDelta must be called while the commit writer lock is held. " +
            "_deltaMap is not thread-safe; concurrent access without the lock corrupts it.");
        if (_deltaMap != null) _deltaMap[key] = (op, value);
    }

    public TreeEngine(
        PageManager pageManager,
        NodeSerializer<TKey, TValue> nodeSerializer,
        TreeMetadata metadata)
    {
        _pageManager    = pageManager;
        _nodeSerializer = nodeSerializer;
        _metadata       = metadata;
        _splitter       = new Splitter<TKey, TValue>(pageManager, nodeSerializer);
        _merger         = new Merger<TKey, TValue>(pageManager, nodeSerializer, metadata);
        _latches        = new PageLatchManager();
        _crab           = new LatchCoupling(_latches);
        _coordinator.SetPageManager(pageManager);

        if (pageManager.Wal != null)
        {
            _checkpointManager = new CheckpointManager(
                pageManager, pageManager.Wal, metadata, pageManager.WalFilePath, _coordinator);
            InitialiseRecovery();
        }

        // Clean up any leftover .compact file from a previous aborted compaction.
        Compactor<TKey, TValue>.CleanupAbortedCompaction(pageManager.DataFilePath);
    }

    /// <summary>
    /// Test-seam constructor. Accepts an externally-constructed
    /// <see cref="CheckpointManager"/> so callers can register
    /// <see cref="CheckpointManager.PostRecoveryValidation"/> before
    /// recovery runs. Not for production use.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    internal TreeEngine(
        PageManager pageManager,
        NodeSerializer<TKey, TValue> nodeSerializer,
        TreeMetadata metadata,
        CheckpointManager checkpointManager)
    {
        _pageManager         = pageManager;
        _nodeSerializer      = nodeSerializer;
        _metadata            = metadata;
        _splitter            = new Splitter<TKey, TValue>(pageManager, nodeSerializer);
        _merger              = new Merger<TKey, TValue>(pageManager, nodeSerializer, metadata);
        _latches             = new PageLatchManager();
        _crab                = new LatchCoupling(_latches);
        _checkpointManager   = checkpointManager;
        InitialiseRecovery();

        // Clean up any leftover .compact file from a previous aborted compaction.
        Compactor<TKey, TValue>.CleanupAbortedCompaction(pageManager.DataFilePath);
    }

    internal TreeMetadata       Metadata           => _metadata;
    internal CheckpointManager? CheckpointManager  => _checkpointManager;

    /// <summary>
    /// Wire WAL size-based auto-checkpoint. Delegates to CheckpointManager with
    /// the engine's TransactionCoordinator as the active-lock probe.
    /// No-op when walSizeThresholdBytes is 0 or CheckpointManager is null.
    /// </summary>
    internal void StartAutoCheckpoint(long walSizeThresholdBytes, int pollIntervalMs = 250)
        => _checkpointManager?.StartAutoCheckpoint(
               walSizeThresholdBytes, () => _coordinator.HasActiveLocks, pollIntervalMs);

    /// <summary>
    /// Registers the DEBUG-only post-recovery validation delegate and runs WAL recovery.
    /// Called by both constructors after <see cref="_checkpointManager"/> is assigned.
    /// </summary>
    private void InitialiseRecovery()
    {
#if DEBUG
        _checkpointManager!.PostRecoveryValidation = () =>
        {
            var result = new TreeValidator<TKey, TValue>(
                _pageManager, _nodeSerializer, _metadata).Validate();
            if (!result.IsValid)
                throw new InvalidOperationException(
                    $"Post-recovery structural corruption detected: " +
                    string.Join("; ", result.Errors));
        };
#endif
        _checkpointManager!.RecoverFromWal();
    }

    /// <summary>Trigger an explicit checkpoint.</summary>
    public void Checkpoint() => _checkpointManager?.TakeCheckpoint();

    /// <summary>Checkpoint + flush + dispose. Prefer over Dispose() for clean shutdowns.</summary>
    public void Close()
    {
        _checkpointManager?.GracefulClose();
        _latches.Dispose();
        _coordinator.Dispose();
    }

    /// <inheritdoc />
    public void Dispose() => Close();

    /// <summary>
    /// Insert or update a key-value pair using the CoW write path.
    /// Returns true if a new key was inserted; false if an existing key was updated.
    /// Thread-safe: single-writer mutex serialises CoW root installation.
    /// </summary>
    public bool Insert(TKey key, TValue value)
    {
        _coordinator.EnterWriterLock();
        // Phase 109b: if any transaction holds page write locks (concurrent write phase),
        // fail fast so auto-commit doesn't race with the transaction's page writes.
        if (_coordinator.HasActiveLocks)
        {
            _coordinator.ExitWriterLock();
            throw new TransactionConflictException(0u, 0u, 0u);
        }
        _coordinator.EnterTransactionLock();
        try
        {
            // Guard: reject entries that can never fit on any page regardless of splits.
            // MeasureSize is allocation-free: fixed-size types return FixedSize directly;
            // variable-size types (StringSerializer, ByteArraySerializer) compute byte count
            // without serializing (e.g., Encoding.UTF8.GetByteCount).
            int actualKs = _nodeSerializer.KeySerializer.MeasureSize(key);
            int actualVs = _nodeSerializer.ValueSerializer.MeasureSize(value);
            int maxKey   = PageLayout.MaxKeySize(_pageManager.PageSize);
            int maxEntry = PageLayout.MaxEntrySize(_pageManager.PageSize);

            if (actualKs > maxKey)
                throw new BPlusTreeEntryTooLargeException(
                    $"Key ({actualKs} B) exceeds MaxKeySize ({maxKey} B) for pageSize={_pageManager.PageSize}. " +
                    "Use a shorter key or a larger page size.",
                    actualSize: actualKs, maxSize: maxKey);

            // Value too large for inline → use overflow chain (Phase 99b).
            // Key + 9-byte pointer record must still fit on the leaf; that is guaranteed
            // by actualKs ≤ maxKey (checked above) for any practical page size.
            bool needsOverflow = actualKs + actualVs > maxEntry;

            // Empty tree: allocate the first leaf page and use it as the root.
            // Protected by a write latch on the meta page (page 0) to prevent concurrent init.
            // No CoW needed here — there is no previous version to shadow.
            if (_metadata.RootPageId == PageLayout.NullPageId)
            {
                using var initLatch = _latches.AcquireWriteLatch(PageLayout.MetaPageId);
                if (_metadata.RootPageId == PageLayout.NullPageId)
                {
                    var newFrame = _pageManager.AllocatePage(PageType.Leaf);
                    var newLeaf  = _nodeSerializer.AsLeaf(newFrame);
                    newLeaf.Initialize();
                    if (needsOverflow)
                    {
                        byte[] vBytes = new byte[actualVs];
                        _nodeSerializer.ValueSerializer.Serialize(value, vBytes);
                        _pageManager.AllocateOverflowChain(vBytes, out uint firstPid, out _, txId: 0);
                        newLeaf.WriteOverflowPointer(key, firstPid, actualVs);
                    }
                    else
                    {
                        newLeaf.TryInsert(key, value);
                    }
                    _pageManager.MarkDirtyAndUnpin(newFrame.PageId);
                    _metadata.SetRoot(newFrame.PageId, treeHeight: 1);
                    _metadata.SetFirstLeaf(newFrame.PageId);
                    _metadata.IncrementRecordCount();
                    _metadata.Flush();
                    if (_deltaMap != null) _deltaMap[key] = (DeltaOp.Insert, value);
                    return true;
                }
                // Another thread already initialised the tree — fall through.
            }

            // Non-empty tree: CoW write path.
            // Phase 1: read-traverse root → leaf, collect path + leafId + keyExists + isLeafFull.
            PathBuffer pathBuf = default;
            Span<(uint pageId, int childPos)> path = pathBuf;
            int  pathLen    = 0;
            uint leafId     = 0;
            bool keyExists  = false;
            bool isLeafFull = false;
            bool cowOldIsOverflow          = false;
            uint cowOldOverflowFirstPageId = 0;

            uint currentId = _metadata.RootPageId;
            while (true)
            {
                using var readLatch = _latches.AcquireReadLatch(currentId);
                var frame = _pageManager.FetchPage(currentId);
                if (NodeSerializer<TKey, TValue>.IsLeaf(frame))
                {
                    // Use TryGetRawValue (no deserialization) to avoid misreading overflow
                    // pointer records as TValue bytes (same fix as Delete Phase 1).
                    bool found = LeafNode<TKey, TValue>.TryGetRawValue(
                        frame, key, _nodeSerializer.KeySerializer,
                        out ReadOnlySpan<byte> cowOldRawBytes, out byte cowOldFlags);
                    keyExists = found;
                    // Capture old overflow info before Unpin (span borrows from frame.Data).
                    if (found && (cowOldFlags & PageLayout.SlotIsOverflow) != 0)
                    {
                        cowOldIsOverflow          = true;
                        cowOldOverflowFirstPageId = BinaryPrimitives.ReadUInt32BigEndian(cowOldRawBytes.Slice(4, 4));
                    }
                    if (!keyExists)
                    {
                        var leaf = _nodeSerializer.AsLeaf(frame);
                        // For overflow values the leaf must fit the 9-byte pointer record, not the value.
                        isLeafFull = needsOverflow
                            ? !leaf.HasSpaceFor(actualKs, PageLayout.OverflowPointerSize)
                            : !leaf.HasSpaceFor(actualKs, actualVs);
                    }
                    leafId = currentId;
                    _pageManager.Unpin(currentId);
                    break;
                }
                else
                {
                    var  node     = _nodeSerializer.AsInternal(frame);
                    int  childPos = node.FindChildPosition(key);
                    uint childId  = node.GetChildIdByPosition(childPos);
                    path[pathLen++] = (currentId, childPos);
                    _pageManager.Unpin(currentId);
                    currentId = childId;
                }
            }

            // ── In-place fast path (no snapshot, no split needed) ─────────────────────────
            // When no ISnapshot is open, _writerMutex prevents BeginSnapshot from opening
            // mid-write (both acquire _writerMutex exclusively). Page latches coordinate
            // with concurrent TryGet callers. No shadow pages, no retirement, no CoW cost.
            if (!_coordinator.HasActiveSnapshots && !isLeafFull)
            {
                using var writeLatch = _latches.AcquireWriteLatch(leafId);
                var leafFrame = _pageManager.FetchPage(leafId);
                var leafNode  = _nodeSerializer.AsLeaf(leafFrame);

                if (needsOverflow)
                {
                    // Peek old slot: if the existing entry is also overflow, free its old chain
                    // AFTER the leaf update is WAL-logged (ordering per design doc).
                    uint oldFirstPageId = 0;
                    bool oldIsOverflow  = false;
                    if (keyExists &&
                        LeafNode<TKey, TValue>.TryGetRawValue(leafFrame, key,
                            _nodeSerializer.KeySerializer, out ReadOnlySpan<byte> oldRaw, out byte oldFlags)
                        && (oldFlags & PageLayout.SlotIsOverflow) != 0)
                    {
                        oldIsOverflow  = true;
                        oldFirstPageId = BinaryPrimitives.ReadUInt32BigEndian(oldRaw.Slice(4, 4));
                    }

                    byte[] vBytes = new byte[actualVs];
                    _nodeSerializer.ValueSerializer.Serialize(value, vBytes);
                    _pageManager.AllocateOverflowChain(vBytes, out uint firstPageId, out _, txId: 0);
                    leafNode.WriteOverflowPointer(key, firstPageId, actualVs);
                    _pageManager.MarkDirtyAndUnpin(leafId);           // appends UpdatePage WAL record
                    if (!keyExists) _metadata.IncrementRecordCount();
                    _metadata.Flush();                                 // appends UpdateMeta WAL record
                    if (oldIsOverflow) _pageManager.FreeOverflowChain(oldFirstPageId);
                }
                else
                {
                    leafNode.TryInsert(key, value);
                    _pageManager.MarkDirtyAndUnpin(leafId);   // appends UpdatePage WAL record
                    if (!keyExists) _metadata.IncrementRecordCount();
                    _metadata.Flush();                         // appends UpdateMeta WAL record
                }
                if (_deltaMap != null) _deltaMap[key] = (DeltaOp.Insert, value);
                return !keyExists;
            }

            // Phase 2: CoW path allocation (stack-allocated buffers — zero heap alloc).
            OldPageIdBuffer      oldIdBuf      = default;
            ShadowAncestorBuffer shadowAncBuf  = default;
            Span<uint>           oldPageIds        = oldIdBuf;
            Span<uint>           shadowAncestorIds = shadowAncBuf;
            var (shadowLeaf, shadowRootId) =
                CopyWritePathAndAllocShadows(path, pathLen, leafId, oldPageIds, shadowAncestorIds);

            // Repair leaf chain: the original leaf's left sibling still has
            // NextLeafPageId pointing to the original.  Update it to the shadow.
            FixLeftSiblingNextPointer(path, pathLen, shadowAncestorIds, shadowLeaf.PageId);

            bool wasFirstLeaf = leafId == _metadata.FirstLeafPageId;

            if (keyExists || !isLeafFull)
            {
                // Key exists (overwrite) or leaf has room — direct shadow-leaf modification.
                var shadowLeafNode = _nodeSerializer.AsLeaf(shadowLeaf);
                if (needsOverflow)
                {
                    byte[] vBytes = new byte[actualVs];
                    _nodeSerializer.ValueSerializer.Serialize(value, vBytes);
                    _pageManager.AllocateOverflowChain(vBytes, out uint firstPid, out _, txId: 0);
                    shadowLeafNode.WriteOverflowPointer(key, firstPid, actualVs);
                }
                else
                {
                    shadowLeafNode.TryInsert(key, value);
                }
                _pageManager.MarkDirtyAndUnpin(shadowLeaf.PageId);
                _metadata.SetRoot(shadowRootId, _metadata.TreeHeight);
                if (wasFirstLeaf) _metadata.SetFirstLeaf(shadowLeaf.PageId);
                if (!keyExists) _metadata.IncrementRecordCount();
                _metadata.Flush();
                if (cowOldIsOverflow) RetireOverflowChain(cowOldOverflowFirstPageId, _coordinator.RetirePage, writeWalRecord: true);
                _coordinator.RetirePages(oldPageIds, pathLen + 1);
                if (_deltaMap != null) _deltaMap[key] = (DeltaOp.Insert, value);
                return !keyExists;
            }

            // Shadow leaf is full (new key) — split in shadow context.
            _pageManager.Unpin(shadowLeaf.PageId); // Splitter will FetchPage it

            PathBuffer shadowPathBuf = default;
            Span<(uint pageId, int childPos)> shadowPath = shadowPathBuf;
            for (int i = 0; i < pathLen; i++)
                shadowPath[i] = (shadowAncestorIds[i], path[i].childPos);

            // Install shadow root so re-traversal always starts from a shadow page.
            _metadata.SetRoot(shadowRootId, _metadata.TreeHeight);
            if (wasFirstLeaf) _metadata.SetFirstLeaf(shadowLeaf.PageId);

            var (newAutoRoot, newAutoHeight) =
                _splitter.SplitLeaf(shadowLeaf.PageId, shadowPath, pathLen, _metadata.TreeHeight);
            if (newAutoRoot != 0)
                _metadata.SetRoot(newAutoRoot, newAutoHeight);

            // Re-traverse shadow tree from the (possibly new) root to insert the key.
            // Split path is only reached for new keys (keyExists=false), so no old overflow chain.
            uint insertId = _metadata.RootPageId;
            while (true)
            {
                var insertFrame = _pageManager.FetchPage(insertId);
                if (NodeSerializer<TKey, TValue>.IsLeaf(insertFrame))
                {
                    var insertLeafNode = _nodeSerializer.AsLeaf(insertFrame);
                    if (needsOverflow)
                    {
                        byte[] vBytes = new byte[actualVs];
                        _nodeSerializer.ValueSerializer.Serialize(value, vBytes);
                        _pageManager.AllocateOverflowChain(vBytes, out uint firstPid, out _, txId: 0);
                        insertLeafNode.WriteOverflowPointer(key, firstPid, actualVs);
                    }
                    else
                    {
                        insertLeafNode.TryInsert(key, value);
                    }
                    _pageManager.MarkDirtyAndUnpin(insertId);
                    break;
                }
                else
                {
                    var  insertNode = _nodeSerializer.AsInternal(insertFrame);
                    uint childId    = insertNode.FindChildId(key);
                    _pageManager.Unpin(insertId);
                    insertId = childId;
                }
            }

            _metadata.IncrementRecordCount();
            _metadata.Flush();
            _coordinator.RetirePages(oldPageIds, pathLen + 1);
            if (_deltaMap != null) _deltaMap[key] = (DeltaOp.Insert, value);
            return true;
        }
        finally
        {
            _coordinator.ExitTransactionLock();
            _coordinator.ExitWriterLock();
        }
    }

    /// <summary>
    /// Remove the entry for the given key using the CoW write path.
    /// Returns true if found and removed; false if not present.
    /// Thread-safe: single-writer mutex serialises CoW root installation.
    /// </summary>
    public bool Delete(TKey key)
    {
        _coordinator.EnterWriterLock();
        // Phase 109b: fail fast if any transaction holds page write locks.
        if (_coordinator.HasActiveLocks)
        {
            _coordinator.ExitWriterLock();
            throw new TransactionConflictException(0u, 0u, 0u);
        }
        _coordinator.EnterTransactionLock();
        try
        {
            if (_metadata.RootPageId == PageLayout.NullPageId) return false;

            // Phase 1: read-traverse root → leaf, collect path + leafId, check key exists.
            PathBuffer pathBuf = default;
            Span<(uint pageId, int childPos)> path = pathBuf;
            int  pathLen   = 0;
            uint leafId    = 0;
            bool keyExists = false;
            int  leafCount = 0;   // captured here to avoid a second FetchPage in the fast path

            uint currentId = _metadata.RootPageId;
            while (true)
            {
                using var readLatch = _latches.AcquireReadLatch(currentId);
                var frame = _pageManager.FetchPage(currentId);
                if (NodeSerializer<TKey, TValue>.IsLeaf(frame))
                {
                    // Use TryGetRawValue (no deserialization) — Delete only needs key existence.
                    // leaf.TryGet would fail for overflow entries (pointer record ≠ valid TValue bytes).
                    keyExists = LeafNode<TKey, TValue>.TryGetRawValue(
                        frame, key, _nodeSerializer.KeySerializer, out _, out _);
                    leafCount = LeafNode<TKey, TValue>.GetSlotCount(frame);
                    leafId    = currentId;
                    _pageManager.Unpin(currentId);
                    break;
                }
                else
                {
                    var  node     = _nodeSerializer.AsInternal(frame);
                    int  childPos = node.FindChildPosition(key);
                    uint childId  = node.GetChildIdByPosition(childPos);
                    path[pathLen++] = (currentId, childPos);
                    _pageManager.Unpin(currentId);
                    currentId = childId;
                }
            }

            if (!keyExists) return false;

            // ── In-place fast path (no snapshot, no underflow) ────────────────────────────
            bool willUnderflow = pathLen > 0 && (leafCount - 1) < _merger.LeafThreshold();
            if (!_coordinator.HasActiveSnapshots && !willUnderflow)
            {
                using var writeLatch = _latches.AcquireWriteLatch(leafId);
                var leafFrame = _pageManager.FetchPage(leafId);

                // Peek overflow pointer BEFORE Remove() destroys the slot entry.
                uint overflowFirstPageId = 0;
                bool isOverflow = false;
                if (LeafNode<TKey, TValue>.TryGetRawValue(leafFrame, key, _nodeSerializer.KeySerializer,
                        out ReadOnlySpan<byte> rawBytes, out byte slotFlags)
                    && (slotFlags & PageLayout.SlotIsOverflow) != 0)
                {
                    isOverflow = true;
                    overflowFirstPageId = BinaryPrimitives.ReadUInt32BigEndian(rawBytes.Slice(4, 4));
                }

                _nodeSerializer.AsLeaf(leafFrame).Remove(key);
                _pageManager.MarkDirtyAndUnpin(leafId);   // appends UpdatePage WAL record
                _metadata.DecrementRecordCount();
                _metadata.Flush();                         // appends UpdateMeta WAL record

                // Free overflow chain AFTER leaf update is WAL-logged (leaf no longer refs chain).
                // Known gap: crash between MarkDirtyAndUnpin and FreeOverflowChain leaks chain pages.
                if (isOverflow)
                    _pageManager.FreeOverflowChain(overflowFirstPageId);

                if (_deltaMap != null) _deltaMap[key] = (DeltaOp.Delete, default);
                return true;
            }

            // Phase 2: CoW path allocation (stack-allocated buffers — zero heap alloc).
            OldPageIdBuffer      oldIdBuf      = default;
            ShadowAncestorBuffer shadowAncBuf  = default;
            Span<uint>           oldPageIds        = oldIdBuf;
            Span<uint>           shadowAncestorIds = shadowAncBuf;
            var (shadowLeaf, shadowRootId) =
                CopyWritePathAndAllocShadows(path, pathLen, leafId, oldPageIds, shadowAncestorIds);

            // Repair leaf chain: the original leaf's left sibling still has
            // NextLeafPageId pointing to the original.  Update it to the shadow.
            FixLeftSiblingNextPointer(path, pathLen, shadowAncestorIds, shadowLeaf.PageId);

            // Phase 3: Remove key from shadow leaf; check underflow before unpinning.
            var shadowLeafNode = _nodeSerializer.AsLeaf(shadowLeaf);
            shadowLeafNode.Remove(key);
            bool underflows = pathLen > 0 && shadowLeafNode.Count < _merger.LeafThreshold();
            _pageManager.MarkDirtyAndUnpin(shadowLeaf.PageId);

            // Install shadow root and update first-leaf pointer before any merger call
            // so that CollapseRoot (if triggered) operates on the shadow root, not the original.
            _metadata.DecrementRecordCount();
            bool wasFirstLeaf = leafId == _metadata.FirstLeafPageId;
            _metadata.SetRoot(shadowRootId, _metadata.TreeHeight);
            if (wasFirstLeaf) _metadata.SetFirstLeaf(shadowLeaf.PageId);

            if (!underflows)
            {
                _metadata.Flush();
                _coordinator.RetirePages(oldPageIds, pathLen + 1);
                if (_deltaMap != null) _deltaMap[key] = (DeltaOp.Delete, default);
                return true;
            }

            // Phase 4: Underflow — rebalance on shadow pages.
            // Shadow root is already installed; Merger's CollapseRoot will collapse it if needed.
            PathBuffer shadowPathBuf = default;
            Span<(uint pageId, int childPos)> shadowPath = shadowPathBuf;
            for (int i = 0; i < pathLen; i++)
                shadowPath[i] = (shadowAncestorIds[i], path[i].childPos);

            _merger.RebalanceLeaf(shadowLeaf.PageId, shadowPath, pathLen);
            // _metadata.RootPageId now reflects the final shadow tree root
            // (either shadowRootId unchanged, or shadow child if CollapseRoot was called).

            _metadata.Flush();
            _coordinator.RetirePages(oldPageIds, pathLen + 1);
            if (_deltaMap != null) _deltaMap[key] = (DeltaOp.Delete, default);
            return true;
        }
        finally
        {
            _coordinator.ExitTransactionLock();
            _coordinator.ExitWriterLock();
        }
    }

    /// <summary>
    /// Atomically read and update the value for an existing key using the CoW write path.
    /// The updater receives the current value and returns the new value.
    /// Returns true if the key was found and updated; false if not found.
    /// Thread-safe: single-writer mutex serialises CoW root installation.
    /// </summary>
    public bool Update(TKey key, Func<TValue, TValue> updater)
    {
        _coordinator.EnterWriterLock();
        // Phase 109b: fail fast if any transaction holds page write locks.
        if (_coordinator.HasActiveLocks)
        {
            _coordinator.ExitWriterLock();
            throw new TransactionConflictException(0u, 0u, 0u);
        }
        _coordinator.EnterTransactionLock();
        try
        {
            if (_metadata.RootPageId == PageLayout.NullPageId) return false;

            // ── Phase 1: read-traverse, collect path and leaf ID ──────────────────
            PathBuffer pathBuf = default;
            Span<(uint pageId, int childPos)> path = pathBuf;
            int   pathLen  = 0;
            uint  leafId   = 0;
            TValue current = default!;

            // Overflow info captured during Phase 1 for use in the write phase.
            bool oldIsOverflow          = false;
            uint oldOverflowFirstPageId = 0;

            uint currentId = _metadata.RootPageId;
            while (true)
            {
                using var readLatch = _latches.AcquireReadLatch(currentId);
                var frame = _pageManager.FetchPage(currentId);
                if (NodeSerializer<TKey, TValue>.IsLeaf(frame))
                {
                    // Use TryGetRawValue so overflow pointer bytes aren't fed to the deserializer.
                    if (!LeafNode<TKey, TValue>.TryGetRawValue(frame, key,
                            _nodeSerializer.KeySerializer, out ReadOnlySpan<byte> rawBytes, out byte slotFlags))
                    {
                        _pageManager.Unpin(currentId);
                        return false; // key not found — no CoW needed
                    }

                    oldIsOverflow = (slotFlags & PageLayout.SlotIsOverflow) != 0;
                    if (oldIsOverflow)
                    {
                        int totalLen           = BinaryPrimitives.ReadInt32BigEndian(rawBytes[..4]);
                        oldOverflowFirstPageId = BinaryPrimitives.ReadUInt32BigEndian(rawBytes.Slice(4, 4));
                        byte[] rented0 = System.Buffers.ArrayPool<byte>.Shared.Rent(totalLen);
                        try
                        {
                            _pageManager.ReadOverflowChain(oldOverflowFirstPageId, totalLen, rented0.AsSpan(0, totalLen));
                            current = _nodeSerializer.ValueSerializer.Deserialize(rented0.AsSpan(0, totalLen));
                        }
                        finally { System.Buffers.ArrayPool<byte>.Shared.Return(rented0); }
                    }
                    else
                    {
                        current = _nodeSerializer.ValueSerializer.Deserialize(rawBytes);
                    }
                    leafId = currentId;
                    _pageManager.Unpin(currentId);
                    break;
                }
                else
                {
                    var  node     = _nodeSerializer.AsInternal(frame);
                    int  childPos = node.FindChildPosition(key);
                    uint childId  = node.GetChildIdByPosition(childPos);
                    path[pathLen++] = (currentId, childPos);
                    _pageManager.Unpin(currentId);
                    currentId = childId;
                }
            }

            // Compute new value once (updater may have side effects).
            TValue newValue  = updater(current);
            int    newVs     = _nodeSerializer.ValueSerializer.MeasureSize(newValue);
            int    updateKs  = _nodeSerializer.KeySerializer.MeasureSize(key);
            int    maxEntry  = PageLayout.MaxEntrySize(_pageManager.PageSize);
            bool   newIsOverflow = updateKs + newVs > maxEntry;

            // ── In-place fast path (no snapshot; Update never splits or causes underflow) ──
            // Root, record count, and first-leaf pointer are all unchanged on an overwrite.
            if (!_coordinator.HasActiveSnapshots)
            {
                using var writeLatch = _latches.AcquireWriteLatch(leafId);
                var leafFrame = _pageManager.FetchPage(leafId);
                var leafNode  = _nodeSerializer.AsLeaf(leafFrame);

                if (newIsOverflow)
                {
                    // Cases: inline→overflow  AND  overflow→overflow
                    byte[] vBytes = new byte[newVs];
                    _nodeSerializer.ValueSerializer.Serialize(newValue, vBytes);
                    _pageManager.AllocateOverflowChain(vBytes, out uint firstPageId, out _, txId: 0);
                    leafNode.WriteOverflowPointer(key, firstPageId, newVs);
                }
                else
                {
                    // Cases: inline→inline  AND  overflow→inline
                    // TryInsert handles the overflow→inline transition correctly after the
                    // flags fix: it detects existingIsOverflow, removes the old slot, and
                    // re-inserts a fresh inline slot with flags=0.
                    leafNode.TryInsert(key, newValue);
                }

                _pageManager.MarkDirtyAndUnpin(leafId);   // appends UpdatePage WAL record

                // Free old overflow chain AFTER leaf is WAL-logged (leaf no longer refs old chain).
                if (oldIsOverflow)
                    _pageManager.FreeOverflowChain(oldOverflowFirstPageId);

                if (_deltaMap != null) _deltaMap[key] = (DeltaOp.Insert, newValue);
                return true;
            }

            // ── Phase 2: CoW path allocation (stack-allocated buffers — zero heap alloc) ──
            OldPageIdBuffer      oldIdBuf  = default;
            Span<uint>           oldPageIds = oldIdBuf;
            ShadowAncestorBuffer _shadowAncBuf = default; // needed by helper signature
            Span<uint>           _shadowAncIds = _shadowAncBuf;
            var (shadowLeaf, shadowRootId) =
                CopyWritePathAndAllocShadows(path, pathLen, leafId, oldPageIds, _shadowAncIds);

            // Modify shadow leaf: overwrite existing key's value (overflow-aware).
            var shadowLeafNode = _nodeSerializer.AsLeaf(shadowLeaf);
            if (newIsOverflow)
            {
                byte[] vBytes = new byte[newVs];
                _nodeSerializer.ValueSerializer.Serialize(newValue, vBytes);
                _pageManager.AllocateOverflowChain(vBytes, out uint firstPid, out _, txId: 0);
                shadowLeafNode.WriteOverflowPointer(key, firstPid, newVs);
            }
            else
            {
                shadowLeafNode.TryInsert(key, newValue);
            }
            _pageManager.MarkDirtyAndUnpin(shadowLeaf.PageId);

            // ── Phase 3: install new root, update first-leaf pointer if needed ─────
            bool wasFirstLeaf = leafId == _metadata.FirstLeafPageId;
            _metadata.SetRoot(shadowRootId, _metadata.TreeHeight);
            if (wasFirstLeaf) _metadata.SetFirstLeaf(shadowLeaf.PageId);
            _metadata.Flush();

            // ── Phase 4: retire all old path pages ────────────────────────────────
            // Retire old overflow chain BEFORE retiring pages (both go to epoch-retirement).
            if (oldIsOverflow) RetireOverflowChain(oldOverflowFirstPageId, _coordinator.RetirePage, writeWalRecord: true);
            _coordinator.RetirePages(oldPageIds, pathLen + 1);

            if (_deltaMap != null) _deltaMap[key] = (DeltaOp.Insert, newValue);
            return true;
        }
        finally
        {
            _coordinator.ExitTransactionLock();
            _coordinator.ExitWriterLock();
        }
    }

    /// <summary>
    /// Insert a key/value pair only if the key does not already exist.
    /// Returns true if inserted; false if the key was present (value unchanged).
    /// Thread-safe: writer lock prevents any interleave between existence check and insert.
    /// </summary>
    public bool TryInsert(TKey key, TValue value)
    {
        _coordinator.EnterWriterLock();
        try
        {
            // TryGet uses read latches only — safe to call while holding the writer lock.
            // No other writer can interleave between the check and the insert.
            if (TryGet(key, out _)) return false;

            // Key is absent. Insert() re-enters writer lock via depth counter (no wait)
            // and acquires transaction lock via SupportsRecursion read lock (no deadlock).
            Insert(key, value); // always returns true (key was confirmed absent)
            return true;
        }
        finally
        {
            _coordinator.ExitWriterLock();
        }
    }

    /// <summary>
    /// Atomically add or update a key/value pair.
    /// If <paramref name="key"/> is absent: inserts <paramref name="addValue"/>; returns <paramref name="addValue"/>.
    /// If <paramref name="key"/> is present: calls <paramref name="updateValueFactory"/>(<paramref name="key"/>, existingValue),
    /// overwrites with the result, and returns the result.
    /// Thread-safe: writer lock prevents any TOCTOU window between check and write.
    /// </summary>
    public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
    {
        _coordinator.EnterWriterLock();
        try
        {
            if (TryGet(key, out TValue existing))
            {
                TValue updated = updateValueFactory(key, existing);
                Insert(key, updated);   // re-enters writer lock via _txWriterDepth counter
                return updated;
            }
            else
            {
                Insert(key, addValue);  // re-enters writer lock via _txWriterDepth counter
                return addValue;
            }
        }
        finally
        {
            _coordinator.ExitWriterLock();
        }
    }

    /// <summary>
    /// Fetch or insert a key/value pair.
    /// If <paramref name="key"/> is present: returns the existing stored value; tree is unchanged.
    /// If <paramref name="key"/> is absent: inserts <paramref name="addValue"/> and returns it.
    /// The check and conditional-insert are atomic under the writer lock — no TOCTOU window.
    /// </summary>
    public TValue GetOrAdd(TKey key, TValue addValue)
    {
        _coordinator.EnterWriterLock();
        try
        {
            if (TryGet(key, out TValue existing))
                return existing;

            Insert(key, addValue);  // re-enters writer lock via _txWriterDepth counter
            return addValue;
        }
        finally
        {
            _coordinator.ExitWriterLock();
        }
    }

    /// <summary>
    /// Atomically read and delete a key/value pair.
    /// If <paramref name="key"/> is absent: sets <paramref name="value"/> to default and returns false.
    /// If <paramref name="key"/> is present: captures the value, deletes the key, and returns true.
    /// The read and delete are atomic under the writer lock — no TOCTOU window.
    /// </summary>
    public bool TryGetAndDelete(TKey key, out TValue value)
    {
        _coordinator.EnterWriterLock();
        try
        {
            if (!TryGet(key, out value)) return false;
            Delete(key);   // re-enters writer lock via _txWriterDepth counter
            return true;
        }
        finally
        {
            _coordinator.ExitWriterLock();
        }
    }

    /// <summary>
    /// Atomically update the value for <paramref name="key"/> only if the current stored value equals
    /// <paramref name="expected"/>. Returns false if key absent or value mismatches (tree unchanged).
    /// Returns true if matched — value updated to <paramref name="newValue"/>.
    /// The read and conditional write are atomic under the writer lock — no TOCTOU window.
    /// <paramref name="comparer"/> defaults to <see cref="EqualityComparer{TValue}.Default"/> when null.
    /// </summary>
    public bool TryCompareAndSwap(TKey key, TValue expected, TValue newValue, IEqualityComparer<TValue>? comparer = null)
    {
        comparer ??= EqualityComparer<TValue>.Default;
        _coordinator.EnterWriterLock();
        try
        {
            if (!TryGet(key, out TValue existing)) return false;
            if (!comparer.Equals(existing, expected)) return false;
            Update(key, _ => newValue);   // re-enters writer lock via _txWriterDepth counter
            return true;
        }
        finally
        {
            _coordinator.ExitWriterLock();
        }
    }

    /// <summary>
    /// Search for the given key. Returns true and sets value if found.
    /// Thread-safe: uses read latch coupling (crab-walking) from root to leaf.
    /// Multiple readers may traverse concurrently.
    /// </summary>
    public bool TryGet(TKey key, out TValue value)
    {
        if (_metadata.RootPageId == PageLayout.NullPageId)
        {
            value = default!;
            return false;
        }

        // Pin-aware FreePage (Phase 65) eliminates the need for EnterReader/ExitReader here:
        // FreePage defers _freeList.Deallocate while the frame is pinned, so AllocatePage
        // cannot reuse a page that TryGet currently holds pinned.
        uint current = _metadata.RootPageId;
        ReadLatchHandle currentLatch = _crab.CrabReadDown(current, default);
        try
        {
            while (true)
            {
                var frame = _pageManager.FetchPage(current);
                if (NodeSerializer<TKey, TValue>.IsLeaf(frame))
                {
                    if (LeafNode<TKey, TValue>.TryGetRawValue(frame, key,
                            _nodeSerializer.KeySerializer, out ReadOnlySpan<byte> rawBytes, out byte slotFlags))
                    {
                        if ((slotFlags & PageLayout.SlotIsOverflow) != 0)
                        {
                            // Pointer record: [TotalLen:4 BE][FirstPageId:4 BE][_:1]
                            int  totalLen    = BinaryPrimitives.ReadInt32BigEndian(rawBytes[..4]);
                            uint firstPageId = BinaryPrimitives.ReadUInt32BigEndian(rawBytes.Slice(4, 4));
                            byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(totalLen);
                            try
                            {
                                _pageManager.ReadOverflowChain(firstPageId, totalLen, rented.AsSpan(0, totalLen));
                                value = _nodeSerializer.ValueSerializer.Deserialize(rented.AsSpan(0, totalLen));
                            }
                            finally { System.Buffers.ArrayPool<byte>.Shared.Return(rented); }
                        }
                        else
                        {
                            value = _nodeSerializer.ValueSerializer.Deserialize(rawBytes);
                        }
                        _pageManager.Unpin(current);
                        currentLatch.Dispose();
                        return true;
                    }
                    value = default!;
                    _pageManager.Unpin(current);
                    currentLatch.Dispose();
                    return false;
                }
                else
                {
                    uint childId = InternalNode<TKey>.FindChildId(frame, key, _nodeSerializer.KeySerializer);
                    _pageManager.Unpin(current);
                    currentLatch = _crab.CrabReadDown(childId, currentLatch);
                    current = childId;
                }
            }
        }
        finally
        {
            currentLatch.Dispose();
        }
    }

    /// <summary>
    /// Search for <paramref name="key"/> within <paramref name="tx"/>'s shadow tree.
    /// Traverses from <c>tx.TxRootId</c>; pin-aware FreePage provides page-retirement safety.
    /// Returns true and sets <paramref name="value"/> if found; false and default otherwise.
    /// </summary>
    internal bool TryGetInTransaction(
        TKey key, Transaction<TKey, TValue> tx, out TValue value)
    {
        uint rootId = tx.TxRootId;
        if (rootId == PageLayout.NullPageId)
        {
            value = default!;
            return false;
        }

        uint current = rootId;
        ReadLatchHandle currentLatch = _crab.CrabReadDown(current, default);
        try
        {
            while (true)
            {
                var frame = _pageManager.FetchPage(current);
                if (NodeSerializer<TKey, TValue>.IsLeaf(frame))
                {
                    bool found = LeafNode<TKey, TValue>.TryGetRawValue(
                        frame, key, _nodeSerializer.KeySerializer,
                        out ReadOnlySpan<byte> rawBytes, out byte slotFlags);
                    if (found)
                    {
                        if ((slotFlags & PageLayout.SlotIsOverflow) != 0)
                        {
                            int  totalLen    = BinaryPrimitives.ReadInt32BigEndian(rawBytes[..4]);
                            uint firstPageId = BinaryPrimitives.ReadUInt32BigEndian(rawBytes.Slice(4, 4));
                            byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(totalLen);
                            try
                            {
                                _pageManager.ReadOverflowChain(firstPageId, totalLen, rented.AsSpan(0, totalLen));
                                value = _nodeSerializer.ValueSerializer.Deserialize(rented.AsSpan(0, totalLen));
                            }
                            finally { System.Buffers.ArrayPool<byte>.Shared.Return(rented); }
                        }
                        else
                        {
                            value = _nodeSerializer.ValueSerializer.Deserialize(rawBytes);
                        }
                    }
                    else
                    {
                        value = default!;
                    }
                    _pageManager.Unpin(current);
                    currentLatch.Dispose();
                    tx.TrackLeafRead(current);   // SSI Phase 88: record leaf visited (hit or miss)
                    return found;
                }
                uint childId = InternalNode<TKey>.FindChildId(
                    frame, key, _nodeSerializer.KeySerializer);
                _pageManager.Unpin(current);
                currentLatch = _crab.CrabReadDown(childId, currentLatch);
                current = childId;
            }
        }
        finally
        {
            currentLatch.Dispose();
        }
    }

    /// <summary>
    /// Enumerate [startKey, endKey] from <paramref name="tx"/>'s shadow tree.
    /// Root and first-leaf IDs are captured at entry (snapshot semantics).
    /// </summary>
    internal IEnumerable<(TKey Key, TValue Value)> ScanInTransaction(
        TKey? startKey, TKey? endKey, Transaction<TKey, TValue> tx)
    {
        uint rootId = tx.TxRootId;
        if (rootId == PageLayout.NullPageId)
            yield break;

        bool hasStartKey = startKey is TKey &&
            !EqualityComparer<TKey>.Default.Equals(startKey!, default!);
        bool hasEndKey = endKey is TKey &&
            !EqualityComparer<TKey>.Default.Equals(endKey!, default!);
        TKey startActual = hasStartKey ? startKey! : default!;
        TKey endActual   = hasEndKey   ? endKey!   : default!;

        uint firstLeafId = hasStartKey
            ? FindLeafForKey(startActual, rootId)
            : tx.TxFirstLeafId(_metadata.FirstLeafPageId);

        using var iter = TreeIterator<TKey, TValue>.Rent(
            _pageManager, _nodeSerializer, _nodeSerializer.KeySerializer,
            firstLeafId,
            hasStartKey, startActual,
            hasEndKey,   endActual);

        uint lastTrackedLeaf = PageLayout.NullPageId;
        while (iter.MoveNext())
        {
            uint leafId = iter.CurrentLeafId;
            if (leafId != lastTrackedLeaf)
            {
                tx.TrackLeafRead(leafId);   // SSI Phase 88: record each leaf traversed
                lastTrackedLeaf = leafId;
            }
            yield return iter.Current;
        }
    }

    /// <summary>
    /// Return all key-value pairs where startKey ≤ key ≤ endKey (both inclusive, both optional).
    /// Results are in ascending sort order. Safe to use in foreach.
    /// Null startKey = from the beginning of the tree.
    /// Null endKey   = to the end of the tree.
    ///
    /// Pinning contract: the iterator pins at most one leaf page at a time.
    /// No pages are pinned between MoveNext() calls returning false and Dispose().
    /// </summary>
    public IEnumerable<(TKey Key, TValue Value)> Scan(
        TKey? startKey = default,
        TKey? endKey   = default)
    {
        // Delegate to a snapshot so the full scan is protected by an epoch token.
        // BeginSnapshot() captures the current root + firstLeaf atomically under the
        // writer lock and registers a read epoch. The epoch prevents CoW page retirement
        // for the full scan duration. Dispose() releases the epoch when the enumerator
        // is exhausted or abandoned (the compiler emits Dispose() on early exit).
        using var snap = BeginSnapshot();
        foreach (var item in snap.Scan(startKey, endKey))
            yield return item;
    }

    /// <summary>
    /// Enumerate all key-value pairs in [startKey, endKey] in descending key order.
    /// Epoch-protected via an internal snapshot (same pattern as <see cref="Scan"/>).
    /// </summary>
    public IEnumerable<(TKey Key, TValue Value)> ScanReverse(
        TKey? endKey   = default,
        TKey? startKey = default)
    {
        using var snap = BeginSnapshot();
        foreach (var item in snap.ScanReverse(endKey, startKey))
            yield return item;
    }

    /// <summary>
    /// Full tree compaction. Builds a compact copy of the tree, atomically renames it
    /// over the original file. Delegates to Compactor.
    /// No-op if no WAL is attached (compaction requires WAL for crash safety).
    /// </summary>
    public CompactionResult Compact()
    {
        if (_pageManager.Wal == null) return default;
        var compactor = new Compactor<TKey, TValue>(
            _pageManager, _nodeSerializer, _metadata,
            _pageManager.Options, _pageManager.Wal, _coordinator,
            enableDelta:    () => EnableDelta(),
            disableDelta:   () => DisableDelta(),
            takeCheckpoint: () => _checkpointManager?.TakeCheckpoint());
        return compactor.Compact();
    }

    /// <summary>
    /// Validates the structural integrity of the tree: key sort order across
    /// the leaf chain, record count consistency, and separator alignment in
    /// internal nodes.
    /// </summary>
    public ValidationResult Validate()
        => new TreeValidator<TKey, TValue>(_pageManager, _nodeSerializer, _metadata).Validate();

    /// <summary>
    /// Traverse root → leaf following separator keys with read latch coupling.
    /// Returns the leaf pageId — the leaf is NOT pinned on return.
    /// TreeIterator pins it on the first MoveNext() call.
    /// </summary>
    private uint FindLeafForKey(TKey key, uint rootId = 0)
    {
        uint current = rootId != 0 ? rootId : _metadata.RootPageId;
        ReadLatchHandle currentLatch = _crab.CrabReadDown(current, default);

        while (true)
        {
            var frame = _pageManager.FetchPage(current);
            if (NodeSerializer<TKey, TValue>.IsLeaf(frame))
            {
                _pageManager.Unpin(current);
                currentLatch.Dispose();
                return current;
            }
            uint child = InternalNode<TKey>.FindChildId(frame, key, _nodeSerializer.KeySerializer);
            _pageManager.Unpin(current);
            currentLatch = _crab.CrabReadDown(child, currentLatch);
            current = child;
        }
    }

    /// <summary>
    /// Traverse from <paramref name="rootId"/>, always following the rightmost child
    /// at each internal level, and return the rightmost leaf page ID.
    /// Used by <see cref="ScanReverseFromSnapshot"/> when no endKey is specified.
    /// Uses the same read-latch-coupling pattern as <see cref="FindLeafForKey"/>.
    /// </summary>
    private uint FindRightmostLeaf(uint rootId)
    {
        var ks = _nodeSerializer.KeySerializer;
        uint current = rootId;
        ReadLatchHandle currentLatch = _crab.CrabReadDown(current, default);
        while (true)
        {
            var frame = _pageManager.FetchPage(current);
            if (NodeSerializer<TKey, TValue>.IsLeaf(frame))
            {
                _pageManager.Unpin(current);
                currentLatch.Dispose();
                return current;
            }
            uint child = InternalNode<TKey>.FindRightmostChildId(frame, ks);
            _pageManager.Unpin(current);
            currentLatch = _crab.CrabReadDown(child, currentLatch);
            current = child;
        }
    }

    // ── CoW (shadow-write) helpers ────────────────────────────────────────────

    /// <summary>
    /// Allocate CoW shadow copies for all pages on the write path from the root down
    /// to <paramref name="leafId"/>. Works bottom-up: shadow leaf first, then shadow
    /// ancestors updating child pointers to refer to shadow children.
    ///
    /// Returns:
    ///   shadowLeaf         — pinned, dirty frame for the new leaf copy; caller must
    ///                        modify content and call MarkDirtyAndUnpin.
    ///   shadowRootId       — new root page ID to install in _metadata after modification.
    ///   oldPageIds         — all original page IDs (ancestors + leaf) to retire after commit.
    ///   shadowAncestorIds  — shadow page IDs for each path ancestor (parallel to path[0..pathLen-1]).
    ///
    /// <paramref name="path"/>[i] = (originalPageId, childPositionFollowed) for each
    /// internal node on the path. <paramref name="pathLen"/> = number of ancestors.
    /// </summary>
    private (Frame shadowLeaf, uint shadowRootId)
        CopyWritePathAndAllocShadows(
            Span<(uint pageId, int childPos)> path, int pathLen, uint leafId,
            Span<uint> oldPageIds, Span<uint> shadowAncestorIds,
            uint txId = 0, IReadOnlySet<uint>? ownedShadowPages = null)
    {
        oldPageIds[pathLen] = leafId;

        // Allocate shadow leaf (returned pinned; caller will modify + unpin).
        // If this leaf was already CoW-copied by an earlier write in the same transaction,
        // reuse it in-place — the WAL before-image was captured on the first copy.
        Frame shadowLeaf;
        if (ownedShadowPages != null && ownedShadowPages.Contains(leafId))
            shadowLeaf = _pageManager.FetchPage(leafId);   // reuse owned shadow
        else
            shadowLeaf = _pageManager.AllocatePageCow(leafId);
        uint childShadowId = shadowLeaf.PageId;

        // Walk ancestors from leaf's parent up to root, allocating shadow copies.
        // Reuse owned ancestors for the same reason as the leaf.
        for (int i = pathLen - 1; i >= 0; i--)
        {
            var (ancestorId, childPos) = path[i];
            Frame shadowAncestor;
            if (ownedShadowPages != null && ownedShadowPages.Contains(ancestorId))
                shadowAncestor = _pageManager.FetchPage(ancestorId);  // reuse owned shadow
            else
                shadowAncestor = _pageManager.AllocatePageCow(ancestorId);
            // Update the child pointer in the shadow ancestor to point to the shadow child.
            _nodeSerializer.AsInternal(shadowAncestor).SetChildIdByPosition(childPos, childShadowId);
            _pageManager.MarkDirtyAndUnpin(shadowAncestor.PageId);
            oldPageIds[i]        = ancestorId;
            shadowAncestorIds[i] = shadowAncestor.PageId;
            childShadowId        = shadowAncestor.PageId;
        }

        uint shadowRootId = pathLen > 0 ? childShadowId : shadowLeaf.PageId;

        // Emit AllocShadowChain WAL record whenever WAL is present (Gap 1 closure for all paths).
        // txId≠0: Undo Pass frees shadow pages on crash-before-commit (existing machinery).
        // txId=0 (auto-commit CoW): Analysis Pass tracks via pendingAutoShadows; frees on crash
        //        before the corresponding UpdateMeta is fsynced.
        // Only include truly new allocations — reused shadow pages were already emitted
        // in a prior AllocShadowChain record for this transaction.
        if (_pageManager.Wal != null)
        {
            // Collect only the page IDs that are new allocations (not reused).
            int newCount = 0;
            var shadowIds = new uint[pathLen + 1];
            for (int i = 0; i < pathLen; i++)
            {
                if (ownedShadowPages == null || !ownedShadowPages.Contains(path[i].pageId))
                    shadowIds[newCount++] = shadowAncestorIds[i];
            }
            if (ownedShadowPages == null || !ownedShadowPages.Contains(leafId))
                shadowIds[newCount++] = shadowLeaf.PageId;
            if (newCount > 0)
                _pageManager.Wal.AppendAllocShadowChain(txId, shadowIds[..newCount]);
        }

        return (shadowLeaf, shadowRootId);
    }

    /// <summary>
    /// Walk an overflow chain from <paramref name="firstPageId"/> and call
    /// <paramref name="retireAction"/> on each page ID.
    /// Used by CoW write paths to epoch-retire old overflow chain pages at commit
    /// (transaction: tx.TrackObsoleteOverflowPage) or immediately (auto-commit CoW:
    /// _coordinator.RetirePage).
    /// When <paramref name="writeWalRecord"/> is true (auto-commit CoW path only),
    /// appends a FreeOverflowChain WAL record before retiring pages — Gap 3 closure.
    /// </summary>
    private void RetireOverflowChain(uint firstPageId, Action<uint> retireAction,
                                     bool writeWalRecord = false)
    {
        // Pass 1: collect all page IDs before any retirement side-effect.
        var ids = new System.Collections.Generic.List<uint>();
        uint pageId = firstPageId;
        while (pageId != 0)
        {
            var frame = _pageManager.FetchPage(pageId);
            uint nextId = BinaryPrimitives.ReadUInt32BigEndian(
                frame.Data.AsSpan(OverflowPageLayout.NextPageIdOffset));
            _pageManager.Unpin(pageId);
            ids.Add(pageId);
            pageId = nextId;
        }

        // Gap 3 closure (auto-commit CoW): write WAL record before epoch-retiring.
        // txId=0 — auto-commit is always committed; record flushed by next WAL flush.
        if (writeWalRecord && ids.Count > 0)
            _pageManager.Wal?.AppendFreeOverflowChain(0, [.. ids]);

        // Retire each page (epoch-queue for auto-commit, or track for transactional Commit).
        foreach (uint id in ids)
            retireAction(id);
    }

    /// <summary>
    /// Read the value at <paramref name="slotIndex"/> on <paramref name="frame"/>,
    /// dispatching to ReadOverflowChain when the slot has the SlotIsOverflow flag set.
    /// Safe to call from iterator methods (returns TValue, not a Span). (Phase 100b)
    /// </summary>
    private TValue ReadValueAtSlotOverflowAware(Frame frame, int slotIndex)
    {
        LeafNode<TKey, TValue>.GetRawValueAtSlot(
            frame, slotIndex, _nodeSerializer.KeySerializer,
            out ReadOnlySpan<byte> rawBytes, out byte slotFlags);
        if ((slotFlags & PageLayout.SlotIsOverflow) != 0)
        {
            int  totalLen    = BinaryPrimitives.ReadInt32BigEndian(rawBytes[..4]);
            uint firstPageId = BinaryPrimitives.ReadUInt32BigEndian(rawBytes.Slice(4, 4));
            byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(totalLen);
            try
            {
                _pageManager.ReadOverflowChain(firstPageId, totalLen, rented.AsSpan(0, totalLen));
                return _nodeSerializer.ValueSerializer.Deserialize(rented.AsSpan(0, totalLen));
            }
            finally { System.Buffers.ArrayPool<byte>.Shared.Return(rented); }
        }
        return _nodeSerializer.ValueSerializer.Deserialize(rawBytes);
    }

    /// <summary>
    /// After a CoW shadow leaf is created, update the in-chain left sibling's
    /// NextLeafPageId to point to <paramref name="shadowLeafPageId"/>.
    ///
    /// When a leaf is CoW-copied, the original's left neighbor still has
    /// NextLeafPageId pointing to the original (now retired).  This method
    /// repairs the forward leaf chain used by Scan() and Compact().
    ///
    /// The left sibling is found via the shadow parent's child pointer at
    /// childPos-1, which always points to the current live left sibling
    /// (shadow ancestors update only the childPos slot, leaving all others intact).
    ///
    /// No-op when pathLen == 0 (single-leaf tree) or childPos == 0
    /// (shadow leaf is the leftmost child in its parent).
    /// </summary>
    private void FixLeftSiblingNextPointer(
        Span<(uint pageId, int childPos)> path,
        int          pathLen,
        ReadOnlySpan<uint> shadowAncestorIds,
        uint               shadowLeafPageId)
    {
        if (pathLen == 0) return;

        // Walk UP the path from the leaf's immediate parent to find the first
        // ancestor level where the child position > 0.  At that level the
        // shadow ancestor's child at (childPos-1) is the root of the left
        // predecessor subtree — never modified by this CoW operation because
        // shadow ancestors only update the child pointer on the write path.
        for (int level = pathLen - 1; level >= 0; level--)
        {
            int childPos = path[level].childPos;
            if (childPos == 0) continue;   // shadow leaf is leftmost descendant at this level

            // Fetch shadow ancestor and read the left predecessor's subtree root.
            uint shadowAncId   = shadowAncestorIds[level];
            var  ancFrame      = _pageManager.FetchPage(shadowAncId);
            uint leftSubtreeId = _nodeSerializer.AsInternal(ancFrame).GetChildIdByPosition(childPos - 1);
            _pageManager.Unpin(shadowAncId);

            // Descend to the rightmost leaf of the left predecessor subtree.
            // levelsToDescend = depth-of-shadow-leaf minus depth-of-leftSubtreeId.
            int  levelsToDescend = pathLen - level - 1;
            uint currentId       = leftSubtreeId;
            for (int d = 0; d < levelsToDescend; d++)
            {
                var  nodeFrame    = _pageManager.FetchPage(currentId);
                int  rightmostPos = _nodeSerializer.AsInternal(nodeFrame).KeyCount;  // KeyCount children − 1 + 1
                uint nextId       = _nodeSerializer.AsInternal(nodeFrame).GetChildIdByPosition(rightmostPos);
                _pageManager.Unpin(currentId);
                currentId = nextId;
            }

            // currentId is the left sibling leaf — update its forward pointer.
            var leftFrame = _pageManager.FetchPage(currentId);
            var leftSibLeaf = _nodeSerializer.AsLeaf(leftFrame);
            leftSibLeaf.NextLeafPageId = shadowLeafPageId;
            _pageManager.MarkDirtyAndUnpin(currentId);
            return;
        }
        // All ancestors had childPos == 0 → shadow leaf IS the leftmost leaf; no left sibling.
    }

    // ── Transaction API ───────────────────────────────────────────────────────

    /// <summary>Test seam: expose the coordinator for barrier / quiescence testing.</summary>
    internal TransactionCoordinator Coordinator => _coordinator;

    /// <summary>
    /// Begin a new multi-operation transaction. Each call allocates a unique txId.
    /// Writes a WAL Begin record immediately. Requires a WAL to be attached.
    /// </summary>
    public ITransaction<TKey, TValue> BeginTransaction()
    {
        if (_pageManager.Wal == null)
            throw new InvalidOperationException("Transactions require a WAL writer.");
        // Reject new transactions while compaction Phase B (swap) is in progress.
        _coordinator.CheckCompactionBarrier();
        uint txId = _coordinator.Allocate();
        return new Transaction<TKey, TValue>(this, _pageManager.Wal, _pageManager, txId, _coordinator);
    }

    /// <summary>
    /// Open a point-in-time read-only snapshot of the tree.
    ///
    /// The snapshot root and first-leaf IDs are captured while the writer lock is held,
    /// and an epoch is registered before releasing the lock. This prevents any CoW
    /// writer from retiring snapshot-visible pages between state capture and epoch
    /// registration.
    ///
    /// The returned snapshot does NOT hold the checkpoint gate lock, so long-lived
    /// snapshots do not block WAL truncation or auto-checkpoints.
    /// </summary>
    public ISnapshot<TKey, TValue> BeginSnapshot()
    {
        _coordinator.EnterWriterLock();
        uint  rootId      = _metadata.RootPageId;
        uint  firstLeafId = _metadata.FirstLeafPageId;
        long  recordCount = (long)_metadata.TotalRecordCount;
        ulong epoch       = _coordinator.EnterReadEpoch();
        _coordinator.ExitWriterLock();
        return new Snapshot<TKey, TValue>(this, _coordinator, rootId, firstLeafId, recordCount, epoch);
    }

    /// <summary>
    /// Search for <paramref name="key"/> starting from <paramref name="snapshotRootId"/>.
    /// Used by <see cref="Snapshot{TKey,TValue}"/>. Pin-aware FreePage provides
    /// retirement safety; EnterReader/ExitReader removed (Phase 65).
    /// </summary>
    internal bool TryGetFromSnapshot(TKey key, uint snapshotRootId, out TValue value)
    {
        if (snapshotRootId == PageLayout.NullPageId)
        {
            value = default!;
            return false;
        }

        uint current = snapshotRootId;
        ReadLatchHandle currentLatch = _crab.CrabReadDown(current, default);
        try
        {
            while (true)
            {
                var frame = _pageManager.FetchPage(current);
                if (NodeSerializer<TKey, TValue>.IsLeaf(frame))
                {
                    bool found = LeafNode<TKey, TValue>.TryGet(
                        frame, key,
                        _nodeSerializer.KeySerializer,
                        _nodeSerializer.ValueSerializer,
                        out value);
                    _pageManager.Unpin(current);
                    currentLatch.Dispose();
                    return found;
                }
                uint childId = InternalNode<TKey>.FindChildId(
                    frame, key, _nodeSerializer.KeySerializer);
                _pageManager.Unpin(current);
                currentLatch = _crab.CrabReadDown(childId, currentLatch);
                current = childId;
            }
        }
        finally
        {
            currentLatch.Dispose();
        }
    }

    /// <summary>
    /// Enumerate [startKey, endKey] starting from <paramref name="snapshotRootId"/>
    /// and <paramref name="snapshotFirstLeafId"/>. Used by <see cref="Snapshot{TKey,TValue}"/>.
    /// Root and first-leaf IDs are fixed at snapshot-open time.
    /// </summary>
    internal IEnumerable<(TKey Key, TValue Value)> ScanFromSnapshot(
        TKey? startKey, TKey? endKey, uint snapshotRootId, uint snapshotFirstLeafId)
    {
        if (snapshotRootId == PageLayout.NullPageId)
            yield break;

        bool hasStartKey = startKey is TKey &&
            !EqualityComparer<TKey>.Default.Equals(startKey!, default!);
        bool hasEndKey = endKey is TKey &&
            !EqualityComparer<TKey>.Default.Equals(endKey!, default!);
        TKey startActual = hasStartKey ? startKey! : default!;
        TKey endActual   = hasEndKey   ? endKey!   : default!;

        uint firstLeafId = hasStartKey
            ? FindLeafForKey(startActual, snapshotRootId)
            : snapshotFirstLeafId;

        using var iter = TreeIterator<TKey, TValue>.Rent(
            _pageManager, _nodeSerializer, _nodeSerializer.KeySerializer,
            firstLeafId,
            hasStartKey, startActual,
            hasEndKey,   endActual);

        while (iter.MoveNext())
            yield return iter.Current;
    }

    /// <summary>
    /// Yield all entries in the closed interval [startKey, endKey] in descending key
    /// order, using the leaf <c>PrevLeafPageId</c> chain for backward traversal.
    /// Epoch protection is provided by the caller (snapshot root). (Phase 76)
    /// </summary>
    internal IEnumerable<(TKey Key, TValue Value)> ScanReverseFromSnapshot(
        TKey? endKey, TKey? startKey, uint snapshotRootId)
    {
        if (snapshotRootId == PageLayout.NullPageId)
            yield break;

        bool hasEndKey   = endKey   is TKey && !EqualityComparer<TKey>.Default.Equals(endKey!,   default!);
        bool hasStartKey = startKey is TKey && !EqualityComparer<TKey>.Default.Equals(startKey!, default!);
        TKey endActual   = hasEndKey   ? endKey!   : default!;
        TKey startActual = hasStartKey ? startKey! : default!;

        // Find the starting leaf — the rightmost leaf whose range covers endKey,
        // or the absolute rightmost leaf if no endKey is specified.
        uint currentLeafId = hasEndKey
            ? FindLeafForKey(endActual, snapshotRootId)
            : FindRightmostLeaf(snapshotRootId);

        bool firstLeaf = true;
        while (currentLeafId != PageLayout.NullPageId)
        {
            var frame = _pageManager.FetchPage(currentLeafId);
            int count = LeafNode<TKey, TValue>.GetSlotCount(frame);

            // First leaf: position at the last slot ≤ endKey (or count-1 if no endKey).
            // Subsequent leaves (to the left via PrevLeafPageId): always start from last slot.
            int slotIndex = (firstLeaf && hasEndKey)
                ? LeafNode<TKey, TValue>.FindLastSlotLe(frame, endActual, _nodeSerializer.KeySerializer)
                : count - 1;
            firstLeaf = false;

            while (slotIndex >= 0)
            {
                TKey key = LeafNode<TKey, TValue>.GetKey(
                    frame, slotIndex, _nodeSerializer.KeySerializer);

                if (hasStartKey && _nodeSerializer.KeySerializer.Compare(key, startActual) < 0)
                {
                    _pageManager.Unpin(currentLeafId);
                    yield break;
                }

                TValue value = ReadValueAtSlotOverflowAware(frame, slotIndex);
                yield return (key, value);
                slotIndex--;
            }

            uint prevId = LeafNode<TKey, TValue>.GetPrevLeafPageId(frame);
            _pageManager.Unpin(currentLeafId);
            currentLeafId = prevId;
        }
    }

    /// <summary>
    /// Reverse scan from within a transaction's shadow tree.
    /// Delegates to <see cref="ScanReverseFromSnapshot"/> using the transaction's
    /// current shadow root. (Phase 76)
    /// </summary>
    internal IEnumerable<(TKey Key, TValue Value)> ScanReverseInTransaction(
        TKey? endKey, TKey? startKey, Transaction<TKey, TValue> tx)
        => ScanReverseFromSnapshot(endKey, startKey, tx.TxRootId);

    // ── TryGetFirst / TryGetLast ──────────────────────────────────────────────

    /// <summary>
    /// Return the smallest key-value pair visible from <paramref name="snapshotFirstLeafId"/>.
    /// Epoch protection is provided by the caller (snapshot or transaction epoch).
    /// </summary>
    internal bool TryGetFirstFromSnapshot(
        uint snapshotFirstLeafId, out TKey key, out TValue value)
    {
        if (snapshotFirstLeafId == PageLayout.NullPageId)
        {
            key = default!; value = default!;
            return false;
        }
        var frame = _pageManager.FetchPage(snapshotFirstLeafId);
        int count = LeafNode<TKey, TValue>.GetSlotCount(frame);
        if (count == 0)
        {
            _pageManager.Unpin(snapshotFirstLeafId);
            key = default!; value = default!;
            return false;
        }
        key   = LeafNode<TKey, TValue>.GetKey(frame, 0, _nodeSerializer.KeySerializer);
        value = ReadValueAtSlotOverflowAware(frame, 0);
        _pageManager.Unpin(snapshotFirstLeafId);
        return true;
    }

    /// <summary>
    /// Return the largest key-value pair reachable from <paramref name="snapshotRootId"/>.
    /// Epoch protection is provided by the caller (snapshot or transaction epoch).
    /// </summary>
    internal bool TryGetLastFromSnapshot(
        uint snapshotRootId, out TKey key, out TValue value)
    {
        if (snapshotRootId == PageLayout.NullPageId)
        {
            key = default!; value = default!;
            return false;
        }
        uint rightmostLeafId = FindRightmostLeaf(snapshotRootId);
        var frame = _pageManager.FetchPage(rightmostLeafId);
        int count = LeafNode<TKey, TValue>.GetSlotCount(frame);
        if (count == 0)
        {
            _pageManager.Unpin(rightmostLeafId);
            key = default!; value = default!;
            return false;
        }
        key   = LeafNode<TKey, TValue>.GetKey(
            frame, count - 1, _nodeSerializer.KeySerializer);
        value = ReadValueAtSlotOverflowAware(frame, count - 1);
        _pageManager.Unpin(rightmostLeafId);
        return true;
    }

    /// <summary>TryGetFirst within a transaction's shadow tree.</summary>
    internal bool TryGetFirstInTransaction(
        Transaction<TKey, TValue> tx, out TKey key, out TValue value)
        => TryGetFirstFromSnapshot(tx.TxFirstLeafId(_metadata.FirstLeafPageId), out key, out value);

    /// <summary>TryGetLast within a transaction's shadow tree.</summary>
    internal bool TryGetLastInTransaction(
        Transaction<TKey, TValue> tx, out TKey key, out TValue value)
        => TryGetLastFromSnapshot(tx.TxRootId, out key, out value);

    /// <inheritdoc />
    public bool TryGetFirst(out TKey key, out TValue value)
    {
        using var snap = BeginSnapshot();
        return snap.TryGetFirst(out key, out value);
    }

    /// <inheritdoc />
    public bool TryGetLast(out TKey key, out TValue value)
    {
        using var snap = BeginSnapshot();
        return snap.TryGetLast(out key, out value);
    }

    // ── TryGetNext / TryGetPrev ──────────────────────────────────────────────

    /// <summary>
    /// Return the smallest key strictly greater than <paramref name="key"/>
    /// that is visible from <paramref name="snapshotRootId"/>.
    /// Epoch protection is provided by the caller.
    /// </summary>
    internal bool TryGetNextFromSnapshot(
        TKey key, uint snapshotRootId, out TKey nextKey, out TValue value)
    {
        if (snapshotRootId == PageLayout.NullPageId)
        {
            nextKey = default!; value = default!;
            return false;
        }
        var ks = _nodeSerializer.KeySerializer;
        uint currentLeafId = FindLeafForKey(key, snapshotRootId);
        var frame = _pageManager.FetchPage(currentLeafId);
        int count = LeafNode<TKey, TValue>.GetSlotCount(frame);
        int slot  = LeafNode<TKey, TValue>.FindFirstSlotGe(frame, key, ks);
        // Skip exact match — we want strictly greater.
        if (slot < count && ks.Compare(LeafNode<TKey, TValue>.GetKey(frame, slot, ks), key) == 0)
            slot++;
        if (slot < count)
        {
            nextKey = LeafNode<TKey, TValue>.GetKey(frame, slot, ks);
            value   = ReadValueAtSlotOverflowAware(frame, slot);
            _pageManager.Unpin(currentLeafId);
            return true;
        }
        // All keys on this leaf ≤ key — advance to next leaf.
        uint nextLeafId = LeafNode<TKey, TValue>.GetNextLeafPageId(frame);
        _pageManager.Unpin(currentLeafId);
        if (nextLeafId == PageLayout.NullPageId)
        {
            nextKey = default!; value = default!;
            return false;
        }
        var nextFrame = _pageManager.FetchPage(nextLeafId);
        nextKey = LeafNode<TKey, TValue>.GetKey(nextFrame, 0, ks);
        value   = ReadValueAtSlotOverflowAware(nextFrame, 0);
        _pageManager.Unpin(nextLeafId);
        return true;
    }

    /// <summary>
    /// Return the largest key strictly less than <paramref name="key"/>
    /// that is visible from <paramref name="snapshotRootId"/>.
    /// Epoch protection is provided by the caller.
    /// </summary>
    internal bool TryGetPrevFromSnapshot(
        TKey key, uint snapshotRootId, out TKey prevKey, out TValue value)
    {
        if (snapshotRootId == PageLayout.NullPageId)
        {
            prevKey = default!; value = default!;
            return false;
        }
        var ks = _nodeSerializer.KeySerializer;
        uint currentLeafId = FindLeafForKey(key, snapshotRootId);
        var frame = _pageManager.FetchPage(currentLeafId);
        int slot  = LeafNode<TKey, TValue>.FindLastSlotLe(frame, key, ks);
        // Skip exact match — we want strictly less.
        if (slot >= 0 && ks.Compare(LeafNode<TKey, TValue>.GetKey(frame, slot, ks), key) == 0)
            slot--;
        if (slot >= 0)
        {
            prevKey = LeafNode<TKey, TValue>.GetKey(frame, slot, ks);
            value   = ReadValueAtSlotOverflowAware(frame, slot);
            _pageManager.Unpin(currentLeafId);
            return true;
        }
        // All keys on this leaf ≥ key — retreat to previous leaf.
        uint prevLeafId = LeafNode<TKey, TValue>.GetPrevLeafPageId(frame);
        _pageManager.Unpin(currentLeafId);
        if (prevLeafId == PageLayout.NullPageId)
        {
            prevKey = default!; value = default!;
            return false;
        }
        var prevFrame = _pageManager.FetchPage(prevLeafId);
        int prevCount = LeafNode<TKey, TValue>.GetSlotCount(prevFrame);
        prevKey = LeafNode<TKey, TValue>.GetKey(prevFrame, prevCount - 1, ks);
        value   = ReadValueAtSlotOverflowAware(prevFrame, prevCount - 1);
        _pageManager.Unpin(prevLeafId);
        return true;
    }

    /// <summary>TryGetNext within a transaction's shadow tree.</summary>
    internal bool TryGetNextInTransaction(
        TKey key, Transaction<TKey, TValue> tx, out TKey nextKey, out TValue value)
        => TryGetNextFromSnapshot(key, tx.TxRootId, out nextKey, out value);

    /// <summary>TryGetPrev within a transaction's shadow tree.</summary>
    internal bool TryGetPrevInTransaction(
        TKey key, Transaction<TKey, TValue> tx, out TKey prevKey, out TValue value)
        => TryGetPrevFromSnapshot(key, tx.TxRootId, out prevKey, out value);

    /// <inheritdoc />
    public bool TryGetNext(TKey key, out TKey nextKey, out TValue value)
    {
        using var snap = BeginSnapshot();
        return snap.TryGetNext(key, out nextKey, out value);
    }

    /// <inheritdoc />
    public bool TryGetPrev(TKey key, out TKey prevKey, out TValue value)
    {
        using var snap = BeginSnapshot();
        return snap.TryGetPrev(key, out prevKey, out value);
    }

    /// <summary>
    /// Count keys in the closed interval [startKey, endKey] from a snapshot root.
    /// O(n_range), constant memory. Returns 0 for an empty range or empty tree.
    /// </summary>
    internal long CountRangeFromSnapshot(TKey startKey, TKey endKey, uint snapshotRootId,
        Action<uint>? onLeafVisit = null)
    {
        if (snapshotRootId == PageLayout.NullPageId) return 0L;
        var ks = _nodeSerializer.KeySerializer;
        if (ks.Compare(startKey, endKey) > 0) return 0L;

        long count  = 0;
        bool first  = true;
        uint leafId = FindLeafForKey(startKey, snapshotRootId);

        while (leafId != PageLayout.NullPageId)
        {
            var frame     = _pageManager.FetchPage(leafId);
            onLeafVisit?.Invoke(leafId);   // SSI Phase 88: record leaf visited
            int slotCount = LeafNode<TKey, TValue>.GetSlotCount(frame);
            int startSlot = first
                ? LeafNode<TKey, TValue>.FindFirstSlotGe(frame, startKey, ks)
                : 0;
            first = false;

            bool done = false;
            for (int i = startSlot; i < slotCount; i++)
            {
                TKey key = LeafNode<TKey, TValue>.GetKey(frame, i, ks);
                if (ks.Compare(key, endKey) > 0) { done = true; break; }
                count++;
            }

            uint nextLeafId = LeafNode<TKey, TValue>.GetNextLeafPageId(frame);
            _pageManager.Unpin(leafId);

            if (done) break;
            leafId = nextLeafId;
        }

        return count;
    }

    /// <summary>CountRange within a transaction's shadow tree.</summary>
    internal long CountRangeInTransaction(TKey startKey, TKey endKey, Transaction<TKey, TValue> tx)
        => CountRangeFromSnapshot(startKey, endKey, tx.TxRootId, tx.TrackLeafRead);

    /// <inheritdoc />
    public long CountRange(TKey startKey, TKey endKey)
    {
        using var snap = BeginSnapshot();
        return snap.CountRange(startKey, endKey);
    }

    /// <summary>
    /// Reloads all in-memory metadata fields from the meta page in the buffer pool.
    /// Called by Transaction.Dispose() after before-image restore, to synchronise
    /// _metadata's cached properties with the restored frame.Data bytes.
    /// </summary>
    internal void ReloadMetadata() => _metadata.Load();

    /// <summary>Current live root page ID — snapshot captured by Transaction constructor.</summary>
    internal uint CurrentRootId => _metadata.RootPageId;

    /// <summary>Current live tree height — snapshot captured by Transaction constructor.</summary>
    internal uint CurrentTreeHeight => _metadata.TreeHeight;

    /// <summary>Current live record count — snapshot captured by Transaction constructor.</summary>
    internal ulong CurrentRecordCount => _metadata.TotalRecordCount;

    /// <summary>Public live record count for BPlusTree.Count.</summary>
    public long GetRecordCount() => (long)_metadata.TotalRecordCount;

    /// <summary>
    /// Restore record count to a snapshot value on transaction rollback.
    /// Undoes all IncrementRecordCount/DecrementRecordCount calls made during the tx.
    /// </summary>
    internal void RestoreSnapshotRecordCount(ulong count) => _metadata.SetTotalRecordCount(count);

    /// <summary>
    /// Atomically install the committed CoW shadow root and optional first-leaf pointer.
    /// Called by Transaction.Commit() while the writer lock is held.
    /// </summary>
    internal void ApplyCoWTxCommit(
        uint newRootId, uint newTreeHeight, bool firstLeafChanged, uint newFirstLeafId)
    {
        _metadata.SetRoot(newRootId, newTreeHeight);
        if (firstLeafChanged) _metadata.SetFirstLeaf(newFirstLeafId);
        _metadata.Flush();
    }

    /// <summary>
    /// Transactional insert using the CoW shadow write path.
    /// Traverses from <see cref="Transaction{TKey,TValue}.TxRootId"/> (the current working
    /// shadow root), allocates shadow copies for all write-path pages, and accumulates the
    /// new shadow root in the transaction without touching the live <see cref="_metadata"/>.
    /// The single-writer mutex is held per-operation (acquired on entry, released on return).
    /// </summary>
    internal void InsertInTransaction(TKey key, TValue value, Transaction<TKey, TValue> tx)
    {
        // Writer lock is held for the full transaction lifetime (acquired in constructor).
        // No per-operation lock needed here.

        // Guard: same entry-size limits as auto-commit Insert.
        int actualKs = _nodeSerializer.KeySerializer.MeasureSize(key);
        int actualVs = _nodeSerializer.ValueSerializer.MeasureSize(value);
        int maxKey   = PageLayout.MaxKeySize(_pageManager.PageSize);
        int maxEntry = PageLayout.MaxEntrySize(_pageManager.PageSize);

        if (actualKs > maxKey)
            throw new BPlusTreeEntryTooLargeException(
                $"Key ({actualKs} B) exceeds MaxKeySize ({maxKey} B) for pageSize={_pageManager.PageSize}. " +
                "Use a shorter key or a larger page size.",
                actualSize: actualKs, maxSize: maxKey);

        bool needsOverflow = actualKs + actualVs > maxEntry;

            // Empty shadow tree: allocate first leaf (no prior version to shadow).
            if (tx.TxRootId == PageLayout.NullPageId)
            {
                var newFrame = _pageManager.AllocatePage(PageType.Leaf);
                var newLeaf  = _nodeSerializer.AsLeaf(newFrame);
                newLeaf.Initialize();
                if (needsOverflow)
                {
                    byte[] vBytes = new byte[actualVs];
                    _nodeSerializer.ValueSerializer.Serialize(value, vBytes);
                    _pageManager.AllocateOverflowChain(vBytes, out uint firstPid, out uint[] chainIds,
                                                       tx.TransactionId);
                    tx.TrackAllocatedOverflowChain(chainIds);
                    newLeaf.WriteOverflowPointer(key, firstPid, actualVs);
                }
                else
                {
                    newLeaf.TryInsert(key, value);
                }
                tx.TrackAllocatedPage(newFrame.PageId);
                _pageManager.MarkDirtyAndUnpin(newFrame.PageId);
                tx.UpdateTxRoot(newFrame.PageId, height: 1);
                if (_metadata.RootPageId == PageLayout.NullPageId)
                    tx.TrackFirstLeafChange(newFrame.PageId);
                _metadata.IncrementRecordCount();
                return;
            }

            // Phase 1: read-traverse from tx.TxRootId, collect path + leafId.
            PathBuffer pathBuf = default;
            Span<(uint pageId, int childPos)> path = pathBuf;
            int  pathLen    = 0;
            uint leafId     = 0;
            bool keyExists  = false;
            bool isLeafFull = false;

            bool oldIsOverflow          = false;
            uint oldOverflowFirstPageId = 0;

            uint currentId = tx.TxRootId;
            while (true)
            {
                using var readLatch = _latches.AcquireReadLatch(currentId);
                var frame = _pageManager.FetchPage(currentId);
                if (NodeSerializer<TKey, TValue>.IsLeaf(frame))
                {
                    bool rawFound = LeafNode<TKey, TValue>.TryGetRawValue(
                        frame, key, _nodeSerializer.KeySerializer,
                        out ReadOnlySpan<byte> oldRaw, out byte oldFlags);
                    keyExists = rawFound;
                    if (rawFound && (oldFlags & PageLayout.SlotIsOverflow) != 0)
                    {
                        oldIsOverflow          = true;
                        oldOverflowFirstPageId = BinaryPrimitives.ReadUInt32BigEndian(oldRaw.Slice(4, 4));
                    }
                    if (!keyExists)
                    {
                        var leaf = _nodeSerializer.AsLeaf(frame);
                        isLeafFull = needsOverflow
                            ? !leaf.HasSpaceFor(actualKs, PageLayout.OverflowPointerSize)
                            : !leaf.HasSpaceFor(actualKs, actualVs);
                    }
                    // Capture before-image for page-level write-lock conflict detection.
                    // May throw TransactionConflictException — propagate naturally (no held latches).
                    try { tx.CaptureBeforeImage(currentId, frame.Data); }
                    catch { _pageManager.Unpin(currentId); throw; }
                    leafId = currentId;
                    _pageManager.Unpin(currentId);
                    break;
                }
                else
                {
                    var  node     = _nodeSerializer.AsInternal(frame);
                    int  childPos = node.FindChildPosition(key);
                    uint childId  = node.GetChildIdByPosition(childPos);
                    path[pathLen++] = (currentId, childPos);
                    _pageManager.Unpin(currentId);
                    currentId = childId;
                }
            }

            // Phase 2: CoW path allocation (stack-allocated buffers — zero heap alloc).
            OldPageIdBuffer      oldIdBuf      = default;
            ShadowAncestorBuffer shadowAncBuf  = default;
            Span<uint>           oldPageIds        = oldIdBuf;
            Span<uint>           shadowAncestorIds = shadowAncBuf;
            var owned = tx.OwnedShadowPages;
            var (shadowLeaf, shadowRootId) =
                CopyWritePathAndAllocShadows(path, pathLen, leafId, oldPageIds, shadowAncestorIds, tx.TransactionId, owned);

            // Repair leaf chain: original left sibling's NextLeafPageId → shadow leaf.
            FixLeftSiblingNextPointer(path, pathLen, shadowAncestorIds, shadowLeaf.PageId);

            // Use tx.TxFirstLeafId so successive shadows of the first leaf all propagate.
            bool wasFirstLeaf = leafId == tx.TxFirstLeafId(_metadata.FirstLeafPageId);

            // Track shadow pages (freed on rollback) and obsolete pages (retired at commit).
            // Skip pages already owned by this transaction — they were tracked on first copy.
            if (!owned.Contains(shadowLeaf.PageId))
                tx.TrackAllocatedPage(shadowLeaf.PageId);
            for (int i = 0; i < pathLen; i++)
                if (!owned.Contains(shadowAncestorIds[i]))
                    tx.TrackAllocatedPage(shadowAncestorIds[i]);
            // For each old-path page: only retire if it is distinct from the shadow
            // (i.e. a genuine old version was replaced, not an in-place reuse).
            for (int i = 0; i < pathLen; i++)
                if (oldPageIds[i] != shadowAncestorIds[i])
                    tx.TrackObsoletePage(oldPageIds[i]);
            if (oldPageIds[pathLen] != shadowLeaf.PageId)
                tx.TrackObsoletePage(oldPageIds[pathLen]);
            if (wasFirstLeaf)
                tx.TrackFirstLeafChange(shadowLeaf.PageId);

            if (keyExists || !isLeafFull)
            {
                // Key exists (overwrite) or leaf has room — direct shadow-leaf modification.
                var shadowLeafNode = _nodeSerializer.AsLeaf(shadowLeaf);
                if (needsOverflow)
                {
                    byte[] vBytes = new byte[actualVs];
                    _nodeSerializer.ValueSerializer.Serialize(value, vBytes);
                    _pageManager.AllocateOverflowChain(vBytes, out uint firstPid, out uint[] chainIds,
                                                       tx.TransactionId);
                    tx.TrackAllocatedOverflowChain(chainIds);
                    shadowLeafNode.WriteOverflowPointer(key, firstPid, actualVs);
                }
                else
                {
                    shadowLeafNode.TryInsert(key, value);
                }
                _pageManager.MarkDirtyAndUnpin(shadowLeaf.PageId);
                tx.UpdateTxRoot(shadowRootId, tx.TxTreeHeight);
                if (!keyExists) _metadata.IncrementRecordCount();
                if (oldIsOverflow) RetireOverflowChain(oldOverflowFirstPageId, tx.TrackObsoleteOverflowPage);
                return;
            }

            // Shadow leaf is full (new key) — split in shadow context.
            _pageManager.Unpin(shadowLeaf.PageId); // Splitter will FetchPage it

            PathBuffer shadowPathBuf = default;
            Span<(uint pageId, int childPos)> shadowPath = shadowPathBuf;
            for (int i = 0; i < pathLen; i++)
                shadowPath[i] = (shadowAncestorIds[i], path[i].childPos);

            {
                var splitter = new Splitter<TKey, TValue>(_pageManager, _nodeSerializer, tx);
                var (newShadowRoot, newShadowHeight) =
                    splitter.SplitLeaf(shadowLeaf.PageId, shadowPath, pathLen, tx.TxTreeHeight);

                uint finalShadowRoot   = newShadowRoot   != 0 ? newShadowRoot   : shadowRootId;
                uint finalShadowHeight = newShadowHeight != 0 ? newShadowHeight : tx.TxTreeHeight;
                tx.UpdateTxRoot(finalShadowRoot, finalShadowHeight);

                // Re-traverse shadow tree from finalShadowRoot to insert the key.
                uint insertId = finalShadowRoot;
                while (true)
                {
                    var insertFrame = _pageManager.FetchPage(insertId);
                    if (NodeSerializer<TKey, TValue>.IsLeaf(insertFrame))
                    {
                        var insertLeaf = _nodeSerializer.AsLeaf(insertFrame);
                        if (needsOverflow)
                        {
                            byte[] vBytes = new byte[actualVs];
                            _nodeSerializer.ValueSerializer.Serialize(value, vBytes);
                            _pageManager.AllocateOverflowChain(vBytes, out uint firstPid,
                                                               out uint[] chainIds, tx.TransactionId);
                            tx.TrackAllocatedOverflowChain(chainIds);
                            insertLeaf.WriteOverflowPointer(key, firstPid, actualVs);
                        }
                        else
                        {
                            insertLeaf.TryInsert(key, value);
                        }
                        _pageManager.MarkDirtyAndUnpin(insertId);
                        break;
                    }
                    else
                    {
                        var  insertNode = _nodeSerializer.AsInternal(insertFrame);
                        uint childId    = insertNode.FindChildId(key);
                        _pageManager.Unpin(insertId);
                        insertId = childId;
                    }
                }

                _metadata.IncrementRecordCount();
            }
    }

    /// <summary>
    /// Transactional delete using the CoW shadow write path.
    /// Traverses from <see cref="Transaction{TKey,TValue}.TxRootId"/>, allocates shadow
    /// copies, removes the key from the shadow leaf, and handles underflow via the Merger
    /// on the shadow path. Accumulates shadow/obsolete pages in the transaction.
    /// The single-writer mutex is held per-operation.
    /// </summary>
    internal bool TryDeleteInTransaction(TKey key, Transaction<TKey, TValue> tx)
    {
        // Writer lock is held for the full transaction lifetime (acquired in constructor).
        // No per-operation lock needed here.
        if (tx.TxRootId == PageLayout.NullPageId) return false;

            // Phase 1: read-traverse from tx.TxRootId, collect path + leafId, check key.
            PathBuffer pathBuf = default;
            Span<(uint pageId, int childPos)> path = pathBuf;
            int  pathLen   = 0;
            uint leafId    = 0;
            bool keyExists = false;

            bool oldIsOverflow          = false;
            uint oldOverflowFirstPageId = 0;

            uint currentId = tx.TxRootId;
            while (true)
            {
                using var readLatch = _latches.AcquireReadLatch(currentId);
                var frame = _pageManager.FetchPage(currentId);
                if (NodeSerializer<TKey, TValue>.IsLeaf(frame))
                {
                    // Pre-check: avoid spurious page-lock acquisition on not-found deletes.
                    // Use TryGetRawValue — avoids overflow pointer deserialisation crash.
                    if (!LeafNode<TKey, TValue>.TryGetRawValue(
                            frame, key, _nodeSerializer.KeySerializer,
                            out ReadOnlySpan<byte> oldRaw, out byte oldFlags))
                    {
                        _pageManager.Unpin(currentId);
                        return false;
                    }
                    if ((oldFlags & PageLayout.SlotIsOverflow) != 0)
                    {
                        oldIsOverflow          = true;
                        oldOverflowFirstPageId = BinaryPrimitives.ReadUInt32BigEndian(oldRaw.Slice(4, 4));
                    }
                    keyExists = true;
                    // Capture before-image for page-level write-lock conflict detection.
                    try { tx.CaptureBeforeImage(currentId, frame.Data); }
                    catch { _pageManager.Unpin(currentId); throw; }
                    leafId = currentId;
                    _pageManager.Unpin(currentId);
                    break;
                }
                else
                {
                    var  node     = _nodeSerializer.AsInternal(frame);
                    int  childPos = node.FindChildPosition(key);
                    uint childId  = node.GetChildIdByPosition(childPos);
                    path[pathLen++] = (currentId, childPos);
                    _pageManager.Unpin(currentId);
                    currentId = childId;
                }
            }

            if (!keyExists) return false;

            // Phase 2: CoW path allocation.
            OldPageIdBuffer      oldIdBuf      = default;
            ShadowAncestorBuffer shadowAncBuf  = default;
            Span<uint>           oldPageIds        = oldIdBuf;
            Span<uint>           shadowAncestorIds = shadowAncBuf;
            var owned = tx.OwnedShadowPages;
            var (shadowLeaf, shadowRootId) =
                CopyWritePathAndAllocShadows(path, pathLen, leafId, oldPageIds, shadowAncestorIds, tx.TransactionId, owned);

            // Repair leaf chain.
            FixLeftSiblingNextPointer(path, pathLen, shadowAncestorIds, shadowLeaf.PageId);

            // Phase 3: Remove key from shadow leaf; check underflow before unpinning.
            var shadowLeafNode = _nodeSerializer.AsLeaf(shadowLeaf);
            shadowLeafNode.Remove(key);
            bool underflows = pathLen > 0 && shadowLeafNode.Count < _merger.LeafThreshold();
            _pageManager.MarkDirtyAndUnpin(shadowLeaf.PageId);

            bool wasFirstLeaf = leafId == tx.TxFirstLeafId(_metadata.FirstLeafPageId);

            // Track shadow and obsolete pages (skip pages already owned — avoid double-free).
            if (!owned.Contains(shadowLeaf.PageId))
                tx.TrackAllocatedPage(shadowLeaf.PageId);
            for (int i = 0; i < pathLen; i++)
                if (!owned.Contains(shadowAncestorIds[i]))
                    tx.TrackAllocatedPage(shadowAncestorIds[i]);
            for (int i = 0; i < pathLen; i++)
                if (oldPageIds[i] != shadowAncestorIds[i])
                    tx.TrackObsoletePage(oldPageIds[i]);
            if (oldPageIds[pathLen] != shadowLeaf.PageId)
                tx.TrackObsoletePage(oldPageIds[pathLen]);
            if (wasFirstLeaf)
                tx.TrackFirstLeafChange(shadowLeaf.PageId);

            // Retire old overflow chain — epoch-gated so in-flight readers can finish.
            if (oldIsOverflow)
                RetireOverflowChain(oldOverflowFirstPageId, tx.TrackObsoleteOverflowPage);

            _metadata.DecrementRecordCount();

            if (!underflows)
            {
                tx.UpdateTxRoot(shadowRootId, tx.TxTreeHeight);
                return true;
            }

            // Phase 4: Underflow — rebalance on shadow path.
            PathBuffer shadowPathBuf = default;
            Span<(uint pageId, int childPos)> shadowPath = shadowPathBuf;
            for (int i = 0; i < pathLen; i++)
                shadowPath[i] = (shadowAncestorIds[i], path[i].childPos);

            // Temporarily set _metadata to shadow root for Merger (CollapseRoot reads it).
            uint savedRoot   = _metadata.RootPageId;
            uint savedHeight = _metadata.TreeHeight;
            _metadata.SetRoot(shadowRootId, tx.TxTreeHeight);
            try
            {
                new Merger<TKey, TValue>(_pageManager, _nodeSerializer, _metadata, tx)
                    .RebalanceLeaf(shadowLeaf.PageId, shadowPath, pathLen);

                // Capture final shadow root (may have changed if CollapseRoot was called).
                uint finalShadowRoot   = _metadata.RootPageId;
                uint finalShadowHeight = _metadata.TreeHeight;
                tx.UpdateTxRoot(finalShadowRoot, finalShadowHeight);

                // Restore metadata isolation.
                _metadata.SetRoot(savedRoot, savedHeight);
                _metadata.Flush(); // restore meta page frame.Data
            }
            catch
            {
                // Ensure in-memory metadata is restored even if Merger throws
                // (e.g. TransactionConflictException on sibling CaptureBeforeImage).
                _metadata.SetRoot(savedRoot, savedHeight);
                throw;
            }

            return true;
    }

    /// <summary>
    /// Transactional range delete. Collects all keys in [startKey, endKey] BEFORE
    /// any structural modification (avoids leaf-chain invalidation from merges during
    /// deletion), then deletes each via the existing TryDeleteInTransaction path.
    /// Returns the number of keys deleted.
    /// </summary>
    internal int DeleteRangeInTransaction(
        TKey startKey, TKey endKey, Transaction<TKey, TValue> tx)
    {
        // Collect all keys in range BEFORE any structural modification.
        // Avoids leaf-chain invalidation from merges during deletion.
        var keys = Scan(startKey, endKey).Select(kv => kv.Key).ToList();
        int deleted = 0;
        foreach (var key in keys)
        {
            if (TryDeleteInTransaction(key, tx))
                deleted++;
        }
        return deleted;
    }

    /// <summary>
    /// Atomically delete all keys in the closed interval [startKey, endKey].
    /// Creates an internal transaction, runs DeleteRangeInTransaction, commits, and disposes.
    /// Returns the number of keys deleted.
    /// </summary>
    public int DeleteRange(TKey startKey, TKey endKey)
    {
        if (_pageManager.Wal == null)
            throw new InvalidOperationException("DeleteRange requires a WAL writer.");
        uint txId = _coordinator.Allocate();
        var tx = new Transaction<TKey, TValue>(this, _pageManager.Wal, _pageManager, txId, _coordinator);
        try
        {
            int deleted = DeleteRangeInTransaction(startKey, endKey, tx);
            tx.Commit();
            return deleted;
        }
        finally
        {
            tx.Dispose();
        }
    }

    /// <summary>
    /// Transactional update using the CoW shadow write path.
    /// Traverses from <see cref="Transaction{TKey,TValue}.TxRootId"/>, allocates shadow
    /// copies, and overwrites the value in the shadow leaf. No metadata changes (updates
    /// do not affect record count or leaf chain). The single-writer mutex is held per-operation.
    /// Returns false if the key is not found.
    /// </summary>
    internal bool TryUpdateInTransaction(TKey key, TValue newValue, Transaction<TKey, TValue> tx)
    {
        // Writer lock is held for the full transaction lifetime (acquired in constructor).
        // No per-operation lock needed here.
        if (tx.TxRootId == PageLayout.NullPageId) return false;

        // Phase 1: read-traverse from tx.TxRootId, collect path + leafId.
        PathBuffer pathBuf = default;
        Span<(uint pageId, int childPos)> path = pathBuf;
        int  pathLen  = 0;
        uint leafId   = 0;

        bool oldIsOverflow          = false;
        uint oldOverflowFirstPageId = 0;

        uint currentId = tx.TxRootId;
        while (true)
        {
            using var readLatch = _latches.AcquireReadLatch(currentId);
            var frame = _pageManager.FetchPage(currentId);
            if (NodeSerializer<TKey, TValue>.IsLeaf(frame))
            {
                if (!LeafNode<TKey, TValue>.TryGetRawValue(
                        frame, key, _nodeSerializer.KeySerializer,
                        out ReadOnlySpan<byte> oldRaw, out byte oldFlags))
                {
                    _pageManager.Unpin(currentId);
                    return false;
                }
                if ((oldFlags & PageLayout.SlotIsOverflow) != 0)
                {
                    oldIsOverflow          = true;
                    oldOverflowFirstPageId = BinaryPrimitives.ReadUInt32BigEndian(oldRaw.Slice(4, 4));
                }
                // Capture before-image for page-level write-lock conflict detection.
                try { tx.CaptureBeforeImage(currentId, frame.Data); }
                catch { _pageManager.Unpin(currentId); throw; }
                leafId = currentId;
                _pageManager.Unpin(currentId);
                break;
            }
            else
            {
                var  node     = _nodeSerializer.AsInternal(frame);
                int  childPos = node.FindChildPosition(key);
                uint childId  = node.GetChildIdByPosition(childPos);
                path[pathLen++] = (currentId, childPos);
                _pageManager.Unpin(currentId);
                currentId = childId;
            }
        }

        // Compute whether new value needs overflow storage.
        int  newVs         = _nodeSerializer.ValueSerializer.MeasureSize(newValue);
        int  newKs         = _nodeSerializer.KeySerializer.MeasureSize(key);
        int  maxEntryU     = PageLayout.MaxEntrySize(_pageManager.PageSize);
        bool newIsOverflow = newKs + newVs > maxEntryU;

        // Phase 2: CoW path allocation (stack-allocated buffers — zero heap alloc).
        OldPageIdBuffer      oldIdBuf  = default;
        ShadowAncestorBuffer ancBuf    = default;
        Span<uint>           oldPageIds        = oldIdBuf;
        Span<uint>           shadowAncestorIds = ancBuf;
        var owned = tx.OwnedShadowPages;
        var (shadowLeaf, shadowRootId) =
            CopyWritePathAndAllocShadows(path, pathLen, leafId, oldPageIds, shadowAncestorIds, tx.TransactionId, owned);

        // Phase 3: Overwrite value in shadow leaf.
        var shadowLeafNode = _nodeSerializer.AsLeaf(shadowLeaf);
        if (newIsOverflow)
        {
            byte[] vBytes = new byte[newVs];
            _nodeSerializer.ValueSerializer.Serialize(newValue, vBytes);
            _pageManager.AllocateOverflowChain(vBytes, out uint firstPid, out uint[] chainIds,
                                               tx.TransactionId);
            tx.TrackAllocatedOverflowChain(chainIds);
            shadowLeafNode.WriteOverflowPointer(key, firstPid, newVs);
        }
        else
        {
            shadowLeafNode.TryInsert(key, newValue);   // handles overflow→inline via flags fix
        }
        _pageManager.MarkDirtyAndUnpin(shadowLeaf.PageId);

        // Phase 4: Update transaction state (skip pages already owned — avoid double-free).
        tx.UpdateTxRoot(shadowRootId, tx.TxTreeHeight);
        if (!owned.Contains(shadowLeaf.PageId))
            tx.TrackAllocatedPage(shadowLeaf.PageId);
        for (int i = 0; i < pathLen; i++)
            if (!owned.Contains(shadowAncestorIds[i]))
                tx.TrackAllocatedPage(shadowAncestorIds[i]);
        for (int i = 0; i < pathLen; i++)
            if (oldPageIds[i] != shadowAncestorIds[i])
                tx.TrackObsoletePage(oldPageIds[i]);
        if (oldPageIds[pathLen] != shadowLeaf.PageId)
            tx.TrackObsoletePage(oldPageIds[pathLen]);
        if (leafId == tx.TxFirstLeafId(_metadata.FirstLeafPageId))
            tx.TrackFirstLeafChange(shadowLeaf.PageId);

        // Retire old overflow chain after shadow leaf is updated (WAL ordering correct).
        if (oldIsOverflow)
            RetireOverflowChain(oldOverflowFirstPageId, tx.TrackObsoleteOverflowPage);

        return true;
    }

    /// <summary>
    /// Insert a key/value pair into the transaction's shadow tree only if the key
    /// does not already exist. Returns true if inserted; false if key was present.
    /// Writer lock is held for the full transaction lifetime — no additional locking needed.
    /// </summary>
    internal bool TryInsertInTransaction(TKey key, TValue value, Transaction<TKey, TValue> tx)
    {
        // TryGetInTransaction traverses from tx.TxRootId — read-your-own-writes.
        if (TryGetInTransaction(key, tx, out _)) return false;

        // Key absent in the shadow tree — full shadow-path insert.
        InsertInTransaction(key, value, tx);
        return true;
    }

    /// <summary>
    /// Atomically add or update within the transaction's shadow tree.
    /// Writer lock is held for the full transaction lifetime — no additional locking needed.
    /// Read-your-own-writes: prior inserts/deletes within this transaction are visible.
    /// </summary>
    internal TValue AddOrUpdateInTransaction(
        TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory,
        Transaction<TKey, TValue> tx)
    {
        if (TryGetInTransaction(key, tx, out TValue existing))
        {
            TValue updated = updateValueFactory(key, existing);
            InsertInTransaction(key, updated, tx);   // overwrite in shadow tree
            return updated;
        }
        else
        {
            InsertInTransaction(key, addValue, tx);
            return addValue;
        }
    }

    /// <summary>
    /// Fetch or insert within the transaction's shadow tree.
    /// Writer lock is held for the full transaction lifetime — no additional locking needed.
    /// Read-your-own-writes: prior inserts/deletes within this transaction are visible.
    /// </summary>
    internal TValue GetOrAddInTransaction(TKey key, TValue addValue, Transaction<TKey, TValue> tx)
    {
        if (TryGetInTransaction(key, tx, out TValue existing))
            return existing;

        InsertInTransaction(key, addValue, tx);
        return addValue;
    }

    /// <summary>
    /// Atomically read and delete within the transaction's shadow tree.
    /// Writer lock is held for the full transaction lifetime — no additional locking needed.
    /// Read-your-own-writes: prior inserts/deletes within this transaction are visible.
    /// </summary>
    internal bool TryGetAndDeleteInTransaction(TKey key, Transaction<TKey, TValue> tx, out TValue value)
    {
        if (!TryGetInTransaction(key, tx, out value)) return false;
        TryDeleteInTransaction(key, tx);   // key confirmed present — will succeed
        return true;
    }

    /// <summary>
    /// Compute and update within the transaction's shadow tree.
    /// Writer lock is held for the full transaction lifetime — no additional locking needed.
    /// Read-your-own-writes: prior inserts/deletes within this transaction are visible.
    /// If the key is absent, the factory is never called and false is returned.
    /// </summary>
    internal bool TryUpdateWithFactoryInTransaction(
        TKey key, Func<TValue, TValue> updateFactory, Transaction<TKey, TValue> tx)
    {
        if (!TryGetInTransaction(key, tx, out TValue existing)) return false;
        TryUpdateInTransaction(key, updateFactory(existing), tx);   // key confirmed present
        return true;
    }

    /// <summary>
    /// Transactional CAS. Writer lock is held for the full transaction lifetime — no additional locking needed.
    /// Read-your-own-writes: own inserts/updates within the transaction are visible to the comparison.
    /// </summary>
    internal bool TryCompareAndSwapInTransaction(
        TKey key, TValue expected, TValue newValue, IEqualityComparer<TValue>? comparer,
        Transaction<TKey, TValue> tx)
    {
        comparer ??= EqualityComparer<TValue>.Default;
        if (!TryGetInTransaction(key, tx, out TValue existing)) return false;
        if (!comparer.Equals(existing, expected)) return false;
        InsertInTransaction(key, newValue, tx);   // upsert semantics — update for existing key
        return true;
    }

}
