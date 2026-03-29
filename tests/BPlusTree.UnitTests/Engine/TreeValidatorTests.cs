using System.Buffers.Binary;
using BPlusTree.Core.Api;
using BPlusTree.Core.Engine;
using BPlusTree.Core.Nodes;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Structural invariant tests for TreeValidator.
/// Uses PageSize=8192 with 1000 inserts to guarantee a multi-level tree (internal nodes present).
/// Record-count test uses 100 inserts only.
/// </summary>
public class TreeValidatorTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private (PageManager mgr, WalWriter wal, TreeEngine<int, int> engine,
             NodeSerializer<int, int> ns, TreeMetadata meta) Open()
    {
        var wal  = WalWriter.Open(_walPath, bufferSize: 512 * 1024);
        var mgr  = PageManager.Open(new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = PageSize,
            BufferPoolCapacity  = 128,
            CheckpointThreshold = 4096,   // high: prevents auto-checkpoint during inserts
        }, wal);
        var ns   = new NodeSerializer<int, int>(Int32Serializer.Instance, Int32Serializer.Instance);
        var meta = new TreeMetadata(mgr);
        meta.Load();
        return (mgr, wal, new TreeEngine<int, int>(mgr, ns, meta), ns, meta);
    }

    // ── Test A: record-count consistency ─────────────────────────────────────

    [Fact]
    public void Validate_RecordCountMismatch_ReturnsFalse()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();
        for (int i = 0; i < 100; i++) engine.Insert(i, i);

        // meta.TotalRecordCount == 100; leaf chain also has 100 records.
        // Decrement once to create a deliberate mismatch (metadata says 99, chain has 100).
        meta.DecrementRecordCount();

        var result = new TreeValidator<int, int>(mgr, ns, meta).Validate();
        result.IsValid.Should().BeFalse("leaf chain has 100 records but metadata reports 99");
        result.Errors.Should().Contain(e => e.Contains("Record count mismatch"));

        wal.Dispose(); mgr.Dispose();
    }

    // ── Test B: separator key alignment ──────────────────────────────────────

    [Fact]
    public void Validate_SeparatorKeyMisaligned_ReturnsFalse()
    {
        // 1000 inserts with PageSize=8192 guarantees at least one split → internal node root.
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();
        for (int i = 0; i < 1000; i++) engine.Insert(i, i);

        // Overwrite separator key[0] in the root internal node with int.MaxValue.
        // This makes separator[0] larger than the first key of its right child → alignment violation.
        var frame = mgr.FetchPage(meta.RootPageId);
        new InternalNode<int>(frame, Int32Serializer.Instance)
            .UpdateSeparatorKey(0, int.MaxValue);
        mgr.Unpin(frame.PageId);

        var result = new TreeValidator<int, int>(mgr, ns, meta).Validate();
        result.IsValid.Should().BeFalse("separator[0]=int.MaxValue must be > first key of right child");
        result.Errors.Should().Contain(e => e.Contains("Separator alignment violated"));

        wal.Dispose(); mgr.Dispose();
    }

    // ── Test C: NullPageId child pointer ─────────────────────────────────────

    [Fact]
    public void Validate_InternalNodeNullChildPointer_ReturnsFalse()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();
        for (int i = 0; i < 1000; i++) engine.Insert(i, i);

        // Corrupt LeftmostChildId of the root internal node to NullPageId.
        // InternalNode exposes LeftmostChildId as a read-write property —
        // this is the first child pointer checked in the DFS.
        var frame = mgr.FetchPage(meta.RootPageId);
        var corruptNode = new InternalNode<int>(frame, Int32Serializer.Instance);
        corruptNode.LeftmostChildId = PageLayout.NullPageId;
        mgr.Unpin(frame.PageId);

        var result = new TreeValidator<int, int>(mgr, ns, meta).Validate();
        result.IsValid.Should().BeFalse("LeftmostChildId=NullPageId must be detected by DFS");
        result.Errors.Should().Contain(e => e.Contains("NullPageId"));

        wal.Dispose(); mgr.Dispose();
    }

    // ── Test F: NullPageId slot-based child pointer ───────────────────────────

    [Fact]
    public void Validate_SlotBasedChildPointerNull_ReturnsFalse()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();
        for (int i = 0; i < 1000; i++) engine.Insert(i, i);

        // Corrupt GetChildId(0) of the root internal node to NullPageId.
        // This exercises the slot-based child pointer check in the DFS —
        // the path distinct from LeftmostChildId tested by Test C.
        var frame = mgr.FetchPage(meta.RootPageId);
        new InternalNode<int>(frame, Int32Serializer.Instance)
            .SetChildId(0, PageLayout.NullPageId);
        mgr.Unpin(frame.PageId);

        var result = new TreeValidator<int, int>(mgr, ns, meta).Validate();
        result.IsValid.Should().BeFalse("GetChildId(0)=NullPageId must be detected by DFS");
        result.Errors.Should().Contain(e => e.Contains("child[0]"));
        result.Errors.Should().Contain(e => e.Contains("NullPageId"));

        wal.Dispose(); mgr.Dispose();
    }

    // ── Test E: separator key too low (backward check) ───────────────────────

    [Fact]
    public void Validate_SeparatorKeyTooLow_ReturnsFalse()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();
        for (int i = 0; i < 1000; i++) engine.Insert(i, i);

        // Overwrite separator key[0] in the root internal node with int.MinValue.
        // After corruption: separator[0] = int.MinValue, but the leftmost subtree
        // contains keys starting at 0 — all > int.MinValue.
        // The backward check fires: separator[0] <= GetLastKey(leftmost subtree).
        var frame = mgr.FetchPage(meta.RootPageId);
        new InternalNode<int>(frame, Int32Serializer.Instance)
            .UpdateSeparatorKey(0, int.MinValue);
        mgr.Unpin(frame.PageId);

        var result = new TreeValidator<int, int>(mgr, ns, meta).Validate();
        result.IsValid.Should().BeFalse("separator[0]=int.MinValue must be <= last key of leftmost subtree");
        result.Errors.Should().Contain(e => e.Contains("backward check"));

        wal.Dispose(); mgr.Dispose();
    }

    // ── Test D: healthy multi-level tree passes all new invariants ────────────

    [Fact]
    public void Validate_MultiLevelTree_PassesAllInvariants()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();
        for (int i = 0; i < 1000; i++) engine.Insert(i, i);

        var result = new TreeValidator<int, int>(mgr, ns, meta).Validate();
        result.IsValid.Should().BeTrue(
            result.Errors.Count > 0
                ? string.Join("; ", result.Errors)
                : "healthy tree must pass all invariants");

        wal.Dispose(); mgr.Dispose();
    }

    // ── Test G: multiple simultaneous violations reported together ────────────

    [Fact]
    public void Validate_MultipleViolations_ReturnsAllErrors()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();
        for (int i = 0; i < 1000; i++) engine.Insert(i, i * 10);

        // Corruption 1 (Pass 1): decrement record count → count mismatch
        meta.DecrementRecordCount();

        // Corruption 2 (Pass 2): misalign separator[0] on root → forward violation
        var frame = mgr.FetchPage(meta.RootPageId);
        new InternalNode<int>(frame, Int32Serializer.Instance)
            .UpdateSeparatorKey(0, int.MaxValue);
        mgr.Unpin(meta.RootPageId);

        var result = new TreeValidator<int, int>(mgr, ns, meta).Validate();

        result.IsValid.Should().BeFalse();
        result.Errors.Count.Should().BeGreaterThan(1,
            "both a record-count mismatch (Pass 1) and a separator violation (Pass 2) " +
            "were introduced — validator should report both");
        result.Errors.Should().Contain(e => e.Contains("Record count mismatch"),
            "Pass 1 should report the count desync");
        result.Errors.Should().Contain(e => e.Contains("forward check"),
            "Pass 2 should report the separator alignment violation");

        wal.Dispose(); mgr.Dispose();
    }

    // ── Test H: empty-leaf produces structured error, not exception ───────────

    [Fact]
    public void Validate_EmptyLeaf_ReturnsStructuredError()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();
        for (int i = 0; i < 1000; i++) engine.Insert(i, i * 10);

        // Corrupt the first (leftmost) leaf: zero its slot count.
        // PageLayout.SlotCountOffset = 6, ushort = 2 bytes.
        var leafFrame = mgr.FetchPage(meta.FirstLeafPageId);
        BinaryPrimitives.WriteUInt16BigEndian(
            leafFrame.Data.AsSpan(PageLayout.SlotCountOffset, sizeof(ushort)), 0);
        mgr.Unpin(meta.FirstLeafPageId);

        // Must not throw — must return a structured ValidationResult with an error.
        var act    = () => new TreeValidator<int, int>(mgr, ns, meta).Validate();
        var result = act.Should().NotThrow().Subject;

        result.IsValid.Should().BeFalse("corrupted leaf must be reported as invalid");
        result.Errors.Should().Contain(e => e.Contains("empty"),
            "validator must report a structured 'empty leaf' error, not throw");

        wal.Dispose(); mgr.Dispose();
    }

    // ── Test I: TryGetFirstKey NullPageId guard — direct unit test ───────────

    [Fact]
    public void TryGetFirstKey_NullPageId_ReturnsErrorWithoutThrowing()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var (mgr, wal, engine, ns, meta) = Open();
        for (int i = 0; i < 100; i++) engine.Insert(i, i * 10);

        var validator = new TreeValidator<int, int>(mgr, ns, meta);

        // Invoke TryGetFirstKey directly with NullPageId — tests the new guard
        // without relying on DFS to route a NullPageId through the helper.
        var (success, _, error) = validator.TryGetFirstKey(PageLayout.NullPageId);

        success.Should().BeFalse("NullPageId is never a valid page to descend into");
        error.Should().Contain("NullPageId",
            "the guard must return a structured error message mentioning NullPageId");

        engine.Close();
        wal.Dispose();
        mgr.Dispose();
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); }  catch { }
        try { File.Delete(_walPath); } catch { }
    }
}
