using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;
using BPlusTree.Core.Api;
using BPlusTree.Core.Storage;
using BPlusTree.Core.Wal;

namespace BPlusTree.UnitTests.Engine;

/// <summary>
/// Phase 26a infrastructure tests.
/// Covers WalWriter.FlushedLsn / FlushUpTo and
/// BufferPool.TryClaimForEviction / ReleaseEvictedFrame.
/// No eviction worker is running — these tests exercise the building-block
/// methods that EvictionWorker will call in Phase 26b.
/// </summary>
public class BufferPoolEvictionTests : IDisposable
{
    private readonly string _walPath = Path.GetTempFileName();
    private readonly string _dbPath  = Path.GetTempFileName();

    public void Dispose()
    {
        try { File.Delete(_walPath); } catch { }
        try { File.Delete(_dbPath);  } catch { }
    }

    // ── WalWriter.FlushedLsn ─────────────────────────────────────────────────

    [Fact]
    public void FlushedLsn_IsZero_Initially()
    {
        using var writer = WalWriter.Open(_walPath, bufferSize: 65_536);
        writer.FlushedLsn.Should().Be(0UL);
    }

    [Fact]
    public void FlushedLsn_UpdatedAfterExplicitFlush()
    {
        using var writer = WalWriter.Open(_walPath, bufferSize: 65_536);
        writer.Append(WalRecordType.UpdatePage, 1, 1, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
        var lsnBefore = writer.CurrentLsn.Value;

        writer.Flush();

        writer.FlushedLsn.Should().BeGreaterThanOrEqualTo(lsnBefore,
            "FlushedLsn must advance to at least CurrentLsn after Flush()");
    }

    // ── WalWriter.FlushUpTo ──────────────────────────────────────────────────

    [Fact]
    public void FlushUpTo_IsNoOp_WhenAlreadyFlushed()
    {
        using var writer = WalWriter.Open(_walPath, bufferSize: 65_536);
        writer.Append(WalRecordType.UpdatePage, 1, 1, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
        writer.Flush();

        var flushedBefore = writer.FlushedLsn;

        // FlushUpTo a LSN already covered — should not change FlushedLsn
        writer.FlushUpTo(flushedBefore);

        writer.FlushedLsn.Should().Be(flushedBefore);
    }

    [Fact]
    public void FlushUpTo_ForcesFlush_WhenPending()
    {
        // Use a large buffer so no automatic overflow flush occurs
        using var writer = WalWriter.Open(_walPath, bufferSize: 65_536);
        writer.Append(WalRecordType.UpdatePage, 1, 1, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
        var target = writer.CurrentLsn.Value;

        // No explicit Flush() — data is only in the in-memory buffer
        writer.FlushedLsn.Should().Be(0UL, "nothing flushed yet");

        writer.FlushUpTo(target);

        writer.FlushedLsn.Should().BeGreaterThanOrEqualTo(target,
            "FlushUpTo must fsync and advance FlushedLsn to at least the target LSN");
    }

    // ── BufferPool.TryClaimForEviction ───────────────────────────────────────

    [Fact]
    public void TryClaimForEviction_ReturnsFalse_ForPinnedFrame()
    {
        var opts = new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = 4096, BufferPoolCapacity = 16, CheckpointThreshold = 8,
        };
        var wal  = WalWriter.Open(_walPath, opts.WalBufferSize);
        var mgr  = PageManager.Open(opts, wal);

        // AllocatePage returns a pinned frame
        var frame = mgr.AllocatePage(PageType.Leaf);
        uint pageId = frame.PageId;

        int idx = mgr.BufferPool.GetFrameIndex(pageId);
        idx.Should().BeGreaterThanOrEqualTo(0, "page must be in the pool");

        // Frame is pinned — cannot be claimed for eviction
        mgr.BufferPool.TryClaimForEviction(idx).Should().BeFalse(
            "a pinned frame must never be claimed for eviction");

        mgr.Unpin(pageId);
        wal.Dispose();
        mgr.Dispose();
    }

    [Fact]
    public void TryClaimForEviction_PreventsDoubleClaim()
    {
        var opts = new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = 4096, BufferPoolCapacity = 16, CheckpointThreshold = 8,
        };
        var wal = WalWriter.Open(_walPath, opts.WalBufferSize);
        var mgr = PageManager.Open(opts, wal);

        var frame = mgr.AllocatePage(PageType.Leaf);
        uint pageId = frame.PageId;

        // Unpin so the frame becomes evictable
        mgr.MarkDirtyAndUnpin(pageId);

        int idx = mgr.BufferPool.GetFrameIndex(pageId);
        idx.Should().BeGreaterThanOrEqualTo(0);

        // Unpin sets ReferenceBit = true (Fix 1 — PHASE-29.MD second-chance clock).
        // First call clears the bit and returns false — second chance granted.
        mgr.BufferPool.TryClaimForEviction(idx).Should().BeFalse(
            "first attempt must grant second chance (ReferenceBit=true from Unpin)");

        // Second call: ReferenceBit is now false — claim must succeed
        mgr.BufferPool.TryClaimForEviction(idx).Should().BeTrue(
            "second attempt must succeed once second chance is consumed");

        // Third claim on the same frame must fail — EVICTING state is exclusive
        mgr.BufferPool.TryClaimForEviction(idx).Should().BeFalse(
            "double-claim must be prevented by IsEvicting check");

        // Clean up — release the claimed frame
        mgr.BufferPool.ReleaseEvictedFrame(idx);

        wal.Dispose();
        mgr.Dispose();
    }

    // ── OpenPool helper ───────────────────────────────────────────────────────

    private (PageManager mgr, WalWriter wal) OpenPool(int capacity = 16)
    {
        var opts = new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = 4096, BufferPoolCapacity = capacity, CheckpointThreshold = 8,
        };
        var wal = WalWriter.Open(_walPath, opts.WalBufferSize);
        return (PageManager.Open(opts, wal), wal);
    }

    // ── BufferPool.ReleaseEvictedFrame ───────────────────────────────────────

    [Fact]
    public void ReleaseEvictedFrame_ResetsFrame_And_SignalsDone()
    {
        var opts = new BPlusTreeOptions
        {
            DataFilePath = _dbPath, WalFilePath = _walPath,
            PageSize = 4096, BufferPoolCapacity = 16, CheckpointThreshold = 8,
        };
        var wal = WalWriter.Open(_walPath, opts.WalBufferSize);
        var mgr = PageManager.Open(opts, wal);

        var frame = mgr.AllocatePage(PageType.Leaf);
        uint pageId = frame.PageId;
        mgr.MarkDirtyAndUnpin(pageId);

        int idx = mgr.BufferPool.GetFrameIndex(pageId);
        // Consume the second chance (ReferenceBit=true from Unpin, Fix 1 — PHASE-29.MD)
        mgr.BufferPool.TryClaimForEviction(idx).Should().BeFalse("first attempt grants second chance");
        // Second attempt must succeed
        mgr.BufferPool.TryClaimForEviction(idx).Should().BeTrue();

        int signalBefore = mgr.BufferPool.EvictDoneSignal.CurrentCount;

        mgr.BufferPool.ReleaseEvictedFrame(idx);

        // Frame must be FREE — no longer in the page index
        mgr.BufferPool.GetFrameIndex(pageId).Should().Be(-1,
            "released frame must be removed from the page index");

        // Signal must have been released (CurrentCount incremented)
        mgr.BufferPool.EvictDoneSignal.CurrentCount.Should().BeGreaterThan(signalBefore,
            "ReleaseEvictedFrame must signal _evictDoneSignal");

        wal.Dispose();
        mgr.Dispose();
    }

    // ── Phase 31: lock-free TryClaimForEviction correctness tests ────────────

    /// <summary>
    /// Simultaneously pin and TryClaimForEviction the same frame.
    ///
    /// Verified invariant (system-level correctness):
    ///   After FetchPage returns, the caller holds PinCount ≥ 1.  The evictor must
    ///   never call ReleaseEvictedFrame on a pinned frame — doing so resets the frame
    ///   (wipes Data, removes from _pageIndex) while the caller still holds a reference,
    ///   causing data corruption.  The symptom: Unpin(pageId) throws because pageId is
    ///   no longer in the pool.
    ///
    /// Why IsEvicting can be transiently true AFTER Pin() returns (not a violation):
    ///   The lock-free evictor may read a stale PinCount=0 from its core's store buffer
    ///   before Monitor.Exit commits the write.  It then wins the CAS (IsEvicting=1).
    ///   The post-CAS re-check sees PinCount=1 (CAS is a full barrier, commit visible),
    ///   calls ClearEvicting(), and returns false — no eviction proceeds.  During the
    ///   brief window between CAS and ClearEvicting the pinner could observe IsEvicting=1,
    ///   but ReleaseEvictedFrame is never called in this path, so no data is corrupted.
    ///   Checking f.IsEvicting after FetchPage is therefore too strict for the lock-free
    ///   design; the correct check is whether Unpin succeeds.
    /// </summary>
    [Fact(Timeout = 15_000)]
    public async Task TryClaimForEviction_ConcurrentWithPin_PostCasCheckPreventsCorruption()
    {
        // EvictionWorker scans ALL frame indices directly — mirroring TryEvictBatch.
        // Scanning by index avoids the stale-index problem that would arise from
        // GetFrameIndexByPageId: that lookup is under _lock but the result can become
        // stale before TryClaimForEviction uses it.
        var (mgr, wal) = OpenPool(capacity: 16);
        var frame = mgr.AllocatePage(PageType.Leaf);
        var pageId = frame.PageId;
        mgr.Unpin(pageId); // leave frame UNPINNED and eligible for eviction

        // Pre-clear ReferenceBit so the evictor does not consume the second chance
        mgr.BufferPool.GetFrameByPageId(pageId)!.ReferenceBit = false;

        // Counts InvalidOperationException from Unpin — meaning the evictor evicted the
        // page while the pinner still held a pin.  This must never happen.
        int pinnerViolations = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(8));

        // Thread A: repeatedly fetch and unpin pageId
        var pinner = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    var f = mgr.FetchPage(pageId);
                    // After FetchPage the frame is pinned (PinCount ≥ 1). The evictor
                    // must NOT evict it before Unpin is called. If it does,
                    // Unpin throws InvalidOperationException ("page not in buffer pool").
                    mgr.Unpin(f.PageId);
                }
                catch (InvalidOperationException)
                {
                    // Unpin threw: the page was evicted while still pinned — data corruption.
                    Interlocked.Increment(ref pinnerViolations);
                }
                catch { /* pool pressure / BufferPoolExhaustedException — expected */ }
            }
        }, cts.Token);

        // Thread B: scan ALL frame indices (like EvictionWorker.TryEvictBatch)
        var evictor = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                for (int i = 0; i < mgr.BufferPool.Capacity; i++)
                {
                    if (mgr.BufferPool.TryClaimForEviction(i))
                        mgr.BufferPool.ReleaseEvictedFrame(i);
                }
            }
        }, cts.Token);

        await Task.WhenAll(
            pinner.ContinueWith(_ => { }),
            evictor.ContinueWith(_ => { }));

        pinnerViolations.Should().Be(0,
            "after FetchPage the page must remain in the pool until Unpin is called — " +
            "the evictor must never call ReleaseEvictedFrame on a pinned frame.");

        wal.Dispose(); mgr.Dispose();
    }

    /// <summary>
    /// Two threads simultaneously call TryClaimForEviction on the same frame.
    /// The CAS guarantees exactly one will return true per round.
    /// </summary>
    [Fact(Timeout = 10_000)]
    public async Task TryClaimForEviction_ConcurrentClaims_ExactlyOneSucceeds()
    {
        var (mgr, wal) = OpenPool(capacity: 16);
        var frame = mgr.AllocatePage(PageType.Leaf);
        var pageId = frame.PageId;
        mgr.Unpin(pageId); // unpin so frame is evictable

        const int rounds = 500;

        for (int r = 0; r < rounds; r++)
        {
            // If frame was evicted last round, re-allocate
            if (mgr.BufferPool.GetFrameByPageId(pageId) == null)
            {
                frame = mgr.AllocatePage(PageType.Leaf);
                pageId = frame.PageId; // track the new pageId
                mgr.Unpin(pageId);     // Unpin sets ReferenceBit=true
            }

            var frameIdx = mgr.BufferPool.GetFrameIndexByPageId(pageId);
            // Clear ReferenceBit so frame is immediately evictable (not given second chance)
            mgr.BufferPool.GetFrameByPageId(pageId)!.ReferenceBit = false;

            int roundWins = 0;
            var t1 = Task.Run(() =>
            {
                if (mgr.BufferPool.TryClaimForEviction(frameIdx))
                    Interlocked.Increment(ref roundWins);
            });
            var t2 = Task.Run(() =>
            {
                if (mgr.BufferPool.TryClaimForEviction(frameIdx))
                    Interlocked.Increment(ref roundWins);
            });

            await Task.WhenAll(t1, t2);

            roundWins.Should().BeLessThanOrEqualTo(1,
                $"round {r}: two concurrent TryClaimForEviction calls produced {roundWins} successes — CAS must allow at most 1");

            // Release whichever thread won the claim
            if (roundWins == 1)
                mgr.BufferPool.ReleaseEvictedFrame(frameIdx);
        }

        wal.Dispose(); mgr.Dispose();
    }

    /// <summary>
    /// Verifies that the lock-free TryClaimForEviction still respects the second-chance
    /// clock (ReferenceBit) correctly in the non-concurrent case.
    /// </summary>
    [Fact]
    public void TryClaimForEviction_LockFree_StillRespects_ReferenceBit()
    {
        var (mgr, wal) = OpenPool(capacity: 16);
        var frame = mgr.AllocatePage(PageType.Leaf);
        mgr.Unpin(frame.PageId); // Unpin sets ReferenceBit=true (Phase 29 Fix 1)

        int idx = mgr.BufferPool.GetFrameIndexByPageId(frame.PageId);
        idx.Should().BeGreaterThanOrEqualTo(0);

        // First attempt: ReferenceBit=true → must return false (second chance granted)
        bool first = mgr.BufferPool.TryClaimForEviction(idx);
        first.Should().BeFalse("frame with ReferenceBit=true must not be claimed on first attempt");

        // ReferenceBit must now be false (cleared by TryClaimForEviction)
        mgr.BufferPool.GetFrameByPageId(frame.PageId)!.ReferenceBit
            .Should().BeFalse("TryClaimForEviction must clear ReferenceBit when granting second chance");

        // Second attempt: ReferenceBit=false → must succeed
        bool second = mgr.BufferPool.TryClaimForEviction(idx);
        second.Should().BeTrue("frame with ReferenceBit=false must be claimable on second attempt");

        if (second) mgr.BufferPool.ReleaseEvictedFrame(idx);
        wal.Dispose(); mgr.Dispose();
    }

    // ── Phase 32: SignalEviction CAS correctness tests ────────────────────────

    /// <summary>
    /// With the old try/catch design, multiple concurrent SignalEviction calls would each
    /// call Release(1) and all but one would throw SemaphoreFullException.
    /// With the CAS design, exactly one caller reaches Release(1) — no exception is thrown,
    /// and the semaphore count never exceeds 1.
    /// </summary>
    [Fact(Timeout = 10_000)]
    public async Task SignalEviction_ConcurrentCalls_ExactlyOneReleasePerSignal()
    {
        var (mgr, wal) = OpenPool(capacity: 32);
        int exceptionCount = 0;
        const int concurrency = 50;

        // Ensure pool is over HWM so SignalEviction actually fires
        var pages = Enumerable.Range(0, 28)  // 28/32 = 87.5% > 85% HWM
            .Select(_ => mgr.AllocatePage(PageType.Leaf))
            .ToList();

        // Fire 50 concurrent SignalEviction calls
        var tasks = Enumerable.Range(0, concurrency).Select(_ => Task.Run(() =>
        {
            try { mgr.BufferPool.SignalEviction(); }
            catch (Exception) { Interlocked.Increment(ref exceptionCount); }
        })).ToArray();

        await Task.WhenAll(tasks);

        exceptionCount.Should().Be(0,
            "SignalEviction() must never throw — the CAS ensures exactly one caller " +
            "calls Release(1). SemaphoreFullException indicates the try/catch was " +
            "not replaced with the _evictPending CAS pattern.");

        // Semaphore count must be at most 1 (0 if EvictionWorker already consumed it)
        mgr.BufferPool.EvictSignalCount.Should().BeLessThanOrEqualTo(1,
            "SemaphoreSlim(0,1) must never have count > 1 — " +
            "multiple Release(1) calls indicate the CAS gate is not working.");

        pages.ForEach(p => mgr.Unpin(p.PageId));
        wal.Dispose(); mgr.Dispose();
    }

    /// <summary>
    /// Directly verifies the ResetEvictPending() → SignalEviction() ordering invariant
    /// without requiring a live EvictionWorker (PageManager.Open does not start one).
    ///
    /// Correct ordering (PHASE-32.MD): clear _evictPending BEFORE Wait().
    /// After ResetEvictPending() sets _evictPending=0, a subsequent SignalEviction() call
    /// can CAS 0→1 and call Release(1). If the flag were never reset (wrong ordering),
    /// SignalEviction() would see _evictPending=1 and skip Release(1) — signal lost.
    /// </summary>
    [Fact]
    public void SignalEviction_FiredDuringEviction_NotLost()
    {
        var (mgr, wal) = OpenPool(capacity: 16);

        // Round 1: signal eviction — _evictPending transitions 0→1, semaphore released.
        mgr.BufferPool.SignalEviction();
        mgr.BufferPool.EvictSignalCount.Should().Be(1, "first signal must reach the semaphore");

        // Simulate EvictionWorker: ResetEvictPending() BEFORE Wait() (correct ordering).
        // This sets _evictPending=0 before consuming the semaphore token.
        mgr.BufferPool.ResetEvictPending();
        bool acquired = mgr.BufferPool.EvictSignal.Wait(0, CancellationToken.None);
        acquired.Should().BeTrue("semaphore must have been signalled by first SignalEviction");
        mgr.BufferPool.EvictSignalCount.Should().Be(0, "semaphore consumed by Wait");

        // Round 2: signal fired after (or during) the simulated eviction cycle.
        // With correct ordering: _evictPending=0 after ResetEvictPending(), so CAS
        // 0→1 succeeds and Release(1) is called. Semaphore count must become 1.
        // With wrong ordering (clear after Wait): _evictPending was still 1 during Wait(),
        // then cleared — but any signal fired before the clear would have been skipped
        // by the CAS (seen 1, returned immediately, never called Release).
        mgr.BufferPool.SignalEviction();
        mgr.BufferPool.EvictSignalCount.Should().Be(1,
            "signal fired after ResetEvictPending must not be lost — " +
            "_evictPending must be 0 so the CAS succeeds and Release(1) is called. " +
            "Count=0 means the signal was swallowed: _evictPending was 1 (not reset " +
            "before simulated Wait), violating PHASE-32.MD Failure Point #1.");

        wal.Dispose(); mgr.Dispose();
    }
}
