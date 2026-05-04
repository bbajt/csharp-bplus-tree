using ByTech.BPlusTree.Core.Engine;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Engine;

/// <summary>
/// Tests for the epoch-based reader registry added to TransactionCoordinator in M+1.
/// Exercises EnterReadEpoch / ExitReadEpoch / OldestActiveEpoch in isolation.
/// </summary>
public class EpochRegistryTests
{
    // ── Test 1 ────────────────────────────────────────────────────────────────
    [Fact]
    public void EpochRegistry_EnterReadEpoch_ReturnsMonotonicallyIncreasingValues()
    {
        var coordinator = new TransactionCoordinator();
        var epochs      = new ulong[10];

        for (int i = 0; i < 10; i++)
            epochs[i] = coordinator.EnterReadEpoch();

        for (int i = 1; i < 10; i++)
            epochs[i].Should().BeGreaterThan(epochs[i - 1]);

        // Clean up — exit all to leave coordinator in clean state.
        foreach (ulong e in epochs)
            coordinator.ExitReadEpoch(e);
    }

    // ── Test 2 ────────────────────────────────────────────────────────────────
    [Fact]
    public void EpochRegistry_NoActiveReaders_OldestEpochIsNull()
    {
        var coordinator = new TransactionCoordinator();

        // Fresh coordinator — no readers.
        coordinator.OldestActiveEpoch.Should().BeNull();

        // Enter one reader.
        ulong e1 = coordinator.EnterReadEpoch();
        coordinator.OldestActiveEpoch.Should().Be(e1);

        // Exit — back to null.
        coordinator.ExitReadEpoch(e1);
        coordinator.OldestActiveEpoch.Should().BeNull();
    }

    // ── Test 3 ────────────────────────────────────────────────────────────────
    [Fact]
    public void EpochRegistry_OldestActiveEpoch_TracksMinimumAcrossEntryAndExit()
    {
        var coordinator = new TransactionCoordinator();

        ulong e1 = coordinator.EnterReadEpoch();
        ulong e2 = coordinator.EnterReadEpoch();
        ulong e3 = coordinator.EnterReadEpoch();

        // All three active — oldest is e1.
        coordinator.OldestActiveEpoch.Should().Be(e1);

        // Exit the oldest.
        coordinator.ExitReadEpoch(e1);
        coordinator.OldestActiveEpoch.Should().Be(e2);

        // Exit out-of-order: newest first.
        coordinator.ExitReadEpoch(e3);
        coordinator.OldestActiveEpoch.Should().Be(e2);

        // Exit the last.
        coordinator.ExitReadEpoch(e2);
        coordinator.OldestActiveEpoch.Should().BeNull();
    }
}
