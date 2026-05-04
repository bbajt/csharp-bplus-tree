using Xunit;
using FluentAssertions;
using ByTech.BPlusTree.Core.Wal;

namespace ByTech.BPlusTree.Core.Tests.Wal;

public class WalRecordLayoutTests
{
    [Fact] public void TotalLengthOffset_Is0()       => WalRecordLayout.TotalLengthOffset.Should().Be(0);
    [Fact] public void TypeOffset_Is4()              => WalRecordLayout.TypeOffset.Should().Be(4);
    [Fact] public void LsnOffset_Is5()               => WalRecordLayout.LsnOffset.Should().Be(5);
    [Fact] public void TransactionIdOffset_Is13()    => WalRecordLayout.TransactionIdOffset.Should().Be(13);
    [Fact] public void PageIdOffset_Is17()           => WalRecordLayout.PageIdOffset.Should().Be(17);
    [Fact] public void PrevLsnOffset_Is21()          => WalRecordLayout.PrevLsnOffset.Should().Be(21);
    [Fact] public void DataLengthOffset_Is29()       => WalRecordLayout.DataLengthOffset.Should().Be(29);
    [Fact] public void DataOffset_Is33()             => WalRecordLayout.DataOffset.Should().Be(33);
    [Fact] public void FixedHeaderSize_Is33()        => WalRecordLayout.FixedHeaderSize.Should().Be(33);
    [Fact] public void MinRecordLength_Is37()        => WalRecordLayout.MinRecordLength.Should().Be(37);

    [Fact]
    public void TotalLength_ZeroData_IsMinRecordLength()
        => WalRecordLayout.TotalLength(0).Should().Be(WalRecordLayout.MinRecordLength);

    [Fact]
    public void TotalLength_8192Data_Is8229()
        => WalRecordLayout.TotalLength(8192).Should().Be(8229);

    [Fact]
    public void LsnOffset_Plus8_EqualsTransactionIdOffset()
        => (WalRecordLayout.LsnOffset + 8).Should().Be(WalRecordLayout.TransactionIdOffset);

    [Fact]
    public void PrevLsnOffset_Plus8_EqualsDataLengthOffset()
        => (WalRecordLayout.PrevLsnOffset + 8).Should().Be(WalRecordLayout.DataLengthOffset);

    // ── WalRecordType enum values ─────────────────────────────────────────────
    [Fact] public void Begin_Is1()           => ((byte)WalRecordType.Begin).Should().Be(1);
    [Fact] public void UpdatePage_Is6()      => ((byte)WalRecordType.UpdatePage).Should().Be(6);
    [Fact] public void CheckpointEnd_Is8()   => ((byte)WalRecordType.CheckpointEnd).Should().Be(8);
}