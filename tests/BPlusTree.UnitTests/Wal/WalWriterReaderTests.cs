using System;
using System.IO;
using System.Linq;
using Xunit;
using FluentAssertions;
using BPlusTree.Core.Wal;

namespace BPlusTree.UnitTests.Wal;

public class WalWriterTests : IDisposable
{
    private readonly string _path = Path.GetTempFileName();

    public void Dispose()
    {
        if (File.Exists(_path))
            File.Delete(_path);
    }

    [Fact]
    public void Open_CreatesWriter_Successfully()
    {
        using var writer = WalWriter.Open(_path, bufferSize: 4096);
        writer.Should().NotBeNull();
    }

    [Fact]
    public void Append_WritesRecordToFile_AfterFlush()
    {
        using var writer = WalWriter.Open(_path, bufferSize: 4096);

        writer.Append(
            WalRecordType.UpdatePage,
            transactionId: 1,
            pageId: 5,
            prevLsn: LogSequenceNumber.None,
            data: ReadOnlySpan<byte>.Empty);

        writer.Flush();

        var fileInfo = new FileInfo(_path);
        fileInfo.Length.Should().BeGreaterThan(0);
    }

    [Fact]
    public void Append_ReturnsMonotonicallyIncreasingLsns()
    {
        using var writer = WalWriter.Open(_path, bufferSize: 65536);

        var lsn1 = writer.Append(WalRecordType.UpdatePage, 1, 1, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
        var lsn2 = writer.Append(WalRecordType.UpdatePage, 1, 2, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);

        lsn1.Value.Should().BeLessThan(lsn2.Value);
    }

    [Fact]
    public void CurrentLsn_AdvancesAfterAppend()
    {
        using var writer = WalWriter.Open(_path, bufferSize: 65536);
        var before = writer.CurrentLsn;

        writer.Append(WalRecordType.UpdatePage, 1, 1, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);

        writer.CurrentLsn.Value.Should().BeGreaterThan(before.Value);
    }
}

public class WalReaderTests : IDisposable
{
    private readonly string _path = Path.GetTempFileName();

    public void Dispose()
    {
        if (File.Exists(_path))
            File.Delete(_path);
    }

    [Fact]
    public void Constructor_InitializesCorrectly()
    {
        var reader = new WalReader(_path);
        reader.Should().NotBeNull();
    }

    [Fact]
    public void ReadForward_ReturnsEmpty_WhenFileIsEmpty()
    {
        var reader  = new WalReader(_path);
        var records = reader.ReadForward(LogSequenceNumber.None);
        records.Should().BeEmpty();
    }

    [Fact]
    public void ReadForward_CanReadBackWrittenRecord()
    {
        using var writer = WalWriter.Open(_path, bufferSize: 65536);
        writer.Append(WalRecordType.UpdatePage, transactionId: 42, pageId: 7,
                      LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
        writer.Flush();

        var reader  = new WalReader(_path);
        var records = reader.ReadForward(LogSequenceNumber.None).ToList();

        records.Should().HaveCount(1);
        records[0].Type.Should().Be(WalRecordType.UpdatePage);
        records[0].TransactionId.Should().Be(42u);
        records[0].PageId.Should().Be(7u);
    }

    [Fact]
    public void Validate_ReturnsValid_ForWellFormedWal()
    {
        using var writer = WalWriter.Open(_path, bufferSize: 65536);
        writer.Append(WalRecordType.UpdatePage, 1, 1, LogSequenceNumber.None, ReadOnlySpan<byte>.Empty);
        writer.Flush();

        var reader = new WalReader(_path);
        var result = reader.Validate();

        result.IsValid.Should().BeTrue();
        result.RecordCount.Should().Be(1);
    }
}
