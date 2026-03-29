namespace BPlusTree.Core.Wal;

/// <summary>
/// Monotonically increasing log sequence number = byte offset in the WAL file.
/// Immutable value type. LSN 0 is the null/invalid sentinel.
/// </summary>
public readonly struct LogSequenceNumber
    : IEquatable<LogSequenceNumber>, IComparable<LogSequenceNumber>
{
    public static readonly LogSequenceNumber None = new(0UL);

    public ulong Value { get; }

    public LogSequenceNumber(ulong value) => Value = value;

    public bool IsValid => Value > 0UL;

    public static bool operator ==(LogSequenceNumber a, LogSequenceNumber b) => a.Value == b.Value;
    public static bool operator !=(LogSequenceNumber a, LogSequenceNumber b) => a.Value != b.Value;
    public static bool operator < (LogSequenceNumber a, LogSequenceNumber b) => a.Value < b.Value;
    public static bool operator > (LogSequenceNumber a, LogSequenceNumber b) => a.Value > b.Value;
    public static bool operator <=(LogSequenceNumber a, LogSequenceNumber b) => a.Value <= b.Value;
    public static bool operator >=(LogSequenceNumber a, LogSequenceNumber b) => a.Value >= b.Value;

    public int CompareTo(LogSequenceNumber other) => Value.CompareTo(other.Value);

    public bool Equals(LogSequenceNumber other) => Value == other.Value;
    public override bool Equals(object? obj) => obj is LogSequenceNumber other && Value == other.Value;
    public override int  GetHashCode() => Value.GetHashCode();
    public override string ToString() => Value.ToString();
}