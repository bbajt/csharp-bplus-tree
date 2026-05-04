namespace ByTech.BPlusTree.Core.Wal;

/// <summary>
/// Monotonically increasing log sequence number = byte offset in the WAL file.
/// Immutable value type. LSN 0 is the null/invalid sentinel.
/// </summary>
internal readonly struct LogSequenceNumber
    : IEquatable<LogSequenceNumber>, IComparable<LogSequenceNumber>
{
    public static readonly LogSequenceNumber None = new(0UL);

    public ulong Value { get; }

    public LogSequenceNumber(ulong value) => Value = value;

    public bool IsValid => Value > 0UL;

    public static bool operator ==(LogSequenceNumber a, LogSequenceNumber b) => a.Value == b.Value;
    public static bool operator !=(LogSequenceNumber a, LogSequenceNumber b) => a.Value != b.Value;
    /// <summary>Less-than comparison.</summary>
    public static bool operator < (LogSequenceNumber a, LogSequenceNumber b) => a.Value < b.Value;
    /// <summary>Greater-than comparison.</summary>
    public static bool operator > (LogSequenceNumber a, LogSequenceNumber b) => a.Value > b.Value;
    /// <summary>Less-than-or-equal comparison.</summary>
    public static bool operator <=(LogSequenceNumber a, LogSequenceNumber b) => a.Value <= b.Value;
    /// <summary>Greater-than-or-equal comparison.</summary>
    public static bool operator >=(LogSequenceNumber a, LogSequenceNumber b) => a.Value >= b.Value;

    /// <inheritdoc />
    public int CompareTo(LogSequenceNumber other) => Value.CompareTo(other.Value);

    /// <inheritdoc />
    public bool Equals(LogSequenceNumber other) => Value == other.Value;
    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is LogSequenceNumber other && Value == other.Value;
    /// <inheritdoc />
    public override int  GetHashCode() => Value.GetHashCode();
    /// <inheritdoc />
    public override string ToString() => Value.ToString();
}