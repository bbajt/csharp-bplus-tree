using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.EventLog;

/// <summary>
/// Append-only event log backed by BPlusTree&lt;long, Event&gt;.
/// Key = UTC millisecond timestamp; value = Event (category + payload).
///
/// Demonstrates: Int64Serializer, ScanReverse for "latest N", Scan for time-window queries.
/// </summary>
public sealed class EventLogStoreWrapper : IDisposable
{
    private readonly BPlusTree<long, Event> _tree;

    public EventLogStoreWrapper(string dbPath, string walPath)
    {
        var options = new BPlusTreeOptions
        {
            DataFilePath = dbPath,
            WalFilePath  = walPath,
        };
        options.Validate();
        _tree = BPlusTree<long, Event>.Open(options, Int64Serializer.Instance, EventSerializer.Instance);
    }

    /// <summary>
    /// Append an event. Key = current UTC milliseconds.
    /// Collisions within the same millisecond are handled by incrementing the timestamp.
    /// </summary>
    public long Append(Event evt)
    {
        long ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        // Ensure uniqueness when events arrive faster than 1 ms resolution.
        while (!_tree.TryInsert(ts, evt))
            ts++;
        return ts;
    }

    /// <summary>Return the most recent <paramref name="n"/> events, newest first.</summary>
    public IEnumerable<(long TimestampMs, Event Event)> GetLatest(int n)
        => _tree.ScanReverse().Take(n);

    /// <summary>Return all events in a UTC time window (inclusive).</summary>
    public IEnumerable<(long TimestampMs, Event Event)> GetWindow(
        DateTimeOffset from, DateTimeOffset to)
        => _tree.Scan(from.ToUnixTimeMilliseconds(), to.ToUnixTimeMilliseconds());

    public long Count => _tree.Count;

    public void Dispose() => _tree.Dispose();
}
