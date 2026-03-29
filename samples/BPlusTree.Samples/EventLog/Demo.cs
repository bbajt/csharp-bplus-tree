namespace BPlusTree.Samples.EventLog;

/// <summary>
/// Append-only event log: long (UTC ms) keys, Event values.
/// Demonstrates ScanReverse for "latest N events" and Scan for time-window queries.
/// </summary>
public static class EventLogDemo
{
    public static void Run()
    {
        string dbPath  = Path.Combine(Path.GetTempPath(), "sample03.db");
        string walPath = Path.Combine(Path.GetTempPath(), "sample03.wal");
        File.Delete(dbPath);
        File.Delete(walPath);

        using var log = new EventLogStoreWrapper(dbPath, walPath);

        // ── Record the window start time before appending ─────────────────────
        DateTimeOffset windowStart = DateTimeOffset.UtcNow;

        // ── Append a burst of events ──────────────────────────────────────────
        var categories = new[] { "Auth", "Order", "Payment", "Inventory", "Shipping" };
        var rng = new Random(42);

        Console.WriteLine("Appending 30 events...");
        for (int i = 0; i < 30; i++)
        {
            string cat     = categories[rng.Next(categories.Length)];
            string payload = $"Event #{i + 1}: {cat.ToLower()} operation completed";
            log.Append(new Event { Category = cat, Payload = payload });

            // Small sleep to spread timestamps so the time-window demo is meaningful.
            if (i == 14)
                Thread.Sleep(5); // pause halfway through
        }

        DateTimeOffset windowEnd = DateTimeOffset.UtcNow;

        Console.WriteLine($"Total events in log: {log.Count}");

        // ── Latest 5 events (most recent first) ───────────────────────────────
        Console.WriteLine("\nLatest 5 events (newest first):");
        foreach (var (ts, evt) in log.GetLatest(5))
        {
            var time = DateTimeOffset.FromUnixTimeMilliseconds(ts).ToString("HH:mm:ss.fff");
            Console.WriteLine($"  {time}  {evt}");
        }

        // ── Time-window query ─────────────────────────────────────────────────
        // Events appended in the second half (after the sleep).
        DateTimeOffset midWindow = windowStart.AddMilliseconds(
            (windowEnd - windowStart).TotalMilliseconds / 2);

        var secondHalf = log.GetWindow(midWindow, windowEnd).ToList();
        Console.WriteLine($"\nEvents in second half of window: {secondHalf.Count}");
        foreach (var (ts, evt) in secondHalf.Take(5))
        {
            var time = DateTimeOffset.FromUnixTimeMilliseconds(ts).ToString("HH:mm:ss.fff");
            Console.WriteLine($"  {time}  {evt}");
        }

        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);
    }
}
