namespace BPlusTree.Samples.EventLog;

public sealed class Event
{
    public string Category { get; init; } = "";
    public string Payload  { get; init; } = "";

    public override string ToString() => $"[{Category}] {Payload}";
}
