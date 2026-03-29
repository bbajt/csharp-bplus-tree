namespace BPlusTree.Samples.CustomerStore;

public sealed class Customer
{
    public string FirstName { get; init; } = "";
    public string LastName  { get; init; } = "";
    public string Email     { get; init; } = "";
    public int    Age       { get; init; }

    public override string ToString()
        => $"[{FirstName} {LastName}, age {Age}, {Email}]";
}
