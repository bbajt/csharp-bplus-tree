namespace BPlusTree.Core.Api;

/// <summary>Base class for all BPlusTree library exceptions.</summary>
public class BPlusTreeException : Exception
{
    public BPlusTreeException(string message) : base(message) { }
    public BPlusTreeException(string message, Exception inner) : base(message, inner) { }
}
