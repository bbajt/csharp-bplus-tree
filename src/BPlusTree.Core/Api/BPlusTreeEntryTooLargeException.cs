namespace ByTech.BPlusTree.Core.Api;

/// <summary>
/// Thrown when the serialized size of a key exceeds the maximum entry size
/// supported by the configured page size. Large values are handled transparently
/// via overflow page chains; only oversized keys trigger this exception.
/// Increase <see cref="BPlusTreeOptions.PageSize"/> to accommodate larger keys.
/// </summary>
public sealed class BPlusTreeEntryTooLargeException : BPlusTreeException
{
    /// <summary>Actual serialized size of the offending key or key+value pair, in bytes.</summary>
    public int ActualSize { get; }

    /// <summary>Maximum allowed size in bytes for the current page size configuration.</summary>
    public int MaxSize    { get; }

    public BPlusTreeEntryTooLargeException(string message, int actualSize, int maxSize)
        : base(message)
    {
        ActualSize = actualSize;
        MaxSize    = maxSize;
    }
}
