namespace BPlusTree.Core.Storage;

/// <summary>
/// Defines the type of a page in the B+ tree storage.
/// </summary>
public enum PageType : byte
{
    /// <summary>
    /// A leaf page containing key-value pairs.
    /// </summary>
    Leaf = 1,

    /// <summary>
    /// An internal page containing keys and pointers to child pages.
    /// </summary>
    Internal = 2,

    /// <summary>
    /// A meta page containing database metadata.
    /// </summary>
    Meta = 3,

    /// <summary>
    /// A free list page containing recycled page IDs.
    /// </summary>
    FreeList = 4,

    /// <summary>
    /// An overflow page storing a chunk of a large value.
    /// </summary>
    Overflow = 5,
}