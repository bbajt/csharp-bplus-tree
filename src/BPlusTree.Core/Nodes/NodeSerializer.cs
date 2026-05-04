using ByTech.BPlusTree.Core.Storage;

namespace ByTech.BPlusTree.Core.Nodes;

/// <summary>
/// Factory for wrapping pinned frames as typed node objects.
/// Does NOT own the frame — caller is responsible for Unpin.
/// </summary>
internal sealed class NodeSerializer<TKey, TValue>
    where TKey : notnull
{
    private readonly IKeySerializer<TKey>     _keySerializer;
    private readonly IValueSerializer<TValue> _valueSerializer;

    public NodeSerializer(IKeySerializer<TKey> keySerializer, IValueSerializer<TValue> valueSerializer)
    {
        _keySerializer   = keySerializer;
        _valueSerializer = valueSerializer;
    }

    public IKeySerializer<TKey>     KeySerializer   => _keySerializer;
    public IValueSerializer<TValue> ValueSerializer => _valueSerializer;

    public LeafNode<TKey, TValue> AsLeaf(Frame frame)     => new(frame, _keySerializer, _valueSerializer);
    /// <summary>Wrap a pinned frame as a typed internal node overlay.</summary>
    public InternalNode<TKey>     AsInternal(Frame frame) => new(frame, _keySerializer);

    /// <summary>Returns true if the page in this frame is a leaf page.</summary>
    public static bool IsLeaf(Frame frame)
        => (PageType)frame.Data[PageLayout.PageTypeOffset] == PageType.Leaf;
}
