using BPlusTree.Core.Api;
using BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace BPlusTree.UnitTests.Api;

/// <summary>
/// Conformance tests for IReadOnlyDictionary&lt;TKey,TValue&gt; on BPlusTree (Phase 94).
/// </summary>
public class IReadOnlyDictionaryTests : IDisposable
{
    private readonly string _dbPath  = Path.GetTempFileName();
    private readonly string _walPath = Path.GetTempFileName();

    private BPlusTree<int, int> Open() => BPlusTree<int, int>.Open(
        new BPlusTreeOptions { DataFilePath = _dbPath, WalFilePath = _walPath },
        Int32Serializer.Instance, Int32Serializer.Instance);

    // ─────────────────────────────────────────────────────────────────────────
    // Cast
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void IReadOnlyDictionary_Cast_Succeeds()
    {
        using var tree = Open();
        var dict = (IReadOnlyDictionary<int, int>)tree;
        dict.Should().NotBeNull();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Indexer
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void IReadOnlyDictionary_Indexer_ExistingKey_ReturnsValue()
    {
        using var tree = Open();
        tree.Put(1, 10);
        var dict = (IReadOnlyDictionary<int, int>)tree;

        dict[1].Should().Be(10);
    }

    [Fact]
    public void IReadOnlyDictionary_Indexer_MissingKey_ThrowsKeyNotFoundException()
    {
        using var tree = Open();
        var dict = (IReadOnlyDictionary<int, int>)tree;

        var act = () => { _ = dict[99]; };
        act.Should().Throw<KeyNotFoundException>();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ContainsKey
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void IReadOnlyDictionary_ContainsKey_True()
    {
        using var tree = Open();
        tree.Put(1, 10);
        var dict = (IReadOnlyDictionary<int, int>)tree;

        dict.ContainsKey(1).Should().BeTrue();
    }

    [Fact]
    public void IReadOnlyDictionary_ContainsKey_False()
    {
        using var tree = Open();
        var dict = (IReadOnlyDictionary<int, int>)tree;

        dict.ContainsKey(99).Should().BeFalse();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // TryGetValue
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void IReadOnlyDictionary_TryGetValue_Found()
    {
        using var tree = Open();
        tree.Put(1, 10);
        var dict = (IReadOnlyDictionary<int, int>)tree;

        dict.TryGetValue(1, out var v).Should().BeTrue();
        v.Should().Be(10);
    }

    [Fact]
    public void IReadOnlyDictionary_TryGetValue_NotFound()
    {
        using var tree = Open();
        var dict = (IReadOnlyDictionary<int, int>)tree;

        dict.TryGetValue(99, out _).Should().BeFalse();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Keys / Values
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void IReadOnlyDictionary_Keys_YieldsAllKeysAscending()
    {
        using var tree = Open();
        tree.Put(3, 30); tree.Put(1, 10); tree.Put(2, 20);
        var dict = (IReadOnlyDictionary<int, int>)tree;

        dict.Keys.Should().Equal(1, 2, 3);
    }

    [Fact]
    public void IReadOnlyDictionary_Values_YieldsMatchingValuesAscending()
    {
        using var tree = Open();
        tree.Put(3, 30); tree.Put(1, 10); tree.Put(2, 20);
        var dict = (IReadOnlyDictionary<int, int>)tree;

        dict.Values.Should().Equal(10, 20, 30);
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath);  } catch { }
        try { File.Delete(_walPath); } catch { }
    }
}
