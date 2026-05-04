using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;

namespace BPlusTree.Samples.CustomerStore;

/// <summary>
/// Thin domain wrapper around <see cref="BPlusTree{TKey,TValue}"/>.
/// Hides tree details behind a Customer-specific API — the pattern most
/// library consumers will follow.
/// </summary>
public sealed class CustomerStoreWrapper : IDisposable
{
    private readonly BPlusTree<int, Customer> _tree;

    public CustomerStoreWrapper(string dbPath, string walPath)
    {
        var options = new BPlusTreeOptions
        {
            DataFilePath = dbPath,
            WalFilePath  = walPath,
        };
        options.Validate();
        _tree = BPlusTree<int, Customer>.Open(options, Int32Serializer.Instance, CustomerSerializer.Instance);
    }

    // ── Writes (auto-commit) ──────────────────────────────────────────────────

    /// <summary>Insert or overwrite a customer record.</summary>
    public void Save(int id, Customer customer) => _tree.Put(id, customer);

    /// <summary>Insert only if the id is not already taken. Returns false if duplicate.</summary>
    public bool TryAdd(int id, Customer customer) => _tree.TryInsert(id, customer);

    /// <summary>Update the email address for an existing customer. Returns false if not found.</summary>
    public bool UpdateEmail(int id, string newEmail)
        => _tree.TryUpdate(id, c => new Customer
        {
            FirstName = c.FirstName,
            LastName  = c.LastName,
            Age       = c.Age,
            Email     = newEmail,
        });

    /// <summary>Increment the age field for an existing customer. Returns false if not found.</summary>
    public bool IncrementAge(int id)
        => _tree.TryUpdate(id, c => new Customer
        {
            FirstName = c.FirstName,
            LastName  = c.LastName,
            Age       = c.Age + 1,
            Email     = c.Email,
        });

    /// <summary>Remove a customer. Returns false if not found.</summary>
    public bool Remove(int id) => _tree.Delete(id);

    // ── Reads ─────────────────────────────────────────────────────────────────

    public Customer? Find(int id)
        => _tree.TryGet(id, out Customer? c) ? c : null;

    public long Count => _tree.Count;

    public IEnumerable<(int Id, Customer Customer)> ListAll()
        => _tree.Scan();

    public IEnumerable<(int Id, Customer Customer)> ListByIdRange(int fromId, int toId)
        => _tree.Scan(fromId, toId);

    /// <summary>Returns the customer with the lowest id.</summary>
    public (int Id, Customer Customer)? First()
        => _tree.TryGetFirst(out int k, out Customer? v) ? (k, v) : null;

    /// <summary>Returns the customer with the highest id.</summary>
    public (int Id, Customer Customer)? Last()
        => _tree.TryGetLast(out int k, out Customer? v) ? (k, v) : null;

    /// <summary>Returns the customer whose id immediately follows <paramref name="id"/>.</summary>
    public (int Id, Customer Customer)? Next(int id)
        => _tree.TryGetNext(id, out int nk, out Customer? nv) ? (nk, nv) : null;

    public long CountRange(int fromId, int toId) => _tree.CountRange(fromId, toId);

    public void Dispose() => _tree.Dispose();
}
