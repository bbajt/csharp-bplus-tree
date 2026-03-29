namespace BPlusTree.Samples.CustomerStore;

/// <summary>
/// Demonstrates the domain-wrapper pattern: BPlusTree hidden behind CustomerStoreWrapper.
/// Shows CRUD, navigation (First/Last/Next), range queries, and in-place updates.
/// </summary>
public static class CustomerStoreDemo
{
    public static void Run()
    {
        string dbPath  = Path.Combine(Path.GetTempPath(), "sample02.db");
        string walPath = Path.Combine(Path.GetTempPath(), "sample02.wal");
        File.Delete(dbPath);
        File.Delete(walPath);

        using var store = new CustomerStoreWrapper(dbPath, walPath);

        // ── Populate ──────────────────────────────────────────────────────────
        store.Save(1,  new Customer { FirstName = "Alice",   LastName = "Smith",   Age = 30, Email = "alice@example.com"   });
        store.Save(2,  new Customer { FirstName = "Bob",     LastName = "Jones",   Age = 25, Email = "bob@example.com"     });
        store.Save(3,  new Customer { FirstName = "Carol",   LastName = "Taylor",  Age = 35, Email = "carol@example.com"   });
        store.Save(10, new Customer { FirstName = "Dave",    LastName = "Brown",   Age = 40, Email = "dave@example.com"    });
        store.Save(11, new Customer { FirstName = "Eve",     LastName = "Davis",   Age = 22, Email = "eve@example.com"     });
        store.Save(20, new Customer { FirstName = "Frank",   LastName = "Wilson",  Age = 55, Email = "frank@example.com"   });

        Console.WriteLine($"Total customers: {store.Count}");

        // ── Point lookup ──────────────────────────────────────────────────────
        Customer? alice = store.Find(1);
        Console.WriteLine($"Find(1) → {alice}");

        // ── Conditional insert — TryAdd ───────────────────────────────────────
        bool added = store.TryAdd(1, new Customer { FirstName = "Duplicate" });
        Console.WriteLine($"TryAdd duplicate id=1: {added}");  // false

        bool addedNew = store.TryAdd(99, new Customer { FirstName = "Grace", LastName = "Lee", Age = 28, Email = "grace@example.com" });
        Console.WriteLine($"TryAdd new id=99:       {addedNew}");  // true

        // ── In-place update ───────────────────────────────────────────────────
        store.UpdateEmail(2, "bob.updated@example.com");
        store.IncrementAge(3);
        Console.WriteLine($"Bob's email after update: {store.Find(2)?.Email}");
        Console.WriteLine($"Carol's age after increment: {store.Find(3)?.Age}");

        // ── Navigation ────────────────────────────────────────────────────────
        var first = store.First();
        var last  = store.Last();
        Console.WriteLine($"First: id={first?.Id} {first?.Customer.FirstName}");
        Console.WriteLine($"Last:  id={last?.Id}  {last?.Customer.FirstName}");

        var nextAfter10 = store.Next(10);
        Console.WriteLine($"Next after id=10: id={nextAfter10?.Id} {nextAfter10?.Customer.FirstName}");

        // ── Range scan ────────────────────────────────────────────────────────
        Console.WriteLine("Customers with id 1–10:");
        foreach (var (id, c) in store.ListByIdRange(1, 10))
            Console.WriteLine($"  {id}: {c}");

        Console.WriteLine($"Count in range [1,10]: {store.CountRange(1, 10)}");

        // ── Delete ────────────────────────────────────────────────────────────
        store.Remove(99);
        Console.WriteLine($"Total after removing id=99: {store.Count}");

        SampleHelpers.TryDelete(dbPath);
        SampleHelpers.TryDelete(walPath);
    }
}
