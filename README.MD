# ByTech.BPlusTree

A disk-based, persistent B+ tree for .NET 10 — a sorted key-value store with
WAL-based durability, ACID transactions, snapshot isolation, online compaction,
and concurrent multi-writer support.

[![NuGet](https://img.shields.io/nuget/v/ByTech.BPlusTree.Core.svg?label=nuget&color=blue)](https://www.nuget.org/packages/ByTech.BPlusTree.Core)
[![NuGet Downloads](https://img.shields.io/nuget/dt/ByTech.BPlusTree.Core.svg?color=blue)](https://www.nuget.org/packages/ByTech.BPlusTree.Core)
[![.NET](https://img.shields.io/badge/.NET-10-512BD4)](#)
[![C%23](https://img.shields.io/badge/C%23-13-239120)](#)
[![Tests](https://img.shields.io/badge/tests-686-brightgreen)](#)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**The only .NET library combining a disk-based B+ tree with WAL-based durability, ACID transactions, and snapshot isolation.**
Existing alternatives ([CSharpTest.Net.BPlusTree](https://github.com/csharptest/CSharpTest.Net.Collections), [DataTanker](https://github.com/VictorScherbakov/DataTanker)) are either abandoned or lack transactions and WAL entirely.
[Microsoft FASTER](https://github.com/microsoft/FASTER) uses a hash index rather than a B+ tree and provides no snapshot isolation.

---

## Features

- **Sorted key-value store** — `TryGet`, `Scan`, `ScanReverse`, range queries, first/last/next/prev navigation
- **WAL durability** — write-ahead log with ARIES-style redo recovery; survives process crashes
- **ACID transactions** — `BeginTransaction()` / `BeginScope()` with commit and automatic rollback
- **Snapshot isolation** — `BeginSnapshot()` returns a frozen, point-in-time read view
- **Online compaction** — `Compact()` reclaims disk space from deleted pages while writers are active
- **Multi-writer concurrency** — commit-serialized model; multiple transactions execute write phases concurrently
- **Generic keys and values** — any type with a custom `IKeySerializer<T>` / `IValueSerializer<T>`
- **Built-in serializers** — `Int32Serializer`, `Int64Serializer`, `StringSerializer`, `GuidSerializer`, `DateTimeSerializer`
- **Large values** — transparent overflow-page chains for values that exceed a single page
- **Configurable** — page size, buffer pool capacity, fill factor, WAL sync mode, eviction watermarks

---

## Installation

```bash
dotnet add package ByTech.BPlusTree.Core
```

NuGet: [https://www.nuget.org/packages/ByTech.BPlusTree.Core](https://www.nuget.org/packages/ByTech.BPlusTree.Core)

---

## Quick Start

```csharp
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;

var options = new BPlusTreeOptions
{
    DataFilePath = "mystore.db",
    WalFilePath  = "mystore.wal",
};

using var store = BPlusTree<int, string>.Open(
    options,
    Int32Serializer.Instance,
    StringSerializer.Instance);

// Write
store.Put(1, "apple");
store.Put(2, "banana");
store.Put(3, "cherry");

// Read
if (store.TryGet(2, out string? value))
    Console.WriteLine(value);  // banana

// Scan ascending
foreach (var (key, val) in store)
    Console.WriteLine($"{key} → {val}");

// Range scan
foreach (var (key, val) in store.Scan(startKey: 1, endKey: 2))
    Console.WriteLine($"{key} → {val}");

Console.WriteLine($"Count: {store.Count}");  // 3
```

Data is flushed and fsync'd on `Dispose`. Reopening the file recovers any
committed writes from the WAL automatically.

---

## Transactions

Use `BeginScope()` for simple commit/rollback (synchronous or async):

```csharp
// Synchronous — Complete() commits; Dispose() without Complete() rolls back.
using var scope = store.BeginScope();
scope.Insert(10, "order-A");
scope.Insert(11, "order-B");
scope.Complete();  // all-or-nothing commit

// Async commit — fsync runs on a background thread.
await using var scope = store.BeginScope();
scope.Insert(20, "order-C");
scope.Complete();
// DisposeAsync() awaits the background fsync before returning.
```

Use `BeginTransaction()` for explicit control (useful when retrying on conflict):

```csharp
while (true)
{
    try
    {
        using var tx = store.BeginTransaction();
        tx.Insert(key, value);
        tx.Commit();
        break;
    }
    catch (TransactionConflictException)
    {
        // Another writer committed a conflicting root update — retry.
    }
}
```

---

## Snapshot Isolation

`BeginSnapshot()` returns a frozen, read-only view of the tree at the moment it
was opened. The live tree can be freely written to; the snapshot is unaffected.

```csharp
// Populate
store.Put(1, "v1");
store.Put(2, "v2");

// Open snapshot — sees keys 1 and 2.
using ISnapshot<int, string> snap = store.BeginSnapshot();

// Mutate live tree.
store.Put(3, "v3");
store.Delete(1);

// Snapshot still reflects the original state.
snap.TryGet(3, out _);  // false — key 3 added after snapshot
snap.TryGet(1, out _);  // true  — key 1 still present in snapshot
snap.Count;             // 2

// Live tree reflects all mutations.
store.Count;            // 2  (key 1 deleted, key 3 added)
```

---

## Custom Serializers

Implement `IKeySerializer<TKey>` or `IValueSerializer<TValue>` for custom types:

```csharp
public sealed class CustomerSerializer : IValueSerializer<Customer>
{
    public static readonly CustomerSerializer Instance = new();

    // Fixed-size type: declare the exact byte size here.
    // MeasureSize() defaults to FixedSize — no override needed.
    public int FixedSize => sizeof(int) + sizeof(int);  // Id + Age = 8 bytes

    public int Serialize(Customer value, Span<byte> destination)
    {
        BinaryPrimitives.WriteInt32LittleEndian(destination,       value.Id);
        BinaryPrimitives.WriteInt32LittleEndian(destination[4..],  value.Age);
        return FixedSize;
    }

    public Customer Deserialize(ReadOnlySpan<byte> source)
        => new Customer
        {
            Id  = BinaryPrimitives.ReadInt32LittleEndian(source),
            Age = BinaryPrimitives.ReadInt32LittleEndian(source[4..]),
        };
}

// Use it:
using var store = BPlusTree<int, Customer>.Open(
    options,
    Int32Serializer.Instance,
    CustomerSerializer.Instance);
```

For variable-length types (strings, byte arrays) set `FixedSize` to `-1` and
override `MeasureSize` to return the exact serialized byte count. Values larger
than roughly half a page are automatically stored in overflow page chains — no
size limit applies.

---

## Configuration

```csharp
var options = new BPlusTreeOptions
{
    DataFilePath        = "store.db",
    WalFilePath         = "store.wal",

    // Page size in bytes (must be a power of two, 4096–65536). Default: 8192.
    PageSize            = 8192,

    // Number of pages held in the in-process buffer pool. Default: 2048.
    BufferPoolCapacity  = 2048,

    // Dirty-page count that triggers an automatic checkpoint. Default: 256 pages.
    CheckpointThreshold = 256,

    // Leaf fill factor during initial bulk-load (0.50–0.95). Default: 0.70.
    FillFactor          = 0.70,

    // Synchronous: fsync on every checkpoint (max durability).
    // GroupCommit:  background thread fsyncs every FlushIntervalMs ms (higher throughput).
    SyncMode            = WalSyncMode.Synchronous,
    FlushIntervalMs     = 5,    // GroupCommit only
    FlushBatchSize      = 256,  // GroupCommit only
};
options.Validate();  // throws ArgumentException on invalid values
```

### Advanced / tuning options

| Property | Default | Description |
|---|---|---|
| `WalBufferSize` | `8 * 1024 * 1024` | In-memory WAL buffer in bytes. Must be ≥ `CheckpointThreshold × PageSize` to avoid mid-checkpoint overflows. |
| `WalAutoCheckpointThresholdBytes` | `0` (disabled) | WAL file size in bytes that triggers a background checkpoint. **Recommended for production: `64 * 1024 * 1024` (64 MB).** With the default of `0` the WAL grows unbounded; only manual `Flush()`/`Checkpoint()` calls keep it bounded. |
| `CoWWriteAmplification` | `3` | Expected dirty pages produced per logical write (≈ tree height H). Scales the eviction batch size so the eviction thread keeps pace with CoW write pressure. Set to H for your expected data volume. |
| `EvictionHighWatermark` | `0.85` | Fraction of pool frames in use at which the background eviction thread wakes. |
| `EvictionLowWatermark` | `0.70` | Fraction of pool frames in use at which the eviction thread stops evicting. |
| `EvictionBatchSize` | `32` | Maximum pages the eviction thread writes per wake-up. |
| `EvictionWaitTimeoutMs` | `5000` | Milliseconds `FetchPage` waits for a frame before throwing `BufferPoolExhaustedException`. Use `-1` for infinite wait. |

---

## Full API Reference

### Reads (available on `BPlusTree<TKey,TValue>`, `ISnapshot`, and `ITransaction`)

| Method | Description |
|--------|-------------|
| `TryGet(key, out value)` | Point lookup |
| `Scan(startKey?, endKey?)` | Forward range scan (inclusive, open-ended if omitted) |
| `ScanReverse(startKey?, endKey?)` | Reverse range scan |
| `TryGetFirst(out key, out value)` | Minimum key |
| `TryGetLast(out key, out value)` | Maximum key |
| `TryGetNext(key, out nextKey, out value)` | Successor key |
| `TryGetPrev(key, out prevKey, out value)` | Predecessor key |
| `Count` | Total record count |
| `CountRange(startKey?, endKey?)` | Count within range |

### Writes on `BPlusTree<TKey,TValue>`

| Method | Description |
|--------|-------------|
| `Put(key, value)` | Insert or overwrite |
| `Delete(key)` | Remove; returns `false` if key absent |
| `TryInsert(key, value)` | Insert only if absent; returns `false` if key exists |
| `TryUpdate(key, value)` | Update only if key exists; returns `false` if absent |
| `AddOrUpdate(key, value)` | Insert or update |
| `GetOrAdd(key, value)` | Return existing value or insert and return new one |
| `TryGetAndDelete(key, out value)` | Atomically read and delete |
| `TryCompareAndSwap(key, expected, newValue)` | CAS on value equality |
| `PutRange(IEnumerable<(TKey,TValue)>)` | Batch insert/overwrite |
| `DeleteRange(startKey?, endKey?)` | Batch delete within range (bounds optional) |

### Writes on `ITransaction` and `BPlusTreeScope`

`ITransaction` and `BPlusTreeScope` use slightly different method names from the
main tree (no `Put`/`Delete` — use `Insert`/`TryDelete` instead):

| Method | Description |
|--------|-------------|
| `Insert(key, value)` | Insert or overwrite (equivalent to `Put` on the main tree) |
| `TryDelete(key)` | Remove; returns `false` if key absent |
| `TryInsert(key, value)` | Insert only if absent; returns `false` if key exists |
| `TryUpdate(key, value)` | Update only if key exists; returns `false` if absent |
| `AddOrUpdate(key, addValue, updateFactory)` | Insert or update via factory |
| `GetOrAdd(key, value)` | Return existing value or insert and return new one |
| `TryGetAndDelete(key, out value)` | Atomically read and delete |
| `TryCompareAndSwap(key, expected, newValue)` | CAS on value equality |
| `InsertRange(IEnumerable<(TKey,TValue)>)` | Batch insert within transaction |
| `DeleteRange(startKey, endKey)` | Batch delete within range (both bounds required) |

### Tree management

| Method | Description |
|--------|-------------|
| `BeginTransaction()` | Returns `ITransaction` — Commit()/Dispose()-rollback |
| `BeginScope()` | Returns `BPlusTreeScope` — Complete()/using-rollback; `IAsyncDisposable` |
| `BeginSnapshot()` | Returns `ISnapshot` — frozen read view |
| `Compact()` | Online compaction — reclaims space from deleted pages |
| `Flush()` | Flush buffer pool and WAL to disk |
| `GetStatistics()` | Returns `TreeStatistics` (record count, page count, height, WAL size, etc.) |
| `Validate()` | Structural integrity check — key order, record count, separator alignment |

---

## Sample Projects

The [`samples/`](samples/BPlusTree.Samples) directory contains thirteen runnable demos:

| # | Sample | What it shows |
|---|--------|--------------|
| 01 | `SimpleKvStore` | Basic put/get/delete/scan, persistence across sessions |
| 02 | `CustomerStore` | Custom struct serializer, wrapper pattern |
| 03 | `EventLog` | Append-only log with time-range scans |
| 04 | `Transactions` | Atomic multi-record writes, rollback on exception, async commit |
| 05 | `Snapshot` | Point-in-time isolation — snapshot unaffected by live mutations |
| 06 | `GroupCommitThroughput` | Throughput comparison: Synchronous vs GroupCommit WAL mode |
| 07 | `CompareAndSwap` | Lock-free optimistic updates under multi-threaded contention |
| 08 | `AtomicCompoundOps` | `GetOrAdd` / `AddOrUpdate` / `TryUpdate(factory)` / `TryGetAndDelete` |
| 09 | `BulkLoad` | `PutRange` (atomic) vs loop-of-`Put` (per-row visible) |
| 10 | `RangeQueries` | `ScanReverse`, `TryGetFirst`/`Last`, `CountRange`, top-N idiom |
| 11 | `CrashRecovery` | WAL durability + transaction abort-on-dispose across restarts |
| 12 | `Maintenance` | `Compact()` + `GetStatistics()` + `Validate()` |
| 13 | `ReadOnlyDictionary` | `IReadOnlyDictionary<,>` consumption (live tree + snapshots) |

Run all demos:
```bash
cd samples/BPlusTree.Samples
dotnet run
```

---

## License

MIT — see [LICENSE](LICENSE).
