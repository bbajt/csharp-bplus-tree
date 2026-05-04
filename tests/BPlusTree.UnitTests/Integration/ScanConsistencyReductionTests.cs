using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Nodes;
using FluentAssertions;
using Xunit;

namespace ByTech.BPlusTree.Core.Tests.Integration;

/// <summary>
/// M139 P1 — pure-BPlusTree reduction of the M138-isolated scan/point-read
/// inconsistency.
///
/// The adapter-level repro at
/// <c>tests/ByTech.Bedrock.Replication.Partitioned.Tests/Adapters/BPlusTreeScanConsistencyTests.cs:Repeated_upsert_with_concurrent_deletes_across_reboot_remains_scannable</c>
/// deterministically loses a scan-reachable record after: reboot → repeated
/// upserts of one key with growing values → interleaved tombstone writes to
/// sibling keys.
///
/// This file reproduces the same shape directly on <see cref="BPlusTree{TKey,TValue}"/>
/// with no adapter wrapper, no versioning, no stripe lock, no codec layer.
///
/// Interpretation of results:
/// <list type="bullet">
///   <item>All pins fail → bug is in pure BPlusTree (engine / storage / WAL).</item>
///   <item>All pins pass → bug is in <c>BTreeOrderedStore</c> wrapper or the
///   adapter's versioning / codec / stripe-lock path.</item>
///   <item>Some fail, some pass → the failing subset pinpoints which condition
///   is the BPlusTree trigger.</item>
/// </list>
/// </summary>
public sealed class ScanConsistencyReductionTests : IDisposable
{
    private const int PageSize = 8192;

    private readonly string _dbPath  = Path.Combine(Path.GetTempPath(), $"bpt-reduction-{Guid.NewGuid():N}.db");
    private readonly string _walPath = Path.Combine(Path.GetTempPath(), $"bpt-reduction-{Guid.NewGuid():N}.wal");

    public void Dispose()
    {
        try { File.Delete(_dbPath); }  catch { }
        try { File.Delete(_walPath); } catch { }
    }

    private BPlusTree<string, byte[]> Open() => BPlusTree<string, byte[]>.Open(
        new BPlusTreeOptions
        {
            DataFilePath        = _dbPath,
            WalFilePath         = _walPath,
            PageSize            = PageSize,
            BufferPoolCapacity  = 2048,
            CheckpointThreshold = 128,
            SyncMode            = WalSyncMode.Synchronous,
        },
        StringSerializer.Instance, ByteArraySerializer.Instance);

    private static byte[] RandomBytes(int size)
    {
        var b = new byte[size]; Random.Shared.NextBytes(b); return b;
    }

    private static List<string> ScanKeys(BPlusTree<string, byte[]> tree)
    {
        var keys = new List<string>();
        foreach (var (k, _) in tree) keys.Add(k);
        return keys;
    }

    // ──────── R1 — baseline: repeated upsert with growing values, no reboot, no tombstones ────────

    [Fact]
    public void R1_Repeated_upsert_growing_value_remains_scannable()
    {
        using var tree = Open();
        const string target = "op.48e3a8b3-e88f-484a-9783-5ee8b84ad889";
        for (int i = 0; i < 15; i++)
            tree.Put(target, RandomBytes(208 + (i * 2)));

        tree.TryGet(target, out var point).Should().BeTrue();
        point.Should().NotBeNull();

        ScanKeys(tree).Should().Contain(target);
    }

    // ──────── R2 — add siblings ────────

    [Fact]
    public void R2_Upsert_growing_value_with_sibling_keys_remains_scannable()
    {
        using var tree = Open();
        string[] siblings =
        [
            "op.909fe8b7-f7b4-440c-8612-a1dbf3f3d3ba",
            "op.a5070bb3-5800-4700-ab33-2be6bc92f905",
            "op.cc8e5c2c-0ab5-4886-8007-4e8475fcd7ba",
            "op.de3a1cd3-993a-41ae-a12b-ecc06a16c9b9",
            "op.e7a473cf-a338-4468-9c52-a74bdd280225",
        ];
        foreach (var k in siblings) tree.Put(k, RandomBytes(200));

        const string target = "op.48e3a8b3-e88f-484a-9783-5ee8b84ad889";
        for (int i = 0; i < 15; i++)
            tree.Put(target, RandomBytes(208 + (i * 2)));

        tree.TryGet(target, out var point).Should().BeTrue();
        var scanned = ScanKeys(tree);
        foreach (var k in siblings) scanned.Should().Contain(k);
        scanned.Should().Contain(target);
    }

    // ──────── R3 — add tombstone shape (writes + explicit deletes to siblings) ────────
    //
    // Pure BPlusTree doesn't carry a tombstone marker; the adapter layer synthesises one
    // by storing zero-length values with the IsTombstone flag in VersionedValueCodec. The
    // closest pure-BPlusTree analogue is Delete(sibling) — actually removes the entry,
    // which is stronger than a tombstone write. If R3 fails but R2 passes, the Delete
    // path is the trigger.

    [Fact]
    public void R3_Upsert_growing_value_with_concurrent_sibling_deletes_remains_scannable()
    {
        using var tree = Open();
        string[] siblings =
        [
            "op.909fe8b7-f7b4-440c-8612-a1dbf3f3d3ba",
            "op.a5070bb3-5800-4700-ab33-2be6bc92f905",
            "op.cc8e5c2c-0ab5-4886-8007-4e8475fcd7ba",
            "op.de3a1cd3-993a-41ae-a12b-ecc06a16c9b9",
            "op.e7a473cf-a338-4468-9c52-a74bdd280225",
        ];
        string[] sidecars =
        [
            "topology.partition.0.transition",
            "topology.partition.1.transition",
            "topology.partition.2.transition",
            "topology.partition.3.transition",
            "topology.partition.4.transition",
        ];
        foreach (var k in siblings.Concat(sidecars)) tree.Put(k, RandomBytes(200));

        const string target = "op.48e3a8b3-e88f-484a-9783-5ee8b84ad889";
        for (int i = 0; i < 15; i++)
        {
            tree.Put(target, RandomBytes(208 + (i * 2)));
            if (i < sidecars.Length) tree.Delete(sidecars[i]);
        }

        tree.TryGet(target, out var point).Should().BeTrue();
        var scanned = ScanKeys(tree);
        foreach (var k in siblings) scanned.Should().Contain(k);
        scanned.Should().Contain(target);
    }

    // ──────── R4 — full shape: seed → close → reopen → upserts + deletes → scan ────────

    // ──────── R4a — same shape as R4, but scan via BeginSnapshot (what BTreeOrderedStore does) ────────

    [Fact]
    public void R4a_Reboot_then_upserts_with_concurrent_sibling_deletes_scans_via_snapshot()
    {
        string[] siblings =
        [
            "op.909fe8b7-f7b4-440c-8612-a1dbf3f3d3ba",
            "op.a5070bb3-5800-4700-ab33-2be6bc92f905",
            "op.cc8e5c2c-0ab5-4886-8007-4e8475fcd7ba",
            "op.de3a1cd3-993a-41ae-a12b-ecc06a16c9b9",
            "op.e7a473cf-a338-4468-9c52-a74bdd280225",
        ];
        string[] sidecars =
        [
            "topology.partition.0.transition",
            "topology.partition.1.transition",
            "topology.partition.2.transition",
            "topology.partition.3.transition",
            "topology.partition.4.transition",
        ];
        const string target = "op.48e3a8b3-e88f-484a-9783-5ee8b84ad889";

        using (var a = Open())
        {
            foreach (var k in siblings.Concat(sidecars).Append(target))
                a.Put(k, RandomBytes(200));
        }

        using (var b = Open())
        {
            for (int i = 0; i < 15; i++)
            {
                b.Put(target, RandomBytes(208 + (i * 2)));
                if (i < sidecars.Length) b.Delete(sidecars[i]);
            }

            // Scan via snapshot (mirrors BTreeOrderedStore.RangeScanAsync).
            var scannedViaSnapshot = new List<string>();
            using (var snap = b.BeginSnapshot())
            {
                foreach (var (k, _) in snap.Scan(null, null))
                    scannedViaSnapshot.Add(k);
            }

            b.TryGet(target, out var _).Should().BeTrue("post-reboot point-read must find the target");
            foreach (var k in siblings) scannedViaSnapshot.Should().Contain(k, $"sibling {k} must survive");
            scannedViaSnapshot.Should().Contain(target, "target must appear in snapshot.Scan(null, null)");
        }
    }

    // ──────── R5 — culture-sensitive compare reproduces in pure BPlusTree ────────
    //
    // R1–R4/R4a all pass because StringSerializer uses ordinal compare.
    // BTreeOrderedStore's CodecKeySerializer uses `x.CompareTo(y)` — culture-sensitive for string.
    // Prove the causal link by swapping StringSerializer for a culture-sensitive shim.

    private sealed class CultureSensitiveStringSerializer : IKeySerializer<string>, IValueSerializer<string>
    {
        public int FixedSize => -1;

        public int Serialize(string key, Span<byte> dst)
        {
            int byteCount = System.Text.Encoding.UTF8.GetByteCount(key);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(dst, (ushort)byteCount);
            System.Text.Encoding.UTF8.GetBytes(key, dst[2..]);
            return 2 + byteCount;
        }

        public string Deserialize(ReadOnlySpan<byte> src)
        {
            int length = System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(src);
            return System.Text.Encoding.UTF8.GetString(src.Slice(2, length));
        }

        // THE ONLY DIFFERENCE from StringSerializer: culture-sensitive compare.
        public int Compare(string x, string y) => x.CompareTo(y);

        public int MeasureSize(string key) => 2 + System.Text.Encoding.UTF8.GetByteCount(key);
        public int GetSerializedSize(ReadOnlySpan<byte> data)
            => 2 + System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(data);
    }

    [Fact]
    public void R5_Culture_sensitive_compare_reproduces_scan_inconsistency()
    {
        var tree = BPlusTree<string, byte[]>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = PageSize,
                BufferPoolCapacity  = 2048,
                CheckpointThreshold = 128,
                SyncMode            = WalSyncMode.Synchronous,
            },
            new CultureSensitiveStringSerializer(), ByteArraySerializer.Instance);

        string[] siblings =
        [
            "op.909fe8b7-f7b4-440c-8612-a1dbf3f3d3ba",
            "op.a5070bb3-5800-4700-ab33-2be6bc92f905",
            "op.cc8e5c2c-0ab5-4886-8007-4e8475fcd7ba",
            "op.de3a1cd3-993a-41ae-a12b-ecc06a16c9b9",
            "op.e7a473cf-a338-4468-9c52-a74bdd280225",
        ];
        string[] sidecars =
        [
            "topology.partition.0.transition",
            "topology.partition.1.transition",
            "topology.partition.2.transition",
            "topology.partition.3.transition",
            "topology.partition.4.transition",
        ];
        const string target = "op.48e3a8b3-e88f-484a-9783-5ee8b84ad889";

        foreach (var k in siblings.Concat(sidecars).Append(target))
            tree.Put(k, RandomBytes(200));

        for (int i = 0; i < 15; i++)
        {
            tree.Put(target, RandomBytes(208 + (i * 2)));
            if (i < sidecars.Length) tree.Delete(sidecars[i]);
        }

        tree.TryGet(target, out var _).Should().BeTrue("point-read must find the target");
        var scanned = ScanKeys(tree);
        tree.Dispose();

        foreach (var k in siblings) scanned.Should().Contain(k, $"sibling {k} must survive");
        scanned.Should().Contain(target, "target must remain scannable under culture-sensitive compare");
    }

    // ──────── R6 — match adapter's options + value-size wrapping ────────
    //
    // Adapter uses CheckpointThreshold=256 (default), not my 128. Adds ~40 bytes per value
    // via VersionedValueCodec wrapping. Simulates that with larger payloads (250..275).

    [Fact]
    public void R6_Adapter_shape_options_and_payload_sizes()
    {
        var tree = BPlusTree<string, byte[]>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 2048,
                CheckpointThreshold = BPlusTreeDefaults.CheckpointThreshold, // adapter default
                SyncMode            = WalSyncMode.Synchronous,
            },
            new CultureSensitiveStringSerializer(), ByteArraySerializer.Instance);

        string[] siblings =
        [
            "op.909fe8b7-f7b4-440c-8612-a1dbf3f3d3ba",
            "op.a5070bb3-5800-4700-ab33-2be6bc92f905",
            "op.cc8e5c2c-0ab5-4886-8007-4e8475fcd7ba",
            "op.de3a1cd3-993a-41ae-a12b-ecc06a16c9b9",
            "op.e7a473cf-a338-4468-9c52-a74bdd280225",
        ];
        string[] sidecars =
        [
            "topology.partition.0.transition",
            "topology.partition.1.transition",
            "topology.partition.2.transition",
            "topology.partition.3.transition",
            "topology.partition.4.transition",
        ];
        const string target = "op.48e3a8b3-e88f-484a-9783-5ee8b84ad889";

        foreach (var k in siblings.Concat(sidecars).Append(target))
            tree.Put(k, RandomBytes(240)); // initial size ≈ 200 + 40 (VersionedValueCodec overhead)

        for (int i = 0; i < 15; i++)
        {
            tree.Put(target, RandomBytes(248 + (i * 2))); // 248..276 — matches adapter wrapping
            if (i < sidecars.Length) tree.Delete(sidecars[i]);
        }

        tree.TryGet(target, out var _).Should().BeTrue("point-read must find the target");
        var scanned = ScanKeys(tree);
        tree.Dispose();

        foreach (var k in siblings) scanned.Should().Contain(k, $"sibling {k} must survive");
        scanned.Should().Contain(target, "target must remain scannable under adapter-matched options");
    }

    // ──────── R7 — tombstones as writes (not deletes) + snapshot scan ────────
    //
    // Adapter represents tombstones as PUTs with a tombstone flag in the payload
    // — NOT as tree deletes. Also scans via BeginSnapshot (R4a shape).

    [Fact]
    public void R7_Tombstones_as_writes_plus_snapshot_scan()
    {
        var tree = BPlusTree<string, byte[]>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 2048,
                CheckpointThreshold = BPlusTreeDefaults.CheckpointThreshold,
                SyncMode            = WalSyncMode.Synchronous,
            },
            new CultureSensitiveStringSerializer(), ByteArraySerializer.Instance);

        string[] siblings =
        [
            "op.909fe8b7-f7b4-440c-8612-a1dbf3f3d3ba",
            "op.a5070bb3-5800-4700-ab33-2be6bc92f905",
            "op.cc8e5c2c-0ab5-4886-8007-4e8475fcd7ba",
            "op.de3a1cd3-993a-41ae-a12b-ecc06a16c9b9",
            "op.e7a473cf-a338-4468-9c52-a74bdd280225",
        ];
        string[] sidecars =
        [
            "topology.partition.0.transition",
            "topology.partition.1.transition",
            "topology.partition.2.transition",
            "topology.partition.3.transition",
            "topology.partition.4.transition",
        ];
        const string target = "op.48e3a8b3-e88f-484a-9783-5ee8b84ad889";

        foreach (var k in siblings.Concat(sidecars).Append(target))
            tree.Put(k, RandomBytes(240));

        for (int i = 0; i < 15; i++)
        {
            tree.Put(target, RandomBytes(248 + (i * 2)));
            // Tombstone as PUT with small payload, NOT Delete.
            if (i < sidecars.Length) tree.Put(sidecars[i], RandomBytes(16));
        }

        tree.TryGet(target, out var _).Should().BeTrue();

        // Scan via snapshot (adapter path).
        var scannedViaSnapshot = new List<string>();
        using (var snap = tree.BeginSnapshot())
        {
            foreach (var (k, _) in snap.Scan(null, null))
                scannedViaSnapshot.Add(k);
        }
        tree.Dispose();

        foreach (var k in siblings) scannedViaSnapshot.Should().Contain(k);
        scannedViaSnapshot.Should().Contain(target, "target must remain scannable via snapshot with tombstones-as-writes");
    }

    // ──────── R8 — culture-sensitive compare + snapshot scan + P2Cut2c sizes ────────

    [Fact]
    public void R8_Culture_sensitive_plus_snapshot_scan_plus_cut2c_sizes()
    {
        var tree = BPlusTree<string, byte[]>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 2048,
                CheckpointThreshold = BPlusTreeDefaults.CheckpointThreshold,
                SyncMode            = WalSyncMode.Synchronous,
            },
            new CultureSensitiveStringSerializer(), ByteArraySerializer.Instance);

        string[] siblings =
        [
            "op.909fe8b7-f7b4-440c-8612-a1dbf3f3d3ba",
            "op.a5070bb3-5800-4700-ab33-2be6bc92f905",
            "op.cc8e5c2c-0ab5-4886-8007-4e8475fcd7ba",
            "op.de3a1cd3-993a-41ae-a12b-ecc06a16c9b9",
            "op.e7a473cf-a338-4468-9c52-a74bdd280225",
        ];
        string[] sidecars =
        [
            "topology.partition.0.transition",
            "topology.partition.1.transition",
            "topology.partition.2.transition",
            "topology.partition.3.transition",
            "topology.partition.4.transition",
        ];
        const string target = "op.48e3a8b3-e88f-484a-9783-5ee8b84ad889";

        foreach (var k in siblings.Concat(sidecars).Append(target))
            tree.Put(k, RandomBytes(249));

        for (int i = 0; i < 15; i++)
        {
            tree.Put(target, RandomBytes(248 + (i % 4) * 3));
            if (i < sidecars.Length) tree.Put(sidecars[i], RandomBytes(45));
        }

        // Snapshot scan (what BTreeOrderedStore does).
        var scanned = new List<string>();
        using (var snap = tree.BeginSnapshot())
        {
            foreach (var (k, _) in snap.Scan(null, null))
                scanned.Add(k);
        }
        tree.Dispose();

        foreach (var k in siblings) scanned.Should().Contain(k);
        scanned.Should().Contain(target, "culture-sensitive + snapshot scan + cut2c sizes — target must remain scannable");
    }

    // ──────── R9 — CodecKeySerializer-shaped (4-byte LE length prefix) ────────

    private sealed class CodecShapedStringSerializer : IKeySerializer<string>, IValueSerializer<string>
    {
        public int FixedSize => -1;
        public int Serialize(string key, Span<byte> dst)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(key);
            BitConverter.TryWriteBytes(dst, bytes.Length);
            bytes.AsSpan().CopyTo(dst[4..]);
            return 4 + bytes.Length;
        }
        public string Deserialize(ReadOnlySpan<byte> src)
        {
            int len = BitConverter.ToInt32(src);
            return System.Text.Encoding.UTF8.GetString(src.Slice(4, len));
        }
        public int Compare(string x, string y) => x.CompareTo(y); // culture-sensitive, matches CodecKeySerializer
        public int MeasureSize(string key) => 4 + System.Text.Encoding.UTF8.GetByteCount(key);
        public int GetSerializedSize(ReadOnlySpan<byte> data) => 4 + BitConverter.ToInt32(data);
    }

    [Fact]
    public void R9_CodecShaped_serializer_with_cut2c_sizes_plus_snapshot_scan()
    {
        var tree = BPlusTree<string, byte[]>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 2048,
                CheckpointThreshold = BPlusTreeDefaults.CheckpointThreshold,
                SyncMode            = WalSyncMode.Synchronous,
            },
            new CodecShapedStringSerializer(), ByteArraySerializer.Instance);

        string[] siblings =
        [
            "op.909fe8b7-f7b4-440c-8612-a1dbf3f3d3ba",
            "op.a5070bb3-5800-4700-ab33-2be6bc92f905",
            "op.cc8e5c2c-0ab5-4886-8007-4e8475fcd7ba",
            "op.de3a1cd3-993a-41ae-a12b-ecc06a16c9b9",
            "op.e7a473cf-a338-4468-9c52-a74bdd280225",
        ];
        string[] sidecars =
        [
            "topology.partition.0.transition",
            "topology.partition.1.transition",
            "topology.partition.2.transition",
            "topology.partition.3.transition",
            "topology.partition.4.transition",
        ];
        const string target = "op.48e3a8b3-e88f-484a-9783-5ee8b84ad889";

        foreach (var k in siblings.Concat(sidecars).Append(target))
            tree.Put(k, RandomBytes(249));

        for (int i = 0; i < 15; i++)
        {
            tree.Put(target, RandomBytes(248 + (i % 4) * 3));
            if (i < sidecars.Length) tree.Put(sidecars[i], RandomBytes(45));
        }

        var scanned = new List<string>();
        using (var snap = tree.BeginSnapshot())
        {
            foreach (var (k, _) in snap.Scan(null, null)) scanned.Add(k);
        }
        tree.Dispose();

        foreach (var k in siblings) scanned.Should().Contain(k);
        scanned.Should().Contain(target, "CodecKeySerializer-shaped key encoding + culture-sensitive compare + cut2c sizes — target must remain scannable");
    }

    // ──────── R10 — 4-byte LE length prefix but ORDINAL compare ────────

    private sealed class CodecShapedOrdinalSerializer : IKeySerializer<string>, IValueSerializer<string>
    {
        public int FixedSize => -1;
        public int Serialize(string key, Span<byte> dst)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(key);
            BitConverter.TryWriteBytes(dst, bytes.Length);
            bytes.AsSpan().CopyTo(dst[4..]);
            return 4 + bytes.Length;
        }
        public string Deserialize(ReadOnlySpan<byte> src)
        {
            int len = BitConverter.ToInt32(src);
            return System.Text.Encoding.UTF8.GetString(src.Slice(4, len));
        }
        public int Compare(string x, string y) => string.CompareOrdinal(x, y); // ORDINAL
        public int MeasureSize(string key) => 4 + System.Text.Encoding.UTF8.GetByteCount(key);
        public int GetSerializedSize(ReadOnlySpan<byte> data) => 4 + BitConverter.ToInt32(data);
    }

    [Fact]
    public void R10_CodecShaped_ordinal_compare_plus_cut2c_sizes_plus_snapshot_scan()
    {
        var tree = BPlusTree<string, byte[]>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 2048,
                CheckpointThreshold = BPlusTreeDefaults.CheckpointThreshold,
                SyncMode            = WalSyncMode.Synchronous,
            },
            new CodecShapedOrdinalSerializer(), ByteArraySerializer.Instance);

        string[] siblings =
        [
            "op.909fe8b7-f7b4-440c-8612-a1dbf3f3d3ba",
            "op.a5070bb3-5800-4700-ab33-2be6bc92f905",
            "op.cc8e5c2c-0ab5-4886-8007-4e8475fcd7ba",
            "op.de3a1cd3-993a-41ae-a12b-ecc06a16c9b9",
            "op.e7a473cf-a338-4468-9c52-a74bdd280225",
        ];
        string[] sidecars =
        [
            "topology.partition.0.transition",
            "topology.partition.1.transition",
            "topology.partition.2.transition",
            "topology.partition.3.transition",
            "topology.partition.4.transition",
        ];
        const string target = "op.48e3a8b3-e88f-484a-9783-5ee8b84ad889";

        foreach (var k in siblings.Concat(sidecars).Append(target))
            tree.Put(k, RandomBytes(249));

        for (int i = 0; i < 15; i++)
        {
            tree.Put(target, RandomBytes(248 + (i % 4) * 3));
            if (i < sidecars.Length) tree.Put(sidecars[i], RandomBytes(45));
        }

        var scanned = new List<string>();
        using (var snap = tree.BeginSnapshot())
        {
            foreach (var (k, _) in snap.Scan(null, null)) scanned.Add(k);
        }
        tree.Dispose();

        foreach (var k in siblings) scanned.Should().Contain(k);
        scanned.Should().Contain(target, "R10: 4-byte LE prefix + ORDINAL compare — does the fix need to be compare-side?");
    }

    // ──────── R11 — scan after every mutation to pinpoint disappearance ────────

    [Fact]
    public void R11_Narrow_when_target_disappears()
    {
        var tree = BPlusTree<string, byte[]>.Open(
            new BPlusTreeOptions
            {
                DataFilePath        = _dbPath,
                WalFilePath         = _walPath,
                PageSize            = 8192,
                BufferPoolCapacity  = 2048,
                CheckpointThreshold = BPlusTreeDefaults.CheckpointThreshold,
                SyncMode            = WalSyncMode.Synchronous,
            },
            new CodecShapedOrdinalSerializer(), ByteArraySerializer.Instance);

        string[] siblings =
        [
            "op.909fe8b7-f7b4-440c-8612-a1dbf3f3d3ba",
            "op.a5070bb3-5800-4700-ab33-2be6bc92f905",
            "op.cc8e5c2c-0ab5-4886-8007-4e8475fcd7ba",
            "op.de3a1cd3-993a-41ae-a12b-ecc06a16c9b9",
            "op.e7a473cf-a338-4468-9c52-a74bdd280225",
        ];
        string[] sidecars =
        [
            "topology.partition.0.transition",
            "topology.partition.1.transition",
            "topology.partition.2.transition",
            "topology.partition.3.transition",
            "topology.partition.4.transition",
        ];
        const string target = "op.48e3a8b3-e88f-484a-9783-5ee8b84ad889";

        foreach (var k in siblings.Concat(sidecars).Append(target))
            tree.Put(k, RandomBytes(249));

        // Verify seed visible.
        tree.TryGet(target, out _).Should().BeTrue("after seed, target is present");
        var postSeed = ScanKeys(tree);
        Console.Error.WriteLine($"DIAG post-seed: scan yields {postSeed.Count} keys, target present={postSeed.Contains(target)}");

        for (int i = 0; i < 15; i++)
        {
            int sz = 248 + (i % 4) * 3;
            tree.Put(target, RandomBytes(sz));
            bool pointHasTarget = tree.TryGet(target, out _);
            var scan = ScanKeys(tree);
            bool scanHasTarget = scan.Contains(target);
            Console.Error.WriteLine($"DIAG after target Put i={i} size={sz}: point={pointHasTarget} scan={scanHasTarget} scanCount={scan.Count}");

            if (i < sidecars.Length)
            {
                tree.Put(sidecars[i], RandomBytes(45));
                pointHasTarget = tree.TryGet(target, out _);
                scan = ScanKeys(tree);
                scanHasTarget = scan.Contains(target);
                Console.Error.WriteLine($"DIAG after side[{i}] Put: point={pointHasTarget} scan={scanHasTarget} scanCount={scan.Count}");
            }
        }
        tree.Dispose();
    }

    [Fact]
    public void R4_Reboot_then_upserts_with_concurrent_sibling_deletes_remains_scannable()
    {
        string[] siblings =
        [
            "op.909fe8b7-f7b4-440c-8612-a1dbf3f3d3ba",
            "op.a5070bb3-5800-4700-ab33-2be6bc92f905",
            "op.cc8e5c2c-0ab5-4886-8007-4e8475fcd7ba",
            "op.de3a1cd3-993a-41ae-a12b-ecc06a16c9b9",
            "op.e7a473cf-a338-4468-9c52-a74bdd280225",
        ];
        string[] sidecars =
        [
            "topology.partition.0.transition",
            "topology.partition.1.transition",
            "topology.partition.2.transition",
            "topology.partition.3.transition",
            "topology.partition.4.transition",
        ];
        const string target = "op.48e3a8b3-e88f-484a-9783-5ee8b84ad889";

        // Phase 1 — seed + close.
        using (var a = Open())
        {
            foreach (var k in siblings.Concat(sidecars).Append(target))
                a.Put(k, RandomBytes(200));
        }

        // Phase 2 — reopen + upserts interleaved with sibling deletes.
        using (var b = Open())
        {
            for (int i = 0; i < 15; i++)
            {
                b.Put(target, RandomBytes(208 + (i * 2)));
                if (i < sidecars.Length) b.Delete(sidecars[i]);
            }

            b.TryGet(target, out var point).Should().BeTrue(
                "post-reboot point-read must find the target");
            var scanned = ScanKeys(b);
            foreach (var k in siblings) scanned.Should().Contain(k, $"sibling {k} must survive");
            scanned.Should().Contain(target, "target must remain scannable after reboot + upserts + concurrent deletes");
        }
    }
}
