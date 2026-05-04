namespace ByTech.BPlusTree.Core.Api;

/// <summary>Controls WAL fsync behaviour.</summary>
public enum WalSyncMode
{
    Synchronous,  // fsync on every buffer overflow (default, safe)
    GroupCommit,  // background thread fsyncs every FlushIntervalMs or FlushBatchSize records
}
