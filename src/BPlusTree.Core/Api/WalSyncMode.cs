namespace BPlusTree.Core.Api;

public enum WalSyncMode
{
    Synchronous,  // fsync on every buffer overflow (default, safe)
    GroupCommit,  // background thread fsyncs every FlushIntervalMs or FlushBatchSize records
}
