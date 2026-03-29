namespace BPlusTree.Core.Wal;

public enum WalRecordType : byte
{
    Begin           = 1,
    Commit          = 2,
    Abort           = 3,
    AllocPage       = 4,
    FreePage        = 5,
    UpdatePage      = 6,   // carries full after-image
    CheckpointBegin = 7,
    CheckpointEnd   = 8,
    UpdateMeta          = 9,
    CompactionComplete  = 10,
    AllocOverflowChain  = 11,  // data = big-endian uint[] of overflow page IDs (chain order)
    AllocShadowChain    = 12,  // data = big-endian uint[] of CoW shadow page IDs
    FreeOverflowChain   = 13,  // data = big-endian uint[] of overflow page IDs to free (Gap 2)
}