namespace ByTech.BPlusTree.Core.Api;

/// <summary>
/// Thrown when a transaction attempts to write a page that is already locked
/// for writing by a concurrent transaction (no-wait conflict detection).
///
/// The throwing transaction should be disposed (triggering rollback and lock
/// release) and the operation retried from the application level.
/// </summary>
public sealed class TransactionConflictException : BPlusTreeException
{
    /// <summary>The transaction that detected the conflict and was aborted.</summary>
    public uint TxId      { get; }

    /// <summary>The transaction that currently holds the write lock.</summary>
    public uint OwnerTxId { get; }

    /// <summary>The page on which the conflict was detected.</summary>
    public uint PageId    { get; }

    public TransactionConflictException(uint txId, uint ownerTxId, uint pageId)
        : base($"Transaction {txId} cannot acquire write lock on page {pageId}: " +
               $"held by transaction {ownerTxId}.")
    {
        TxId      = txId;
        OwnerTxId = ownerTxId;
        PageId    = pageId;
    }
}
