using System;

namespace BPlusTree.Core.Api;

/// <summary>
/// Exception thrown when the buffer pool is exhausted and no more frames can be pinned.
/// </summary>
public sealed class BufferPoolExhaustedException : BPlusTreeException
{
    /// <summary>
    /// Initializes a new instance of the BufferPoolExhaustedException class.
    /// </summary>
    public BufferPoolExhaustedException()
        : base("Buffer pool is exhausted. No more frames available.")
    {
    }

    /// <summary>
    /// Initializes a new instance of the BufferPoolExhaustedException class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public BufferPoolExhaustedException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the BufferPoolExhaustedException class with a specified error message and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public BufferPoolExhaustedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}