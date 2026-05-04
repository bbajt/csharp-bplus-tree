using System;

namespace ByTech.BPlusTree.Core.Api;

/// <summary>
/// Exception thrown when a page is not found in the storage file.
/// </summary>
public sealed class PageNotFoundException : BPlusTreeException
{
    /// <summary>
    /// Initializes a new instance of the PageNotFoundException class.
    /// </summary>
    public PageNotFoundException()
        : base("Page not found in storage file.")
    {
    }

    /// <summary>
    /// Initializes a new instance of the PageNotFoundException class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public PageNotFoundException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the PageNotFoundException class with a specified error message and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public PageNotFoundException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}