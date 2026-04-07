namespace BPlusTree.Core.Api;

/// <summary>Result of a tree structure validation.</summary>
public sealed class ValidationResult
{
    /// <summary>True when no invariant violations were found.</summary>
    public bool IsValid => _errors.Count == 0;

    /// <summary>
    /// All invariant violations found in a single validation pass.
    /// Empty when <see cref="IsValid"/> is true.
    /// </summary>
    public IReadOnlyList<string> Errors => _errors;

    /// <summary>
    /// The first error, or null if valid. Preserved for backward compatibility
    /// with call sites that check a single error message.
    /// </summary>
    public string? Error => _errors.Count > 0 ? _errors[0] : null;

    private readonly IReadOnlyList<string> _errors;

    private ValidationResult(List<string> errors) => _errors = errors;

    /// <summary>Shared singleton for a valid result — zero allocation.</summary>
    public static readonly ValidationResult Valid = new(new List<string>());

    /// <summary>Returns a failed result. Returns <see cref="Valid"/> if the list is empty.</summary>
    internal static ValidationResult WithErrors(List<string> errors)
        => errors.Count == 0 ? Valid : new ValidationResult(errors);
}
