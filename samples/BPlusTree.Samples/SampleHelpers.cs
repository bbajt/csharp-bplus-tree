namespace BPlusTree.Samples;

/// <summary>Shared utilities used across all sample demos.</summary>
internal static class SampleHelpers
{
    /// <summary>
    /// Delete a file, retrying up to 3 times (100 ms apart) if the attempt fails
    /// (e.g. the eviction worker thread hasn't fully released the handle yet).
    /// Silently ignores failures — sample cleanup is best-effort.
    /// </summary>
    public static void TryDelete(string path)
    {
        for (int attempt = 0; attempt < 3; attempt++)
        {
            try
            {
                File.Delete(path);
                return;
            }
            catch (IOException)
            {
                Thread.Sleep(100);
            }
            catch { return; }
        }
    }
}
