using System;

namespace LogExporter.Sinks
{
    /// <summary>
    /// Explicit opt-in to replaying an entire failed batch. Implementations must guarantee
    /// that retrying after an ambiguous failure cannot produce unacceptable duplicates.
    /// </summary>
    public interface IBatchRetryPolicy
    {
        bool CanRetry(Exception exception);
    }
}
