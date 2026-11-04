using System;

namespace LogForwarder.Sinks
{
    /// <summary>
    /// Explicit opt-in to replaying an entire failed batch. Implementations must guarantee
    /// that retrying after an ambiguous failure cannot produce unacceptable duplicates.
    /// </summary>
    public interface IBatchRetryPolicy
    {
        /// <summary>Whether this sink explicitly permits whole-batch replay.</summary>
        bool IsReplayEnabled => true;

        bool CanRetry(Exception exception);
    }
}
