using System;

namespace LogExporter.Sinks
{
    /// <summary>Immutable configuration for one sink's in-memory delivery worker.</summary>
    public sealed record SinkBatchOptions
    {
        public int QueueCapacity { get; init; } = 100000;
        public int BatchSize { get; init; } = 1000;
        public TimeSpan BatchDelay { get; init; } = TimeSpan.FromMilliseconds(25);
        public TimeSpan RetryTimeLimit { get; init; } = TimeSpan.FromSeconds(2);
        public TimeSpan InitialRetryDelay { get; init; } = TimeSpan.FromMilliseconds(50);
        public TimeSpan MaxRetryDelay { get; init; } = TimeSpan.FromMilliseconds(500);

        public void Validate()
        {
            if (QueueCapacity <= 0)
                throw new InvalidOperationException($"{nameof(QueueCapacity)} must be positive.");
            if (BatchSize <= 0)
                throw new InvalidOperationException($"{nameof(BatchSize)} must be positive.");
            if (BatchDelay <= TimeSpan.Zero)
                throw new InvalidOperationException($"{nameof(BatchDelay)} must be positive.");
            if (RetryTimeLimit < TimeSpan.Zero)
                throw new InvalidOperationException($"{nameof(RetryTimeLimit)} cannot be negative.");
            if (InitialRetryDelay <= TimeSpan.Zero)
                throw new InvalidOperationException($"{nameof(InitialRetryDelay)} must be positive.");
            if (MaxRetryDelay < InitialRetryDelay)
                throw new InvalidOperationException($"{nameof(MaxRetryDelay)} cannot be less than {nameof(InitialRetryDelay)}.");
        }
    }

    public readonly record struct SinkDeliveryCounters(
        long DroppedEvents,
        long SuccessfulEvents,
        long FailedEvents,
        long SuccessfulBatches,
        long RetriedAttempts,
        long FailedBatches);
}
