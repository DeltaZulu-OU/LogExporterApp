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
            if (QueueCapacity <= 0) throw new ArgumentOutOfRangeException(nameof(QueueCapacity));
            if (BatchSize <= 0) throw new ArgumentOutOfRangeException(nameof(BatchSize));
            if (BatchDelay <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(BatchDelay));
            if (RetryTimeLimit < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(RetryTimeLimit));
            if (InitialRetryDelay <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(InitialRetryDelay));
            if (MaxRetryDelay < InitialRetryDelay) throw new ArgumentOutOfRangeException(nameof(MaxRetryDelay));
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
