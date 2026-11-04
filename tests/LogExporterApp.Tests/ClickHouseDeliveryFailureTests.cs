using LogExporter.Sinks;
using Serilog.Core;
using Serilog.Events;

namespace LogExporterApp.Tests;

/// <summary>
/// Fault-injection contract tests for the ClickHouse adapter and the existing
/// dispatcher. These do not simulate the ClickHouse wire protocol or guarantee
/// server-side deduplication.
/// </summary>
[TestClass]
public sealed class ClickHouseDeliveryFailureTests
{
    private static SinkBatchOptions Options => new()
    {
        QueueCapacity = 16,
        BatchSize = 1,
        BatchDelay = TimeSpan.FromMilliseconds(10),
        RetryTimeLimit = TimeSpan.FromMilliseconds(50),
        InitialRetryDelay = TimeSpan.FromMilliseconds(1),
        MaxRetryDelay = TimeSpan.FromMilliseconds(10)
    };

    [TestMethod]
    public async Task AmbiguousAcknowledgmentDoesNotReplayCommittedBatchAsync()
    {
        var upstream = new AmbiguousCommitSink();
        using var dispatcher = new SinkDispatcher();
        var adapter = new ClickHouseSink("Host=localhost;Port=8123", "dns", "events", upstream);
        dispatcher.Add(adapter, Options);

        await dispatcher.DispatchAsync(
            [TestFixtures.CreateLogEntry("ambiguous.example")],
            TestContext.CancellationToken);
        await dispatcher.DrainAsync()
            .WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.AreEqual(1, upstream.Attempts);
        Assert.HasCount(1, upstream.CommittedEvents);
        Assert.AreEqual("ambiguous.example",
            (upstream.CommittedEvents[0].Properties["QName"] as ScalarValue)?.Value as string);

        var counters = dispatcher.GetCounters(typeof(ClickHouseSink));
        Assert.AreEqual(0L, counters.RetriedAttempts);
        Assert.AreEqual(0L, counters.SuccessfulEvents);
        Assert.AreEqual(1L, counters.FailedEvents);
        Assert.AreEqual(1L, counters.FailedBatches);
    }

    [TestMethod]
    public async Task DrainDeadlineCannotCancelAnAlreadyStartedUpstreamInsertAsync()
    {
        var upstream = new PendingInsertSink();
        var dispatcher = new SinkDispatcher();
        try
        {
            var adapter = new ClickHouseSink("Host=localhost;Port=8123",
                "dns", "events", upstream);
            dispatcher.Add(adapter, Options);

            await dispatcher.DispatchAsync(
                [TestFixtures.CreateLogEntry("pending.example")],
                TestContext.CancellationToken);
            await upstream.Started.Task
                .WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

            var drained = await dispatcher.DrainAsync(TimeSpan.FromMilliseconds(30))
                .WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

            Assert.IsFalse(drained);
            Assert.AreEqual(1, upstream.Attempts);
            Assert.AreEqual(0L,
                dispatcher.GetCounters(typeof(ClickHouseSink)).RetriedAttempts);
            Assert.IsFalse(upstream.Finished.Task.IsCompleted,
                "The upstream Serilog API exposes no cancellation token.");
        }
        finally
        {
            // Always release the fake insert before dispatcher disposal. Real
            // noncooperative I/O may outlive the shutdown deadline.
            upstream.Complete();
            await upstream.Finished.Task
                .WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
            dispatcher.Dispose();
        }

        Assert.AreEqual(1, upstream.Attempts);
    }

    private sealed class AmbiguousCommitSink : IBatchedLogEventSink
    {
        public int Attempts { get; private set; }
        public List<LogEvent> CommittedEvents { get; } = [];

        public Task EmitBatchAsync(IReadOnlyCollection<LogEvent> batch)
        {
            Attempts++;
            CommittedEvents.AddRange(batch);
            throw new IOException("Simulated lost acknowledgment after commit.");
        }

        public Task OnEmptyBatchAsync() => Task.CompletedTask;
    }

    private sealed class PendingInsertSink : IBatchedLogEventSink
    {
        private readonly TaskCompletionSource _complete =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Finished { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Attempts { get; private set; }

        public async Task EmitBatchAsync(IReadOnlyCollection<LogEvent> batch)
        {
            Attempts++;
            Started.TrySetResult();
            try
            {
                await _complete.Task;
            }
            finally
            {
                Finished.TrySetResult();
            }
        }

        public void Complete() => _complete.TrySetResult();
        public Task OnEmptyBatchAsync() => Task.CompletedTask;
    }

    public TestContext TestContext { get; set; }
}
