using LogForwarder.Sinks;
using Microsoft.Extensions.Time.Testing;

namespace LogForwarderApp.Tests;

[TestClass]
public sealed class SinkDispatcherTimingTests
{
    private static SinkBatchOptions Options => new()
    {
        QueueCapacity = 16,
        BatchSize = 1,
        BatchDelay = TimeSpan.FromMilliseconds(10),
        RetryTimeLimit = TimeSpan.FromSeconds(2),
        InitialRetryDelay = TimeSpan.FromMilliseconds(100),
        MaxRetryDelay = TimeSpan.FromMilliseconds(500)
    };

    [TestMethod]
    public async Task RetriesFollowBackoffWithoutWaitingForWallClockAsync()
    {
        var clock = new FakeTimeProvider();
        using var dispatcher = new SinkDispatcher(clock, () => 0.5);
        using var sink = new RecoveringSink(failuresBeforeSuccess: 2);
        dispatcher.Add(sink, Options);

        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await sink.WaitForAttemptAsync(1).WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        clock.Advance(TimeSpan.FromMilliseconds(99));
        Assert.AreEqual(1, sink.Attempts);
        clock.Advance(TimeSpan.FromMilliseconds(1));
        await sink.WaitForAttemptAsync(2).WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        clock.Advance(TimeSpan.FromMilliseconds(199));
        Assert.AreEqual(2, sink.Attempts);
        clock.Advance(TimeSpan.FromMilliseconds(1));
        await sink.WaitForAttemptAsync(3).WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        Assert.AreEqual(3, sink.Attempts);
        Assert.AreEqual(2L, dispatcher.GetCounters(typeof(RecoveringSink)).RetriedAttempts);
        Assert.AreEqual(1L, dispatcher.GetCounters(typeof(RecoveringSink)).SuccessfulEvents);
    }

    [TestMethod]
    public async Task RetryDeadlineUsesInjectedClockAsync()
    {
        var clock = new FakeTimeProvider();
        using var dispatcher = new SinkDispatcher(clock, () => 0.5);
        using var sink = new ClockBlockedSink();
        dispatcher.Add(sink, Options with { RetryTimeLimit = TimeSpan.FromSeconds(1) });

        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await sink.Started.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        clock.Advance(TimeSpan.FromMilliseconds(999));
        Assert.IsFalse(dispatcher.GetCounters(typeof(ClockBlockedSink)).FailedEvents > 0);
        clock.Advance(TimeSpan.FromMilliseconds(1));

        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        Assert.AreEqual(1L, dispatcher.GetCounters(typeof(ClockBlockedSink)).FailedEvents);
        Assert.AreEqual(1, sink.Attempts);
    }

    [TestMethod]
    public async Task RetryBudgetZeroMakesOnlyOneAttemptAsync()
    {
        var clock = new FakeTimeProvider();
        using var dispatcher = new SinkDispatcher(clock, () => 0.5);
        using var sink = new RecoveringSink(failuresBeforeSuccess: int.MaxValue);
        dispatcher.Add(sink, Options with { RetryTimeLimit = TimeSpan.Zero });

        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.AreEqual(1, sink.Attempts);
        Assert.AreEqual(0L, dispatcher.GetCounters(typeof(RecoveringSink)).RetriedAttempts);
        Assert.AreEqual(1L, dispatcher.GetCounters(typeof(RecoveringSink)).FailedEvents);
    }

    private sealed class ClockBlockedSink : IOutputSink, IBatchRetryPolicy
    {
        public int Attempts { get; private set; }
        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool CanRetry(Exception exception) => true;

        public async Task ForwardAsync(IReadOnlyList<LogForwarder.LogEntry> logs, CancellationToken token)
        {
            Attempts++;
            Started.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
        }

        public void Dispose() { }
    }

    public TestContext TestContext { get; set; }
}
