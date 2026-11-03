using LogExporter.Sinks;

namespace LogExporterApp.Tests;

[TestClass]
public sealed class SinkDispatcherTests
{
    [TestMethod]
    public async Task SlowSinkDoesNotBlockFastSinkAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var slow = new BlockingSink();
        using var fast = new CountingSink(expectedCount: 1);

        dispatcher.Add(slow, 16);
        dispatcher.Add(fast, 16);

        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);

        await slow.Started.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        await fast.ReachedExpectedCount.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.AreEqual(1, fast.Count);
        Assert.IsFalse(slow.Release.Task.IsCompleted);

        slow.Release.TrySetResult();
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
    }

    [TestMethod]
    public async Task SaturatedSinkDoesNotDropOtherSinkAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var slow = new BlockingSink();
        using var fast = new CountingSink(expectedCount: 4);

        dispatcher.Add(slow, 1);
        dispatcher.Add(fast, 16);

        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await slow.Started.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        for (var i = 0; i < 3; i++)
        {
            await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        }

        await fast.ReachedExpectedCount.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        slow.Release.TrySetResult();
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.AreEqual(4, fast.Count);
        Assert.IsLessThan(fast.Count, slow.Count);
    }

    [TestMethod]
    public async Task DrainWaitsForAcceptedWorkAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var slow = new BlockingSink();

        dispatcher.Add(slow, 16);

        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await slow.Started.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        var drain = dispatcher.DrainAsync();

        await Task.Delay(50, TestContext.CancellationToken);
        Assert.IsFalse(drain.IsCompleted);

        slow.Release.TrySetResult();

        await drain.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        Assert.AreEqual(1, slow.Count);
    }

    [TestMethod]
    public async Task BoundedDrainAbortsBlockedSinkAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var slow = new BlockingSink();

        dispatcher.Add(slow, 16);
        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await slow.Started.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        var drained = await dispatcher
            .DrainAsync(TimeSpan.FromMilliseconds(50))
            .WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.IsFalse(drained);
    }

    private static SinkBatchOptions FastRetryOptions => new()
    {
        QueueCapacity = 16,
        BatchSize = 1,
        BatchDelay = TimeSpan.FromMilliseconds(10),
        RetryTimeLimit = TimeSpan.FromMilliseconds(150),
        InitialRetryDelay = TimeSpan.FromMilliseconds(5),
        MaxRetryDelay = TimeSpan.FromMilliseconds(20)
    };

    [TestMethod]
    public async Task FailedBatchIsRetriedWithoutNewEnqueueAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var sink = new RecoveringSink(failuresBeforeSuccess: 2);
        dispatcher.Add(sink, FastRetryOptions);

        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.AreEqual(3, sink.Attempts);
        Assert.AreEqual(1, sink.Successes);
        var counters = dispatcher.GetCounters(typeof(RecoveringSink));
        Assert.AreEqual(2L, counters.RetriedAttempts);
        Assert.AreEqual(1L, counters.SuccessfulEvents);
        Assert.AreEqual(0L, counters.FailedEvents);
    }

    [TestMethod]
    public async Task TerminalFailuresAreCountedAndReportedAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var sink = new RecoveringSink(failuresBeforeSuccess: int.MaxValue);
        var errors = 0;
        var terminalMessages = 0;
        dispatcher.Add(sink, FastRetryOptions with { RetryTimeLimit = TimeSpan.FromMilliseconds(30) },
            _ => Interlocked.Increment(ref errors),
            message => { if (message.Contains("permanently lost")) Interlocked.Increment(ref terminalMessages); });

        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.IsTrue(errors >= 1);
        Assert.AreEqual(1, terminalMessages);
        var counters = dispatcher.GetCounters(typeof(RecoveringSink));
        Assert.AreEqual(1L, counters.FailedEvents);
        Assert.AreEqual(1L, counters.FailedBatches);
        Assert.AreEqual(0L, counters.SuccessfulEvents);
    }

    [TestMethod]
    public async Task RetryDoesNotBlockAnotherSinkAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var failing = new RecoveringSink(failuresBeforeSuccess: int.MaxValue);
        using var healthy = new CountingSink(expectedCount: 1);
        dispatcher.Add(failing, FastRetryOptions with { RetryTimeLimit = TimeSpan.FromMilliseconds(200) });
        dispatcher.Add(healthy, FastRetryOptions);

        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await healthy.ReachedExpectedCount.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        Assert.AreEqual(1, healthy.Count);

        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        Assert.AreEqual(1L, dispatcher.GetCounters(typeof(RecoveringSink)).FailedEvents);
    }

    [TestMethod]
    public void InvalidBatchOptionsAreRejected()
    {
        using var dispatcher = new SinkDispatcher();
        using var sink = new CountingSink(expectedCount: 1);
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            dispatcher.Add(sink, new SinkBatchOptions { BatchSize = 0 }));
    }

    public TestContext TestContext { get; set; }
}
