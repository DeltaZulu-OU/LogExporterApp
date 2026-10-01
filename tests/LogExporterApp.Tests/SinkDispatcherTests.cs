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

    [TestMethod]
    public async Task SinkWorkerSurvivesRepeatedFailuresAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var sink = new RecoveringSink(failuresBeforeSuccess: 2);

        dispatcher.Add(sink, 16);

        for (var i = 1; i <= 3; i++)
        {
            await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
            await sink.WaitForAttemptAsync(i).WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        }

        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.AreEqual(3, sink.Attempts);
        Assert.AreEqual(1, sink.Successes);
    }

    [TestMethod]
    public async Task SinkErrorsAreRateLimitedAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var sink = new RecoveringSink(failuresBeforeSuccess: int.MaxValue);
        var errorCount = 0;

        dispatcher.Add(
            sink,
            16,
            _ => Interlocked.Increment(ref errorCount));

        for (var i = 1; i <= 3; i++)
        {
            await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
            await sink.WaitForAttemptAsync(i).WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        }

        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.AreEqual(1, errorCount);
    }

    [TestMethod]
    public async Task FailingSinkDoesNotAffectHealthySinkAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var failing = new RecoveringSink(failuresBeforeSuccess: int.MaxValue);
        using var healthy = new CountingSink(expectedCount: 3);

        dispatcher.Add(failing, 16);
        dispatcher.Add(healthy, 16);

        for (var i = 1; i <= 3; i++)
        {
            await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
            await failing.WaitForAttemptAsync(i).WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        }

        await healthy.ReachedExpectedCount.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.AreEqual(3, healthy.Count);
    }

    public TestContext TestContext { get; set; }
}
