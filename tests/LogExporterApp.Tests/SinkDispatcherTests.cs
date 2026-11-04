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
            message => { if (message.Contains("delivery unconfirmed")) Interlocked.Increment(ref terminalMessages); });

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
        Assert.ThrowsExactly<InvalidOperationException>(() =>
            dispatcher.Add(sink, new SinkBatchOptions { BatchSize = 0 }));
    }

    [TestMethod]
    public async Task PartialWriteIsNotReplayedWithoutExplicitSinkOptInAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var sink = new PartialWriteSink();
        dispatcher.Add(sink, FastRetryOptions with { BatchSize = 2 });
        await dispatcher.DispatchAsync(new[] {
            TestFixtures.CreateLogEntry("one.example"),
            TestFixtures.CreateLogEntry("two.example")
        }, CancellationToken.None);
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.AreEqual(1, sink.Attempts);
        Assert.AreEqual("one.example", sink.Delivered[0]);
        Assert.AreEqual(1, sink.Delivered.Count);
        Assert.AreEqual(2L, dispatcher.GetCounters(typeof(PartialWriteSink)).FailedEvents);
    }

    [TestMethod]
    public async Task RetryPolicyRejectsPermanentFailureAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var sink = new PermanentFailureSink();
        dispatcher.Add(sink, FastRetryOptions);
        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.AreEqual(1, sink.Attempts);
        Assert.AreEqual(0L, dispatcher.GetCounters(typeof(PermanentFailureSink)).RetriedAttempts);
    }

    [TestMethod]
    public async Task CooperativeAttemptIsCanceledAtRetryDeadlineAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var sink = new DeadlineSink();
        dispatcher.Add(sink, FastRetryOptions with { RetryTimeLimit = TimeSpan.FromMilliseconds(80) });
        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.AreEqual(1, sink.Attempts);
        Assert.AreEqual(1L, dispatcher.GetCounters(typeof(DeadlineSink)).FailedEvents);
    }

    [TestMethod]
    public async Task QueueOverflowIsCountedExactlyAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var blocked = new BlockingSink();
        dispatcher.Add(blocked, FastRetryOptions with { QueueCapacity = 1 });

        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await blocked.Started.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(3), CancellationToken.None);

        Assert.AreEqual(2L, dispatcher.GetCounters(typeof(BlockingSink)).DroppedEvents);
        blocked.Release.TrySetResult();
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
    }

    [TestMethod]
    public async Task CountersSurviveRemovingSinkAndRegisteringReplacementAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var first = new CountingSink(expectedCount: 1);
        dispatcher.Add(first, FastRetryOptions);
        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await first.ReachedExpectedCount.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        dispatcher.Remove(typeof(CountingSink));

        using var second = new CountingSink(expectedCount: 1);
        dispatcher.Add(second, FastRetryOptions);
        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.AreEqual(2L, dispatcher.GetCounters(typeof(CountingSink)).SuccessfulEvents);
    }

    [TestMethod]
    public void FailedRegistrationDisposesOwnedSink()
    {
        using var dispatcher = new SinkDispatcher();
        using var sink = new DisposalTrackingSink();
        Assert.ThrowsExactly<InvalidOperationException>(() =>
            dispatcher.Add(sink, new SinkBatchOptions { BatchSize = 0 }));
        Assert.IsTrue(sink.WasDisposed);
    }

    [TestMethod]
    public async Task CancellationDuringBackoffPreventsAnotherAttemptAsync()
    {
        var dispatcher = new SinkDispatcher();
        using var sink = new RecoveringSink(failuresBeforeSuccess: int.MaxValue);
        dispatcher.Add(sink, FastRetryOptions with
        {
            InitialRetryDelay = TimeSpan.FromSeconds(1),
            MaxRetryDelay = TimeSpan.FromSeconds(1),
            RetryTimeLimit = TimeSpan.FromSeconds(5)
        });
        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(1), CancellationToken.None);
        await sink.WaitForAttemptAsync(1).WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        Assert.IsFalse(await dispatcher.DrainAsync(TimeSpan.FromMilliseconds(50)));
        dispatcher.Dispose();
        Assert.AreEqual(1, sink.Attempts);
    }

    [TestMethod]
    public async Task ConfiguredBatchLimitIsRespectedAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var sink = new BatchSizeRecordingSink();
        dispatcher.Add(sink, FastRetryOptions with { BatchSize = 2 });
        await dispatcher.DispatchAsync(TestFixtures.CreateBatch(5), CancellationToken.None);
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);

        CollectionAssert.AreEqual(new[] { 2, 2, 1 }, sink.BatchSizes);
    }

    private sealed class BatchSizeRecordingSink : IOutputSink
    {
        public List<int> BatchSizes { get; } = new();
        public Task ExportAsync(IReadOnlyList<LogExporter.LogEntry> logs, CancellationToken token)
        {
            BatchSizes.Add(logs.Count);
            return Task.CompletedTask;
        }
        public void Dispose() { }
    }

    private sealed class DisposalTrackingSink : IOutputSink
    {
        public bool WasDisposed { get; private set; }
        public Task ExportAsync(IReadOnlyList<LogExporter.LogEntry> logs, CancellationToken token) =>
            Task.CompletedTask;
        public void Dispose() => WasDisposed = true;
    }

    private sealed class PartialWriteSink : IOutputSink
    {
        public int Attempts { get; private set; }
        public List<string> Delivered { get; } = new();
        public Task ExportAsync(IReadOnlyList<LogExporter.LogEntry> logs, CancellationToken token)
        {
            Attempts++;
            Delivered.Add(logs[0].Question!.QuestionName!);
            throw new IOException("Simulated partial write");
        }
        public void Dispose() { }
    }

    private sealed class PermanentFailureSink : IOutputSink, IBatchRetryPolicy
    {
        public int Attempts { get; private set; }
        public bool CanRetry(Exception exception) => false;
        public Task ExportAsync(IReadOnlyList<LogExporter.LogEntry> logs, CancellationToken token)
        {
            Attempts++;
            throw new InvalidOperationException("Permanent failure");
        }
        public void Dispose() { }
    }

    private sealed class DeadlineSink : IOutputSink, IBatchRetryPolicy
    {
        private int _attempts;
        public int Attempts => Volatile.Read(ref _attempts);
        public bool CanRetry(Exception exception) => true;
        public async Task ExportAsync(IReadOnlyList<LogExporter.LogEntry> logs, CancellationToken token)
        {
            Interlocked.Increment(ref _attempts);
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
        }
        public void Dispose() { }
    }

    public TestContext TestContext { get; set; }
}
