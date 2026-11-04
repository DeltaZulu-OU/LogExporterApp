using LogForwarder.Sinks;
using Serilog.Core;
using Serilog.Events;

namespace LogForwarderApp.Tests;

[TestClass]
public sealed class ClickHouseSinkTests
{
    [TestMethod]
    public async Task ConvertsQueryLogsToSerilogEventsAsync()
    {
        var recording = new RecordingSink();
        using var sink = new ClickHouseSink("Host=localhost;Port=8123;Database=dns",
            "dns", "events", recording);

        await sink.ForwardAsync([
            TestFixtures.CreateLogEntry("first.example"),
            TestFixtures.CreateLogEntry("second.example")
        ], TestContext.CancellationToken);

        Assert.AreEqual(1, recording.Batches);
        Assert.IsNotNull(recording.LastBatch);
        Assert.AreEqual(2, recording.LastBatch.Length);
        Assert.AreEqual(LogEventLevel.Information, recording.LastBatch[0].Level);
        Assert.AreEqual("first.example", Scalar(recording.LastBatch[0], "QName"));
        Assert.AreEqual("second.example", Scalar(recording.LastBatch[1], "QName"));
        var payload = Scalar(recording.LastBatch[0], "PayloadJson")!;
        StringAssert.Contains(payload, "first.example");
    }

    [TestMethod]
    public async Task PropagatesFailuresForDispatcherAccountingAsync()
    {
        var recording = new RecordingSink { Failure = new InvalidOperationException("Insert failed") };
        using var sink = new ClickHouseSink("Host=localhost;Port=8123",
            "dns", "events", recording);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            sink.ForwardAsync([TestFixtures.CreateLogEntry("failure.example")],
                TestContext.CancellationToken));
        Assert.AreEqual("Insert failed", exception.Message);
    }

    [TestMethod]
    public async Task DoesNotForwardEmptyOrCanceledBatchesAsync()
    {
        var recording = new RecordingSink();
        using var sink = new ClickHouseSink("Host=localhost;Port=8123",
            "dns", "events", recording);

        await sink.ForwardAsync([], TestContext.CancellationToken);
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(() =>
            sink.ForwardAsync([TestFixtures.CreateLogEntry("canceled.example")],
                canceled.Token));
        Assert.AreEqual(0, recording.Batches);
    }

    [TestMethod]
    public async Task PreservesTypedFieldsAndOriginalMetadataAsync()
    {
        var record = TestFixtures.CreateLogEntry("dns.example");
        record.Meta["tenant"] = "team-a";
        var recording = new RecordingSink();
        using var sink = new ClickHouseSink("Host=localhost;Port=8123",
            "dns", "events", recording);

        await sink.ForwardAsync([record], TestContext.CancellationToken);

        Assert.IsNotNull(recording.LastBatch);
        var evt = recording.LastBatch[0];
        Assert.AreEqual(new DateTimeOffset(record.Timestamp.ToUniversalTime()), evt.Timestamp);
        Assert.AreEqual(LogEventLevel.Information, evt.Level);
        Assert.AreEqual(record.ClientIp, Scalar(evt, "ClientIp"));
        Assert.AreEqual(record.Question!.QuestionName, Scalar(evt, "QName"));
        Assert.AreEqual(record.Question.QuestionType.ToString(), Scalar(evt, "QType"));
        Assert.AreEqual(record.ResponseCode.ToString(), Scalar(evt, "ResponseCode"));
        Assert.AreEqual(record.ResponseType.ToString(), Scalar(evt, "ResponseType"));
        using var json = System.Text.Json.JsonDocument.Parse(Scalar(evt, "PayloadJson")!);
        Assert.AreEqual("team-a",
            json.RootElement.GetProperty("meta").GetProperty("tenant").GetString());
        Assert.AreEqual(record.ClientIp,
            json.RootElement.GetProperty("clientIp").GetString());
    }

    [TestMethod]
    public async Task RejectsNullBatchesAndNullEntriesWithoutEmissionAsync()
    {
        var recording = new RecordingSink();
        using var sink = new ClickHouseSink("Host=localhost;Port=8123",
            "dns", "events", recording);

        await Assert.ThrowsExactlyAsync<ArgumentNullException>(() =>
            sink.ForwardAsync(null!, TestContext.CancellationToken));
        await Assert.ThrowsExactlyAsync<ArgumentNullException>(() =>
            sink.ForwardAsync([null!], TestContext.CancellationToken));
        Assert.AreEqual(0, recording.Batches);
    }

    [TestMethod]
    public void RejectsMissingRequiredConstructorArguments()
    {
        var recording = new RecordingSink();
        foreach (var value in new[] { "", "  " })
        {
            Assert.ThrowsExactly<ArgumentException>(() =>
                new ClickHouseSink(value, "dns", "events", recording));
            Assert.ThrowsExactly<ArgumentException>(() =>
                new ClickHouseSink("Host=localhost;Port=8123", value, "events", recording));
            Assert.ThrowsExactly<ArgumentException>(() =>
                new ClickHouseSink("Host=localhost;Port=8123", "dns", value, recording));
        }
    }

    [TestMethod]
    public async Task DisposalIsIdempotentAndDoesNotDisposeInjectedSinkAsync()
    {
        var recording = new RecordingSink();
        var sink = new ClickHouseSink("Host=localhost;Port=8123",
            "dns", "events", recording);

        sink.Dispose();
        sink.Dispose();

        Assert.AreEqual(0, recording.DisposeCalls);
        await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() =>
            sink.ForwardAsync([TestFixtures.CreateLogEntry("after-disposal.example")],
                TestContext.CancellationToken));
        Assert.AreEqual(0, recording.Batches);
    }

    [TestMethod]
    public async Task InFlightCancellationDoesNotAbandonUpstreamBatchAsync()
    {
        var completion = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var recording = new RecordingSink { Completion = completion.Task };
        using var sink = new ClickHouseSink("Host=localhost;Port=8123",
            "dns", "events", recording);
        using var cancellation = new CancellationTokenSource();

        var export = sink.ForwardAsync([TestFixtures.CreateLogEntry("inflight.example")],
            cancellation.Token);
        Assert.AreEqual(1, recording.Batches);
        await cancellation.CancelAsync();

        try
        {
            Assert.IsFalse(export.IsCompleted,
                "The upstream batch API does not support canceling an in-flight insert.");
        }
        finally
        {
            completion.SetResult();
            await export;
        }
        Assert.AreEqual(1, recording.Batches);
    }

    private static string? Scalar(LogEvent logEvent, string name) =>
        (logEvent.Properties[name] as ScalarValue)?.Value as string;

    private sealed class RecordingSink : IBatchedLogEventSink, IDisposable
    {
        public int Batches { get; private set; }
        public LogEvent[]? LastBatch { get; private set; }
        public Exception? Failure { get; set; }
        public Task? Completion { get; set; }
        public int DisposeCalls { get; private set; }

        public Task EmitBatchAsync(IReadOnlyCollection<LogEvent> batch)
        {
            Batches++;
            LastBatch = batch.ToArray();
            if (Failure is not null)
                throw Failure;
            return Completion ?? Task.CompletedTask;
        }

        public Task OnEmptyBatchAsync() => Task.CompletedTask;
        public void Dispose() => DisposeCalls++;
    }

    public TestContext TestContext { get; set; }
}
