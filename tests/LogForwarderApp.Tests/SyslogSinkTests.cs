using System.Net.Sockets;
using LogForwarder.Sinks;
namespace LogForwarderApp.Tests;

[TestClass]
public sealed class SyslogSinkTests
{
    [TestMethod]
    public void ResolveRetryDelayAppliesBoundedJitter()
    {
        Assert.AreEqual(TimeSpan.FromSeconds(1.6), SyslogSink.GetRetryDelay(1, 0));
        Assert.AreEqual(TimeSpan.FromSeconds(2.4), SyslogSink.GetRetryDelay(1, 1));
        Assert.AreEqual(TimeSpan.FromSeconds(24), SyslogSink.GetRetryDelay(100, 0));
        Assert.AreEqual(TimeSpan.FromSeconds(30), SyslogSink.GetRetryDelay(100, 1));
    }

    [TestMethod]
    public void ResolveRetryDelayRejectsInvalidInputs()
    {
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => SyslogSink.GetRetryDelay(0, 0.5));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => SyslogSink.GetRetryDelay(1, -0.1));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => SyslogSink.GetRetryDelay(1, 1.1));
    }

    [TestMethod]
    public void ResolveRetryDelayUsesExponentialBackoff()
    {
        Assert.AreEqual(TimeSpan.FromSeconds(2), SyslogSink.GetRetryDelay(1, 0.5));
        Assert.AreEqual(TimeSpan.FromSeconds(4), SyslogSink.GetRetryDelay(2, 0.5));
        Assert.AreEqual(TimeSpan.FromSeconds(8), SyslogSink.GetRetryDelay(3, 0.5));
        Assert.AreEqual(TimeSpan.FromSeconds(16), SyslogSink.GetRetryDelay(4, 0.5));
        Assert.AreEqual(TimeSpan.FromSeconds(30), SyslogSink.GetRetryDelay(5, 0.5));
    }

    [TestMethod]
    public async Task ResolutionRetryUsesInjectedDelayFactoryAsync()
    {
        var attempts = 0;
        var delayCalls = new List<int>();

        using var sink = new SyslogSink(
            "syslog.example",
            514,
            "udp",
            Task.CompletedTask,
            _ => { },
            _ => new TestSyslogTransport(failureAttempt: int.MaxValue),
            failureCount =>
            {
                delayCalls.Add(failureCount);
                return TimeSpan.Zero;
            },
            (_, _) =>
            {
                attempts++;
                if (attempts == 1)
                {
                    throw new SocketException((int)SocketError.TryAgain);
                }

                return Task.FromResult(System.Net.IPAddress.Loopback);
            });

        await sink.ExportAsync(
            [TestFixtures.CreateLogEntry("resolution-retry.example")],
            CancellationToken.None);

        Assert.AreSequenceEqual(new[] { 1 }, delayCalls);
    }

    [TestMethod]
    public async Task TransportFailureRetainsFailedAndRemainingEntriesAsync()
    {
        using var transport = new TestSyslogTransport(failureAttempt: 2);
        var messages = new List<string>();

        using var sink = new SyslogSink(
            "127.0.0.1",
            514,
            "udp",
            Task.CompletedTask,
            messages.Add,
            _ => transport,
            _ => TimeSpan.Zero);

        await sink.ExportAsync(
            [
                TestFixtures.CreateLogEntry("one.example"),
                TestFixtures.CreateLogEntry("two.example"),
                TestFixtures.CreateLogEntry("three.example")
            ],
            CancellationToken.None);

        CollectionAssert.AreEqual(
            new[] { "one.example", "two.example", "two.example", "three.example" },
            transport.AttemptedNames.ToArray());
        CollectionAssert.AreEqual(
            new[] { "one.example", "two.example", "three.example" },
            transport.DeliveredNames.ToArray());

        Assert.AreEqual(1, messages.Count(m => m.Contains("buffering logs and retrying")));
        Assert.AreEqual(1, messages.Count(m => m.Contains("transport recovered")));
    }

    [TestMethod]
    public async Task OversizedUdpEntryDoesNotBlockFollowingEntriesAsync()
    {
        using var transport = new TestSyslogTransport(
            failureAttempt: 1,
            failure: new SocketException((int)SocketError.MessageSize));
        var messages = new List<string>();

        using var sink = new SyslogSink(
            "127.0.0.1",
            514,
            "udp",
            Task.CompletedTask,
            messages.Add,
            _ => transport,
            _ => TimeSpan.Zero);

        await sink.ExportAsync(
            [
                TestFixtures.CreateLogEntry("oversized.example"),
                TestFixtures.CreateLogEntry("next.example")
            ],
            CancellationToken.None);

        Assert.AreSequenceEqual(
            new[] { "oversized.example", "next.example" },
            transport.AttemptedNames.ToArray());
        Assert.AreSequenceEqual(
            new[] { "next.example" },
            transport.DeliveredNames.ToArray());
        Assert.ContainsSingle(
            message => message.Contains("maximum message size"),
            messages);
    }

    [TestMethod]
    public async Task TransportRetryCanBeCancelledAsync()
    {
        using var transport = new TestSyslogTransport(failureAttempt: 1);
        using var cts = new CancellationTokenSource();
        var retryStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        using var sink = new SyslogSink(
            "127.0.0.1",
            514,
            "udp",
            Task.CompletedTask,
            _ => retryStarted.TrySetResult(),
            _ => transport,
            _ => TimeSpan.FromMinutes(1));

        var export = sink.ExportAsync(
            [TestFixtures.CreateLogEntry("cancel.example")],
            cts.Token);

        await retryStarted.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
        cts.Cancel();

        await Assert.ThrowsExactlyAsync<TaskCanceledException>(
            async () => await export);
    }

    [TestMethod]
    public async Task SyslogPreservesEnrichmentAndOmitsNullRttAsync()
    {
        using var transport = new PropertyCaptureTransport();
        using var sink = new SyslogSink(
            "127.0.0.1", 514, "udp", Task.CompletedTask,
            _ => { }, _ => transport, _ => TimeSpan.Zero);

        var entry = TestFixtures.CreateLogEntry("example.com");
        entry.Meta["tags"] = new[] { "tenant:alpha", "environment:test" };
        entry.Meta["domainInfo"] = new
        {
            domain = "example",
            topLevelDomain = "com",
            registrableDomain = "example.com",
            fullyQualifiedDomainName = "example.com",
            topLevelDomainRule = new { name = "com", type = "Normal", labelCount = 1, division = "ICANN" }
        };

        await sink.ExportAsync([entry], CancellationToken.None);

        Assert.HasCount(1, transport.Events);
        var properties = transport.Events[0].Properties;
        Assert.IsFalse(properties.ContainsKey("responseRtt"));
        Assert.IsFalse(properties.ContainsKey("tags"));
        Assert.IsFalse(properties.ContainsKey("domainInfo"));
        Assert.AreEqual("tenant:alpha", GetScalar(properties, "tag_0"));
        Assert.AreEqual("environment:test", GetScalar(properties, "tag_1"));
        Assert.AreEqual("example.com", GetScalar(properties, "domainRegistrable"));
        Assert.AreEqual("com", GetScalar(properties, "domainTld"));
        Assert.AreEqual("ICANN", GetScalar(properties, "domainRuleDivision"));
        Assert.AreEqual("1", GetScalar(properties, "domainRuleLabels"));
    }

    private static string? GetScalar(
        IReadOnlyDictionary<string, Serilog.Events.LogEventPropertyValue> properties,
        string name) =>
        ((Serilog.Events.ScalarValue)properties[name]).Value?.ToString();

    public TestContext TestContext { get; set; }
}

internal sealed class TestSyslogTransport : ISyslogTransport
{
    private readonly List<string> _attemptedNames = new();
    private readonly List<string> _deliveredNames = new();
    private readonly int _failureAttempt;
    private readonly Exception _failure;
    private int _attempts;
    private bool _failed;

    internal TestSyslogTransport(
        int failureAttempt,
        Exception? failure = null)
    {
        _failureAttempt = failureAttempt;
        _failure = failure
            ?? new SocketException((int)SocketError.NetworkUnreachable);
    }

    internal IReadOnlyList<string> AttemptedNames => _attemptedNames;
    internal IReadOnlyList<string> DeliveredNames => _deliveredNames;

    public Task SendAsync(Serilog.Events.LogEvent logEvent, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();

        var attempt = Interlocked.Increment(ref _attempts);
        var name = logEvent.Properties["qName"].ToString().Trim('"');
        _attemptedNames.Add(name);

        if (!_failed && attempt == _failureAttempt)
        {
            _failed = true;
            throw _failure;
        }

        _deliveredNames.Add(name);
        return Task.CompletedTask;
    }

    public void Dispose()
    {
    }
}

internal sealed class PropertyCaptureTransport : ISyslogTransport
{
    public List<Serilog.Events.LogEvent> Events { get; } = new();

    public Task SendAsync(Serilog.Events.LogEvent logEvent, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        Events.Add(logEvent);
        return Task.CompletedTask;
    }

    public void Dispose() { }
}
