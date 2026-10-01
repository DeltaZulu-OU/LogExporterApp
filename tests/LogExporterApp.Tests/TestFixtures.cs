using DnsServerCore.ApplicationCommon;
using LogExporter;
using LogExporter.Pipeline;
using LogExporter.Sinks;
using System.Net;
using System.Reflection;
using System.Text.Json;
using TechnitiumLibrary.Net.Dns;
using TechnitiumLibrary.Net.Dns.ResourceRecords;

namespace LogExporterApp.Tests;

internal static class TestFixtures
{
    internal static LogEntry CreateLogEntry(string name)
    {
        var (request, response) = CreateDnsExchange(name);

        return new LogEntry(
            DateTime.UtcNow,
            new IPEndPoint(IPAddress.Loopback, 53000),
            DnsTransportProtocol.Udp,
            request,
            response);
    }

    internal static (DnsDatagram Request, DnsDatagram Response) CreateDnsExchange(string name)
    {
        var question = new DnsQuestionRecord(
            name,
            DnsResourceRecordType.A,
            DnsClass.IN);

        var request = new DnsDatagram(
            1,
            false,
            DnsOpcode.StandardQuery,
            false,
            false,
            true,
            false,
            false,
            false,
            DnsResponseCode.NoError,
            [question]);

        request.SetMetadata(new NameServerAddress(IPAddress.Loopback));

        var response = new DnsDatagram(
            1,
            true,
            DnsOpcode.StandardQuery,
            false,
            false,
            true,
            true,
            false,
            false,
            DnsResponseCode.NoError,
            [question]);

        return (request, response);
    }

    internal static async Task InsertAsync(App app, string name)
    {
        var (request, response) = CreateDnsExchange(name);

        await app.InsertLogAsync(
            DateTime.UtcNow,
            request,
            new IPEndPoint(IPAddress.Loopback, 53000),
            DnsTransportProtocol.Udp,
            response);
    }

    internal static string FileConfig(string path, int queueSize = 4096) =>
        JsonSerializer.Serialize(new
        {
            sinks = new
            {
                maxQueueSize = queueSize,
                file = new
                {
                    enabled = true,
                    path
                }
            },
            pipeline = new { }
        });

    internal static List<string> ReadQuestionNames(string path)
    {
        var names = new List<string>();

        if (!File.Exists(path))
        {
            return names;
        }

        foreach (var line in File.ReadLines(path))
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            using var json = JsonDocument.Parse(line);

            if (json.RootElement.TryGetProperty("question", out var question) &&
                question.TryGetProperty("questionName", out var questionName) &&
                questionName.GetString() is { } name)
            {
                names.Add(name);
            }
        }

        return names;
    }

    internal static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), $"LogExporterApp-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    internal static IDnsServer CreateDnsServer() =>
        DispatchProxy.Create<IDnsServer, DnsServerProxy>();

    internal static IReadOnlyList<LogEntry> CreateBatch(int count)
    {
        var entries = new LogEntry[count];

        for (var i = 0; i < entries.Length; i++)
        {
            entries[i] = null!;
        }

        return entries;
    }
}

internal class DnsServerProxy : DispatchProxy
{
    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        if (targetMethod is null)
        {
            throw new InvalidOperationException("IDnsServer proxy received an unknown invocation.");
        }

        if (targetMethod.Name == nameof(IDnsServer.WriteLog))
        {
            return null;
        }

        throw new NotSupportedException(
            $"Unexpected IDnsServer member used by tests: {targetMethod.Name}");
    }
}

internal sealed class FirstTestProcessor : IPipelineProcessor
{
    private readonly List<string> _order;

    internal FirstTestProcessor(List<string> order)
    {
        _order = order;
    }

    public void Process(LogEntry logEntry) => _order.Add("first");

    public void Dispose()
    {
    }
}

internal sealed class SecondTestProcessor : IPipelineProcessor
{
    private readonly List<string> _order;

    internal SecondTestProcessor(List<string> order)
    {
        _order = order;
    }

    public void Process(LogEntry logEntry) => _order.Add("second");

    public void Dispose()
    {
    }
}

internal sealed class ThrowingTestProcessor : IPipelineProcessor
{
    public void Process(LogEntry logEntry) =>
        throw new InvalidOperationException("Synthetic processor failure.");

    public void Dispose()
    {
    }
}

internal sealed class RecoveringSink : IOutputSink
{
    private readonly int _failuresBeforeSuccess;
    private readonly object _sync = new();
    private readonly Dictionary<int, TaskCompletionSource> _attemptSignals = new();
    private int _attempts;
    private int _successes;

    internal RecoveringSink(int failuresBeforeSuccess)
    {
        _failuresBeforeSuccess = failuresBeforeSuccess;
    }

    internal int Attempts => Volatile.Read(ref _attempts);
    internal int Successes => Volatile.Read(ref _successes);

    internal Task WaitForAttemptAsync(int attempt)
    {
        lock (_sync)
        {
            if (Attempts >= attempt)
            {
                return Task.CompletedTask;
            }

            if (!_attemptSignals.TryGetValue(attempt, out var signal))
            {
                signal = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                _attemptSignals.Add(attempt, signal);
            }

            return signal.Task;
        }
    }

    public Task ExportAsync(IReadOnlyList<LogEntry> logs, CancellationToken token)
    {
        var attempt = Interlocked.Increment(ref _attempts);

        lock (_sync)
        {
            foreach (var item in _attemptSignals
                .Where(item => item.Key <= attempt)
                .ToArray())
            {
                item.Value.TrySetResult();
                _attemptSignals.Remove(item.Key);
            }
        }

        if (attempt <= _failuresBeforeSuccess)
        {
            throw new InvalidOperationException($"Synthetic sink failure {attempt}.");
        }

        Interlocked.Increment(ref _successes);
        return Task.CompletedTask;
    }

    public void Dispose()
    {
    }
}

internal sealed class CountingSink : IOutputSink
{
    private readonly int _expectedCount;
    private int _count;

    internal CountingSink(int expectedCount)
    {
        _expectedCount = expectedCount;
    }

    internal TaskCompletionSource ReachedExpectedCount { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal int Count => Volatile.Read(ref _count);

    public Task ExportAsync(IReadOnlyList<LogEntry> logs, CancellationToken token)
    {
        var count = Interlocked.Add(ref _count, logs.Count);

        if (count >= _expectedCount)
        {
            ReachedExpectedCount.TrySetResult();
        }

        return Task.CompletedTask;
    }

    public void Dispose()
    {
    }
}

internal sealed class BlockingSink : IOutputSink
{
    private int _count;

    internal TaskCompletionSource Started { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal TaskCompletionSource Release { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal int Count => Volatile.Read(ref _count);

    public async Task ExportAsync(IReadOnlyList<LogEntry> logs, CancellationToken token)
    {
        Interlocked.Add(ref _count, logs.Count);
        Started.TrySetResult();

        await Release.Task.WaitAsync(token);
    }

    public void Dispose()
    {
        Release.TrySetResult();
    }
}
