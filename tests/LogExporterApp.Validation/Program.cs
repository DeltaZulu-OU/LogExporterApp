using DnsServerCore.ApplicationCommon;
using LogExporter;
using System;
using System.IO;
using System.Linq;
using LogExporter.Sinks;
using LogExporter.Pipeline;
using Nager.PublicSuffix;
using Nager.PublicSuffix.RuleProviders;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TechnitiumLibrary.Net.Dns;
using TechnitiumLibrary.Net.Dns.ResourceRecords;

internal static class Program
{
    private static async Task Main()
    {
        await SlowSinkDoesNotBlockFastSinkAsync();
        await SaturatedSinkDoesNotDropOtherSinkAsync();
        await DrainWaitsForAcceptedWorkAsync();
        await BoundedDrainAbortsBlockedSinkAsync();

        await ReloadDrainsOldGenerationBeforeSwitchAsync();
        await InvalidReloadPreservesActiveGenerationAsync();
        await InvalidEnabledSinkReloadPreservesActiveGenerationAsync();
        await DisposeDuringActiveIngestionIsSafeAsync();
        await InitializeAfterDisposeIsRejectedAsync();

        PipelineProcessorsRunInRegistrationOrder();
        DuplicatePipelineProcessorTypesAreRejected();
        RemoveAndReAddMovesProcessorToEnd();

        await DomainCacheDoesNotBlockOrCacheBeforeParserIsReadyAsync();

        await SinkWorkerSurvivesRepeatedFailuresAsync();
        await SinkErrorsAreRateLimitedAsync();
        await FailingSinkDoesNotAffectHealthySinkAsync();
        await HttpSinkRecoversAfterNonSuccessResponseAsync();

        Console.WriteLine("LogExporter validation passed.");
    }

    private static async Task SlowSinkDoesNotBlockFastSinkAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var slow = new BlockingSink();
        using var fast = new CountingSink(expectedCount: 1);

        dispatcher.Add(slow, 16);
        dispatcher.Add(fast, 16);

        await dispatcher.DispatchAsync(CreateBatch(1), CancellationToken.None);

        await slow.Started.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await fast.ReachedExpectedCount.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert(fast.Count == 1, "Fast sink did not receive the dispatched entry.");
        Assert(!slow.Release.Task.IsCompleted, "Slow sink was not actually blocked.");

        slow.Release.TrySetResult();
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2));
    }

    private static async Task SaturatedSinkDoesNotDropOtherSinkAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var slow = new BlockingSink();
        using var fast = new CountingSink(expectedCount: 4);

        dispatcher.Add(slow, 1);
        dispatcher.Add(fast, 16);

        await dispatcher.DispatchAsync(CreateBatch(1), CancellationToken.None);
        await slow.Started.Task.WaitAsync(TimeSpan.FromSeconds(2));

        for (var i = 0; i < 3; i++)
        {
            await dispatcher.DispatchAsync(CreateBatch(1), CancellationToken.None);
        }

        await fast.ReachedExpectedCount.Task.WaitAsync(TimeSpan.FromSeconds(2));

        slow.Release.TrySetResult();
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2));

        Assert(fast.Count == 4, "Healthy sink lost entries because another sink was saturated.");
        Assert(
            slow.Count < fast.Count,
            "Saturated sink did not exhibit queue-local dropping as expected.");
    }

    private static async Task DrainWaitsForAcceptedWorkAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var slow = new BlockingSink();

        dispatcher.Add(slow, 16);

        await dispatcher.DispatchAsync(CreateBatch(1), CancellationToken.None);
        await slow.Started.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var drain = dispatcher.DrainAsync();

        await Task.Delay(50);
        Assert(!drain.IsCompleted, "Drain completed before an accepted sink export finished.");

        slow.Release.TrySetResult();

        await drain.WaitAsync(TimeSpan.FromSeconds(2));
        Assert(slow.Count == 1, "Drain did not complete the accepted sink export.");
    }

    private static async Task BoundedDrainAbortsBlockedSinkAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var slow = new BlockingSink();

        dispatcher.Add(slow, 16);
        await dispatcher.DispatchAsync(CreateBatch(1), CancellationToken.None);
        await slow.Started.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var drained = await dispatcher
            .DrainAsync(TimeSpan.FromMilliseconds(50))
            .WaitAsync(TimeSpan.FromSeconds(2));

        Assert(!drained, "Bounded drain unexpectedly reported a graceful completion.");
    }

    private static async Task ReloadDrainsOldGenerationBeforeSwitchAsync()
    {
        var directory = CreateTempDirectory();
        var oldPath = Path.Combine(directory, "old.ndjson");
        var newPath = Path.Combine(directory, "new.ndjson");

        try
        {
            using var app = new App();
            var dnsServer = CreateDnsServer();

            await app.InitializeAsync(dnsServer, FileConfig(oldPath));

            for (var i = 0; i < 64; i++)
            {
                await InsertAsync(app, $"old-{i}.example");
            }

            await app.InitializeAsync(dnsServer, FileConfig(newPath));

            for (var i = 0; i < 64; i++)
            {
                await InsertAsync(app, $"new-{i}.example");
            }

            app.Dispose();

            var oldNames = ReadQuestionNames(oldPath);
            var newNames = ReadQuestionNames(newPath);

            Assert(oldNames.Count == 64, "Reload failed to drain all entries accepted by the old generation.");
            Assert(newNames.Count == 64, "New generation failed to export all post-reload entries.");
            Assert(oldNames.All(name => name.StartsWith("old-", StringComparison.Ordinal)),
                "Post-reload entries leaked into the old generation.");
            Assert(newNames.All(name => name.StartsWith("new-", StringComparison.Ordinal)),
                "Old-generation entries leaked into the replacement generation.");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task InvalidReloadPreservesActiveGenerationAsync()
    {
        var directory = CreateTempDirectory();
        var path = Path.Combine(directory, "active.ndjson");

        try
        {
            using var app = new App();
            var dnsServer = CreateDnsServer();

            await app.InitializeAsync(dnsServer, FileConfig(path));
            await InsertAsync(app, "before-invalid.example");

            var rejected = false;

            try
            {
                await app.InitializeAsync(dnsServer, """{"sinks":{} }""");
            }
            catch
            {
                rejected = true;
            }

            Assert(rejected, "Invalid replacement configuration was unexpectedly accepted.");

            await InsertAsync(app, "after-invalid.example");
            app.Dispose();

            var names = ReadQuestionNames(path);

            Assert(names.Contains("before-invalid.example"),
                "Active generation lost data before invalid reload.");
            Assert(names.Contains("after-invalid.example"),
                "Invalid reload stopped the previously active generation.");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task InvalidEnabledSinkReloadPreservesActiveGenerationAsync()
    {
        var directory = CreateTempDirectory();
        var path = Path.Combine(directory, "active-nested.ndjson");

        try
        {
            using var app = new App();
            var dnsServer = CreateDnsServer();

            await app.InitializeAsync(dnsServer, FileConfig(path));
            await InsertAsync(app, "before-invalid-sink.example");

            var rejected = false;

            try
            {
                await app.InitializeAsync(
                    dnsServer,
                    """{"sinks":{"file":{"enabled":true}},"pipeline":{}}""");
            }
            catch (System.ComponentModel.DataAnnotations.ValidationException)
            {
                rejected = true;
            }

            Assert(rejected,
                "Invalid enabled sink configuration was unexpectedly accepted.");

            await InsertAsync(app, "after-invalid-sink.example");
            app.Dispose();

            var names = ReadQuestionNames(path);

            Assert(names.Contains("before-invalid-sink.example"),
                "Active generation lost data before invalid enabled-sink reload.");
            Assert(names.Contains("after-invalid-sink.example"),
                "Invalid enabled-sink reload stopped the previously active generation.");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task DisposeDuringActiveIngestionIsSafeAsync()
    {
        var directory = CreateTempDirectory();
        var path = Path.Combine(directory, "dispose.ndjson");

        try
        {
            var app = new App();
            var dnsServer = CreateDnsServer();

            await app.InitializeAsync(dnsServer, FileConfig(path, queueSize: 4096));

            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Exception? ingestionFailure = null;

            var producer = Task.Run(async () =>
            {
                try
                {
                    for (var i = 0; i < 1000; i++)
                    {
                        await InsertAsync(app, $"dispose-{i}.example");

                        if (i == 10)
                        {
                            started.TrySetResult();
                        }

                        if ((i & 31) == 0)
                        {
                            await Task.Yield();
                        }
                    }
                }
                catch (Exception ex)
                {
                    ingestionFailure = ex;
                }
            });

            await started.Task.WaitAsync(TimeSpan.FromSeconds(2));

            await Task.Run(app.Dispose).WaitAsync(TimeSpan.FromSeconds(5));
            await producer.WaitAsync(TimeSpan.FromSeconds(5));

            Assert(ingestionFailure is null,
                $"Concurrent ingestion threw during disposal: {ingestionFailure}");
            Assert(File.Exists(path), "Dispose validation did not produce the configured output file.");
            Assert(ReadQuestionNames(path).Count > 0,
                "Dispose failed to drain any entries accepted before ingestion was unpublished.");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task InitializeAfterDisposeIsRejectedAsync()
    {
        var app = new App();
        var dnsServer = CreateDnsServer();

        app.Dispose();

        var rejected = false;

        try
        {
            await app.InitializeAsync(dnsServer, """{"sinks":{},"pipeline":{}}""");
        }
        catch (ObjectDisposedException)
        {
            rejected = true;
        }

        Assert(rejected, "InitializeAsync succeeded after App disposal.");
    }

    private static void PipelineProcessorsRunInRegistrationOrder()
    {
        using var dispatcher = new PipelineDispatcher();
        var order = new List<string>();

        dispatcher.Add(new FirstTestProcessor(order));
        dispatcher.Add(new SecondTestProcessor(order));

        dispatcher.Run(CreateLogEntry("order.example"));

        Assert(order.SequenceEqual(["first", "second"]),
            "Pipeline processors did not run in registration order.");
    }

    private static void DuplicatePipelineProcessorTypesAreRejected()
    {
        using var dispatcher = new PipelineDispatcher();
        var order = new List<string>();

        dispatcher.Add(new FirstTestProcessor(order));

        var rejected = false;
        var duplicate = new FirstTestProcessor(order);

        try
        {
            dispatcher.Add(duplicate);
        }
        catch (InvalidOperationException)
        {
            rejected = true;
            duplicate.Dispose();
        }

        Assert(rejected, "Duplicate pipeline processor type was not rejected.");
    }

    private static void RemoveAndReAddMovesProcessorToEnd()
    {
        using var dispatcher = new PipelineDispatcher();
        var order = new List<string>();

        dispatcher.Add(new FirstTestProcessor(order));
        dispatcher.Add(new SecondTestProcessor(order));

        dispatcher.Remove(typeof(FirstTestProcessor));
        dispatcher.Add(new FirstTestProcessor(order));

        dispatcher.Run(CreateLogEntry("reorder.example"));

        Assert(order.SequenceEqual(["second", "first"]),
            "Removed and re-added processor did not move to the end of execution order.");
    }

    private static LogEntry CreateLogEntry(string name)
    {
        var (request, response) = CreateDnsExchange(name);

        return new LogEntry(
            DateTime.UtcNow,
            new IPEndPoint(IPAddress.Loopback, 53000),
            DnsTransportProtocol.Udp,
            request,
            response);
    }

    private static (DnsDatagram Request, DnsDatagram Response) CreateDnsExchange(string name)
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

    private static async Task DomainCacheDoesNotBlockOrCacheBeforeParserIsReadyAsync()
    {
        var parserSource = new TaskCompletionSource<DomainParser?>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var cache = new Normalize.DomainCache(parserSource.Task);
        const string domainName = "www.example.com";

        var pendingLookup = Task.Run(() => cache.GetOrAdd(domainName));
        var beforeReady = await pendingLookup.WaitAsync(TimeSpan.FromSeconds(1));

        Assert(beforeReady.RegistrableDomain is null,
            "DomainCache produced parsed metadata before the parser was ready.");

        var directory = CreateTempDirectory();
        var rulesPath = Path.Combine(directory, "public_suffix_list.dat");

        try
        {
            await File.WriteAllTextAsync(
                rulesPath,
                """
                // ===BEGIN ICANN DOMAINS===
                com
                org
                co.uk
                // ===END ICANN DOMAINS===
                """);

            var ruleProvider = new LocalFileRuleProvider(rulesPath);
            await ruleProvider.BuildAsync();

            parserSource.TrySetResult(new DomainParser(ruleProvider));

            var afterReady = cache.GetOrAdd(domainName);

            Assert(afterReady.RegistrableDomain == "example.com",
                "DomainCache did not begin parsing after the PSL parser became ready.");
            Assert(afterReady.Domain == "example",
                "DomainCache returned unexpected parsed domain metadata.");

            var normalizedLookup = cache.GetOrAdd("WWW.EXAMPLE.COM.");
            Assert(normalizedLookup.RegistrableDomain == "example.com",
                "DomainCache normalization failed after parser initialization.");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task SinkWorkerSurvivesRepeatedFailuresAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var sink = new RecoveringSink(failuresBeforeSuccess: 2);

        dispatcher.Add(sink, 16);

        for (var i = 1; i <= 3; i++)
        {
            await dispatcher.DispatchAsync(CreateBatch(1), CancellationToken.None);
            await sink.WaitForAttemptAsync(i).WaitAsync(TimeSpan.FromSeconds(2));
        }

        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2));

        Assert(sink.Attempts == 3,
            "Sink worker stopped after an export exception.");
        Assert(sink.Successes == 1,
            "Sink worker did not recover after repeated export failures.");
    }

    private static async Task SinkErrorsAreRateLimitedAsync()
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
            await dispatcher.DispatchAsync(CreateBatch(1), CancellationToken.None);
            await sink.WaitForAttemptAsync(i).WaitAsync(TimeSpan.FromSeconds(2));
        }

        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2));

        Assert(errorCount == 1,
            $"Repeated sink failures were not rate-limited; observed {errorCount} callbacks.");
    }

    private static async Task FailingSinkDoesNotAffectHealthySinkAsync()
    {
        using var dispatcher = new SinkDispatcher();
        using var failing = new RecoveringSink(failuresBeforeSuccess: int.MaxValue);
        using var healthy = new CountingSink(expectedCount: 3);

        dispatcher.Add(failing, 16);
        dispatcher.Add(healthy, 16);

        for (var i = 1; i <= 3; i++)
        {
            await dispatcher.DispatchAsync(CreateBatch(1), CancellationToken.None);
            await failing.WaitForAttemptAsync(i).WaitAsync(TimeSpan.FromSeconds(2));
        }

        await healthy.ReachedExpectedCount.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(2));

        Assert(healthy.Count == 3,
            "A repeatedly failing sink prevented a healthy sink from receiving entries.");
    }

    private static async Task HttpSinkRecoversAfterNonSuccessResponseAsync()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();

        var endpoint = (IPEndPoint)listener.LocalEndpoint;
        using var sink = new HttpSink($"http://127.0.0.1:{endpoint.Port}/logs");

        var server = Task.Run(async () =>
        {
            await ServeHttpResponseAsync(listener, "500 Internal Server Error");
            await ServeHttpResponseAsync(listener, "204 No Content");
        });

        var firstFailed = false;

        try
        {
            await sink.ExportAsync(
                [CreateLogEntry("http-failure.example")],
                CancellationToken.None);
        }
        catch (HttpRequestException)
        {
            firstFailed = true;
        }

        Assert(firstFailed,
            "HttpSink did not propagate a non-success HTTP response.");

        await sink.ExportAsync(
            [CreateLogEntry("http-recovery.example")],
            CancellationToken.None);

        await server.WaitAsync(TimeSpan.FromSeconds(2));
    }

    private static async Task ServeHttpResponseAsync(
        TcpListener listener,
        string status)
    {
        using var client = await listener.AcceptTcpClientAsync()
            .WaitAsync(TimeSpan.FromSeconds(2));
        await using var stream = client.GetStream();

        var buffer = new byte[4096];
        var received = 0;

        while (received < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(received));
            if (read == 0)
            {
                break;
            }

            received += read;

            if (Encoding.ASCII.GetString(buffer, 0, received)
                .Contains("\r\n\r\n", StringComparison.Ordinal))
            {
                break;
            }
        }

        var response =
            $"HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        var bytes = Encoding.ASCII.GetBytes(response);
        await stream.WriteAsync(bytes);
        await stream.FlushAsync();
    }

    private static async Task InsertAsync(App app, string name)
    {
        var (request, response) = CreateDnsExchange(name);

        await app.InsertLogAsync(
            DateTime.UtcNow,
            request,
            new IPEndPoint(IPAddress.Loopback, 53000),
            DnsTransportProtocol.Udp,
            response);
    }

    private static string FileConfig(string path, int queueSize = 4096)
    {
        return JsonSerializer.Serialize(new
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
    }

    private static List<string> ReadQuestionNames(string path)
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

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), $"LogExporterApp-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static IDnsServer CreateDnsServer() =>
        DispatchProxy.Create<IDnsServer, DnsServerProxy>();

    private static IReadOnlyList<LogEntry> CreateBatch(int count)
    {
        var entries = new LogEntry[count];

        for (var i = 0; i < entries.Length; i++)
        {
            entries[i] = null!;
        }

        return entries;
    }

    private static void Assert(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }

    private class DnsServerProxy : DispatchProxy
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
                $"Unexpected IDnsServer member used by validation harness: {targetMethod.Name}");
        }
    }

    private sealed class FirstTestProcessor : IPipelineProcessor
    {
        private readonly List<string> _order;

        public FirstTestProcessor(List<string> order)
        {
            _order = order;
        }

        public void Process(LogEntry logEntry) => _order.Add("first");

        public void Dispose()
        {
        }
    }

    private sealed class SecondTestProcessor : IPipelineProcessor
    {
        private readonly List<string> _order;

        public SecondTestProcessor(List<string> order)
        {
            _order = order;
        }

        public void Process(LogEntry logEntry) => _order.Add("second");

        public void Dispose()
        {
        }
    }

    private sealed class RecoveringSink : IOutputSink
    {
        private readonly int _failuresBeforeSuccess;
        private readonly object _sync = new();
        private readonly Dictionary<int, TaskCompletionSource> _attemptSignals = new();
        private int _attempts;
        private int _successes;

        public RecoveringSink(int failuresBeforeSuccess)
        {
            _failuresBeforeSuccess = failuresBeforeSuccess;
        }

        public int Attempts => Volatile.Read(ref _attempts);
        public int Successes => Volatile.Read(ref _successes);

        public Task WaitForAttemptAsync(int attempt)
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

    private sealed class CountingSink : IOutputSink
    {
        private readonly int _expectedCount;
        private int _count;

        public CountingSink(int expectedCount)
        {
            _expectedCount = expectedCount;
        }

        public TaskCompletionSource ReachedExpectedCount { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Count => Volatile.Read(ref _count);

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

    private sealed class BlockingSink : IOutputSink
    {
        private int _count;

        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Count => Volatile.Read(ref _count);

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
}
