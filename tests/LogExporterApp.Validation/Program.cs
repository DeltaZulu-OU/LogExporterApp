using DnsServerCore.ApplicationCommon;
using LogExporter;
using System;
using System.IO;
using System.Linq;
using LogExporter.Sinks;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
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

        await ReloadDrainsOldGenerationBeforeSwitchAsync();
        await InvalidReloadPreservesActiveGenerationAsync();
        await DisposeDuringActiveIngestionIsSafeAsync();
        await InitializeAfterDisposeIsRejectedAsync();

        Console.WriteLine("LogExporter lifecycle validation passed.");
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

    private static async Task InsertAsync(App app, string name)
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
                $"Unexpected IDnsServer member used by lifecycle validation: {targetMethod.Name}");
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
