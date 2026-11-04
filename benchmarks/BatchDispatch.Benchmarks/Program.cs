using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;
using LogExporter;
using LogExporter.Sinks;

// This is an end-to-end in-memory dispatcher microbenchmark, not a sink I/O benchmark.
// No-op sink and null LogEntry references isolate queueing, batching and worker overhead.
const int defaultEvents = 100_000;
const int defaultIterations = 5;
var events = ReadInt("--events", defaultEvents);
var iterations = ReadInt("--iterations", defaultIterations);
var output = ReadString("--output", "benchmarks/results");
if (events <= 0 || iterations <= 0)
    throw new ArgumentOutOfRangeException(nameof(events), "Event count and iterations must be positive.");

var runs = new List<CaseResult>();
foreach (var batchSize in new[] { 1, 64, 1000 })
{
    // Separate warm-up from measured trials; also verifies correctness.
    await RunOnce(Math.Min(events, 4_096), batchSize);
    var samples = new List<Sample>();
    for (var i = 0; i < iterations; i++)
    {
        samples.Add(await RunOnce(events, batchSize));
    }

    var sortedDurations = samples.Select(x => x.Milliseconds).OrderBy(x => x).ToArray();
    var sortedAllocations = samples.Select(x => (double)x.AllocatedBytes).OrderBy(x => x).ToArray();
    var ms = Median(sortedDurations);
    runs.Add(new CaseResult(batchSize, events, iterations, ms,
        events * 1000d / ms, Median(sortedAllocations),
        samples.Sum(x => x.Batches) / (double)iterations,
        samples.Min(x => x.Milliseconds), samples.Max(x => x.Milliseconds)));
}

var result = new BenchmarkReport(
    DateTimeOffset.UtcNow,
    RuntimeInformation.FrameworkDescription,
    RuntimeInformation.OSDescription,
    Environment.ProcessorCount,
    "In-memory dispatch + drain; no-op sink; batch=1 is a configuration baseline, not the old master implementation",
    runs);

Directory.CreateDirectory(output);
var jsonPath = Path.Combine(output, "batch-dispatch.json");
var markdownPath = Path.Combine(output, "batch-dispatch.md");
File.WriteAllText(jsonPath, JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true }));

var markdown = new List<string>
{
    "# Batching dispatcher microbenchmark",
    "",
    $"Runtime: {result.Runtime}; OS: {result.OS}; CPUs: {result.ProcessorCount}.",
    $"Events/trial: {events:N0}; measured trials/case: {iterations}; warm-up: 1.",
    "",
    "Measures in-memory enqueue through worker drain using a counting no-op sink.",
    "No serialization, HTTP, syslog, persistence or actual DNS records are measured.",
    "Allocations are process-wide GC allocated bytes and may include runtime background work.",
    "BatchSize=1 is a configuration baseline, NOT a comparison with the previous master implementation.",
    "No absolute timing threshold is enforced across shared CI runners.",
    "",
    "| Batch size | Median ms | Events/s | Median allocated bytes | Mean export calls | Min ms | Max ms |",
    "|---:|---:|---:|---:|---:|---:|---:|"
};
foreach (var item in runs)
{
    markdown.Add($"| {item.BatchSize} | {item.MedianMilliseconds:F2} | {item.EventsPerSecond:F0} | {item.MedianAllocatedBytes:F0} | {item.MeanExportCalls:F0} | {item.MinMilliseconds:F2} | {item.MaxMilliseconds:F2} |");
}
File.WriteAllLines(markdownPath, markdown);
foreach (var line in markdown)
    Console.WriteLine(line);

static double Median(double[] sorted) =>
    sorted.Length % 2 == 0
        ? (sorted[sorted.Length / 2 - 1] + sorted[sorted.Length / 2]) / 2
        : sorted[sorted.Length / 2];

static int ReadInt(string name, int fallback)
{
    var index = Array.IndexOf(Environment.GetCommandLineArgs(), name);
    return index >= 0 && index + 1 < Environment.GetCommandLineArgs().Length
        ? int.Parse(Environment.GetCommandLineArgs()[index + 1])
        : fallback;
}

static string ReadString(string name, string fallback)
{
    var args = Environment.GetCommandLineArgs();
    var index = Array.IndexOf(args, name);
    return index >= 0 && index + 1 < args.Length ? args[index + 1] : fallback;
}

static async Task<Sample> RunOnce(int count, int batchSize)
{
    using var dispatcher = new SinkDispatcher();
    using var sink = new CountingSink();
    dispatcher.Add(sink, new SinkBatchOptions
    {
        QueueCapacity = count + 1,
        BatchSize = batchSize,
        BatchDelay = TimeSpan.FromMilliseconds(10),
        RetryTimeLimit = TimeSpan.Zero
    });

    var records = new LogEntry[count];
    var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
    var timer = Stopwatch.StartNew();
    await dispatcher.DispatchAsync(records, CancellationToken.None);
    await dispatcher.DrainAsync();
    timer.Stop();
    var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
    var counters = dispatcher.GetCounters(typeof(CountingSink));
    if (sink.Events != count || sink.Batches <= 0 ||
        counters.DroppedEvents != 0 || counters.FailedEvents != 0 ||
        counters.SuccessfulEvents != count || counters.SuccessfulBatches != sink.Batches ||
        sink.LargestBatch > batchSize)
        throw new InvalidOperationException($"Delivery integrity failure at batch size {batchSize}.");

    return new Sample(timer.Elapsed.TotalMilliseconds, allocated, sink.Batches);
}

internal sealed class CountingSink : IOutputSink
{
    public long Events { get; private set; }
    public int Batches { get; private set; }
    public int LargestBatch { get; private set; }

    public Task ExportAsync(IReadOnlyList<LogEntry> batch, CancellationToken cancellationToken)
    {
        Events += batch.Count;
        Batches++;
        LargestBatch = Math.Max(LargestBatch, batch.Count);
        return Task.CompletedTask;
    }

    public void Dispose() { }
}

internal sealed record Sample(double Milliseconds, long AllocatedBytes, int Batches);
internal sealed record CaseResult(int BatchSize, int Events, int Iterations,
    double MedianMilliseconds, double EventsPerSecond, double MedianAllocatedBytes,
    double MeanExportCalls, double MinMilliseconds, double MaxMilliseconds);
internal sealed record BenchmarkReport(DateTimeOffset Timestamp, string Runtime, string OS,
    int ProcessorCount, string Scope, List<CaseResult> Results);
