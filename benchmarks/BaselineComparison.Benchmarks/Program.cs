using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;
using LogExporter;
using LogExporter.Sinks;

const int size = 1000;
var options = Parse(args);
int count = Positive(options, "--events", 100_000), trials = Positive(options, "--iterations", 7);
string output = options["--output"], label = options["--label"];
#if BASELINE
const string implementation = "master";
#else
const string implementation = "pr";
#endif
var cases = new List<CaseResult>();
foreach (var scenario in new[] { new Scenario("single-burst", count), new Scenario("chunked", 1000) })
{
    await Measure(Math.Min(count, 4096), scenario);
    var samples = new List<Sample>();
    for (int i = 0; i < trials; i++) samples.Add(await Measure(count, scenario));
    var times = samples.Select(x => x.Milliseconds).Order().ToArray();
    var allocs = samples.Select(x => (double)x.AllocatedBytes).Order().ToArray();
    cases.Add(new CaseResult(scenario.Name, count, scenario.Chunk,
        Median(times), Median(allocs), times.First(), times.Last(),
        samples.Select(x => x.Milliseconds).ToArray(),
        samples.Select(x => x.AllocatedBytes).ToArray()));
}
var report = new ComparisonRun(label, implementation, DateTimeOffset.UtcNow,
    RuntimeInformation.FrameworkDescription, RuntimeInformation.OSDescription,
    Environment.ProcessorCount, size, 25, cases);
Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(output))!);
await File.WriteAllTextAsync(output, JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }));
Console.WriteLine($"Validated {implementation}, {count} events, {trials} trials.");
foreach (var c in cases) Console.WriteLine($"{c.Name}: {c.MedianMilliseconds:F3} ms; {c.MedianAllocatedBytes:F0} allocated bytes.");

static Dictionary<string, string> Parse(string[] args)
{
    var values = new Dictionary<string, string>(StringComparer.Ordinal);
    for (int i = 0; i < args.Length; i += 2)
    {
        if (i + 1 >= args.Length || !args[i].StartsWith("--", StringComparison.Ordinal) ||
            !values.TryAdd(args[i], args[i + 1]))
            throw new ArgumentException("Expected unique --option value pairs.");
    }
    return values;
}

static int Positive(Dictionary<string, string> args, string key, int fallback)
{
    if (!args.TryGetValue(key, out var value)) return fallback;
    if (!int.TryParse(value, out var parsed) || parsed <= 0)
        throw new ArgumentException($"{key} must be positive.");
    return parsed;
}

static double Median(double[] sorted) => sorted.Length % 2 == 1
    ? sorted[sorted.Length / 2]
    : (sorted[sorted.Length / 2 - 1] + sorted[sorted.Length / 2]) / 2;

static async Task<Sample> Measure(int count, Scenario scenario)
{
    using var dispatcher = new SinkDispatcher();
    using var sink = new CountingSink();
#if BASELINE
    dispatcher.Add(sink, count + 1);
#else
    var config = new SinkBatchOptions
    {
        QueueCapacity = count + 1,
        BatchSize = size,
        BatchDelay = TimeSpan.FromMilliseconds(25),
        RetryTimeLimit = TimeSpan.Zero
    };
    dispatcher.Add(sink, config);
#endif
    // Null element values deliberately isolate dispatcher overhead from LogEntry construction.
    var entries = new LogEntry[count];
    long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
    var timer = Stopwatch.StartNew();
    for (int offset = 0; offset < count; offset += scenario.Chunk)
    {
        int n = Math.Min(scenario.Chunk, count - offset);
        await dispatcher.DispatchAsync(new ArraySegment<LogEntry>(entries, offset, n), CancellationToken.None);
    }
    await dispatcher.DrainAsync();
    timer.Stop();
    long allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
    Verify(sink, count);
#if !BASELINE
    VerifyCounters(dispatcher, sink, count);
#endif
    return new Sample(timer.Elapsed.TotalMilliseconds, allocated);
}

static void Verify(CountingSink sink, int count)
{
    if (sink.Events != count || sink.Batches == 0 || sink.LargestBatch > size)
        throw new InvalidOperationException($"Integrity failure: expected {count}, got {sink.Events}.");
}
#if !BASELINE
static void VerifyCounters(SinkDispatcher dispatcher, CountingSink sink, int count)
{
    var counters = dispatcher.GetCounters(sink.GetType());
    if (counters.SuccessfulEvents != count || counters.DroppedEvents != 0 ||
        counters.FailedEvents != 0 || counters.SuccessfulBatches != sink.Batches)
        throw new InvalidOperationException("PR dispatcher counters disagree with acknowledgments.");
}
#endif

internal sealed class CountingSink : IOutputSink
{
    public int Events { get; private set; }
    public int Batches { get; private set; }
    public int LargestBatch { get; private set; }
    public Task ExportAsync(IReadOnlyList<LogEntry> logs, CancellationToken token)
    {
        Events += logs.Count;
        Batches++;
        LargestBatch = Math.Max(LargestBatch, logs.Count);
        return Task.CompletedTask;
    }
    public void Dispose() { }
}
internal sealed record Scenario(string Name, int Chunk);
internal sealed record Sample(double Milliseconds, long AllocatedBytes);
internal sealed record CaseResult(string Name, int Events, int Chunk,
    double MedianMilliseconds, double MedianAllocatedBytes, double MinMilliseconds,
    double MaxMilliseconds, double[] DurationSamples, long[] AllocationSamples);
internal sealed record ComparisonRun(string Label, string Implementation, DateTimeOffset Timestamp,
    string Runtime, string OS, int ProcessorCount, int BatchSize, int BatchDelayMs,
    List<CaseResult> Cases);
