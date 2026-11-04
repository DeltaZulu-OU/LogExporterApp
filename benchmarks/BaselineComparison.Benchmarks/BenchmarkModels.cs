using LogExporter;
using LogExporter.Sinks;

namespace LogExporter.Benchmarks;

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
