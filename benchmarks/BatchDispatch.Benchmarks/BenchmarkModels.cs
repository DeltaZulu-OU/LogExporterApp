using LogExporter.Sinks;

namespace LogExporter.Benchmarks;

internal sealed class CountingSink : IOutputSink
{
    public long Events { get; private set; }
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

internal sealed record Sample(double Milliseconds, long AllocatedBytes, int Batches);

internal sealed record CaseResult(int BatchSize, int Events, int Iterations,
    double MedianMilliseconds, double EventsPerSecond, double MedianAllocatedBytes,
    double MeanExportCalls, double MinMilliseconds, double MaxMilliseconds);

internal sealed record BenchmarkReport(DateTimeOffset Timestamp, string Runtime, string OS,
    int ProcessorCount, string Scope, List<CaseResult> Results);
