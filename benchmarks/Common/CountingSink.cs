using LogExporter.Sinks;

namespace LogExporter.Benchmarks;

/// <summary>A no-op sink shared by the dispatcher benchmarking projects.</summary>
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

    public void Dispose()
    {
        // The counting sink has no owned resources to release.
    }
}
