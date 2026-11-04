
namespace LogExporter.Benchmarks;

internal sealed record Sample(double Milliseconds, long AllocatedBytes, int Batches);

internal sealed record CaseResult(int BatchSize, int Events, int Iterations,
    double MedianMilliseconds, double EventsPerSecond, double MedianAllocatedBytes,
    double MeanExportCalls, double MinMilliseconds, double MaxMilliseconds);

internal sealed record BenchmarkReport(DateTimeOffset Timestamp, string Runtime, string OS,
    int ProcessorCount, string Scope, List<CaseResult> Results);
