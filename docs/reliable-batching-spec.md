# Reliable batching — implementation specification

Status: proposed; implementation and runtime validation pending.
Branch: `feat/reliable-batching`
Scope: the shared `SinkDispatcher` and tests; no ClickHouse implementation.

## Goal

Improve the resilience of DNS telemetry export by borrowing failure-aware scheduling principles from Serilog's batching system while preserving `LogEntry`, `IOutputSink`, per-sink isolation and bounded shutdown. Serilog is a behavioral reference, not a required dependency.

## Existing behavior to characterize

- One bounded channel and worker per sink type.
- Queue saturation uses `DropWrite` and periodic drop reporting.
- Batches contain at most 1,000 events and use a 25 ms accumulation window.
- An `ExportAsync` exception is reported and the batch is cleared.
- `DrainAsync(timeout)` aborts workers when the deadline expires; disposal is bounded.
- The source queue is memory-only. Neither the existing nor proposed design guarantees persistence across process termination.

## Proposed behavior

1. Retain the current failed batch and retry with bounded exponential backoff and jitter, with a configurable retry-time budget. A subsequent batch must not overtake a failed batch within a sink.
2. Distinguish cancellation from export failure. Cancellation during shutdown must not enter a retry loop.
3. Make maximum batch size, accumulation time, queue capacity and retry budget configurable per sink. Preserve existing values as defaults where possible.
4. Keep sink workers independent; failure or backoff in one sink must not stall another.
5. Record counters for enqueue drops, attempted batches, successful batches/events, retries, failed batches/events, and exhausted retries. Report terminal failures without exposing secrets or full DNS payloads.
6. Preserve a bounded shutdown and clearly document that in-flight data may be lost if the deadline expires.
7. Do not promise exactly-once delivery. An ambiguous remote acknowledgment can produce duplicates after retry. Document at-least-once *attempts* only while retained in memory, not guaranteed delivery.
8. Avoid unbounded retained memory, retry storms, blocking of DNS request handling, and nested retry/batching mechanisms.

## Red–green–refactor acceptance tests

| ID | Scenario | Expected assertion |
| --- | --- | --- |
| RB-01 | First export fails transiently, second succeeds | Same batch contents retried; no silent loss |
| RB-02 | Export repeatedly fails | Retry/backoff respects configured budget, eventually reports terminal loss |
| RB-03 | One sink is stalled or retrying | Other sink receives events independently |
| RB-04 | Queue reaches capacity | No unbounded memory growth; dropped events counted |
| RB-05 | Worker succeeds | No duplicate application-level sends |
| RB-06 | Low event rate | Partial batch flushed by configured accumulation deadline |
| RB-07 | High event rate | Batches never exceed configured maximum |
| RB-08 | Shutdown during backoff | Drain/abort finishes within bounded deadline |
| RB-09 | Shutdown during stalled export | Cancellation propagates to cooperative sinks; noncooperative sinks cannot block indefinitely |
| RB-10 | Acknowledgment lost after server commit | Retry may duplicate rows; test explicitly documents this limitation |
| RB-11 | Sink-specific configuration | Defaults remain compatible; invalid limits rejected |
| RB-12 | Multiple sinks, repeated failures | No cross-sink queue sharing or retry interference |

Use deterministic fake sinks and an injectable clock/delay abstraction where necessary; do not use wall-clock sleeps for retry timing tests. Add a benchmark contrasting baseline and revised dispatcher under equivalent load, allocations, slow-sink conditions, and error rates.

## Migration sequence

1. Add baseline characterization tests against the existing dispatcher.
2. Add failing tests for retained-batch retries and terminal failure accounting.
3. Implement the minimum failure-aware scheduler.
4. Expose per-sink options while keeping existing defaults.
5. Add deterministic cancellation, saturation, and fault-injection tests.
6. Benchmark and inspect for throughput or allocation regressions.
7. Validate the change separately with existing File, HTTP, Syslog and Console sinks.

## Exit criteria

No merge until tests establish retry correctness, nonblocking DNS request handling, isolation, bounded memory and shutdown, accurate loss reporting, and no unacceptable performance regression. Update delivery-semantics documentation before release.

## Reference

Serilog batching source: https://github.com/serilog/serilog/blob/dev/src/Serilog/Core/Sinks/Batching/BatchingSink.cs

## Initial implementation (2026-11-04)

Implemented on this branch: per-sink `SinkBatchOptions`, configurable `batching` blocks, bounded exponential retry with jitter, retention of the current batch during retry, per-sink delivery counters, and terminal loss notices.

Example sink configuration:

```json
{
  "sinks": {
    "maxQueueSize": 100000,
    "http": {
      "enabled": true,
      "endpoint": "https://example.com/logs",
      "batching": {
        "batchSize": 1000,
        "batchDelayMs": 25,
        "retryTimeLimitMs": 2000,
        "initialRetryDelayMs": 50,
        "maxRetryDelayMs": 500
      }
    }
  },
  "pipeline": {}
}
```

The dispatcher exposes a point-in-time `GetCounters(typeof(HttpSink))` snapshot for diagnostics. These are in-memory counters, not yet exported through a monitoring endpoint. Terminal loss is separately logged, even when transient error logging is rate-limited.

Important limits: the retry budget bounds delays between cooperative calls; it cannot interrupt a sink implementation that indefinitely ignores its cancellation token. No disk spool is provided. Successful remote writes followed by lost acknowledgments can result in duplicates on retry. A 2-second retry budget is a deliberately conservative default and is not a guarantee against prolonged outages.

**Validation status:** source commits and tests are present, but they have not been compiled or run in this environment. Integration testing, fault injection and performance benchmarks remain required before merge. This stage does not claim all twelve acceptance tests have been satisfied.
