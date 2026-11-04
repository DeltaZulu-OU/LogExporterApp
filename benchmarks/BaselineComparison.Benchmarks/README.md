# Master vs PR dispatcher comparison

This harness is compiled twice from **identical source**: against the PR's
`SinkDispatcher` and against the PR base revision of `master`. CI checks out
both revisions with their corresponding upstream submodules. Only the benchmark
harness is copied into the baseline checkout; neither production implementation
is copied or modified.

The legacy dispatcher has fixed batch size 1,000 and a 25 ms flush delay. The
candidate is configured with these same values, the same bounded queue capacity,
and no batch retry. A successful `DrainAsync` must be followed by exactly
100,000 acknowledged events per active sink. The PR additionally validates its
delivery counters. Each batch must have at most 1,000 entries.

## Scenarios

| Scenario | Sinks | Enqueue shape | Purpose |
|---|---:|---|---|
| single-burst | 1 | One 100,000-event call | Enqueue/worker overhead |
| chunked | 1 | 100 calls of 1,000 events | Incremental producer handoff |

The comparison intentionally measures only one sink. Multi-sink fan-out and sink-isolation behavior are outside its performance scope.

The benchmark deliberately uses a counting no-op sink and does not serialize
DNS records, perform network I/O, write files, or inject failures. It cannot
measure retry recovery, storage throughput, or end-to-end ingestion latency.
A separate suite would be needed for those features, since the original
dispatcher lacks retries and delivery accounting.

CI warms each scenario before seven timed trials, then runs the full process
in **A-B-B-A order** (master, PR, PR, master). The report combines 14 measured
trials per scenario per implementation, using their medians. Each run is
compiled independently and executes sequentially on the same GitHub runner.

Elapsed time includes enqueueing and draining, but excludes process startup,
build time and record-array allocation. GC allocated bytes are process-wide
and may include unrelated background activity. Small differences are subject
to host scheduling, CPU frequency, JIT and GC noise. Results are diagnostic
and do not impose a hard throughput gate. The raw samples, comparison
ratios and execution metadata are uploaded as a workflow artifact.

## Reproduction

Open the **Compare dispatcher against master** workflow in GitHub Actions and
trigger `workflow_dispatch` on the feature branch, or update the PR.

This is a performance comparison between the original and new dispatcher
implementations under equivalent features; it must not be confused with the
existing within-PR batch-size tuning benchmark.
