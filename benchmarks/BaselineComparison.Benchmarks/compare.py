#!/usr/bin/env python3
"""Compare identically configured master/PR dispatcher runs. Diagnostic only."""
import argparse
import json
import math
import statistics
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--baseline", nargs="+", required=True)
parser.add_argument("--candidate", nargs="+", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

def load(paths, expected):
    runs = [json.loads(Path(p).read_text(encoding="utf-8")) for p in paths]
    if any(r["Implementation"] != expected for r in runs):
        raise ValueError(f"Expected {expected} runs only")
    return runs

baseline = load(args.baseline, "master")
candidate = load(args.candidate, "pr")
all_runs = baseline + candidate
reference = all_runs[0]
for run in all_runs[1:]:
    for key in ("BatchSize", "BatchDelayMs", "Runtime", "OS", "ProcessorCount"):
        if run[key] != reference[key]:
            raise ValueError(f"Incomparable runs: {key}")
    if [(c["Name"], c["Events"], c["Sinks"], c["Chunk"]) for c in run["Cases"]] != [
        (c["Name"], c["Events"], c["Sinks"], c["Chunk"]) for c in reference["Cases"]
    ]:
        raise ValueError("Incomparable scenarios")

lines = [
    "# Original master versus PR #9 — dispatcher benchmark",
    "",
    f"Runtime: {reference['Runtime']}; OS: {reference['OS']}; CPUs: {reference['ProcessorCount']}.",
    f"Batch size: {reference['BatchSize']}; batch delay: {reference['BatchDelayMs']} ms.",
    "Identical benchmark source is compiled separately against the baseline and candidate source revisions.",
    f"Runs: {len(baseline)} master, {len(candidate)} PR; process order is recorded in the CI workflow.",
    "Elapsed time includes enqueue and complete drain with an in-memory no-op sink; neither variant performs I/O.",
    "Allocations are process-wide and may include background runtime work.",
    "Median over every measured iteration from all runs. Warm-ups are excluded.",
    "Shared-runner measurements are descriptive; differences are not significance-tested.",
    "No fixed throughput regression gate is imposed. Delivery integrity is checked in each trial.",
    "",
    "| Scenario | Master median ms | PR median ms | PR speed ratio | Master events/s | PR events/s | Master alloc KB | PR alloc KB |",
    "|---|---:|---:|---:|---:|---:|---:|---:|",
]
comparison = {"master_runs": len(baseline), "pr_runs": len(candidate), "cases": []}
for i, original in enumerate(reference["Cases"]):
    def samples(runs, key):
        return [
            value
            for run in runs
            for value in run["Cases"][i][key]
        ]
    mtime = statistics.median(samples(baseline, "DurationSamples"))
    ptime = statistics.median(samples(candidate, "DurationSamples"))
    malloc = statistics.median(samples(baseline, "AllocationSamples"))
    palloc = statistics.median(samples(candidate, "AllocationSamples"))
    ratio = mtime / ptime if ptime > 0 else math.nan
    events = original["Events"]
    lines.append(
        f"| {original['Name']} | {mtime:.3f} | {ptime:.3f} | {ratio:.2f}x "
        f"| {events * 1000 / mtime:.0f} | {events * 1000 / ptime:.0f} "
        f"| {malloc / 1024:.1f} | {palloc / 1024:.1f} |"
    )
    comparison["cases"].append({
        "scenario": original["Name"],
        "master_median_ms": mtime,
        "pr_median_ms": ptime,
        "pr_speed_ratio": ratio,
        "master_alloc_bytes": malloc,
        "pr_alloc_bytes": palloc,
        "master_duration_samples": samples(baseline, "DurationSamples"),
        "pr_duration_samples": samples(candidate, "DurationSamples"),
    })

out = Path(args.output)
out.mkdir(parents=True, exist_ok=True)
(out / "comparison.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
(out / "comparison.json").write_text(json.dumps(comparison, indent=2) + "\n", encoding="utf-8")
print("\n".join(lines))
