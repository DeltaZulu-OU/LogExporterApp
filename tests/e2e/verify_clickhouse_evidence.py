"""Validate persisted ClickHouse E2E evidence independently of pytest fixtures."""

import hashlib
import json
import os
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).with_name("results") / "clickhouse"


def source_commit():
    return os.environ.get("E2E_SOURCE_COMMIT") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2],
        text=True,
    ).strip()
EXPECTED = {
    "test_clickhouse_persists_live_dns_event": 1,
    "test_clickhouse_preserves_authoritative_nxdomain": 1,
    "test_clickhouse_disabled_does_not_capture_events": 0,
    "test_clickhouse_invalid_replay_config_keeps_forwarder_active": 0,
}


def verify():
    manifests = sorted(ROOT.glob("*.manifest.json"))
    actual = {path.name.removesuffix(".manifest.json") for path in manifests}
    assert actual == set(EXPECTED), (actual, set(EXPECTED))

    for manifest_path in manifests:
        name = manifest_path.name.removesuffix(".manifest.json")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert manifest["test"] == name
        assert manifest["test_outcome"] == "passed", name
        assert re.fullmatch(r"[0-9a-f]{40}", manifest["commit"]), name
        assert manifest["commit"] == source_commit(), name
        assert manifest["expected_row_count"] == EXPECTED[name], name
        assert manifest["format"] == "JSONEachRow"
        assert manifest["table"].startswith("dns.dns_e2e_")
        assert manifest["data_file"] == name + ".jsonl"

        data = ROOT / manifest["data_file"]
        assert data.is_file(), data
        raw = data.read_bytes()
        assert manifest["sha256"] == hashlib.sha256(raw).hexdigest(), name
        lines = raw.decode("utf-8").splitlines()
        rows = [json.loads(line) for line in lines if line.strip()]
        assert len(rows) == manifest["row_count"], name
        assert len(rows) == EXPECTED[name], name
        for row in rows:
            assert isinstance(json.loads(row["payload_json"]), dict), name


if __name__ == "__main__":
    verify()
    print("Validated ClickHouse E2E query-result artifacts.")
