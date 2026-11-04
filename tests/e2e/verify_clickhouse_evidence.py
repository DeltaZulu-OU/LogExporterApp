"""Validate persisted ClickHouse E2E evidence independently of pytest fixtures."""

import json
from pathlib import Path

ROOT = Path(__file__).with_name("results") / "clickhouse"
EXPECTED = {
    "test_clickhouse_persists_live_dns_event": True,
    "test_clickhouse_preserves_authoritative_nxdomain": True,
    "test_clickhouse_disabled_does_not_capture_events": False,
    "test_clickhouse_invalid_replay_config_keeps_exporter_active": False,
}


def verify():
    manifests = sorted(ROOT.glob("*.manifest.json"))
    actual = {path.name.removesuffix(".manifest.json") for path in manifests}
    assert actual == set(EXPECTED), (actual, set(EXPECTED))

    for manifest_path in manifests:
        name = manifest_path.name.removesuffix(".manifest.json")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert manifest["test"] == name
        assert manifest["format"] == "JSONEachRow"
        assert manifest["table"].startswith("dns.dns_e2e_")
        assert manifest["data_file"] == name + ".jsonl"

        data = ROOT / manifest["data_file"]
        assert data.is_file(), data
        lines = data.read_text(encoding="utf-8").splitlines()
        rows = [json.loads(line) for line in lines if line.strip()]
        assert len(rows) == manifest["row_count"], name
        assert bool(rows) == EXPECTED[name], name
        for row in rows:
            assert isinstance(json.loads(row["payload_json"]), dict), name


if __name__ == "__main__":
    verify()
    print("Validated ClickHouse E2E query-result artifacts.")
