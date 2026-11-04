"""Live Technitium-to-ClickHouse E2E tests using the shared release-ZIP harness.

The separate MSTest suite owns mocked Serilog adapter contract tests.
"""
import json
import uuid

import pytest
import requests

from conftest import probe, wait_until


CLICKHOUSE_URL = "http://127.0.0.1:18123/"


def query(sql):
    response = requests.post(CLICKHOUSE_URL, data=sql.encode("utf-8"), timeout=15)
    response.raise_for_status()
    return response.text.strip()


@pytest.fixture
def clickhouse_table():
    name = "dns_e2e_" + uuid.uuid4().hex
    query("CREATE DATABASE IF NOT EXISTS dns")
    query(f"""CREATE TABLE dns.{name} (
        timestamp DateTime64(3, 'UTC'),
        level LowCardinality(String),
        message String,
        message_template String,
        exception Nullable(String),
        properties JSON,
        client_ip String,
        q_name String,
        q_type LowCardinality(String),
        response_code LowCardinality(String),
        response_type LowCardinality(String),
        payload_json String
    ) ENGINE = MergeTree ORDER BY (timestamp, q_name)""")
    try:
        yield name
    finally:
        query(f"DROP TABLE IF EXISTS dns.{name}")


def enable_clickhouse(runtime, table):
    config = runtime.config()
    config["sinks"]["clickhouse"] = {
        "enabled": True,
        "connectionString": "Host=clickhouse;Port=8123;Database=dns;Username=default",
        "database": "dns",
        "table": table,
        "batching": {"batchSize": 1, "batchDelayMs": 20},
    }
    runtime.set_config(config)


def stored_rows(table, name):
    rows = query(
        f"SELECT client_ip, q_name, q_type, response_code, response_type, "
        f"payload_json FROM dns.{table} WHERE q_name = '{name}' FORMAT JSONEachRow"
    )
    return [json.loads(line) for line in rows.splitlines() if line]


def test_clickhouse_persists_live_dns_event(runtime, unique_name, clickhouse_table):
    enable_clickhouse(runtime, clickhouse_table)
    result = probe(unique_name)
    assert "status: NOERROR" in result

    rows = wait_until(lambda: stored_rows(clickhouse_table, unique_name),
                      "ClickHouse DNS insertion", timeout=25)
    assert len(rows) == 1
    row = rows[0]
    assert row["q_name"] == unique_name
    assert row["q_type"] == "A"
    assert row["response_code"] == "NoError"
    assert row["response_type"] != "Blocked"
    assert row["client_ip"]
    payload = json.loads(row["payload_json"])
    assert payload["question"]["questionName"] == unique_name
    assert payload["meta"]["tags"] == ["tenant:ci"]
    assert payload["clientIp"] == row["client_ip"]
    assert runtime.event(unique_name)["question"]["questionName"] == unique_name


def test_clickhouse_preserves_blocking_evidence(runtime, clickhouse_table):
    enable_clickhouse(runtime, clickhouse_table)
    result = probe("blocked.e2e.invalid")
    assert "status: NXDOMAIN" in result
    assert "EDE: 15" in result

    rows = wait_until(lambda: stored_rows(clickhouse_table, "blocked.e2e.invalid"),
                      "blocked query in ClickHouse", timeout=25)
    row = rows[0]
    assert row["response_code"] == "NxDomain"
    assert row["response_type"] == "Blocked"
    payload = json.loads(row["payload_json"])
    assert payload["responseType"] == "Blocked"
    assert any(error.get("errType") == "Blocked" for error in payload["edns"])


def test_clickhouse_disabled_does_not_capture_events(runtime, unique_name, clickhouse_table):
    config = runtime.config()
    config["sinks"]["clickhouse"] = {
        "enabled": False,
        "connectionString": "Host=clickhouse;Port=8123;Database=dns;Username=default",
        "database": "dns",
        "table": clickhouse_table,
    }
    runtime.set_config(config)
    assert "status: NOERROR" in probe(unique_name)
    assert runtime.event(unique_name)["question"]["questionName"] == unique_name
    assert stored_rows(clickhouse_table, unique_name) == []


def test_clickhouse_invalid_replay_config_keeps_exporter_active(runtime, unique_name, clickhouse_table):
    config = runtime.config()
    config["sinks"]["clickhouse"] = {
        "enabled": True,
        "connectionString": "Host=clickhouse;Port=8123;Database=dns;Username=default",
        "database": "dns",
        "table": clickhouse_table,
        "allowBatchReplay": True,
    }
    from conftest import API
    response = runtime.session.post(
        f"{API}/apps/config/set?name=LogExporterApp",
        data={"config": json.dumps(config)},
        timeout=20,
    )
    try:
        result = response.json()
    except requests.exceptions.JSONDecodeError:
        result = {}
    assert result.get("status") != "ok"
    assert "status: NOERROR" in probe(unique_name)
    assert runtime.event(unique_name)["question"]["questionName"] == unique_name
