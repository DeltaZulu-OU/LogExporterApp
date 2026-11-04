"""Real DNS, real Technitium plugin, and real HTTP/file/Syslog sink tests."""

import json
import re
import subprocess
import time
from datetime import datetime, timezone

from conftest import API, event_names, matches, probe, wait_until


def assert_common(record, name, qtype, protocol="Udp"):
    assert record["question"]["questionName"] == name
    assert record["question"]["questionType"] == qtype
    assert record["question"]["questionClass"] == "IN"
    assert record["responseCode"] == "NoError"
    assert record["protocol"] == protocol
    assert record["meta"]["tags"] == ["tenant:ci"]
    parsed = datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00"))
    assert abs((datetime.now(timezone.utc) - parsed).total_seconds()) < 120
    assert record["clientIp"]
    assert record["nameServer"]


def test_benign_udp_exports_http_file_and_syslog(runtime, unique_name):
    output = probe(unique_name)
    assert "status: NOERROR" in output
    assert "192.0.2.10" in output
    http = runtime.event(unique_name)
    assert_common(http, unique_name, "A")
    assert http["responseType"] != "Blocked"
    assert http["edns"] == []
    assert any(a["recordData"] == "192.0.2.10" for a in http["answers"])
    assert_common(runtime.file_event(unique_name), unique_name, "A")
    packet = wait_until(lambda: next((line for line in runtime.collector()["syslog_udp"]
                                    if f'qName="{unique_name}"' in line), None), "RFC 5424 UDP event")
    assert re.match(r"^<\d{1,3}>1\s", packet)
    assert 'rCode="NoError"' in packet


def test_pinned_fixture_public_blocked_examples(runtime):
    # These names were present in StevenBlack/hosts on 2026-11-04.
    # Their presence in a blocklist does not independently establish maliciousness.
    # Technitium 15.6.2 classifies these as Blocked, unlike the synthetic
    # .invalid names that resolve to authoritative NXDOMAIN in this harness.
    for name in ("adsflame.com", "aircovid19virus.com"):
        output = probe(name)
        assert "status: NXDOMAIN" in output
        record = runtime.event(name)
        assert record["responseCode"] == "NxDomain"
        assert record["responseType"] == "Blocked"



def test_edns_client_subnet_is_used_as_trusted_source_address(runtime, unique_name):
    # The Docker host is allowed by dnsReverseProxyNetworkACL in this isolated
    # environment. ECS source-address substitution is distinct from EDE.
    subnet = "198.51.100.42/32"
    output = probe(unique_name, ecs=subnet)
    assert "status: NOERROR" in output
    record = runtime.event(unique_name)
    assert record["clientIp"] == "198.51.100.42"


def test_benign_tcp_aaaa(runtime, unique_name):
    output = probe(unique_name, qtype="AAAA", tcp=True)
    assert "status: NOERROR" in output
    assert "2001:db8::10" in output
    record = runtime.event(unique_name, qtype="AAAA")
    assert_common(record, unique_name, "AAAA", "Tcp")
    assert any(a["recordData"] == "2001:db8::10" for a in record["answers"])


def test_fixture_nxdomain_is_exported_to_http_and_file(runtime):
    output = probe("blocked.e2e.invalid")
    assert "status: NXDOMAIN" in output
    record = runtime.event("blocked.e2e.invalid")
    assert record["responseType"] == "Authoritative"
    assert record["responseCode"] == "NxDomain"
    assert record["edns"] == []
    assert runtime.file_event("blocked.e2e.invalid")["responseType"] == "Authoritative"


def test_fixture_nxdomain_without_edns_is_exported(runtime):
    output = probe("noedns.e2e.invalid", edns=False)
    assert "status: NXDOMAIN" in output
    assert "EDE:" not in output
    record = runtime.event("noedns.e2e.invalid")
    assert record["responseType"] == "Authoritative"
    assert record["edns"] == []


def test_http_ndjson_contract(runtime, unique_name):
    probe(unique_name)
    runtime.event(unique_name)
    packets = runtime.collector()["http"]
    matched = [packet for packet in packets if matches(
        [json.loads(line) for line in packet["body"].splitlines() if line], unique_name)]
    assert matched
    assert all(packet["content_type"].startswith("application/x-ndjson") for packet in matched)
    records = [json.loads(line) for packet in matched for line in packet["body"].splitlines() if line]
    assert all(isinstance(r.get("hostname"), str) for r in records)
    assert all(r["hostname"] for r in records)


def test_console_ndjson(runtime, unique_name):
    probe(unique_name)
    runtime.event(unique_name)

    def find():
        result = subprocess.run(["docker", "compose", "-f", "tests/e2e/compose.yml",
                                 "logs", "--no-color", "dns"],
                                capture_output=True, text=True, timeout=10, check=True)
        for line in result.stdout.splitlines():
            if unique_name not in line or "{" not in line:
                continue
            try:
                record = json.loads(line[line.index("{"):])
            except json.JSONDecodeError:
                continue
            if matches([record], unique_name):
                return record
        return None

    assert_common(wait_until(find, "console NDJSON event"), unique_name, "A")


def test_batch_size_is_bounded(runtime):
    config = runtime.config()
    config["sinks"]["http"]["batching"]["batchSize"] = 2
    runtime.set_config(config)
    names = [f"batch-{i}-{time.time_ns()}.e2e.test" for i in range(7)]
    for name in names:
        assert "status: NOERROR" in probe(name)
    wait_until(lambda: all(any(matches(event_names(runtime.collector()), name) for name in names)
                           for name in names), "all bounded-batch events", timeout=25)
    batch_sizes = [len([line for line in packet["body"].splitlines() if line.strip()])
                   for packet in runtime.collector()["http"]]
    assert all(size >= 1 for size in batch_sizes)
    assert all(size <= 2 for size in batch_sizes)


def test_disabling_http_does_not_disable_file(runtime, unique_name):
    config = runtime.config()
    config["sinks"]["http"]["enabled"] = False
    runtime.set_config(config)
    assert "status: NOERROR" in probe(unique_name)
    runtime.file_event(unique_name)
    assert not matches(event_names(runtime.collector()), unique_name)


def test_http_503_retries_only_when_enabled(runtime, unique_name):
    config = runtime.config()
    config["sinks"]["http"]["allowBatchReplay"] = True
    runtime.set_config(config)
    runtime.collector("/control", {"failure_count": 1, "status": 503})
    probe(unique_name)
    runtime.event(unique_name)
    attempts = [a for a in runtime.collector()["attempts"] if unique_name in a["body"]]
    assert len(attempts) >= 2
    assert attempts[0]["status"] == 503
    assert attempts[1]["status"] == 200
    assert runtime.file_event(unique_name)["responseCode"] == "NoError"


def test_http_503_without_replay_leaves_file_healthy(runtime, unique_name):
    runtime.collector("/control", {"failure_count": 1, "status": 503})
    probe(unique_name)
    runtime.file_event(unique_name)
    attempts = wait_until(lambda: [a for a in runtime.collector()["attempts"] if unique_name in a["body"]],
                          "HTTP failed attempt")
    assert attempts[0]["status"] == 503
    time.sleep(1)  # Negative assertion: permit forbidden replay to emerge.
    assert len([a for a in runtime.collector()["attempts"] if unique_name in a["body"]]) == 1


def test_http_400_is_not_replayed(runtime, unique_name):
    config = runtime.config()
    config["sinks"]["http"]["allowBatchReplay"] = True
    runtime.set_config(config)
    runtime.collector("/control", {"failure_count": 1, "status": 400})
    probe(unique_name)
    runtime.file_event(unique_name)
    attempts = wait_until(lambda: [a for a in runtime.collector()["attempts"] if unique_name in a["body"]],
                          "HTTP rejected attempt")
    assert attempts[0]["status"] == 400
    time.sleep(1)  # Negative assertion: permit forbidden replay to emerge.
    assert len([a for a in runtime.collector()["attempts"] if unique_name in a["body"]]) == 1


def test_syslog_tcp_octet_counting(runtime, unique_name):
    config = runtime.config()
    config["sinks"]["syslog"].update({"protocol": "TCP", "port": 5515})
    runtime.set_config(config)
    probe(unique_name)
    packet = wait_until(lambda: next((line for line in runtime.collector()["syslog_tcp"]
                                    if f'qName="{unique_name}"' in line), None), "RFC 5424 TCP event")
    assert re.match(r"^<\d{1,3}>1\s", packet)
    assert 'rCode="NoError"' in packet


def test_reload_changes_http_target(runtime, unique_name):
    config = runtime.config()
    config["sinks"]["http"]["endpoint"] = "http://collector:8080/events/alternate"
    runtime.set_config(config)
    probe(unique_name)
    runtime.event(unique_name, path="/events/alternate")
    assert not matches(event_names(runtime.collector(), path="/events"), unique_name)


def test_edns_disabled_does_not_emit_optional_ede(runtime):
    config = runtime.config()
    config["sinks"]["enableEdnsLogging"] = False
    runtime.set_config(config)
    output = probe("blocked.e2e.invalid")
    assert "status: NXDOMAIN" in output
    assert "EDE:" not in output
    record = runtime.event("blocked.e2e.invalid")
    assert record["responseType"] == "Authoritative"
    assert record["edns"] == []


def test_invalid_config_preserves_active_forwarder(runtime, unique_name):
    import requests
    response = runtime.session.post(
        f"{API}/apps/config/set?name=LogForwarderApp",
        data={"config": json.dumps({"sinks": {"file": {"enabled": True}}, "pipeline": {}})},
        timeout=20,
    )
    try:
        payload = response.json()
    except requests.exceptions.JSONDecodeError:
        payload = {}
    assert payload.get("status") != "ok", payload
    assert "status: NOERROR" in probe(unique_name)
    assert_common(runtime.event(unique_name), unique_name, "A")


def test_unavailable_file_sink_does_not_disable_http(runtime, unique_name):
    config = runtime.config()
    config["sinks"]["file"]["path"] = "/missing/parent/dns.ndjson"
    runtime.set_config(config)
    assert "status: NOERROR" in probe(unique_name)
    assert_common(runtime.event(unique_name), unique_name, "A")


def test_reapplying_configuration_does_not_duplicate_http(runtime, unique_name):
    runtime.set_config(runtime.config())
    runtime.set_config(runtime.config())
    probe(unique_name)
    runtime.event(unique_name)
    attempts = [a for a in runtime.collector()["attempts"] if unique_name in a["body"]]
    assert len(attempts) == 1
