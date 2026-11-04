"""Live Technitium provisioning and query/collector assertions for E2E tests."""

import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import pytest
import requests

API = "http://127.0.0.1:15380/api"
COLLECTOR = "http://127.0.0.1:18080"
ROOT = Path(__file__).resolve().parents[2]
RESULTS = Path(__file__).with_name("results")


def wait_until(check, description, timeout=20):
    end = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < end:
        try:
            result = check()
            if result:
                return result
        except (AssertionError, requests.RequestException, OSError) as exc:
            last_error = str(exc)
        time.sleep(0.2)
    pytest.fail(f"Timed out waiting for {description}. {last_error or ''}")


def event_names(state, sink="http", path=None):
    records = []
    for packet in state[sink]:
        if path and packet["path"] != path:
            continue
        for line in packet["body"].splitlines():
            if line.strip():
                records.append(json.loads(line))
    return records


def matches(records, name, qtype="A"):
    return [r for r in records if (r.get("question") or {}).get("questionName", "").rstrip(".").lower() == name.lower()
            and (r.get("question") or {}).get("questionType") == qtype]


def probe(name, qtype="A", tcp=False, edns=True):
    cmd = ["dig", "@127.0.0.1", "-p", "1053", name, qtype, "+tries=1", "+time=2",
           "+noall", "+comments", "+answer", "+additional"]
    cmd.append("+tcp" if tcp else "+notcp")
    cmd.append("+edns=0" if edns else "+noedns")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=8, check=False)
    assert result.returncode == 0, f"dig failed: {result.stdout} {result.stderr}"
    assert "status:" in result.stdout, result.stdout
    return result.stdout


class Runtime:
    def __init__(self, token):
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    def api(self, path, data=None, files=None, *, check=True):
        response = self.session.post(f"{API}/{path}", data=data, files=files, timeout=20)
        response.raise_for_status()
        payload = response.json()
        if check:
            assert payload.get("status") == "ok", f"{path}: {payload.get('status')} {payload.get('errorMessage', '')}"
        return payload

    def config(self, **overrides):
        sinks = {
            "maxQueueSize": 4096,
            "enableEdnsLogging": True,
            "console": {"enabled": True},
            "file": {"enabled": True, "path": "/results/dns.ndjson"},
            "http": {"enabled": True, "endpoint": "http://collector:8080/events", "allowBatchReplay": False,
                     "batching": {"batchSize": 1, "batchDelayMs": 20, "retryTimeLimitMs": 800,
                                  "initialRetryDelayMs": 30, "maxRetryDelayMs": 100}},
            "syslog": {"enabled": True, "address": "collector", "port": 5514, "protocol": "UDP"}
        }
        sinks.update(overrides)
        return {"sinks": sinks, "pipeline": {"normalize": {"enabled": False},
                                            "tagging": {"enabled": True, "tags": ["tenant:ci"]}}}

    def set_config(self, config):
        return self.api("apps/config/set?name=LogExporterApp", {"config": json.dumps(config)})

    def collector(self, path="/state", data=None):
        if data is None:
            res = requests.get(f"{COLLECTOR}{path}", timeout=5)
        else:
            res = requests.post(f"{COLLECTOR}{path}", json=data, timeout=5)
        res.raise_for_status()
        return res.json()

    def event(self, name, sink="http", qtype="A", path=None, timeout=12):
        def find():
            found = matches(event_names(self.collector(), sink, path=path), name, qtype)
            return found[0] if found else None
        return wait_until(find, f"{name} in {sink}", timeout)

    def file_event(self, name, qtype="A"):
        def find():
            if not (RESULTS / "dns.ndjson").exists():
                return None
            records = []
            for line in (RESULTS / "dns.ndjson").read_text().splitlines():
                if not line.strip():
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    # The writer may still be finishing the last NDJSON line.
                    continue
            found = matches(records, name, qtype)
            return found[0] if found else None
        return wait_until(find, f"{name} in file")


@pytest.fixture(scope="session")
def runtime():
    password = os.environ.get("E2E_ADMIN_PASSWORD")
    package = os.environ.get("E2E_PACKAGE")
    assert password, "E2E_ADMIN_PASSWORD must be set"
    assert package, "E2E_PACKAGE must be set"
    artifact = ROOT / package
    assert artifact.is_file(), f"Missing plugin ZIP: {artifact}"
    wait_until(lambda: requests.get(f"{API}/status", timeout=3).json().get("status") == "ok",
               "Technitium API readiness", timeout=90)
    login = requests.post(f"{API}/user/login", data={"user": "admin", "pass": password}, timeout=10).json()
    assert login.get("status") == "ok", "Technitium authentication failed"
    assert login.get("token"), "Technitium login returned no token"
    server = Runtime(login["token"])
    server.api("zones/create", {"zone": "e2e.test", "type": "Primary"})
    for qtype, address in (("A", "192.0.2.10"), ("AAAA", "2001:db8::10")):
        server.api("zones/records/add", {"zone": "e2e.test", "domain": "*.e2e.test",
                                         "type": qtype, "ipAddress": address, "ttl": 60})
    server.api("settings/set", {"enableBlocking": "true", "blockingType": "NxDomain",
                                "allowRecursion": "false", "logQueries": "true",
                                "allowTxtBlockingReport": "true",
                                "blockListUrls": "http://collector:8080/blocklist.txt"})
    with artifact.open("rb") as fp:
        response = server.api("apps/install?name=LogExporterApp",
                              files={"file": (artifact.name, fp, "application/zip")})
    apps = server.api("apps/list")["response"]["apps"]
    installed = next((app for app in apps if app.get("name") == "LogExporterApp"), None)
    assert installed is not None, f"Plugin not installed: {response}"
    assert any(entry.get("isQueryLogger") for entry in installed.get("dnsApps", [])), (
        f"Plugin does not register a query logger: {installed}")
    server.set_config(server.config())
    server.api("settings/forceUpdateBlockLists")
    wait_until(lambda: server.collector()["blocklist_hits"] > 0, "blocklist fetch", timeout=60)
    wait_until(lambda: "EDE: 15" in probe("blocked.e2e.invalid"),
               "blocked DNS response with EDE 15", timeout=60)
    yield server
    server.session.close()


@pytest.fixture(autouse=True)
def baseline(runtime):
    runtime.set_config(runtime.config())
    runtime.collector("/reset", {})


@pytest.fixture
def unique_name():
    return f"run-{uuid.uuid4().hex[:16]}.e2e.test"
