# Live Technitium DNS end-to-end tests

The E2E suite installs the actual release ZIP in a disposable Technitium DNS Server 15.6.2, sends dig queries over UDP and TCP, and validates HTTP NDJSON, file NDJSON, console NDJSON, and RFC 5424 UDP/TCP Syslog, and the official Serilog ClickHouse sink against ClickHouse 25.8. A local pinned fixture is imported into Technitium's blocked zone through its API, without asynchronous blocklist downloads or external feeds. It includes synthetic domains and two pinned examples observed in StevenBlack/hosts (adsflame.com, aircovid19virus.com, checked 2026-11-04); list membership is not independent evidence of maliciousness.

## CI and local reproduction

The GitHub Actions workflow is triggered by pushes to master, relevant pull requests and manual workflow dispatch. There are no scheduled triggers. It builds the .NET 10 plugin against the pinned git submodules and uses the existing New-ReleasePackage.ps1 script before starting Docker Compose.

To reproduce, install Docker Compose, .NET 10, PowerShell, Python 3.12, dig, pytest and requests. Build TechnitiumLibrary, build src/LogExporterApp.csproj in Release, then package with .github/scripts/New-ReleasePackage.ps1. Set E2E_PACKAGE to the relative ZIP path and E2E_ADMIN_PASSWORD to a random value. Run docker compose -f tests/e2e/compose.yml up -d --wait; pytest -q tests/e2e/test_e2e.py; then docker compose -f tests/e2e/compose.yml down -v.

JUnit XML, sanitized collector state, container diagnostics, and the file sink output are captured in the `e2e-evidence` artifact.

### ClickHouse query-result evidence

Every ClickHouse E2E test exports the **actual SQL query response** from its
disposable table before dropping the table. Files are written to
`tests/e2e/results/clickhouse/` and included in the existing `e2e-evidence`
workflow artifact:

- `<test_name>.jsonl`: complete `SELECT * ... FORMAT JSONEachRow` output,
  including `payload_json` and all typed columns. An intentionally empty file
  indicates that no events were inserted (disabled/rejected configuration).
- `<test_name>.manifest.json`: test name, temporary table, **expected and
  actual row counts**, query-result format, data filename, SHA-256 of the raw
  JSONL bytes, source Git commit and pytest call-phase outcome. The expected
  count is predefined per test, not inferred from the returned data.

Exports run during pytest fixture teardown, including when an individual
test fails. Cleanup still drops the temporary table if evidence export fails.
The E2E workflow validates file/manifest consistency, expected counts, digest,
source commit and passing test outcomes on successful test runs. The recorded
status describes the pytest call phase; fixture teardown may subsequently fail.
SHA-256 detects changes to the saved JSONL bytes but is not CI attestation.
These records use synthetic test domains and addresses and do not include
production DNS traffic. Artifact retention is seven days.



## EDNS Client Subnet test configuration

The live DNS harness explicitly sends `dig +edns=0` for EDNS-enabled
queries, rather than depending on the installed `dig` version. The ECS
regression test also sends `+subnet=198.51.100.42/32` and verifies that
Technitium records the supplied client address.

For this **isolated Docker E2E environment only**, Technitium has
`enableEDnsClientSubnetSourceAddress=true` and trusts the Docker bridge
gateway range through `dnsReverseProxyNetworkACL=172.16.0.0/12`. Production
deployments must instead allow only the actual trusted proxy addresses;
untrusted clients must not be permitted to spoof the source address using ECS.

ECS and Extended DNS Errors (EDE) are different EDNS options. Enabling
ECS source-address handling does not make blocked-zone responses emit EDE 15.

## Scope

Coverage includes ZIP installation and isQueryLogger registration, benign authoritative responses, local-fixture blocked-domain NXDOMAIN, EDNS-disabled behavior, file/console/HTTP/Syslog delivery, HTTP batch limits, replay-enabled and replay-disabled HTTP 503, permanent HTTP 400, sink isolation, configuration reload and validation failure.

The blocked-zone tests exercise explicit domain blocking through Technitium's API, not automatic blocklist download or evidence of maliciousness. Manually blocked-zone replies in Technitium 15.6.2 produce authoritative NXDOMAIN without EDE 15; this fixture therefore does not validate EDE generation from downloaded block lists. EDE serialization remains covered by C# unit tests. Tests use a disposable DNS environment and do not contact any real malicious domains.

Missing EDE data on manually blocked-zone responses and no-EDNS queries is expected; the current plugin is an IDnsQueryLogger implementation rather than IDnsQueryLoggerEx. HTTP replay is opt-in and can produce duplicates after ambiguous acknowledgments. ClickHouse whole-batch replay is rejected by configuration. The E2E ClickHouse tests provision a real table, send live DNS queries through Technitium, verify stored typed fields and payload JSON, test disabled and invalid configurations, then remove the test table. Tests inject failures before collector commit and never assert exactly-once semantics.

Syslog TLS, local Unix-domain Syslog, source-public-feed compatibility, forced termination, ClickHouse network fault injection and saturation are not yet included in this initial regression gate.
