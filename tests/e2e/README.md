# Live Technitium DNS end-to-end tests

The E2E suite installs the actual release ZIP in a disposable Technitium DNS Server 15.6.2, sends dig queries over UDP and TCP, and validates HTTP NDJSON, file NDJSON, console NDJSON, and RFC 5424 UDP/TCP Syslog. A local blocklist fixture avoids test failures caused by third-party feeds. It includes synthetic domains and two pinned examples observed in StevenBlack/hosts (adsflame.com, aircovid19virus.com, checked 2026-11-04); list membership is not independent evidence of maliciousness.

## CI and local reproduction

The GitHub Actions workflow is triggered exclusively by pushes to master and manual workflow dispatch. There are no PR or scheduled triggers. It builds the .NET 10 plugin against the pinned git submodules and uses the existing New-ReleasePackage.ps1 script before starting Docker Compose.

To reproduce, install Docker Compose, .NET 10, PowerShell, Python 3.12, dig, pytest and requests. Build TechnitiumLibrary, build src/LogExporterApp.csproj in Release, then package with .github/scripts/New-ReleasePackage.ps1. Set E2E_PACKAGE to the relative ZIP path and E2E_ADMIN_PASSWORD to a random value. Run docker compose -f tests/e2e/compose.yml up -d --wait; pytest -q tests/e2e/test_e2e.py; then docker compose -f tests/e2e/compose.yml down -v.

JUnit XML, sanitized collector state, container diagnostics, and the file sink output are captured in the e2e-evidence artifact.

## Scope

Coverage includes ZIP installation and isQueryLogger registration, benign authoritative responses, synthetic blocked-domain NXDOMAIN with EDE 15, EDNS-disabled behavior, file/console/HTTP/Syslog delivery, HTTP batch limits, replay-enabled and replay-disabled HTTP 503, permanent HTTP 400, sink isolation, configuration reload and validation failure.

The blocked list tests demonstrate list-based blocking, not the maliciousness of any domain. Tests use a disposable DNS environment and do not contact any real malicious domains.

Missing EDE data with a no-EDNS query is expected; the current plugin is an IDnsQueryLogger implementation rather than IDnsQueryLoggerEx. HTTP replay is opt-in and can produce duplicates after ambiguous acknowledgments. Tests inject failures before collector commit and never assert exactly-once semantics.

Syslog TLS, local Unix-domain Syslog, source-public-feed compatibility, forced termination and saturation are not yet included in this initial regression gate.
