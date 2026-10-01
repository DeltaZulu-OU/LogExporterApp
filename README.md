# Log Exporter for Technitium DNS Server

[![Manual Release](https://github.com/DeltaZulu-OU/LogExporterApp/actions/workflows/release.yml/badge.svg)](https://github.com/DeltaZulu-OU/LogExporterApp/actions/workflows/release.yml)
[![CodeQL](https://github.com/DeltaZulu-OU/LogExporterApp/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/DeltaZulu-OU/LogExporterApp/actions/workflows/github-code-scanning/codeql)
[![Dependabot Updates](https://github.com/DeltaZulu-OU/LogExporterApp/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/DeltaZulu-OU/LogExporterApp/actions/workflows/dependabot/dependabot-updates)

A plugin that exports DNS query logs from [Technitium DNS Server](https://github.com/TechnitiumSoftware/DnsServer) to external sinks such as standard output, files, HTTP endpoints, and Syslog servers.

It uses a bounded asynchronous pipeline: query logs are captured, optionally processed through enrichment stages, and then exported in batches to the configured sinks.

> NOTE: This app is not included in the main Technitium DNS Server repository as of [v15](https://github.com/TechnitiumSoftware/DnsServer/blob/master/CHANGELOG.md#version-150).

## Features

- Captures DNS queries and responses through the Technitium DNS Server `IDnsQueryLogger` interface.
- Uses bounded processing stages and an independent bounded ingress queue for each sink, preventing one slow sink from blocking the others or causing unbounded memory growth.
- Exports logs through pluggable sinks: console, file, HTTP POST, and Syslog.
- Supports optional pipeline processors before export:
  - domain normalization with Public Suffix List based metadata;
  - static tag injection for downstream routing or tenant identification.
- Includes client address, nameserver, query, answers, response metadata, RTT, and optional EDNS Extended DNS Error data.
- Supports newline-delimited JSON for HTTP export.
- Drops new entries when the pipeline is full and periodically logs the number of dropped records.
- Drains pending logs during shutdown.

## Configuration

> Note that the configuration differs from the `LogExporterApp` in the Technitium DNS Server App Store. Therefore, you cannot use the same configuration.
>
> The top-level `sinks` and `pipeline` objects are required. Individual sink and pipeline sections are optional: if a section is omitted, that feature is treated as disabled. If a section is present, `enabled` defaults to `true` unless explicitly set to `false`.

Provide JSON configuration similar to the following:

```json
{
  "sinks": {
    "maxQueueSize": 50000,
    "enableEdnsLogging": true,
    "console": {
      "enabled": true
    },
    "file": {
      "enabled": false,
      "path": "/var/log/technitium/dns.log"
    },
    "http": {
      "enabled": true,
      "endpoint": "https://collector.example.com/dns",
      "headers": {
        "Authorization": "Bearer token"
      }
    },
    "syslog": {
      "enabled": false,
      "address": "syslog.example.com",
      "port": 6514,
      "protocol": "TLS"
    }
  },
  "pipeline": {
    "normalize": {
      "enabled": true
    },
    "tagging": {
      "enabled": false,
      "tags": [
        "tenant:alpha"
      ]
    }
  }
}
```

### Sink options

* `maxQueueSize` sets the capacity, in log entries, of the bounded processing stages and of each sink's independent ingress queue. A sink worker may additionally hold one in-flight batch while exporting or retrying. When a queue is full, new entries for that queue are dropped instead of allowing memory usage to grow without limit.
* `enableEdnsLogging` controls whether EDNS Extended DNS Error data is included in exported logs.
* `console` writes logs to standard output, which is useful for containerized deployments and debugging.
* `file` writes logs to the configured local file path.
* `http` sends batches to the configured endpoint using HTTP POST as newline-delimited JSON (NDJSON). HTTP records include the responding server's hostname.
* `syslog` exports logs through Syslog. Supported protocols are `UDP`, `TCP`, `TLS`, and `LOCAL`.
  * For normal host installations, prefer `LOCAL` and let the operating system's Syslog daemon, such as rsyslog or an equivalent implementation, handle remote forwarding, buffering, retry, and transport policy. The app intentionally does not try to duplicate those responsibilities. `LOCAL` is available on supported Unix-like hosts and ignores `address`.
  * In containers, prefer `console` when your platform already collects standard output. A sidecar or the platform's logging infrastructure can then handle forwarding and delivery policy. If that is not available or not desired, configure `UDP`, `TCP`, or `TLS` to send directly to a remote Syslog target.
  * For remote Syslog, `address` accepts an IP address or an FQDN. Use an FQDN for `TLS`, because the server certificate is validated against it.
  * An FQDN is resolved once the DNS server starts answering queries, not while the app loads. If resolution is temporarily unavailable, the Syslog sink retries in the background with capped exponential backoff and jitter until resolution succeeds or the sink is disposed. With `UDP`, the successfully resolved address is kept for the lifetime of the sink. If the Syslog server's IP address changes, restart Technitium DNS Server or reload the Log Exporter configuration so the target is resolved again. `TCP` and `TLS` resolve the name again whenever they reconnect.
  * For `TCP` and `TLS`, an ambiguous write failure may cause the current record to be retried even if the remote peer already received it. Direct remote Syslog therefore favors avoiding loss over avoiding duplicates.

Individual sink sections may be omitted entirely. An omitted sink is disabled. When a sink section is present, its sink-specific required values are validated only if that sink is enabled. For example, a disabled or omitted HTTP sink does not require an `endpoint`.

At least one sink must be enabled for logs to be exported. If no sink is enabled, logging remains disabled.

A minimal console-only configuration is therefore valid:

```json
{
  "sinks": {
    "console": {
      "enabled": true
    }
  },
  "pipeline": {}
}
```

### Pipeline options

* `normalize` adds parsed domain metadata under `meta.domainInfo`.
* `tagging` adds the configured static tags under `meta.tags`.

Pipeline sections may also be omitted. An omitted processor is disabled.

The normalization stage uses the Public Suffix List to derive domain structure. PSL loading is best-effort: if the list cannot be obtained, logging continues and the normalization output is simply unavailable for affected entries.

## Sample `dig` result for `technitium.com`

```bash
dig '@127.0.0.1' technitium.com

; <<>> DiG 9.16.25 <<>> @127.0.0.1 technitium.com
; (1 server found)
;; global options: +cmd
;; Got answer:
;; ->>HEADER<<- opcode: QUERY, status: NOERROR, id: 62685
;; flags: qr rd ra; QUERY: 1, ANSWER: 1, AUTHORITY: 0, ADDITIONAL: 1

;; OPT PSEUDOSECTION:
; EDNS: version: 0, flags:; udp: 1232
;; QUESTION SECTION:
;technitium.com.                        IN      A

;; ANSWER SECTION:
technitium.com.         8218    IN      A       206.189.140.177

;; Query time: 24 msec
;; SERVER: 127.0.0.1#53(127.0.0.1)
;; WHEN: Mon Dec 08 22:50:37 FLE Standard Time 2025
;; MSG SIZE  rcvd: 59
```

## Sample log for `technitium.com`

Raw log:

```json
{"answers":[{"dnssecStatus":"Disabled","name":"technitium.com","recordClass":"IN","recordData":"206.189.140.177","recordTtl":8218,"recordType":"A"}],"clientIp":"127.0.0.1","edns":[],"nameServer":"127.0.0.1","protocol":"Udp","question":{"questionClass":"IN","questionName":"technitium.com","questionType":"A"},"responseCode":"NoError","responseType":"Cached","timestamp":"2025-12-08T20:50:37.321Z","meta":{"domainInfo":{"domain":"technitium","topLevelDomain":"com","registrableDomain":"technitium.com","fullyQualifiedDomainName":"technitium.com","topLevelDomainRule":{"name":"com","type":"Normal","labelCount":1,"division":"ICANN"}}}}
```

Formatted for easier review:

```json
{
  "answers": [
    {
      "dnssecStatus": "Disabled",
      "name": "technitium.com",
      "recordClass": "IN",
      "recordData": "206.189.140.177",
      "recordTtl": 8218,
      "recordType": "A"
    }
  ],
  "clientIp": "127.0.0.1",
  "edns": [],
  "nameServer": "127.0.0.1",
  "protocol": "Udp",
  "question": {
    "questionClass": "IN",
    "questionName": "technitium.com",
    "questionType": "A"
  },
  "responseCode": "NoError",
  "responseType": "Cached",
  "timestamp": "2025-12-08T20:50:37.321Z",
  "meta": {
    "domainInfo": {
      "domain": "technitium",
      "topLevelDomain": "com",
      "registrableDomain": "technitium.com",
      "fullyQualifiedDomainName": "technitium.com",
      "topLevelDomainRule": {
        "name": "com",
        "type": "Normal",
        "labelCount": 1,
        "division": "ICANN"
      }
    }
  }
}
```

## Operational notes

* The app uses bounded channels rather than unbounded queues. Each sink has its own ingress queue and worker, so a slow or unavailable sink does not block the other sinks. Under sustained overload, only the full queue drops new entries and reports the drop count periodically.
* The normalization cache uses Public Suffix List based parsing and is optimized for DNS-style query patterns where many domains may be seen only once.
* EDNS logging records Extended DNS Error data when enabled and parses malformed EDE payloads defensively so they do not break the logging pipeline.
* Static tags are intended for downstream processing, for example tenant labels, environment labels, or collector-side routing keys.
* On Linux host installations, prefer `LOCAL` Syslog and delegate remote forwarding and delivery policy to rsyslog or another local Syslog daemon. For containers, prefer `console` with the platform's logging stack or a sidecar when available; direct remote Syslog is an alternative when container logging infrastructure is not used.
* For UDP Syslog targets configured by FQDN, the resolved IP address is intentionally not refreshed periodically after the sink has started. Restart Technitium DNS Server or reload the Log Exporter configuration after the Syslog server's IP address changes.


### Reload accounting

Configuration reload validates the replacement configuration before stopping the active pipeline, then drains the old generation before rebuilding sinks and processors. During the brief cutover interval, query logs that arrive while no ingestion generation is published are intentionally ignored. Those entries are not included in the queue-drop counters. This is an accepted lifecycle trade-off: adding a dedicated reload-state accounting path would increase control-plane complexity for a short, bounded transition.
