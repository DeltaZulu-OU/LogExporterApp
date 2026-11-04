# Log Exporter for Technitium DNS Server

[![Manual Release](https://github.com/DeltaZulu-OU/LogExporterApp/actions/workflows/release.yml/badge.svg)](https://github.com/DeltaZulu-OU/LogExporterApp/actions/workflows/release.yml)
[![CodeQL](https://github.com/DeltaZulu-OU/LogExporterApp/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/DeltaZulu-OU/LogExporterApp/actions/workflows/github-code-scanning/codeql)
[![Dependabot Updates](https://github.com/DeltaZulu-OU/LogExporterApp/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/DeltaZulu-OU/LogExporterApp/actions/workflows/dependabot/dependabot-updates)

A plugin that exports DNS query logs from [Technitium DNS Server](https://github.com/TechnitiumSoftware/DnsServer) to external sinks such as standard output, files, HTTP endpoints, and Syslog servers.

It uses a bounded asynchronous pipeline: query logs are captured, optionally processed through enrichment stages, and then exported in batches to the configured sinks.

> NOTE: This app is not included in the main Technitium DNS Server repository as of [v15](https://github.com/TechnitiumSoftware/DnsServer/blob/master/CHANGELOG.md#version-150).

> Last tested with Technitium DNS Server 15.6.2.

## Features

- Captures DNS queries and responses through the Technitium DNS Server `IDnsQueryLogger` interface.
- Uses bounded processing stages and an independent bounded ingress queue for each sink, preventing one slow sink from blocking the others or causing unbounded memory growth.
- Exports logs through pluggable sinks: console, file, HTTP POST, Syslog, and ClickHouse.
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
      "allowBatchReplay": false,
      "batching": {
        "batchSize": 1000,
        "batchDelayMs": 25,
        "retryTimeLimitMs": 2000,
        "initialRetryDelayMs": 50,
        "maxRetryDelayMs": 500
      },
      "headers": {
        "Authorization": "Bearer token"
      }
    },
    "syslog": {
      "enabled": false,
      "address": "syslog.example.com",
      "port": 6514,
      "protocol": "TLS"
    },
    "clickhouse": {
      "enabled": false,
      "connectionString": "Host=clickhouse.example.com;Port=8443;Database=dns;Username=dns_ingest;Password=REPLACE_ME;Protocol=https",
      "database": "dns",
      "table": "events",
      "batching": {
        "batchSize": 1000,
        "batchDelayMs": 1000
      }
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

The HTTP example explicitly disables whole-batch replay, matching the default.
The optional `batching` object is configured independently on each enabled
sink; if it is omitted, the defaults shown above apply. `batchSize` must be
positive, `batchDelayMs` must be positive, `retryTimeLimitMs` may be zero,
and `maxRetryDelayMs` must not be less than `initialRetryDelayMs`.

For receivers that implement idempotent ingestion or tolerate duplicate events,
opt into HTTP whole-batch replay by changing only the HTTP sink setting:

```json
{
  "sinks": {
    "http": {
      "enabled": true,
      "endpoint": "https://collector.example.com/dns",
      "allowBatchReplay": true
    }
  },
  "pipeline": {}
}
```

### Sink options

* `maxQueueSize` sets the capacity, in log entries, of the bounded processing stages and of each sink's independent ingress queue. A sink worker may additionally hold one in-flight batch while exporting or retrying. When a queue is full, new entries for that queue are dropped instead of allowing memory usage to grow without limit.
* `enableEdnsLogging` controls whether EDNS Extended DNS Error data is included in exported logs.
* `console` writes logs to standard output, which is useful for containerized deployments and debugging.
* `file` writes logs to the configured local file path.
* `http` sends batches to the configured endpoint using HTTP POST as newline-delimited JSON (NDJSON). HTTP records include the responding server's hostname.
* `clickhouse` exports DNS events through the official [Serilog.Sinks.ClickHouse](https://github.com/ClickHouse/Serilog.Sinks.ClickHouse) adapter and ClickHouse driver. It requires an existing, externally managed table; it does not create or migrate the schema. The table must provide Serilog's `timestamp`, `level`, `message`, `message_template`, `exception` and `properties` columns, plus `client_ip`, `q_name`, `q_type`, `response_code`, `response_type` and `payload_json`. The last column preserves the complete DNS log as JSON text. Set `connectionString`, `database` and `table` as shown above; `Protocol=https` uses HTTPS, and credentials in the connection string must be protected. Use a dedicated account with insert and table-inspection permissions. The integration is tested against ClickHouse 25.8; older installations may require enabling support for the native `JSON` type. ClickHouse whole-batch replay is not supported: `allowBatchReplay: true` is rejected, and an in-flight insert cannot reliably be cancelled through the upstream Serilog batch API.
* `syslog` exports logs through Syslog. Supported protocols are `UDP`, `TCP`, `TLS`, and `LOCAL`.
  * For normal host installations, prefer `LOCAL` and let the operating system's Syslog daemon, such as rsyslog or an equivalent implementation, handle remote forwarding, buffering, retry, and transport policy. The app intentionally does not try to duplicate those responsibilities. `LOCAL` is available on supported Unix-like hosts and ignores `address`.
  * In containers, prefer `console` when your platform already collects standard output. A sidecar or the platform's logging infrastructure can then handle forwarding and delivery policy. If that is not available or not desired, configure `UDP`, `TCP`, or `TLS` to send directly to a remote Syslog target.
  * For remote Syslog, `address` accepts an IP address or an FQDN. Use an FQDN for `TLS`, because the server certificate is validated against it.
  * An FQDN is resolved once the DNS server starts answering queries, not while the app loads. If resolution is temporarily unavailable, the Syslog sink retries in the background with capped exponential backoff and jitter until resolution succeeds or the sink is disposed. With `UDP`, the successfully resolved address is kept for the lifetime of the sink. If the Syslog server's IP address changes, restart Technitium DNS Server or reload the Log Exporter configuration so the target is resolved again. `TCP` and `TLS` resolve the name again whenever they reconnect.
  * For `TCP` and `TLS`, an ambiguous write failure may cause the current record to be retried even if the remote peer already received it. Direct remote Syslog therefore favors avoiding loss over avoiding duplicates.

Each sink uses an independent bounded in-memory queue and configurable `batching` settings. The retry budget applies only where whole-batch replay is explicitly enabled; it is not a universal export timeout. File, console and Syslog sinks do not replay whole batches through the dispatcher, as an interrupted operation may already have delivered some records. HTTP replay is opt-in and may duplicate records when an acknowledgment is lost; HTTP 408/429, HTTP 5xx and connection failures are eligible for retry, but HTTP 400 is not. The dispatcher maintains in-memory delivery counters through `SinkDispatcher.GetCounters(Type)`, not a durable audit log or metrics endpoint. Delivery remains best-effort: queue overflow, crashes, failed destinations and forced shutdown may cause losses; exactly-once and guaranteed at-least-once delivery are not provided.

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


After update. Dig output:
```bash
dig '@127.0.0.1' -p 535 adsflame.com

; <<>> DiG 9.16.25 <<>> @127.0.0.1 -p 535 adsflame.com
; (1 server found)
;; global options: +cmd
;; Got answer:
;; ->>HEADER<<- opcode: QUERY, status: NXDOMAIN, id: 6128
;; flags: qr rd; QUERY: 1, ANSWER: 0, AUTHORITY: 1, ADDITIONAL: 1
;; WARNING: recursion requested but not available

;; OPT PSEUDOSECTION:
; EDNS: version: 0, flags:; udp: 1232
; EDE: 15 (Blocked): (source=block-list-zone; blockListUrl=https://raw.githubusercontent.com/StevenBlack/hosts/master/hosts; domain=adsflame.com)
;; QUESTION SECTION:
;adsflame.com.                  IN      A

;; AUTHORITY SECTION:
com.                    30      IN      SOA     lap29. hostadmin.lap29. 1 14400 3600 604800 30

;; Query time: 0 msec
;; SERVER: 127.0.0.1#535(127.0.0.1)
;; WHEN: Sun Nov 01 21:50:50 FLE Standard Time 2026
;; MSG SIZE  rcvd: 220
```

## Sample log

Syslog:
```
<182>1 2026-11-01T19:50:50.742761+00:00 LAP29 TechnitiumDNSServer 10800 - [meta timestamp="2026-11-01T19:50:50.742Z" clientIp="127.0.0.1" protocol="Udp" responseType="Blocked" rCode="NxDomain" qName="adsflame.com" qType="A" qClass="IN" questionsSummary="QNAME: adsflame.com, QTYPE: A, QCLASS: IN" answersSummary="" ednsErrType_0="Blocked" ednsMessage_0="source=block-list-zone; blockListUrl=https://raw.githubusercontent.com/StevenBlack/hosts/master/hosts; domain=adsflame.com" domainName="adsflame" domainTld="com" domainRegistrable="adsflame.com" domainFqdn="adsflame.com" domainRuleName="com" domainRuleType="Normal" domainRuleLabels="1" domainRuleDivision="ICANN" tag_0="tenant:alpha"] "QNAME: adsflame.com, QTYPE: A, QCLASS: IN"; RCODE: "NxDomain"; ANSWER: ""
```

JSON:
```
{"answers":[],"clientIp":"127.0.0.1","edns":[{"errType":"Blocked","message":"source=block-list-zone; blockListUrl=https://raw.githubusercontent.com/StevenBlack/hosts/master/hosts; domain=adsflame.com"}],"nameServer":"127.0.0.1","protocol":"Udp","question":{"questionClass":"IN","questionName":"adsflame.com","questionType":"A"},"responseCode":"NxDomain","responseType":"Blocked","timestamp":"2026-11-01T19:50:50.742Z","meta":{"domainInfo":{"domain":"adsflame","topLevelDomain":"com","registrableDomain":"adsflame.com","fullyQualifiedDomainName":"adsflame.com","topLevelDomainRule":{"name":"com","type":"Normal","labelCount":1,"division":"ICANN"}},"tags":["tenant:alpha"]}}
```

Formatted for easier review:

```json
{
    "answers": [],
    "clientIp": "127.0.0.1",
    "edns": [
        {
            "errType": "Blocked",
            "message": "source=block-list-zone; blockListUrl=https://raw.githubusercontent.com/StevenBlack/hosts/master/hosts; domain=adsflame.com"
        }
    ],
    "nameServer": "127.0.0.1",
    "protocol": "Udp",
    "question": {
        "questionClass": "IN",
        "questionName": "adsflame.com",
        "questionType": "A"
    },
    "responseCode": "NxDomain",
    "responseType": "Blocked",
    "timestamp": "2026-11-01T19:50:50.742Z",
    "meta": {
        "domainInfo": {
            "domain": "adsflame",
            "topLevelDomain": "com",
            "registrableDomain": "adsflame.com",
            "fullyQualifiedDomainName": "adsflame.com",
            "topLevelDomainRule": {
                "name": "com",
                "type": "Normal",
                "labelCount": 1,
                "division": "ICANN"
            }
        },
        "tags": [
            "tenant:alpha"
        ]
    }
}
```

## Operational notes

* The app uses bounded channels rather than unbounded queues. Each sink has its own ingress queue and worker, so a slow or unavailable sink does not block the other sinks. Under sustained overload, only the full queue drops new entries and reports the drop count periodically.
* The normalization cache uses Public Suffix List based parsing and is optimized for DNS-style query patterns where many domains may be seen only once.
* EDNS logging records Extended DNS Error (EDE) information when enabled. The exporter reads the information code and optional extra text directly from TechnitiumLibrary's typed EDE representation, preserving extra text without modification.
* Static tags are intended for downstream processing, for example tenant labels, environment labels, or collector-side routing keys.
* On Linux host installations, prefer `LOCAL` Syslog and delegate remote forwarding and delivery policy to rsyslog or another local Syslog daemon. For containers, prefer `console` with the platform's logging stack or a sidecar when available; direct remote Syslog is an alternative when container logging infrastructure is not used.
* For UDP Syslog targets configured by FQDN, the resolved IP address is intentionally not refreshed periodically after the sink has started. Restart Technitium DNS Server or reload the Log Exporter configuration after the Syslog server's IP address changes.


### Reload accounting

Configuration reload validates the replacement configuration before stopping the active pipeline, then drains the old generation before rebuilding sinks and processors. During the brief cutover interval, query logs that arrive while no ingestion generation is published are intentionally ignored. Those entries are not included in the queue-drop counters. This is an accepted lifecycle trade-off: adding a dedicated reload-state accounting path would increase control-plane complexity for a short, bounded transition.
