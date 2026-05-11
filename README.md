# Log Exporter for Technitium DNS Server

A plugin that exports DNS query logs from [Technitium DNS Server](https://github.com/TechnitiumSoftware/DnsServer) to external sinks such as standard output, files, HTTP endpoints, and Syslog servers.

It uses a bounded asynchronous pipeline: query logs are captured, optionally processed through enrichment stages, and then exported in batches to the configured sinks.

> NOTE: This app is not included in the main Technitium DNS Server repository as of [v15](https://github.com/TechnitiumSoftware/DnsServer/blob/master/CHANGELOG.md#version-150).

## Features

- Captures DNS queries and responses through the Technitium DNS Server `IDnsQueryLogger` interface.
- Uses a two-stage bounded pipeline for processing and export, preventing unbounded memory growth under load.
- Exports logs through pluggable sinks: console, file, HTTP POST, and Syslog.
- Supports optional pipeline processors before export:
  - domain normalization with Public Suffix List based metadata;
  - static tag injection for downstream routing or tenant identification.
- Includes client address, nameserver, query, answers, response metadata, RTT, and optional EDNS Extended DNS Error data.
- Supports newline-delimited JSON for HTTP export.
- Drops new entries when the pipeline is full and periodically logs the number of dropped records.
- Drains pending logs during shutdown.

## Configuration

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
      },
      "ndjson": true
    },
    "syslog": {
      "enabled": false,
      "address": "10.0.0.5",
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

* `maxQueueSize` sets the capacity of each bounded pipeline stage. When a stage is full, new entries are dropped instead of allowing memory usage to grow without limit.
* `enableEdnsLogging` controls whether EDNS Extended DNS Error data is included in exported logs.
* `console` writes logs to standard output, which is useful for containerized deployments and debugging.
* `file` writes logs to the configured local file path.
* `http` sends logs to the configured endpoint using HTTP POST. When `ndjson` is `true`, batches are sent as newline-delimited JSON.
* `syslog` exports logs to a Syslog server. Supported protocols are `UDP`, `TCP`, `TLS`, and `LOCAL`.

At least one sink must be enabled. If no sink is configured, logging remains disabled.

### Pipeline options

* `normalize` adds parsed domain metadata under `meta.domainInfo`.
* `tagging` adds the configured static tags under `meta.tags`.

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

* The app uses bounded channels rather than an unbounded queue. Under sustained overload, it drops new entries and reports the drop count periodically instead of consuming memory indefinitely.
* The normalization cache uses Public Suffix List based parsing and is optimized for DNS-style query patterns where many domains may be seen only once.
* EDNS logging records Extended DNS Error data when enabled and parses malformed EDE payloads defensively so they do not break the logging pipeline.
* Static tags are intended for downstream processing, for example tenant labels, environment labels, or collector-side routing keys.
