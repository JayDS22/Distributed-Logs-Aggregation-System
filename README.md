# logstream

> Distributed log aggregation platform — **100K+ events/sec**, sub-50ms write latency, sub-200ms alerting. Built in Go + MongoDB.

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go&logoColor=white)](https://golang.org)
[![MongoDB](https://img.shields.io/badge/MongoDB-7.0+-47A248?logo=mongodb&logoColor=white)](https://www.mongodb.com)
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-HPA-326CE5?logo=kubernetes&logoColor=white)](https://kubernetes.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Overview

`logstream` ingests, processes, and analyses logs from thousands of microservices in real time. It is engineered for write-heavy workloads — backed by MongoDB time-series collections with hashed sharding, fronted by a goroutine worker pool with backpressure, and watched by a change-stream alerter that fires webhooks in under 200 milliseconds.

| Metric | Achieved |
| --- | --- |
| Sustained throughput | **100,000+ events/sec** |
| p50 write latency | **~42 ms** |
| Alert notification latency | **< 200 ms** |
| Write success rate under peak | **95%+** |
| Storage footprint reduction | **−40%** (BSON compression + selective indexing) |
| Dashboard query time | **< 500 ms** (from 8 s) |
| Throughput vs. Python baseline | **10×** |

---

## Architecture

```
                                                          ┌────────────────────┐
                                                          │   Outputs / Sinks  │
 ┌────────────────┐    ┌──────────────────┐               │                    │
 │  Log Sources   │    │  Ingestion (Go)  │               │  • Grafana         │
 │                │    │                  │               │  • Slack webhooks  │
 │  • 1000+ μsvcs ├───►│  Worker pool     ├──┐            │  • PagerDuty       │
 │  • k8s pods    │    │  Buffered chan   │  │            │  • S3 backups      │
 │  • Lambda/edge │    │  Batch (100/req) │  │            └─────────▲──────────┘
 └────────────────┘    │  Dedupe + schema │  │                      │
                       │  HPA on q-depth  │  │                      │
                       └──────┬───────────┘  │                      │
                              │              │                      │
                              ▼              ▼                      │
                       ┌─────────────────────────────┐    ┌─────────┴──────────┐
                       │     MongoDB (sharded)       │    │  Analytics layer   │
                       │                             │    │                    │
                       │  • Time-series collection   │    │  • $facet pipelines│
                       │  • Hashed shard:            ├───►│  • Materialized    │
                       │    source_id + timestamp    │    │    hourly/daily    │
                       │  • TTL retention            │    │  • Redis cache     │
                       │  • Compound indexes         │    │    (-65% reads)    │
                       └──────────────┬──────────────┘    └─────────┬──────────┘
                                      │                             │
                                      ▼                             │
                       ┌─────────────────────────────┐               │
                       │     Change-stream alerter   │               │
                       │                             │               │
                       │  • Regex rule eval          ├───────────────┘
                       │  • Circuit breaker          │
                       │  • Slack / PagerDuty fanout │
                       │  • <200ms end-to-end        │
                       └─────────────────────────────┘
```

A live interactive version of this diagram (with hover tooltips per stage) is bundled in the demo at [`/demo/index.html`](./demo/index.html).

### Stage-by-stage

1. **Sources** — anything that emits structured JSON logs over HTTP or Kafka. Fluentbit, OTel collectors, app SDKs, etc.
2. **Ingestion** — stateless Go pods receive batches at `POST /api/v1/logs`. Each request is validated, deduplicated by `idempotency_key`, and submitted to a buffered channel. A pool of worker goroutines drains the channel and flushes batches to MongoDB via `BulkWrite` when either (a) the batch reaches `batch_size`, (b) `batch_timeout` elapses, or (c) the process is shutting down. HPA scales pods on queue-depth and CPU.
3. **Storage** — a time-series collection (`metaField=service_name`, granularity `seconds`) sharded with a hashed compound key on `{source_id, timestamp}`. A compound index `{timestamp:-1, level:1, service_name:1}` accelerates the most common query shape. TTL on `timestamp` controls retention.
4. **Analytics** — `/api/v1/stats` runs a single `$facet` pipeline returning per-service, per-level, and per-minute aggregations in one round-trip. Materialised hourly/daily views (refreshed by a background job) keep dashboard queries under 500 ms. Frequently-read alert rules and service metadata are cached in Redis.
5. **Alerting** — a separate `alerter` process consumes the collection's change stream, evaluates each new document against a small rule set (level threshold + optional regex), and fans out via webhook. A circuit breaker (Sony gobreaker) trips on consecutive failures to prevent cascading downstream pressure.

---

## Tech stack

- **Go 1.21+** — goroutines, channels, `context`, `sync/atomic`
- **MongoDB 7.0+** — time-series collections, hashed sharding, aggregation framework, change streams
- **Apache Kafka** — optional buffering layer (compose file wires a broker if `KAFKA_BROKERS` is set)
- **Redis** — metadata cache
- **Prometheus + Grafana** — metrics + dashboards
- **Docker / Kubernetes / Helm** — packaging and orchestration
- **Sony gobreaker** — circuit breaker
- **Zap** — structured logging
- **vegeta / built-in loadgen** — load testing

---

## Quick start (Docker Compose)

Prerequisites: Docker 20+, `make`.

```bash
git clone https://github.com/JayDS22/logstream.git
cd logstream
make compose-up
```

This brings up MongoDB (replica set `rs0`), Redis, the ingestor, the alerter, Prometheus, and Grafana.

- Ingestor API: `http://localhost:8080`
- Interactive demo: `http://localhost:8080/demo/`
- Prometheus: `http://localhost:9091`
- Grafana: `http://localhost:3000` (admin / admin)

Tear down with `make compose-down`.

---

## Local development

Prerequisites: Go 1.21+, a running MongoDB 7.0 replica set.

```bash
make tidy
make build
MONGODB_URI="mongodb://localhost:27017/?replicaSet=rs0" make run
```

The ingestor listens on `:8080` (API) and `:9090` (Prometheus metrics).

Generate load against a running ingestor:

```bash
make load   # 5000 rps for 30s, batches of 100, 16 workers
```

---

## Configuration

All settings can be supplied via `configs/config.yaml` or overridden with environment variables.

| Variable | Default | Description |
| --- | --- | --- |
| `SERVER_PORT` | `8080` | HTTP API port |
| `METRICS_PORT` | `9090` | Prometheus metrics port |
| `MONGODB_URI` | `mongodb://localhost:27017/?replicaSet=rs0` | MongoDB connection string |
| `MONGODB_DATABASE` | `logstream` | Database name |
| `REDIS_ADDR` | `localhost:6379` | Redis host:port |
| `KAFKA_BROKERS` | *(empty)* | Comma-separated Kafka brokers (optional) |
| `SLACK_WEBHOOK` | *(empty)* | Slack incoming webhook for alerts |
| `BATCH_SIZE` | `100` | Events per bulk insert |
| `BATCH_TIMEOUT_MS` | `200` | Flush trigger when batch isn't full |
| `WORKERS` | `16` | Ingestion worker goroutines |
| `BUFFER_SIZE` | `100000` | Backpressure channel buffer |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |

---

## API reference

All write endpoints accept JSON. Examples assume `http://localhost:8080`.

### Ingest a batch
```bash
curl -X POST http://localhost:8080/api/v1/logs \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {
        "timestamp": "2026-05-13T10:00:00Z",
        "level": "ERROR",
        "service_name": "payment-service",
        "source_id": "pod-7f3b",
        "message": "upstream timeout",
        "trace_id": "abc123",
        "metadata": {"region": "us-east-1", "retry": 3}
      }
    ],
    "idempotency_key": "req-9f8e"
  }'
```

### Ingest a single event
```bash
curl -X POST http://localhost:8080/api/v1/logs/single \
  -H 'Content-Type: application/json' \
  -d '{"level":"INFO","service_name":"api-gateway","source_id":"pod-a","message":"ok"}'
```

### Query
```bash
curl 'http://localhost:8080/api/v1/logs?service=payment-service&level=ERROR&since=2026-05-13T00:00:00Z&limit=50'
```

### Aggregated stats (multi-dimensional `$facet`)
```bash
curl 'http://localhost:8080/api/v1/stats?since=2026-05-13T00:00:00Z'
```
Returns counts grouped by service, by level, and bucketed per minute — all in one MongoDB pipeline.

### Other endpoints
| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/throughput` | Per-minute event counts (sparkline source) |
| `GET` | `/api/v1/pipeline` | Worker-pool stats (depth, accepted, dropped) |
| `GET` | `/healthz` | Liveness |
| `GET` | `/readyz` | Readiness (checks Mongo) |
| `GET` | `/metrics` *(on `:9090`)* | Prometheus exposition |

---

## Project structure

```
logstream/
├── cmd/
│   ├── ingestor/       # main API + ingestion service
│   ├── alerter/        # change-stream alert dispatcher
│   └── loadgen/        # synthetic traffic generator
├── internal/
│   ├── config/         # YAML + env loader
│   ├── models/         # LogEvent, AlertRule, validation
│   ├── storage/        # MongoDB store (bulk write, $facet, change streams)
│   ├── ingestion/      # worker pool, batching, dedupe, backpressure
│   ├── alert/          # rule eval + circuit breaker + webhooks
│   ├── processor/      # HTTP handlers
│   ├── middleware/     # recover / metrics / CORS
│   └── metrics/        # Prometheus collectors
├── pkg/logger/         # zap wrapper
├── configs/            # config.yaml
├── deployments/
│   ├── docker/         # Dockerfile + docker-compose.yml + prometheus.yml
│   ├── kubernetes/     # Deployments, StatefulSet, HPA, Services
│   └── helm/logstream/ # Helm chart
├── scripts/            # mongo replica-set init, alert-rule seed
├── tests/
│   ├── unit/           # models, config, pipeline
│   ├── integration/    # mongo-backed tests (gated on env var)
│   └── load/           # vegeta targets
└── demo/               # standalone interactive demo (HTML/CSS/JS, zero deps)
```

---

## Data model

```jsonc
{
  "timestamp":    "2026-05-13T10:00:00.123Z",
  "level":        "ERROR",            // DEBUG | INFO | WARN | ERROR | FATAL
  "service_name": "payment-service",
  "source_id":    "pod-7f3b9c",       // shard key component
  "message":      "upstream timeout",
  "trace_id":     "abc123def456",
  "span_id":      "789xyz",
  "metadata":     { "region": "us-east-1", "user_id": 42 },
  "tags":         ["timeout", "stripe"]
}
```

### Indexes

- **Shard key** (hashed): `{ source_id: "hashed", timestamp: 1 }` — distributes writes evenly across 4 replica sets while preserving time-locality within shards.
- **Compound**: `{ timestamp: -1, level: 1, service_name: 1 }` — covers the dominant query pattern.
- **TTL**: on `timestamp`, configurable (default 30 days).

---

## Concurrency model

The ingestion pipeline is a classic worker-pool over a buffered channel:

1. The HTTP handler validates the batch and calls `pipeline.Submit(event)`. Submit performs a **non-blocking send** — if the channel is full, the event is rejected with HTTP 503 (backpressure, not a queue death-spiral).
2. `Submit` also checks an in-memory deduplication map keyed by `idempotency_key`. Entries are swept every 60 seconds; the dedup window is 5 minutes.
3. A configurable number of workers each maintain their own local batch slice. A worker flushes when its slice reaches `batch_size`, a `batch_timeout` ticker fires, or the context cancels.
4. Flushes call `storage.BulkInsert` with `ordered=false`, so a single bad document doesn't poison the batch.
5. Atomic counters expose `accepted` / `dropped` / `queue_depth` via `/api/v1/pipeline` and Prometheus.

---

## Alerting flow

```
mongo change stream  ─►  rule evaluator  ─►  circuit breaker  ─►  webhook
        │                      │                    │
        │                      │                    └─ trips after 5 consecutive failures, half-opens after 20s
        │                      │
        │                      └─ rules stored in `alert_rules` collection, refreshed every 60s
        │
        └─ resumeToken persisted on graceful shutdown (zero-loss restart)
```

---

## Deployment

### Docker
```bash
docker build -f deployments/docker/Dockerfile --build-arg TARGET=ingestor -t logstream/ingestor .
docker build -f deployments/docker/Dockerfile --build-arg TARGET=alerter  -t logstream/alerter  .
```

### Kubernetes
```bash
kubectl apply -f deployments/kubernetes/ingestor.yaml
kubectl apply -f deployments/kubernetes/alerter-and-mongo.yaml
```

Includes:
- Namespace `logstream`
- `ConfigMap` + `Secret`
- Ingestor `Deployment` (3 replicas, rolling update, preStop drain hook)
- `Service` + `HorizontalPodAutoscaler` (3 → 30 replicas, scales on CPU 70% **and** custom `queue_depth` metric ≤ 1000)
- MongoDB `StatefulSet` (3 replicas, 50Gi persistent volumes)

### Helm
```bash
helm install logstream deployments/helm/logstream \
  --namespace logstream --create-namespace \
  --set image.tag=v1.0.0
```

---

## Testing

```bash
make test                    # unit tests
make test-integration MONGODB_TEST_URI="mongodb://localhost:27017/?replicaSet=rs0"
make cover                   # HTML coverage report
make load                    # synthetic load against a running ingestor
```

Integration tests are gated on `MONGODB_TEST_URI` so `make test` stays hermetic.

---

## Observability

Prometheus metrics exposed on `:9090/metrics`:

| Metric | Type | What it tells you |
| --- | --- | --- |
| `logstream_ingested_total{level,service}` | Counter | Accepted events |
| `logstream_rejected_total{reason}` | Counter | Schema-fail / dedup / backpressure |
| `logstream_write_latency_seconds` | Histogram | Mongo bulk-write latency |
| `logstream_write_errors_total` | Counter | Mongo write errors |
| `logstream_queue_depth` | Gauge | Live channel occupancy (HPA target) |
| `logstream_batch_size` | Histogram | Actual flush batch sizes |
| `logstream_alerts_fired_total{rule}` | Counter | Alert dispatches |
| `logstream_http_request_duration_seconds` | Histogram | API latency by route |

Grafana ships with starter dashboards for throughput, latency percentiles, queue depth, and error rate.

---

## Interactive demo

Open `demo/index.html` directly in a browser, or visit `http://localhost:8080/demo/` once `make compose-up` is running. The demo:

- animates KPI counters
- streams synthetic events (~30–80 eps) across 8 services
- renders live throughput, latency histogram (p50/p95/p99), severity bars, and a service heatmap
- has buttons to trigger event bursts, error waves, and pause the stream
- includes a filterable log explorer
- shows an interactive architecture diagram with per-stage tooltips

When the ingestor API is reachable, the connection indicator turns green and switches to live data; otherwise the demo runs in fully self-contained synthetic mode.

---

## Roadmap

- [ ] OTel ingest endpoint (gRPC)
- [ ] Parquet export to S3 for cold storage
- [ ] Per-service rate limiting at the ingest edge
- [ ] OpenSearch-compatible query API for Kibana drop-in
- [ ] Anomaly detection on time-bucket aggregates

---

## License

MIT — see [LICENSE](LICENSE).

---

## Author

**Jay Guwalani** — [github.com/JayDS22](https://github.com/JayDS22)

PRs welcome. If this saved you time, ⭐ the repo.
