# High-Throughput Async Worker & Webhook Dispatcher in Go

A low-latency, bounded-concurrency asynchronous event ingestion and webhook delivery engine written in Go. Built with standard library `net/http` and pure-Go SQLite (`modernc.org/sqlite`) in Write-Ahead Logging (WAL) mode with adaptive group-commit batching for zero-CGO durable persistence and an embedded Dead Letter Queue (DLQ).

---

## Core Engineering Highlights

- **Explicit Bounded Backpressure (`HTTP 429`):** Decouples HTTP ingestion from outbound delivery via a fixed-capacity admission semaphore and bounded job channel. When downstream subscribers degrade and saturate the queue, excess load is shed immediately in `< 50 µs` with `429 Too Many Requests` and a `Retry-After` header rather than spawning unbounded goroutines.
- **Zero-CGO SQLite WAL + Adaptive Group Commit:** Every accepted event is durably persisted to SQLite WAL before `202 Accepted` is returned. Concurrent writes are coalesced into batched WAL transactions (`<= 128` ops/tx), eliminating `SQLITE_BUSY` lock contention while sustaining multi-thousand req/s throughput.
- **Cryptographic Payload Signing & Idempotency:** Outbound requests are signed using `HMAC-SHA256` (`X-Signature-256: sha256=<hex>`) over `<unix_timestamp>.<raw_payload>` alongside `X-Idempotency-Key` and `X-Webhook-ID` headers.
- **Failure-Aware Retry Engine & Queryable DLQ:**
  - `5xx` & network timeouts trigger **Exponential Backoff with Full Jitter** up to `max_retries`.
  - `4xx` client errors bypass retries and route immediately to the **Dead Letter Queue (`DLQ`)**.
  - Exhausted retries persist diagnostic traces in the DLQ (`GET /v1/dlq`) with one-call atomic replay (`POST /v1/dlq/{id}/replay`).
- **Deterministic Graceful Shutdown:** Catches `SIGINT`/`SIGTERM`, stops accepting new connections, drains in-flight workers within a configurable deadline, checkpoints the SQLite WAL (`PRAGMA wal_checkpoint(TRUNCATE)`), and exits cleanly.

---

## Quickstart

### 1. Run Automated Race-Detector Tests

```bash
go test -race -v ./...
```

### 2. Start the Dispatcher Server

```bash
go run ./cmd/dispatcher \
  -addr=:8080 \
  -db=./dispatcher.db \
  -workers=64 \
  -queue-capacity=4096 \
  -max-retries=3
```

### 3. Run the Built-in Load & Resilience Benchmark (`cmd/loadgen`)

```bash
go run ./cmd/loadgen \
  -url=http://127.0.0.1:8080 \
  -requests=10000 \
  -concurrency=100 \
  -receiver-latency=1ms \
  -failure-rate=0.015
```

### 4. Container Build (`< 15 MB` Distroless Image)

```bash
docker build -t go-webhook-dispatcher:latest .
docker run --rm -p 8080:8080 go-webhook-dispatcher:latest
```

---

## HTTP API Specification

### `POST /v1/events` — Ingest & Enqueue Webhook Event

**Request:**
```bash
curl -i -X POST http://127.0.0.1:8080/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "invoice.settlement.completed",
    "target_url": "https://example.com/webhooks/receiver",
    "idempotency_key": "idem_2026_inv_9941",
    "max_retries": 3,
    "payload": {
      "invoice_id": "INV-2026-9941",
      "amount_cents": 245000,
      "currency": "EUR"
    }
  }'
```

**Responses:**
- `202 Accepted`: Event durably persisted to SQLite WAL and enqueued into bounded worker pool.
- `400 Bad Request`: Malformed JSON or failed schema validation (`target_url`, `event_type`, `payload`).
- `429 Too Many Requests`: Bounded queue is full; includes `Retry-After: 1` header.
- `503 Service Unavailable`: Server is draining in-flight jobs during graceful shutdown.

### `GET /v1/events/{id}` — Inspect Event Lifecycle State

Returns the current delivery status (`PENDING`, `RETRYING`, `DELIVERED`, or `DLQ`), attempt count, delivery latency (`ms`), and last error trace.

### `GET /v1/dlq` — Query Dead Letter Queue

```bash
curl -s "http://127.0.0.1:8080/v1/dlq?limit=20&offset=0" | jq .
```

### `POST /v1/dlq/{id}/replay` — Replay Failed Event from DLQ

Atomically resets a `DLQ` event back to `PENDING` (`attempts = 0`) and re-enqueues it into the worker pool.

```bash
curl -i -X POST http://127.0.0.1:8080/v1/dlq/evt_0192b4a0/replay
```

### `GET /metrics` — JSON & Prometheus Telemetry

- Default (`application/json`): Exposes `enqueued`, `delivered`, `retried`, `dlq_count`, `shed_count`, `p50_latency_ms`, `p95_latency_ms`, `p99_latency_ms`, and `peak_rss_mb`.
- Prometheus format (`GET /metrics?format=prometheus` or `Accept: text/plain`): Standard Prometheus exposition counters and summaries.

---

## Local Benchmark Results

Executed on an **Apple M3 (8 cores, 24.0 GB RAM, `darwin/arm64`, Go `1.27.1`)** against `modernc.org/sqlite` in WAL mode with adaptive group-commit batching (`10,000` events, `100` concurrent client connections, `1ms` simulated subscriber latency, `1.50%` injected fault rate combining transient `503`s and permanent `422`s):

| Metric | Measured Result | Notes |
| :--- | :--- | :--- |
| **Ingestion Throughput (`POST /v1/events`)** | **23,526.7 req/s** | `10,000` durable SQLite WAL writes in `425 ms` |
| **End-to-End Webhook Drain Throughput** | **16,311.1 webhooks/s** | Full pipeline drain in `613 ms` (`64` workers) |
| **Ingestion Latency (`p50` / `p95` / `p99`)** | **3.61 ms / 8.55 ms / 10.57 ms** | Includes JSON validation + SQLite WAL commit |
| **Outbound Delivery Latency (`p50` / `p95` / `p99`)** | **1.41 ms / 4.21 ms / 6.43 ms** | Includes HMAC-SHA256 signing + `1ms` target delay |
| **Resilience & DLQ Routing** | **9,970 Delivered / 119 Retried / 30 DLQ** | `100%` of transient `503`s recovered; `422`s routed to DLQ |
| **Cryptographic Signature Verification** | **10,119 / 10,119 (100.0%)** | `X-Signature-256` constant-time HMAC verification |
| **Dispatcher Peak RSS Memory** | **60.94 MB** (Heap Alloc: **10.74 MB**) | Bounded channel memory footprint |

### Raw `cmd/loadgen` Output

```text
================================================================================
  GO WEBHOOK DISPATCHER — HIGH-THROUGHPUT LOAD & RESILIENCE BENCHMARK
================================================================================
  Timestamp            : 2026-11-14T08:34:27Z
  CPU / SoC            : Apple M3 (8 logical cores)
  System Memory        : 24.0 GB RAM
  OS / Architecture    : darwin/arm64 (go1.27.1)
  Storage Engine       : SQLite 3 (Pure-Go modernc.org/sqlite, WAL + Group Commit)
--------------------------------------------------------------------------------
  WORKLOAD CONFIGURATION
--------------------------------------------------------------------------------
  Total Events Sent    : 10000
  Client Concurrency   : 100 concurrent connections
  Subscriber Latency   : 1ms simulated per webhook
  Fault Injection Rate : 1.50% (transient 503s + permanent 422s)
--------------------------------------------------------------------------------
  INGESTION PERFORMANCE (HTTP POST /v1/events -> SQLite WAL -> 202 Accepted)
--------------------------------------------------------------------------------
  Ingestion Duration   : 425ms
  Ingestion Throughput : 23526.7 req/s
  Events Accepted (202): 10000
  Events Shed (429)    : 0
  Transport Errors     : 0
  Ingestion p50 Latency: 3.61 ms
  Ingestion p95 Latency: 8.55 ms
  Ingestion p99 Latency: 10.57 ms
--------------------------------------------------------------------------------
  ASYNC WORKER DELIVERY, RETRIES & DEAD LETTER QUEUE
--------------------------------------------------------------------------------
  Total Drain Time     : 613ms
  End-to-End Throughput: 16311.1 webhooks/s
  Webhooks Delivered   : 9970
  Valid HMAC-SHA256    : 10119 / 10119 (100.0% verified)
  Backoff Retries (503): 119 (all recovered on retry)
  Routed to DLQ (422)  : 30
  Delivery p50 Latency : 1.41 ms
  Delivery p95 Latency : 4.21 ms
  Delivery p99 Latency : 6.43 ms
--------------------------------------------------------------------------------
  RESOURCE & MEMORY FOOTPRINT
--------------------------------------------------------------------------------
  Dispatcher Peak RSS  : 60.94 MB (Heap Alloc: 10.74 MB)
  Loadgen Peak RSS     : 30.11 MB
================================================================================
```

### Graceful Shutdown Trace (`SIGINT`)

```json
{"time":"2026-11-14T09:34:26.138789+01:00","level":"INFO","msg":"webhook dispatcher listening","addr":"127.0.0.1:18080","db_path":"./bench_temp.db","workers":64,"queue_capacity":12000,"max_retries":3}
{"time":"2026-11-14T09:34:27.391566+01:00","level":"INFO","msg":"received termination signal; initiating graceful shutdown","signal":"interrupt","shutdown_timeout":"15s"}
{"time":"2026-11-14T09:34:27.394253+01:00","level":"INFO","msg":"graceful shutdown complete","enqueued":10000,"delivered":9970,"retried":119,"dlq_count":30,"shed_count":0,"peak_rss_mb":61.047}
```

---

## Architecture Deep Dive

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the system topology diagram, bounded channel vs. unbounded goroutine analysis, SQLite WAL group-commit internals, and failure recovery state machine.
