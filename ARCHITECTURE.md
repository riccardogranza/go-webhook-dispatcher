# System Architecture: High-Throughput Async Worker & Webhook Dispatcher

## 1. High-Level Topology

```text
                       +-----------------------------------------------------------------------------------+
                       |                        GO WEBHOOK DISPATCHER ENGINE                               |
                       |                                                                                   |
  HTTP Producers       |  +----------------------+     1. Non-blocking      +---------------------------+  |
+----------------+     |  |  Ingestion API       |        Slot Acquire      | Bounded Semaphore (slots) |  |
| POST /v1/events|---->|  |  - JSON Schema Check |------------------------->| Capacity = QueueCapacity  |  |
+----------------+     |  |  - 1 MiB Body Limit  |        If Full:          +---------------------------+  |
        |              |  +----------+-----------+        HTTP 429 +                      |                |
        | 429 Shed     |             |                    Retry-After                     | 3. Enqueue     |
        |< - - - - - - |             | 2. Durable Write                                   v                |
        |              |             v                                      +---------------------------+  |
        | 202 Accepted |  +----------------------+                          | Bounded Job Channel       |  |
        |<-------------|  | Group-Commit Writer  |                          | chan Event [QueueCapacity] |  |
                       |  | (Coalesces <=128 ops |                          +-------------+-------------+  |
                       |  |  per WAL transaction)|                                        |                |
                       |  +----------+-----------+                                        | 4. Consume     |
                       |             |                                                    v                |
                       |             v                                      +---------------------------+  |
                       |  +----------------------+                          | Bounded Worker Pool (N)   |  |
                       |  | SQLite 3 (Pure-Go)   |<-------------------------| - HMAC-SHA256 Signing     |  |
                       |  | - journal_mode = WAL |   5. Status Transitions  | - Idempotency Headers     |  |
                       |  | - synchronous=NORMAL |      (DELIVERED /        | - HTTP Keep-Alive Pool    |  |
                       |  | - Read Pool (N CPUs) |       RETRYING / DLQ)    +------+-------------+------+  |
                       |  +----------+-----------+                                 |             |         |
                       |             ^                                             |             |         |
                       |             | 6. Query & Replay                           | 2xx OK      | 5xx /   |
                       |  +----------+-----------+                                 |             | Timeout |
                       |  | DLQ & Telemetry API  |                                 |             v         |
                       |  | - GET /v1/dlq        |                                 |    +----------------+ |
                       |  | - POST /dlq/{id}/..  |                                 |    | Full-Jitter    | |
                       |  | - GET /metrics       |                                 |    | Exp. Backoff   | |
                       |  +----------------------+                                 |    +----------------+ |
                       +-----------------------------------------------------------|-----------------------+
                                                                                   |
                                                                                   v
                                                                     +---------------------------+
                                                                     | Target Subscriber Webhook |
                                                                     | Verify X-Signature-256    |
                                                                     +---------------------------+
```

---

## 2. Bounded Channel Backpressure vs. Unbounded Goroutines

A common anti-pattern in Go webhook services is spawning a new goroutine per incoming request (`go dispatchWebhook(evt)`). While trivial to implement, unbounded goroutine spawning breaks down catastrophically under downstream degradation:

1. **Memory Amplification & OOM Kills:** Although a fresh goroutine stack starts at `2 KiB`, each blocked webhook goroutine retains its HTTP request buffer, serialized JSON payload (`1–64 KiB`), TLS state, and `net/http` transport structures. If a downstream subscriber experiences a 60-second brownout at `5,000 req/s`, `300,000` concurrent goroutines accumulate in memory, exhausting resident memory (RSS) and triggering an OS OOM kill.
2. **File Descriptor & Ephemeral Port Exhaustion:** Unbounded concurrent outbound connections exhaust the host's ephemeral TCP port range (`net.ipv4.ip_local_port_range`) and file descriptor limits (`ulimit -n`), causing cascading `EADDRNOTAVAIL` and `EMFILE` failures across all tenants.
3. **Scheduler & GC Thrashing:** The Go runtime garbage collector must scan all active goroutine stacks during mark termination. Hundreds of thousands of parked goroutines inflate GC pause latencies (`STW`) and degrade CPU cache locality.

### How This Engine Enforces Strict Bounds

This engine decouples **ingestion admission** from **outbound execution** using two fixed-capacity concurrency primitives:

- **Admission Semaphore (`slots chan struct{}` of size `QueueCapacity`):** Before touching the storage layer, `POST /v1/events` executes a non-blocking `select` on `slots`. If the buffer is saturated, the handler immediately increments `shed_count` and returns `429 Too Many Requests` with a `Retry-After` header in `< 50 µs` without allocating database transactions or worker state.
- **Fixed Worker Pool (`WorkerCount` goroutines):** Exactly `N` long-lived worker goroutines drain `jobCh` over a shared, connection-pooled `*http.Transport`. When a worker dequeues an event from `jobCh`, it releases one token from `slots`, freeing capacity for new admissions.

---

## 3. SQLite WAL Tuning & Group-Commit Coalescing

Persisting every event to disk before returning `202 Accepted` guarantees durability across process restarts, but naive SQLite usage in Go suffers from severe write lock contention (`SQLITE_BUSY`) because SQLite permits only one active writer transaction at a time.

To achieve multi-thousand requests/second on local NVMe storage with zero CGO dependencies (`modernc.org/sqlite`), the storage layer (`internal/dispatcher/store.go`) implements three optimizations:

1. **Write-Ahead Logging (`PRAGMA journal_mode = WAL`):** Readers do not block writers, and writers append sequential frames to the `-wal` file rather than modifying B-tree pages in place.
2. **Split Read/Write Handle Topology:**
   - `writeDB`: Locked to a single connection (`SetMaxOpenConns(1)`) owned exclusively by the group-commit loop, eliminating lock contention at the SQLite driver layer.
   - `readDB`: Configured with `SetMaxOpenConns(runtime.NumCPU())` and `PRAGMA query_only = ON` so `GET /v1/dlq` and `GET /v1/events/{id}` queries execute concurrently without serializing behind writes.
3. **Adaptive Group-Commit Batching (`writeLoop`):**
   - When concurrency is `1`, the writer goroutine wakes immediately on the first operation in `writeCh`, sees no additional queued operations in its non-blocking drain loop, and commits the transaction with **zero artificial sleep latency**.
   - When concurrency is `100`, while transaction $T_k$ is committing to the WAL (~`0.3 ms`), dozens of subsequent HTTP handlers push their operations into `writeCh`. Upon completing $T_k$, the writer drains up to `maxBatchSize` (`128`) queued operations in a single `BEGIN ... COMMIT` transaction using pre-compiled `*sql.Stmt` handles, amortizing the cost of WAL frame syncs across the entire batch.

---

## 4. Failure Modes, Retries & Dead Letter Queue (DLQ)

Not all webhook delivery failures should be treated equally. Retrying a permanent client error wastes worker capacity, whereas dropping a transient network blip violates delivery guarantees.

| Failure Classification | Condition | Engine Action | Rationale |
| :--- | :--- | :--- | :--- |
| **Success** | `HTTP 200–299` | Transition to `DELIVERED`, record latency sample in ring buffer. | Subscriber acknowledged receipt. |
| **Transient Server / Network Fault** | `HTTP 500–599`, TCP reset, DNS failure, or timeout (`context.DeadlineExceeded`) | Transition to `RETRYING`, sleep using **Exponential Backoff with Full Jitter**, retry up to `MaxRetries`. | Prevents synchronized retry storms (thundering herd) against recovering subscribers. |
| **Permanent Client Error** | `HTTP 400–499` (e.g., `400`, `401`, `404`, `422`) | **Immediate routing to `DLQ`** (`PERMANENT_CLIENT_ERROR_4XX`) on attempt 1. | Payload or endpoint configuration is rejected by subscriber; retrying immediately will fail identically. |
| **Exhausted Retries** | `attempt > MaxRetries` on `5xx`/timeout | Transition to `DLQ` (`MAX_RETRIES_EXCEEDED`), preserve `last_status_code` & `last_error`. | Frees worker capacity while preserving the full event in SQLite for operator inspection and replay (`POST /v1/dlq/{id}/replay`). |

### Full-Jitter Backoff Formula

For retry attempt $k \in \{1, \dots, \text{MaxRetries}\}$, the sleep duration $T_k$ is sampled uniformly from:

$$T_{\text{ceil}} = \min\left(T_{\text{max}}, \; T_{\text{base}} \cdot 2^{k-1}\right), \quad T_k \sim \mathcal{U}\left(0.5 \cdot T_{\text{ceil}}, \; T_{\text{ceil}}\right)$$

---

## 5. Graceful Shutdown & Synchronization Invariants

When the process receives `SIGINT` or `SIGTERM`:
1. `httpServer.Shutdown(ctx)` stops accepting new TCP connections and waits for active HTTP handlers to finish.
2. `Engine.Shutdown(ctx)` acquires an exclusive lock on `enqueueMu` (`sync.RWMutex`), sets `closing = true`, and closes `jobCh`. Because `POST /v1/events` and `POST /v1/dlq/{id}/replay` hold `enqueueMu.RLock()` while checking `closing` and writing to `jobCh`, it is impossible to trigger a send-on-closed-channel panic under concurrent load.
3. All `N` workers drain remaining events in `jobCh`. If an event is sleeping in a retry timer when the shutdown deadline expires, `drainCtx` cancels the sleep and persists the event to the `DLQ` (`SHUTDOWN_DRAIN_EXCEEDED`) so zero events are lost.
4. `Store.Close()` closes `writeCh`, flushes the final batch transaction, executes `PRAGMA wal_checkpoint(TRUNCATE);`, and closes the SQLite file descriptors cleanly.
