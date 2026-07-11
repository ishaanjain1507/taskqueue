# Implementation Deep Dive

This document explains how RateSentry Core actually works, subsystem by subsystem, including the reasoning behind each design decision and the specific problems each one solves. It's written to be defensible in a technical interview — every claim here should be something you can explain unprompted, not just recite.

---

## 1. Core Architecture

The system has four layers, each with a single responsibility:

1. **API layer** (`internal/api`) — accepts HTTP requests, validates input, writes initial job state, enqueues work, returns immediately
2. **Message broker** (`internal/queue`) — Redis-backed queue that decouples job submission from job execution
3. **Worker pool** (`internal/worker`) — a dynamically resizable set of goroutines that pull jobs and process them concurrently
4. **Durable store** (`internal/db`) — PostgreSQL, the permanent system of record for every job's full lifecycle

These layers communicate only through two Go interfaces defined in `internal/models/interfaces.go`:

```go
type Store interface {
    UpsertJob(job *Job) error
    GetJob(id string) (*Job, error)
    ListJobsByStatus(status JobStatus, limit int) ([]Job, error)
    ListRecentJobs(limit int) ([]Job, error)
    CountByStatus() (map[string]int, error)
    Purge() error
}

type Queue interface {
    Enqueue(ctx context.Context, job *Job) error
    Dequeue(ctx context.Context) (*Delivery, error)
    Ack(ctx context.Context, receipt string) error
    Requeue(ctx context.Context, receipt string, job *Job) error
    SendToDead(ctx context.Context, receipt string, job *Job) error
    QueueLength(ctx context.Context) (int64, int64, error)
    Purge(ctx context.Context) error
}
```

Neither `Handler` nor `Pool` ever references a concrete `*RedisQueue` or `*PostgresStore` directly — they hold these interfaces instead. This is dependency inversion: the consumers of a queue/store define the contract they need, and any type satisfying that contract (real Redis, real Postgres, or a test mock) can be substituted with zero changes to the consumer's code. This is what makes `handler_test.go` and `worker_test.go` able to run without any real infrastructure — `mockQueue` and `mockStore` satisfy the interfaces purely through method-signature matching, which Go checks at compile time.

---

## 2. Why Redis for the Queue

**The problem:** a naive worker checking for jobs would poll — `SELECT * FROM jobs WHERE status = 'PENDING'` in a tight loop — burning CPU constantly even when there's no work, and creating race conditions if multiple workers try to claim the same row.

**The solution:** Redis Streams with a consumer group.

- `Enqueue` calls `XADD`, storing the serialized job as a stream message.
- `Dequeue` calls `XREADGROUP`, which reserves a message for a worker instead of deleting it.
- A successful, durably persisted job is completed with `XACK` and `XDEL`.
- Retry and dead-letter transitions use Redis transactions so adding the replacement message and acknowledging the original happen together.
- `XAUTOCLAIM` reassigns messages that remain unacknowledged for 30 seconds, recovering jobs abandoned by a crash, shutdown, or worker scale-down.

This provides **at-least-once delivery**. A worker can perform work and then fail before acknowledgment, so real job handlers must be idempotent. The important guarantee is that interruption no longer silently removes the only queued copy. Before processing reclaimed work, the worker checks PostgreSQL and acknowledges jobs already in a terminal state without executing them again.

An empty `XREADGROUP` timeout returns `redis.Nil` and is treated as the normal idle state. Actual Redis failures are returned to the worker loop and logged.

**Connection pool sizing:** blocked stream reads hold connections while waiting for work. With a dynamically scalable pool (see §5), the Redis client pool must be provisioned with worker headroom plus room for API enqueue and statistics calls:

```go
opts.PoolSize = 200
opts.MinIdleConns = 10
```

---

## 3. Why PostgreSQL as the System of Record

Redis holds jobs only while they're **in flight** — waiting to be picked up. It is not meant to be a permanent audit log; if Redis restarted with persistence misconfigured, in-flight job data could be lost, and there's no efficient way to ask Redis "show me every job that failed today."

PostgreSQL is the durable, queryable history. Every state transition a job goes through — `PENDING → PROCESSING → SUCCESS/FAILED/DEAD` — is written via a single `UpsertJob` call:

```go
INSERT INTO jobs (id, type, payload, status, retries, ...)
VALUES (:id, :type, :payload, :status, :retries, ...)
ON CONFLICT (id) DO UPDATE SET
    status = EXCLUDED.status,
    retries = EXCLUDED.retries,
    ...
```

`ON CONFLICT (id) DO UPDATE` means the same function handles both the very first insert and every subsequent update to that job's row, keyed on the job's UUID primary key. Without this, a second write to the same ID would fail outright with a duplicate-key violation, since a job's ID never changes across its lifecycle.

**Indexes:** `idx_jobs_status` and `idx_jobs_created_at` exist so that `ListJobsByStatus` and `ListRecentJobs` don't require a full table scan as the jobs table grows — Postgres can jump close to the matching rows rather than checking every row.

**Struct-to-column mapping:** the `Job` struct carries both `json:"..."` tags (API serialization) and `db:"..."` tags (SQL column mapping). `sqlx`'s `NamedExec` uses reflection over the `db` tags to match named placeholders (`:id`, `:type`, ...) to struct fields automatically, rather than requiring positional arguments in a fragile, error-prone order.

---

## 4. Circuit Breaker Around PostgreSQL

**The problem this solves:** if Postgres becomes slow or unavailable under load, every worker's `UpsertJob` call would hang or fail slowly. With many concurrent workers doing this simultaneously, connections pile up, memory grows, and the application can degrade or crash — compounding the original database problem rather than isolating it.

**The solution:** wrap every Postgres write in a circuit breaker (`gobreaker`), conceptually identical to an electrical breaker in a house — if too much is going wrong, it trips and cuts the connection immediately rather than letting the fault propagate and cause more damage.

```go
cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
    Name:        "Postgres",
    MaxRequests: 5,
    Interval:    10 * time.Second,
    Timeout:     5 * time.Second,
    ReadyToTrip: func(counts gobreaker.Counts) bool {
        failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
        return counts.Requests >= 10 && failureRatio >= 0.6
    },
})
```

**The three states:**
- **Closed** (normal) — all requests pass through to Postgres as usual
- **Open** (tripped) — triggered once `ReadyToTrip` returns true (here: at least 10 requests observed, and at least 60% of them failed). While open, every call fails **instantly** without even attempting to reach Postgres, for the `Timeout` duration (5 seconds)
- **Half-open** — after the timeout, the breaker cautiously allows up to `MaxRequests` (5) trial requests through. If they succeed, the breaker closes again (recovered); if they fail, it reopens

Every write goes through `Execute`, which wraps the actual database call:

```go
_, err := s.cb.Execute(func() (interface{}, error) {
    _, execErr := s.db.NamedExec(query, job)
    return nil, execErr
})
```

The breaker tracks the success/failure of whatever runs inside this closure and updates its internal counters accordingly — the calling code (`UpsertJob`) doesn't need any breaker-specific logic beyond this wrapping.

**Connection pool tuning**, addressed alongside the breaker:

```go
db.SetMaxOpenConns(50)
db.SetMaxIdleConns(25)
```

With a worker pool that can scale up to 100 workers (per the UI's max), each potentially calling `UpsertJob` concurrently, an unbounded number of simultaneous connections to Postgres would risk overwhelming it. Capping open connections at 50 bounds the maximum concurrent load the database will ever see from this application, regardless of how many workers are running.

---

## 5. Dynamic Worker Pool Scaling

Earlier versions of this project started a fixed number of workers once, at boot, with no way to change that count without restarting the process. The current implementation supports live scaling via `POST /workers/scale`.

**The data structure enabling this:**

```go
type Pool struct {
    queue      models.Queue
    store      models.Store
    mu         sync.Mutex
    numWorkers int
    workers    map[int]context.CancelFunc
    nextID     int
    parentCtx  context.Context
}
```

Each running worker is tracked by an ID mapped to its own `context.CancelFunc` — a function that, when called, cancels that specific worker's context and nothing else. This is the key mechanism: rather than one shared shutdown signal for all workers (as in the original fixed-pool design), **each worker gets its own individually cancellable context**, derived from the pool's parent context:

```go
wCtx, cancel := context.WithCancel(p.parentCtx)
p.workers[id] = cancel
go func(workerID int) {
    p.runWorker(wCtx, workerID)
    p.mu.Lock()
    delete(p.workers, workerID)
    p.mu.Unlock()
}(id)
```

**Scaling up** (`target > current`) simply starts `target - current` new workers, each getting a fresh ID and its own cancellable context.

**Scaling down** (`target < current`) iterates over the `workers` map, calling `cancel()` on however many workers need to stop. That worker's `runWorker` loop detects cancellation via its `select { case <-ctx.Done(): return }` check (or via `Dequeue` returning `context.Canceled`), exits its loop, and the goroutine cleans itself up.

**Why a `sync.Mutex` (`mu`) guards all of this:** `Scale`, `ActiveWorkers`, and the cleanup goroutine all read or write the shared `workers` map concurrently. Without a mutex, one goroutine could be iterating the map while another deletes from it — a genuine data race in Go, since maps are not safe for concurrent read/write without external synchronization. Every method that touches `workers` takes the lock first.

**`ActiveWorkers()`** simply returns `len(p.workers)` under the same lock — used by `/stats` and the dashboard to show a live, accurate worker count.

This is a materially harder concurrency problem than the original fixed-size pool: instead of "start N goroutines, they all share one shutdown signal," it's "maintain a live, mutable set of independently controllable goroutines, safely, under concurrent access." This is a legitimate example of the kind of concurrent data structure management that comes up in real distributed systems work.

---

## 6. The Race Condition Fix: Persist-Before-Enqueue

**The original design** (early version of this project) called `Enqueue` first, then `UpsertJob` second:

```go
h.queue.Enqueue(ctx, job)   // job is now visible to workers
h.store.UpsertJob(job)      // job's initial state is written to Postgres
```

**The race:** between these two lines, a worker could already dequeue the job (Redis delivers it the instant `Enqueue` succeeds) and start processing it — potentially calling its own `UpsertJob` to mark it `PROCESSING` — **before** the API's own `UpsertJob` call ever ran. Depending on timing, the worker's write could be overwritten by the API's late-arriving "initial state" write, silently reverting a job's status backward from `PROCESSING` to `PENDING` in the database, even though the job was actually already being handled.

**The fix** reverses the order:

```go
// CRITICAL: persist to DB first so a fast worker can't overwrite with stale data
if err := h.store.UpsertJob(job); err != nil {
    c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist job"})
    return
}

if err := h.queue.Enqueue(c.Request.Context(), job); err != nil {
    // Roll back DB state if enqueue fails
    job.Status = models.StatusFailed
    job.Error = "failed to enqueue"
    h.store.UpsertJob(job)
    c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue job"})
    return
}
```

Now the job's initial `PENDING` state is guaranteed to exist in Postgres **before** it becomes visible to any worker via Redis. There is no longer a window where a worker can race ahead of the job's own initial persistence.

**The added rollback path** handles the new failure mode this ordering introduces: if `UpsertJob` succeeds but the subsequent `Enqueue` fails (e.g., Redis is briefly unreachable), the job would otherwise sit in Postgres marked `PENDING` forever, with no worker ever able to see it (since it was never actually pushed to the queue). The rollback explicitly marks it `FAILED` with a descriptive error, so it shows up correctly in the DLQ/failed views rather than silently vanishing into a permanent, invisible `PENDING` state.

`RetryJob` follows the identical pattern for the same reason — it resets a job's state and re-enqueues it, so it's subject to the same race if enqueue happened first.

---

## 7. Graceful Shutdown

**The original approach** used Gin's `router.Run(":8080")`, which blocks until the process is forcibly killed — there is no hook to run cleanup logic before exit. A `Ctrl+C` or container stop signal would terminate the process immediately, mid-request and mid-job, with no opportunity to finish in-flight work.

**The current approach** replaces this with an explicit `http.Server` and a two-phase shutdown:

```go
srv := &http.Server{Addr: ":" + port, Handler: router}

go func() {
    log.Printf("starting server on port %s with %d workers", port, workerCount)
    if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        log.Fatalf("server failed: %v", err)
    }
}()

<-ctx.Done() // blocks here until Ctrl+C / SIGTERM
log.Println("shutdown signal received, draining in-flight jobs...")

shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

if err := srv.Shutdown(shutdownCtx); err != nil {
    log.Printf("HTTP server shutdown error: %v", err)
}
log.Println("server stopped gracefully")
```

The HTTP server now runs in its own goroutine, freeing the main goroutine to simply block on `<-ctx.Done()` — the same cancellation signal from `signal.NotifyContext` that the worker pool also observes. When a shutdown signal arrives, `srv.Shutdown(shutdownCtx)` stops accepting new connections but **waits up to 10 seconds** for in-flight HTTP requests to complete before forcibly closing them.

**Job-level shutdown responsiveness** was also improved. The original worker used `time.Sleep(duration)` to simulate job processing — a real `time.Sleep` cannot be interrupted, meaning a worker mid-"processing" a long job would ignore a shutdown signal entirely until that sleep finished. The current version replaces this with a `select`:

```go
select {
case <-time.After(duration):
    // work completed normally
case <-ctx.Done():
    log.Printf("worker %d: interrupted during job %s", workerID, job.ID)
    return
}
```

This makes the simulated work itself cancellable — if a shutdown signal arrives mid-job, the worker exits immediately rather than finishing an arbitrarily long sleep first. The same pattern replaces the backoff `time.Sleep` in `handleFailure`, so scaling a worker down or shutting down the whole process doesn't get blocked behind a pending retry's backoff delay either.

---

## 8. Observability: Prometheus + Grafana

Four metrics are tracked, defined in `internal/metrics/metrics.go`:

- `taskqueue_jobs_processed_total` (counter, labeled by `status` and `type`) — how many jobs have completed, broken down by outcome and job type
- `taskqueue_job_processing_duration_seconds` (histogram, labeled by `type`) — how long jobs actually take to process, per type
- `taskqueue_http_requests_total` (counter, labeled by `method`, `endpoint`, `status`) — API traffic volume and outcome
- `taskqueue_http_request_duration_seconds` (histogram, labeled by `method`, `endpoint`) — API latency distribution

**Counters vs histograms:** a counter only ever increases (total jobs processed so far); a histogram buckets observations (e.g., "how many requests took 0-100ms, 100-500ms, 500ms-1s...") so you can reason about latency distribution, not just an average.

**How metrics get recorded:** a Gin middleware wraps every request:

```go
func PrometheusMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        start := time.Now()
        c.Next() // run the actual handler
        duration := time.Since(start).Seconds()
        status := strconv.Itoa(c.Writer.Status())
        if c.Request.URL.Path == "/metrics" || c.Request.URL.Path == "/health" {
            return
        }
        HttpRequestsTotal.WithLabelValues(c.Request.Method, c.FullPath(), status).Inc()
        HttpRequestDuration.WithLabelValues(c.Request.Method, c.FullPath()).Observe(duration)
    }
}
```

`c.Next()` is Gin's mechanism for "run the rest of the middleware chain and the actual handler now, then come back here" — allowing this middleware to measure the *total* time the request took, including the handler's own work. `/metrics` and `/health` are excluded from tracking to avoid the scrape endpoint measuring itself.

**The pipeline:** Prometheus (`prometheus.yml`) polls the app's `/metrics` endpoint every 5 seconds and stores the time-series data. Grafana queries Prometheus and renders it as live dashboards — job throughput, success/failure rates, latency percentiles — all without any custom dashboard code, using Grafana's provisioning files (`grafana/provisioning/`) to auto-configure the datasource and dashboard on startup.

---

## 9. Testing Strategy

Both `internal/api/handler_test.go` and `internal/worker/worker_test.go` define local `mockQueue`/`mockStore` types — plain structs with pre-settable fields (`enqueueErr`, `getJobRes`, etc.) whose methods just return those pre-set values.

Because `Handler` and `Pool` depend on the `Queue`/`Store` **interfaces** rather than concrete Redis/Postgres types, these mocks can be substituted freely: Go's compiler only checks that method names and signatures match the interface, not what the methods actually do internally. This means:

- Tests run in milliseconds, with no real Redis or Postgres required
- Failure scenarios (e.g., "what does `CreateJob` return if the queue is down?") can be tested deterministically by setting `mockQueue{enqueueErr: errors.New("...")}`, rather than needing to actually break real infrastructure on demand
- `httptest.NewRecorder()` + `router.ServeHTTP(w, req)` exercises the real Gin routing and handler logic end-to-end, entirely in-memory, without a real server listening on any port

---

## 10. Known Tradeoffs and Honest Limitations

Worth being upfront about, rather than glossing over, in an interview:

- **Simulated job execution.** Jobs don't perform real work (no actual video encoding, email sending, etc.) — durations and failure rates are randomized per type to exercise the queueing/retry/DLQ machinery realistically, without needing real external integrations.
- **Silent payload parse failures.** In `processJob`, a malformed `job.Payload` that fails `json.Unmarshal` is currently swallowed silently (only the log line is skipped) rather than being treated as a job failure. In a system doing real work, this should set `simulatedErr = true` explicitly rather than allowing the job to proceed and succeed/fail based purely on the random roll, unrelated to whether its data was even valid.
- **Migrations are inline, not tool-managed.** `migrate()` uses `CREATE TABLE IF NOT EXISTS` and manual `ALTER TABLE ADD COLUMN IF NOT EXISTS` statements rather than a dedicated migration tool (e.g. `golang-migrate`, `goose`). Fine at this scale; a real production system with a team would want versioned, reversible migrations.
- **Single circuit breaker instance for all Postgres writes.** All calls share one breaker's state — a real production system might separate reads and writes, or shard breakers by operation type, for finer-grained protection.

---

## 11. Benchmark Methodology

Load generated via k6 (`loadtest/k6-script.js`) using a `constant-arrival-rate` executor: a fixed target rate of requests per second, sustained for a fixed duration, independent of how long each individual response takes. This specifically measures the API layer's **ingestion** throughput — whether it can keep accepting jobs at a steady rate — decoupled from how fast the worker pool processes the backlog those requests create.

```bash
docker run --rm -i --network host grafana/k6 run - < loadtest/k6-script.js
```

Each request is checked for `status === 202` — confirming acceptance, not completion. Processing throughput (how fast the backlog actually drains) is a separate measurement, observable via `/stats`' historical counts over time, or the Grafana dashboard's job-processed-per-second panel.
