# Reliable Job Queue with Dead-Letter Recovery

A production-grade, crash-resilient background job queue engine built in **Node.js** with zero external native dependencies, utilizing Node's built-in **`node:sqlite`** in **Write-Ahead Logging (WAL)** mode for ACID durability.

---

## Core Features & Mechanics

| Requirement | Implementation |
|---|---|
| **01. Durable Storage & Crash Recovery** | SQLite in WAL mode with `PRAGMA synchronous = NORMAL;`. State changes are committed atomically. Survived ungraceful kills (`kill -9`) and system crashes. |
| **02. Priority & Delayed Execution** | Composite index on `(status, run_at, priority DESC)`. Workers lease strictly by highest priority first and respect delayed execution timestamps. |
| **03. Exponential Backoff Retries** | When a job fails, the next retry delay is calculated as $\text{base\_delay} \times 2^{\text{attempts}-1} + \text{jitter}$ (capped at 5 minutes). |
| **04. Dead-Letter Queue (DLQ) & Replay** | Jobs exceeding `max_attempts` transition to `dead_letter` status with complete error traces. Supports inspection and individual or batch replay. |
| **05. Visibility Timeout & Lease Pattern** | Atomic leasing via `BEGIN IMMEDIATE` transactions. Workers maintain heartbeats. If a worker dies mid-flight, the lease expires and is automatically recovered by other workers. |
| **06. CLI & Observability** | Full CLI suite to enqueue, inspect, list in-flight leases, manage DLQ, and view status. |
| **07. Idempotency Demonstration** | Enqueue-level deduplication via `idempotency_key` and step-level transaction logging (`job_steps`) so retrying a partially completed job never duplicates side effects (e.g. charging credit card). |
| **+ Stretch 1: DAG Dependencies** | Job parent-child dependencies (`job_dependencies`). Child jobs remain blocked until parent jobs successfully complete. |
| **+ Stretch 2: Real-time Web Dashboard** | Interactive dashboard with live metrics, Server-Sent Events (SSE) log stream, and one-click architectural simulation buttons. |

---

## Quick Start

### 1. Run Automated Test Suite
```bash
npm test
```
Runs the 13 automated tests verifying crash recovery, lease timeouts, exponential backoff, DLQ replay, step idempotency, and DAG dependencies.

### 2. Start the Real-time Web UI Dashboard
```bash
npm start
```
Open **`http://localhost:3000`** in your browser to access the dashboard.
- Live counters (Queued, In-Flight, Completed, Dead-Letter).
- Real-time event feed via Server-Sent Events (SSE).
- One-click demo triggers for crash simulation, exponential backoff DLQ, and DAG pipelines.
- Modal inspector for job details, execution steps, and DLQ replay.

### 3. Using the CLI

```bash
# Check queue metrics
node src/cli.js status

# Enqueue a job
node src/cli.js enqueue email '{"to":"user@example.com","subject":"Hello"}' --priority 5

# Enqueue with delay (in seconds)
node src/cli.js enqueue email '{"to":"delayed@example.com"}' --delay 10

# Enqueue with idempotency key
node src/cli.js enqueue payment '{"amount":50}' --idempotency-key tx_unique_101

# Inspect in-flight jobs
node src/cli.js inflight

# Inspect a specific job and its completed steps
node src/cli.js inspect <jobId>

# Inspect Dead-Letter Queue
node src/cli.js dlq list

# Replay a failed job
node src/cli.js dlq replay <jobId>
# Or replay all
node src/cli.js dlq replay all

# Run dedicated background worker process
node src/cli.js worker --concurrency 3 --visibility 15
```

---

## Architecture

```
reliable-job-queue/
├── src/
│   ├── db.js              # SQLite WAL mode initialization & schema setup
│   ├── queue.js           # Core JobQueue engine (lease, heartbeat, retry, DLQ)
│   ├── worker.js          # WorkerPool with heartbeats, concurrency & event emitter
│   ├── idempotency.js     # Step-level execution ledger
│   ├── tasks/
│   │   └── registry.js    # Task definitions (email, payment_order, failing_task)
│   ├── server.js          # HTTP REST API & SSE event broadcast server
│   └── cli.js             # CLI tool
├── public/
│   ├── index.html         # Web dashboard UI
│   ├── styles.css         # Modern dark dashboard styling
│   └── app.js             # SSE client listener & interactive controls
└── test/
    ├── crash-recovery.test.js # Visibility lease & dead-worker recovery
    ├── idempotency.test.js    # Multi-step crash idempotency & deduplication
    ├── dlq-retry.test.js      # Backoff calculation, DLQ & replay
    └── priority-dag.test.js   # Priority ordering & DAG dependencies
```
