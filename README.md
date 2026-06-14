# TaskForge

# TaskForge

[![CI](https://github.com/AGlitch1/TaskForge/actions/workflows/ci.yml/badge.svg)](https://github.com/AGlitch1/TaskForge/actions/workflows/ci.yml)

TaskForge is a distributed job scheduler built with **FastAPI**, **PostgreSQL**, **Redis**, and Python worker processes.

It is designed to demonstrate real distributed-systems concepts: durable job state, Redis-backed queueing, worker heartbeats, retries, scheduled jobs, lease renewal, crash recovery, queue reconciliation, idempotency, and observable job history.

## Why TaskForge Exists

Many backend systems need to run work outside the request-response cycle: report generation, data ingestion, webhooks, document processing, model jobs, retries, and scheduled tasks.

TaskForge implements a simplified but realistic version of that infrastructure.

Instead of relying on an existing queue framework, TaskForge builds the core mechanisms directly:

* PostgreSQL is the source of truth.
* Redis is used as a fast ready queue.
* Workers claim and execute jobs.
* A scheduler repairs and advances job state.
* Leases and heartbeats prevent jobs from getting stuck forever.

## Features

* FastAPI REST API
* PostgreSQL-backed durable job state
* Redis sorted-set ready queue
* Worker process registration
* Worker heartbeat tracking
* Worker status API
* At-least-once job execution
* Atomic PostgreSQL job claiming
* Job attempts table
* Job event history
* Idempotency keys
* Request fingerprinting
* Payload validation with Pydantic
* Scheduled jobs
* Retry support with backoff
* DEAD job state after retry exhaustion
* Dead job replay
* Job cancellation before execution
* Worker lease renewal
* Expired lease recovery
* Stale worker detection
* Queue reconciliation between PostgreSQL and Redis
* Graceful worker shutdown
* File-based structured logging
* Docker Compose deployment
* Unit tests
* Integration tests
* GitHub Actions CI

## Tech Stack

| Layer      | Technology              |
| ---------- | ----------------------- |
| API        | FastAPI                 |
| Database   | PostgreSQL              |
| Queue      | Redis sorted set        |
| ORM        | SQLAlchemy              |
| Migrations | Alembic                 |
| Validation | Pydantic                |
| Workers    | Python worker processes |
| Tests      | Pytest                  |
| Deployment | Docker Compose          |
| CI         | GitHub Actions          |

## Architecture

At a high level:

```text
Client
  |
  v
FastAPI API
  |
  | writes durable job state
  v
PostgreSQL
  |
  | stores source-of-truth job state
  |
  +---- Redis ready queue
              |
              v
          Worker process
              |
              v
          Job handler
```

The scheduler runs separately:

```text
Scheduler
  |
  +-- releases scheduled jobs
  +-- releases retrying jobs
  +-- detects dead workers
  +-- recovers expired job leases
  +-- reconciles PostgreSQL QUEUED jobs with Redis
```

## Core Design

TaskForge uses **PostgreSQL as the source of truth** and **Redis as a performance optimization**.

Redis only stores ready-to-run job IDs. If Redis loses data, the scheduler can rebuild the ready queue from PostgreSQL using queue reconciliation.

This avoids treating Redis as the durable system of record.

## Job Lifecycle

Normal successful path:

```text
CREATED
  -> QUEUED
  -> RUNNING
  -> COMPLETED
```

Scheduled job path:

```text
CREATED
  -> SCHEDULED
  -> QUEUED
  -> RUNNING
  -> COMPLETED
```

Failure and retry path:

```text
RUNNING
  -> RETRYING
  -> QUEUED
  -> RUNNING
  -> DEAD
```

Cancellation path:

```text
QUEUED / SCHEDULED / RETRYING
  -> CANCELLED
```

Replay path:

```text
DEAD job
  -> replay creates a new QUEUED job
```

The original DEAD job is not mutated. A new job is created with `replayed_from_job_id`.

## At-Least-Once Execution

TaskForge uses **at-least-once execution**.

This means a job may run more than once if a worker crashes after starting it but before completing it.

For example:

```text
Worker claims job
Worker starts job
Worker crashes
Lease expires
Scheduler requeues job
Another worker runs job again
```

This is intentional. The system prioritizes recovery over exactly-once execution.

Exactly-once execution is not guaranteed. Job handlers should be designed to be idempotent if duplicate execution is dangerous.

## Worker Heartbeats

Each worker registers itself in PostgreSQL when it starts.

Workers periodically update:

```text
last_heartbeat_at
```

The scheduler checks worker heartbeats. If a worker has not heartbeated within the configured timeout, it is marked:

```text
DEAD
```

This allows the system to detect crashed workers.

## Job Leases

When a worker claims a job, the job receives a lease:

```text
leased_by = worker_id
lease_expires_at = now + JOB_LEASE_SECONDS
```

While the worker is running the job, it renews the lease.

If the worker crashes, lease renewal stops. The scheduler later sees the expired lease and recovers the job.

## Expired Lease Recovery

If a job is stuck in `RUNNING` and its lease expires, the scheduler moves it back to `QUEUED` and pushes it into Redis again.

```text
RUNNING job with expired lease
  -> QUEUED
  -> Redis ready queue
```

The abandoned running attempt is marked as `FAILED`.

This prevents jobs from being stuck forever after worker crashes.

## Queue Reconciliation

Redis is not the source of truth. If a job is `QUEUED` in PostgreSQL but missing from Redis, the scheduler restores it.

```text
PostgreSQL: job is QUEUED
Redis: job ID missing
Scheduler: re-adds job ID to Redis
```

This fixes Redis/PostgreSQL mismatches caused by crashes, manual Redis clears, or transient enqueue failures.

## Job Attempts

Every execution attempt is tracked.

A job can have multiple attempts:

```text
attempt 1 -> FAILED
attempt 2 -> FAILED
attempt 3 -> COMPLETED
```

Attempts store:

* attempt number
* worker ID
* status
* start time
* finish time
* duration
* error message

## Job Events

TaskForge records job lifecycle events such as:

* `JOB_CREATED`
* `JOB_QUEUED`
* `JOB_SCHEDULED`
* `JOB_CLAIMED`
* `JOB_STARTED`
* `JOB_COMPLETED`
* `JOB_ATTEMPT_FAILED`
* `JOB_RETRY_SCHEDULED`
* `JOB_DEAD`
* `JOB_CANCELLED`

This gives every job an auditable timeline.

## Supported Job Types

Current built-in job handlers:

| Job Type        | Description                             |
| --------------- | --------------------------------------- |
| `sum_numbers`   | Sums a list of numbers                  |
| `sleep`         | Sleeps for a given number of seconds    |
| `fail_randomly` | Fails based on a configured probability |

Additional planned handlers:

| Job Type          | Description                                       |
| ----------------- | ------------------------------------------------- |
| `webhook`         | Sends an HTTP request                             |
| `generate_report` | Simulates report generation with progress updates |

## API Endpoints

### Jobs

```http
POST /jobs
GET /jobs
GET /jobs/{job_id}
GET /jobs/{job_id}/events
GET /jobs/{job_id}/attempts
POST /jobs/{job_id}/cancel
POST /jobs/{job_id}/replay
```

### Workers

```http
GET /workers
```

### Queue

```http
GET /queue/stats
```

### Health

```http
GET /health
```

## Example: Create a Job

```bash
curl -X POST "http://localhost:8000/jobs" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-sum-1" \
  -d '{
    "job_type": "sum_numbers",
    "payload": {
      "numbers": [1, 2, 3, 4]
    },
    "priority": 5,
    "max_retries": 3
  }'
```

Example response:

```json
{
  "id": "job-id",
  "job_type": "sum_numbers",
  "status": "QUEUED",
  "priority": 5,
  "retry_count": 0
}
```

## Example: Check Job Status

```bash
curl "http://localhost:8000/jobs/YOUR_JOB_ID"
```

Example completed result:

```json
{
  "status": "COMPLETED",
  "result": {
    "sum": 10.0,
    "count": 4
  }
}
```

## Example: Cancel a Job

Only jobs in `QUEUED`, `SCHEDULED`, or `RETRYING` can be cancelled.

```bash
curl -X POST "http://localhost:8000/jobs/YOUR_JOB_ID/cancel"
```

## Example: Replay a DEAD Job

Only `DEAD` jobs can be replayed.

```bash
curl -X POST "http://localhost:8000/jobs/YOUR_JOB_ID/replay"
```

This creates a new job and keeps the original DEAD job unchanged.

## Running Locally

Start PostgreSQL and Redis locally.

Create a `.env` file based on `.env.example`.

Example:

```env
APP_NAME=TaskForge
APP_ENV=local
DEBUG=true

DATABASE_URL=postgresql+psycopg://taskforge:taskforge@localhost:5432/taskforge
REDIS_URL=redis://localhost:6379/0

WORKER_POLL_INTERVAL_SECONDS=1
WORKER_HEARTBEAT_INTERVAL_SECONDS=5
DEAD_WORKER_TIMEOUT_SECONDS=30

JOB_LEASE_SECONDS=30
LEASE_RENEW_INTERVAL_SECONDS=10

SCHEDULER_INTERVAL_SECONDS=2
SCHEDULER_LOCK_TTL_SECONDS=10

MAX_RETRIES_DEFAULT=3
```

Run migrations:

```bash
alembic upgrade head
```

Start the API:

```bash
uvicorn app.main:app --reload --no-access-log
```

Start a worker:

```bash
python3 -m worker.main
```

Start the scheduler:

```bash
python3 -m scheduler.main
```

## Running with Docker Compose

Start the full system:

```bash
docker compose up --build
```

This starts:

* PostgreSQL
* Redis
* FastAPI API
* worker
* scheduler

Stop the system:

```bash
docker compose down
```

Stop and delete Docker volumes:

```bash
docker compose down -v
```

Only use `-v` if you want to delete the Docker PostgreSQL data.

## Running Tests

Run all tests:

```bash
pytest
```

Run unit tests only:

```bash
pytest tests/unit
```

Run integration tests only:

```bash
pytest tests/integration
```

Current test coverage includes:

* payload validation
* fingerprinting
* retry delay logic
* queue scoring
* job handlers
* worker execution flow
* failure handling
* queue reconciliation
* scheduled job release
* retry release
* lease recovery
* cancellation
* dead job replay

## CI

TaskForge uses GitHub Actions to run tests automatically on every push and pull request.

The CI workflow starts PostgreSQL and Redis service containers, installs dependencies, and runs:

```bash
pytest
```

## Load Testing

TaskForge includes a small load testing script that submits many jobs to the API quickly.

The goal of this script is not to fully benchmark production performance. Instead, it gives a simple local sanity check that the API can accept a burst of jobs and that workers can drain the Redis-backed ready queue.

### Run the Load Test

Make sure the API, worker, and scheduler are running.

Terminal 1:

```bash
uvicorn app.main:app --reload --no-access-log
```

Terminal 2:

```bash
python3 -m worker.main
```

Terminal 3:

```bash
python3 -m scheduler.main
```

Then run the load test:

```bash
python3 scripts/load_test.py --jobs 100
```

### Example Result

On a local development machine, TaskForge submitted 100 jobs successfully:

```text
Submitted 100 jobs in 0.62 seconds
Submission rate: 161.90 jobs/second
```

### Verify Processing

After submitting jobs, check system metrics:

```bash
curl http://localhost:8000/admin/metrics
```

Useful metrics:

```text
taskforge_jobs_queued
taskforge_jobs_running
taskforge_jobs_completed
taskforge_jobs_dead
taskforge_ready_queue_depth
```

A healthy result should show completed jobs increasing and the ready queue depth eventually returning to zero:

```text
taskforge_jobs_completed 100
taskforge_ready_queue_depth 0
```

### What This Demonstrates

This load test demonstrates that TaskForge can:

- accept a burst of job creation requests
- persist jobs durably in PostgreSQL
- enqueue ready jobs in Redis
- allow workers to process jobs asynchronously
- expose system state through metrics

This is a lightweight development load test, not a production benchmark.

## Logging

TaskForge uses Python’s built-in logging library.

Logs are written to separate files:

```text
logs/api.log
logs/worker.log
logs/scheduler.log
```

Worker and scheduler logs are also printed in a clean terminal format.

Log files are ignored by Git.

## Design Tradeoffs

### PostgreSQL as Source of Truth

PostgreSQL stores durable job state, attempts, workers, and events.

Redis is only used for fast ready-job lookup.

This makes the system more recoverable if Redis loses data.

### At-Least-Once Instead of Exactly-Once

TaskForge chooses at-least-once execution because it is simpler and more realistic for many distributed job systems.

Exactly-once execution would require stronger idempotency guarantees at the job-handler level.

### Polling Workers

Workers poll Redis instead of using push-based messaging.

This keeps the design simple and makes worker behavior easy to reason about.

### Scheduler-Based Repair

The scheduler handles delayed transitions and repair work:

* scheduled jobs
* retrying jobs
* dead workers
* expired leases
* Redis/PostgreSQL queue reconciliation

This centralizes recovery logic.

## Future Improvements

Planned improvements:

* Prometheus metrics
* Grafana dashboard
* load testing
* webhook job handler
* generate_report job handler
* job progress updates
* scheduler locking for multiple schedulers
* authentication
* frontend dashboard
* more advanced retry policies
* better pagination and filtering
* OpenAPI documentation examples

## Project Status

TaskForge currently implements the core backend of a distributed job scheduler, including worker execution, retries, crash recovery, queue reconciliation, Docker deployment, tests, and CI.

The remaining work is mostly observability, documentation polish, load testing, and demo handlers.
