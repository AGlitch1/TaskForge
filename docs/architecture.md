# TaskForge Architecture

TaskForge is a distributed job scheduler built around three main services:

1. FastAPI API server
2. Worker process
3. Scheduler process

It uses PostgreSQL as the durable source of truth and Redis as a fast ready-job queue.

## High-Level Architecture

```text
Client
  |
  v
FastAPI API Server
  |
  +------------------+
  |                  |
  v                  v
PostgreSQL         Redis Ready Queue
  |                  |
  |                  v
  |                Worker
  |                  |
  |                  v
  |              Job Handler
  |
  v
Job State, Workers, Attempts, Events
```

The scheduler runs as a separate process:

```text
Scheduler
  |
  +-- releases scheduled jobs
  +-- releases retrying jobs
  +-- detects stale workers
  +-- recovers expired job leases
  +-- reconciles PostgreSQL jobs with Redis
```

## Main Components

## API Server

The API server is the entry point into the system.

It handles client requests but does not execute jobs directly.

Responsibilities:

* Create jobs
* Validate job payloads
* Enforce idempotency keys
* Generate request fingerprints
* Store job state in PostgreSQL
* Push ready jobs into Redis
* Expose job status endpoints
* Expose job event history
* Expose job attempt history
* Expose queue statistics
* Expose worker status
* Cancel jobs before execution
* Replay DEAD jobs

The API server writes durable state first, then enqueues ready jobs into Redis.

This keeps PostgreSQL as the source of truth.

## PostgreSQL

PostgreSQL is the durable system of record.

It stores:

* Jobs
* Workers
* Job attempts
* Job events

PostgreSQL answers the important questions:

* What jobs exist?
* What state is each job in?
* Which worker is running a job?
* How many attempts has a job had?
* Why did a job fail?
* What lifecycle events happened?

Redis can be rebuilt from PostgreSQL, but PostgreSQL cannot be rebuilt from Redis.

That is why PostgreSQL is treated as the source of truth.

## Redis

Redis is used as a fast ready-job queue.

TaskForge uses a Redis sorted set named:

```text
ready_jobs
```

Each entry is:

```text
job_id -> score
```

The score is calculated using:

* queued time
* priority

This allows workers to fetch jobs efficiently while still respecting priority.

Redis only stores jobs that are ready to run.

It does not store:

* full job payloads
* job results
* attempts
* worker state
* event history

Those belong in PostgreSQL.

## Worker Process

Workers execute jobs.

A worker is responsible for:

* Registering itself in PostgreSQL
* Sending periodic heartbeats
* Polling Redis for ready jobs
* Atomically claiming jobs in PostgreSQL
* Running the correct job handler
* Recording job attempts
* Marking jobs as completed
* Marking jobs as retrying or dead after failure
* Renewing job leases while running jobs
* Shutting down gracefully when possible

Workers do not trust Redis alone.

Even if a worker pops a job ID from Redis, it still has to claim that job in PostgreSQL.

A job can only be claimed if it is currently in the `QUEUED` state.

This prevents multiple workers from safely claiming the same job at the same time.

## Scheduler Process

The scheduler handles delayed transitions and repair work.

It runs continuously in the background.

Responsibilities:

* Move due `SCHEDULED` jobs to `QUEUED`
* Move due `RETRYING` jobs to `QUEUED`
* Mark stale workers as `DEAD`
* Recover `RUNNING` jobs with expired leases
* Rebuild missing Redis queue entries for `QUEUED` jobs

The scheduler is what prevents the system from getting stuck.

Without it:

* Scheduled jobs would never start.
* Retry jobs would never be released.
* Dead workers would remain active forever.
* Crashed jobs could stay `RUNNING` forever.
* Redis queue data loss could strand jobs in PostgreSQL.

## Source of Truth Design

TaskForge deliberately separates durable state from fast queueing.

PostgreSQL is the source of truth.

Redis is a performance optimization.

This means Redis is allowed to be incomplete or temporarily inconsistent.

For example:

```text
PostgreSQL says job is QUEUED
Redis does not contain the job ID
Scheduler detects the mismatch
Scheduler re-adds the job ID to Redis
```

This design makes the system more recoverable.

If Redis is cleared, TaskForge can repair itself by scanning PostgreSQL.

## Job Creation Flow

When a client creates a job:

```text
Client
  |
  v
POST /jobs
  |
  v
Validate payload
  |
  v
Check idempotency key
  |
  v
Create job row in PostgreSQL
  |
  v
Create job events
  |
  v
If job is ready now, enqueue job ID into Redis
```

Immediate jobs become:

```text
QUEUED
```

Future jobs become:

```text
SCHEDULED
```

Scheduled jobs are not pushed into Redis until their scheduled time arrives.

## Worker Execution Flow

When a worker runs:

```text
Worker polls Redis
  |
  v
Gets candidate job ID
  |
  v
Attempts atomic PostgreSQL claim
  |
  v
If claim succeeds, job becomes RUNNING
  |
  v
Worker starts job attempt
  |
  v
Worker runs job handler
  |
  +----------------------+
  |                      |
  v                      v
Success                Failure
  |                      |
  v                      v
COMPLETED             RETRYING or DEAD
```

Redis provides candidate jobs.

PostgreSQL decides whether the worker is actually allowed to run the job.

## Atomic Job Claiming

Atomic claiming is one of the most important parts of the system.

A worker claims a job using a PostgreSQL update that only succeeds if the job is still `QUEUED`.

Conceptually:

```sql
UPDATE jobs
SET status = 'RUNNING',
    leased_by = :worker_id,
    lease_expires_at = :lease_expiration
WHERE id = :job_id
  AND status = 'QUEUED'
RETURNING *;
```

If two workers try to claim the same job, only one update succeeds.

The losing worker does not run the job.

This prevents duplicate execution from normal worker races.

## At-Least-Once Execution

TaskForge provides at-least-once execution.

This means every recoverable job should eventually run, but a job may run more than once if a worker crashes during execution.

Example:

```text
Worker claims job
Worker starts job
Worker crashes before completion
Lease expires
Scheduler requeues job
Another worker runs the job
```

This is a deliberate tradeoff.

Exactly-once execution is not guaranteed.

For dangerous side effects, job handlers should be idempotent.

## Worker Heartbeats

Each worker periodically updates:

```text
last_heartbeat_at
```

The scheduler checks this timestamp.

If a worker has not sent a heartbeat within the configured timeout, the scheduler marks it:

```text
DEAD
```

This allows TaskForge to detect crashed or disconnected workers.

## Job Leases

When a worker claims a job, the job receives a lease:

```text
leased_by = worker_id
lease_expires_at = now + JOB_LEASE_SECONDS
```

While the job is running, the worker renews this lease.

If the worker is alive, the lease keeps extending.

If the worker crashes, the lease stops renewing.

The scheduler can then detect that the job is abandoned.

## Lease Recovery

If a job is `RUNNING` but its lease has expired, the scheduler recovers it.

Recovery flow:

```text
RUNNING job
  |
  v
lease_expires_at is in the past
  |
  v
Scheduler marks abandoned attempt as FAILED
  |
  v
Scheduler clears lease fields
  |
  v
Scheduler moves job back to QUEUED
  |
  v
Scheduler re-adds job ID to Redis
```

This prevents jobs from staying `RUNNING` forever after a worker crash.

## Queue Reconciliation

Redis is not durable source-of-truth storage.

A job can be `QUEUED` in PostgreSQL but missing from Redis.

This could happen if:

* Redis was cleared
* Redis restarted without persistence
* The API crashed after committing to PostgreSQL but before enqueueing
* A manual debugging command deleted the queue

Queue reconciliation fixes this.

```text
Scheduler scans PostgreSQL for QUEUED jobs
  |
  v
For each QUEUED job, check Redis
  |
  v
If missing, re-add job ID to Redis
```

This makes the system self-repairing.

## Retry Flow

When a job fails, the worker decides whether it should retry.

If retries remain:

```text
RUNNING
  |
  v
RETRYING
  |
  v
Scheduler waits until next_run_at
  |
  v
QUEUED
  |
  v
Worker runs it again
```

If retries are exhausted:

```text
RUNNING
  |
  v
DEAD
```

A `DEAD` job is not automatically retried again.

It can only be replayed manually.

## Dead Job Replay

Replaying a `DEAD` job does not mutate the original job.

Instead, TaskForge creates a new job with copied fields:

* job type
* payload
* priority
* max retries

The new job stores:

```text
replayed_from_job_id = original_dead_job_id
```

This preserves history while allowing the work to be tried again.

## Cancellation

TaskForge supports cancellation before execution.

Jobs can be cancelled from:

```text
QUEUED
SCHEDULED
RETRYING
```

Cancelled jobs become:

```text
CANCELLED
```

If a queued job is cancelled, it is also removed from Redis.

TaskForge does not currently cancel jobs that are already running.

## Why This Architecture Is Strong

This architecture demonstrates several real backend and distributed systems concepts:

* Durable state management
* Queueing
* Worker coordination
* Atomic job claiming
* Heartbeats
* Leases
* Crash recovery
* Retry scheduling
* Dead-letter handling
* Queue reconciliation
* Idempotent job creation
* Event logging
* Integration testing with PostgreSQL and Redis

It is intentionally simple enough to understand but realistic enough to discuss in a system design interview.

## Local Process Layout

When running locally, the system usually uses three terminals:

```text
Terminal 1: FastAPI API
Terminal 2: Worker
Terminal 3: Scheduler
```

Commands:

```bash
uvicorn app.main:app --reload --no-access-log
python3 -m worker.main
python3 -m scheduler.main
```

## Docker Compose Layout

With Docker Compose, the system runs as five services:

```text
postgres
redis
api
worker
scheduler
```

Command:

```bash
docker compose up --build
```

This starts the full TaskForge stack.
