# TaskForge Failure Recovery

TaskForge is designed around the assumption that failures will happen.

Workers can crash. Redis can lose data. Jobs can fail. The API can commit to PostgreSQL and then fail before enqueueing into Redis. A worker can start a job and disappear before completing it.

The goal of TaskForge is not to prevent every failure. The goal is to make failures recoverable.

## Recovery Philosophy

TaskForge follows a few core rules:

1. PostgreSQL is the source of truth.
2. Redis is only a ready-job queue.
3. Workers must claim jobs atomically in PostgreSQL.
4. Workers must heartbeat while alive.
5. Running jobs must have leases.
6. The scheduler repairs stuck or inconsistent state.
7. Jobs are executed at least once, not exactly once.

This design makes the system easier to reason about when something breaks.

## Failure Scenario 1: Worker Crashes Before Claiming a Job

A worker may pop or inspect a job from Redis but crash before claiming it in PostgreSQL.

In this case, the job is still `QUEUED` in PostgreSQL.

If the Redis entry is missing, queue reconciliation restores it.

```text
QUEUED job in PostgreSQL
Redis entry missing
Scheduler reconciliation restores Redis entry
```

Result:

```text
Job can still be picked up by another worker.
```

## Failure Scenario 2: Worker Crashes After Claiming a Job

A more serious failure happens when a worker claims a job and then crashes.

The job is now:

```text
RUNNING
```

It also has:

```text
leased_by = worker_id
lease_expires_at = future timestamp
```

While the worker is alive, it renews this lease.

If the worker crashes, lease renewal stops.

Eventually:

```text
lease_expires_at < now
```

The scheduler detects this and recovers the job.

Recovery flow:

```text
RUNNING job
  |
  v
Lease expires
  |
  v
Scheduler marks abandoned attempt as FAILED
  |
  v
Scheduler clears leased_by and lease_expires_at
  |
  v
Scheduler moves job back to QUEUED
  |
  v
Scheduler pushes job ID into Redis
```

Result:

```text
Another worker can run the job.
```

## Failure Scenario 3: Worker Becomes Stale

Each worker sends periodic heartbeats.

The heartbeat updates:

```text
last_heartbeat_at
```

If the scheduler sees that a worker has not sent a heartbeat within the configured timeout, it marks the worker:

```text
DEAD
```

This does not automatically mean every job from that worker is immediately dead.

Job recovery is handled separately through leases.

This separation matters:

* Heartbeats track worker liveness.
* Leases track whether a specific job is still actively owned.

## Failure Scenario 4: Redis Loses the Ready Queue

Redis is not treated as durable storage.

If Redis is cleared or restarted, TaskForge can recover because PostgreSQL still stores job state.

The scheduler periodically scans for jobs that are:

```text
QUEUED
```

For each `QUEUED` job, it checks whether the job ID exists in Redis.

If missing, it re-adds the job.

```text
PostgreSQL: job is QUEUED
Redis: job ID missing
Scheduler: re-adds job ID to Redis
```

Result:

```text
Redis data loss does not permanently strand jobs.
```

## Failure Scenario 5: API Commits Job but Fails Before Enqueue

A subtle failure can happen during job creation.

The API may:

```text
Create job in PostgreSQL
Commit transaction
Crash before adding job to Redis
```

Now PostgreSQL has a `QUEUED` job, but Redis does not.

Queue reconciliation fixes this the same way it fixes Redis data loss.

```text
QUEUED job missing from Redis
Scheduler detects mismatch
Scheduler restores Redis entry
```

This is why TaskForge does not depend on Redis as the source of truth.

## Failure Scenario 6: Job Handler Fails

If a job handler raises an exception, the worker catches it.

The worker then:

1. Marks the current attempt as `FAILED`.
2. Stores the error message.
3. Increments `retry_count`.
4. Checks whether retries remain.

If retries remain:

```text
RUNNING
  |
  v
RETRYING
```

The job receives a future `next_run_at`.

The scheduler later moves it back to:

```text
QUEUED
```

If retries are exhausted:

```text
RUNNING
  |
  v
DEAD
```

A `DEAD` job is not automatically retried again.

## Failure Scenario 7: Retry Job Gets Stuck

A retrying job waits in:

```text
RETRYING
```

It should not be in Redis yet.

The scheduler is responsible for releasing it when:

```text
next_run_at <= now
```

Then the scheduler moves it to:

```text
QUEUED
```

and pushes it to Redis.

If the scheduler is temporarily down, retry jobs simply wait longer. They are not lost.

When the scheduler comes back, it releases due retries.

## Failure Scenario 8: Scheduled Job Gets Stuck

Scheduled jobs wait in:

```text
SCHEDULED
```

They are not in Redis until their scheduled time arrives.

The scheduler checks:

```text
scheduled_at <= now
```

Then it moves the job to:

```text
QUEUED
```

and pushes it to Redis.

If the scheduler is down, scheduled jobs are delayed but not lost.

## Failure Scenario 9: Duplicate Job Creation Request

Clients may retry API requests.

Without protection, a client retry could create duplicate jobs.

TaskForge supports idempotency keys.

When a request includes an idempotency key, TaskForge stores:

```text
idempotency_key
request_fingerprint
```

If the same key is used with the same request, TaskForge returns the existing job.

If the same key is reused with a different request, TaskForge rejects it.

This prevents accidental duplicate job creation from client retries.

## Failure Scenario 10: Stale Redis Entry

Redis may contain a job ID that should no longer run.

For example:

```text
Job was cancelled
Old job ID still exists in Redis
```

This is safe because workers do not trust Redis alone.

The worker still has to claim the job in PostgreSQL with a condition like:

```sql
WHERE id = :job_id
  AND status = 'QUEUED'
```

If the job is no longer `QUEUED`, the claim fails.

Result:

```text
The worker does not run cancelled, completed, dead, or running jobs.
```

## Worker Heartbeats

Workers register themselves when they start.

They periodically update:

```text
last_heartbeat_at
```

The scheduler uses this to detect stale workers.

Important fields:

| Field               | Meaning                                                    |
| ------------------- | ---------------------------------------------------------- |
| `status`            | Worker status such as `IDLE`, `BUSY`, `STOPPED`, or `DEAD` |
| `last_heartbeat_at` | Last time the worker reported it was alive                 |
| `current_job_id`    | Job currently owned by the worker, if any                  |

Heartbeats answer:

```text
Is this worker process still alive?
```

They do not alone decide whether a job should be retried.

That is handled by job leases.

## Job Leases

A job lease is a temporary ownership record.

When a worker claims a job, the job stores:

```text
leased_by = worker_id
lease_expires_at = now + JOB_LEASE_SECONDS
```

While the worker runs the job, it renews the lease.

If the worker dies, the lease stops being renewed.

The scheduler can then safely assume the job was abandoned after the lease expires.

## Why Heartbeats and Leases Are Separate

Heartbeats and leases solve different problems.

Worker heartbeat:

```text
Is the worker alive?
```

Job lease:

```text
Is this job still actively owned?
```

A worker might be alive but fail to renew a job lease because of a bug.

A worker might stop heartbeating but the lease still has not expired yet.

Keeping these mechanisms separate makes recovery more precise.

## Queue Reconciliation

Queue reconciliation is the repair mechanism between PostgreSQL and Redis.

Expected rule:

```text
Every QUEUED job in PostgreSQL should exist in Redis.
```

If not, the scheduler repairs it.

This protects against:

* Redis data loss
* API crash after database commit
* manual Redis deletion
* temporary Redis enqueue failure
* queue inconsistency during development

## At-Least-Once Execution

TaskForge guarantees at-least-once execution for recoverable jobs.

This means a job should eventually run if the system keeps making progress.

But it may run more than once.

Example:

```text
Worker starts job
Job performs partial side effect
Worker crashes
Scheduler recovers job
Another worker runs job again
```

This is why job handlers with external side effects should be idempotent.

## Why Not Exactly Once?

Exactly-once execution is extremely difficult in distributed systems.

A worker can crash at any point:

* before running the handler
* during the handler
* after the handler succeeds
* before saving the result
* after saving the result but before acknowledging completion

TaskForge does not pretend to solve exactly-once execution.

Instead, it makes the guarantee explicit:

```text
At-least-once execution with idempotency support at the job creation layer.
```

Handler-level idempotency is left to the job implementation.

## Recovery Responsibilities

| Component  | Recovery Responsibility                                                                        |
| ---------- | ---------------------------------------------------------------------------------------------- |
| API        | Durable job creation, idempotency                                                              |
| Worker     | Attempt tracking, retries, lease renewal                                                       |
| Scheduler  | Scheduled release, retry release, stale worker detection, lease recovery, queue reconciliation |
| PostgreSQL | Durable state                                                                                  |
| Redis      | Fast ready queue only                                                                          |

## Why This Design Is Reliable

TaskForge avoids a common beginner mistake: putting too much trust in the queue.

Instead, Redis is treated as disposable.

The true state is always in PostgreSQL.

That gives the system a path to recover from several real-world failures:

* worker crash
* scheduler downtime
* Redis data loss
* duplicate API requests
* stale queue entries
* failed job handlers
* stuck running jobs

The result is a simple but realistic distributed job scheduler design.
