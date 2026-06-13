# TaskForge Job State Machine

TaskForge models every job as a state machine.

The job state machine makes execution predictable, debuggable, and recoverable.

Each job has one current status stored in PostgreSQL.

Redis is only used to store ready-to-run job IDs. The real job state always lives in PostgreSQL.

## Job States

TaskForge currently supports these job states:

| State       | Meaning                                       |
| ----------- | --------------------------------------------- |
| `QUEUED`    | Job is ready to run and should exist in Redis |
| `SCHEDULED` | Job is waiting for a future scheduled time    |
| `RUNNING`   | Job has been claimed by a worker              |
| `RETRYING`  | Job failed but will be retried later          |
| `COMPLETED` | Job finished successfully                     |
| `DEAD`      | Job failed permanently after retry exhaustion |
| `CANCELLED` | Job was cancelled before execution            |

## Normal Successful Flow

Most immediate jobs follow this path:

```text
QUEUED
  |
  v
RUNNING
  |
  v
COMPLETED
```

Explanation:

1. The API creates the job as `QUEUED`.
2. The API pushes the job ID into Redis.
3. A worker reads the job ID from Redis.
4. The worker atomically claims the job in PostgreSQL.
5. The job becomes `RUNNING`.
6. The worker executes the job handler.
7. If the handler succeeds, the job becomes `COMPLETED`.

## Scheduled Job Flow

Scheduled jobs are created with a future `scheduled_at` timestamp.

```text
SCHEDULED
  |
  v
QUEUED
  |
  v
RUNNING
  |
  v
COMPLETED
```

Explanation:

1. The API creates the job as `SCHEDULED`.
2. The job is not pushed into Redis yet.
3. The scheduler checks for due scheduled jobs.
4. When `scheduled_at <= now`, the scheduler moves the job to `QUEUED`.
5. The scheduler pushes the job ID into Redis.
6. A worker eventually claims and executes the job.

## Retry Flow

If a job fails and still has retries remaining, it enters `RETRYING`.

```text
RUNNING
  |
  v
RETRYING
  |
  v
QUEUED
  |
  v
RUNNING
```

Explanation:

1. Worker runs the job.
2. Job handler raises an exception.
3. Worker records the failed attempt.
4. Worker increments `retry_count`.
5. If retries remain, the job becomes `RETRYING`.
6. Worker sets `next_run_at`.
7. Scheduler later moves the job back to `QUEUED`.
8. Job is pushed back into Redis.

## Dead Job Flow

If a job fails and has no retries remaining, it becomes `DEAD`.

```text
RUNNING
  |
  v
DEAD
```

A `DEAD` job is not automatically retried again.

It requires manual replay.

## Replay Flow

Replaying a `DEAD` job creates a new job.

The original job remains `DEAD`.

```text
Original job:
DEAD

New replayed job:
QUEUED
  |
  v
RUNNING
  |
  v
COMPLETED
```

The new job stores:

```text
replayed_from_job_id = original_dead_job_id
```

This preserves the failure history while allowing the work to be tried again.

## Cancellation Flow

TaskForge supports cancellation before execution.

Allowed cancellation states:

```text
QUEUED
SCHEDULED
RETRYING
```

Cancellation transition:

```text
QUEUED / SCHEDULED / RETRYING
  |
  v
CANCELLED
```

If a `QUEUED` job is cancelled, TaskForge also removes it from Redis.

TaskForge does not currently cancel `RUNNING` jobs.

This avoids unsafe interruption of job handlers.

## Lease Recovery Flow

If a worker crashes while running a job, the job may stay `RUNNING`.

TaskForge uses leases to recover these jobs.

```text
RUNNING
  |
  | lease expires
  v
QUEUED
```

Explanation:

1. Worker claims job and sets `lease_expires_at`.
2. Worker renews the lease while the job runs.
3. If the worker crashes, renewal stops.
4. Scheduler detects that the lease expired.
5. Scheduler marks the abandoned attempt as `FAILED`.
6. Scheduler clears lease fields.
7. Scheduler moves the job back to `QUEUED`.
8. Scheduler pushes the job back into Redis.

This is one of the main recovery mechanisms in TaskForge.

## State Transition Table

| From State  | To State         | Trigger                                     |
| ----------- | ---------------- | ------------------------------------------- |
| `SCHEDULED` | `QUEUED`         | Scheduler releases due scheduled job        |
| `QUEUED`    | `RUNNING`        | Worker claims job                           |
| `RUNNING`   | `COMPLETED`      | Job handler succeeds                        |
| `RUNNING`   | `RETRYING`       | Job handler fails and retries remain        |
| `RUNNING`   | `DEAD`           | Job handler fails and retries are exhausted |
| `RETRYING`  | `QUEUED`         | Scheduler releases due retry                |
| `RUNNING`   | `QUEUED`         | Scheduler recovers expired lease            |
| `QUEUED`    | `CANCELLED`      | User cancels queued job                     |
| `SCHEDULED` | `CANCELLED`      | User cancels scheduled job                  |
| `RETRYING`  | `CANCELLED`      | User cancels retrying job                   |
| `DEAD`      | new `QUEUED` job | User replays dead job                       |

## Terminal States

Terminal states are states where the job will not continue automatically.

TaskForge terminal states:

```text
COMPLETED
DEAD
CANCELLED
```

Meaning:

* `COMPLETED`: work finished successfully
* `DEAD`: work failed permanently
* `CANCELLED`: work was intentionally stopped before running

A `DEAD` job can be replayed, but replay creates a new job.

The original `DEAD` job does not leave the `DEAD` state.

## Redis Expectations by State

Redis should contain only ready jobs.

| Job State   | Should be in Redis? |
| ----------- | ------------------- |
| `QUEUED`    | Yes                 |
| `SCHEDULED` | No                  |
| `RUNNING`   | No                  |
| `RETRYING`  | No                  |
| `COMPLETED` | No                  |
| `DEAD`      | No                  |
| `CANCELLED` | No                  |

If a job is `QUEUED` in PostgreSQL but missing from Redis, queue reconciliation restores it.

If a job is not `QUEUED`, it should not be in Redis.

## Worker Claim Rule

Workers can only claim jobs from `QUEUED`.

Conceptually:

```sql
UPDATE jobs
SET status = 'RUNNING'
WHERE id = :job_id
  AND status = 'QUEUED'
RETURNING *;
```

This prevents stale Redis entries from causing invalid execution.

For example, if Redis contains an old job ID but the job was already cancelled, the claim fails because the job is no longer `QUEUED`.

## Why the State Machine Matters

The state machine gives TaskForge clear rules.

It answers:

* Can this job run?
* Can this job be cancelled?
* Should this job be in Redis?
* Should the scheduler touch this job?
* Is this job done forever?
* Can this job be retried?
* Can this job be replayed?

Without strict states, distributed job systems become very hard to debug.

TaskForge keeps the state machine explicit so failures are easier to reason about.
