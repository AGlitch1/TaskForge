import argparse
import sys
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

TERMINAL_STATUSES = {"COMPLETED", "DEAD", "CANCELLED"}
ACTIVE_WORKER_STATUSES = {"IDLE", "BUSY"}
JOB_STATUSES = {
    "CREATED",
    "SCHEDULED",
    "QUEUED",
    "RUNNING",
    "RETRYING",
    "COMPLETED",
    "DEAD",
    "CANCELLED",
}
ALLOWED_STATUS_TRANSITIONS = {
    (None, "CREATED"),
    ("CREATED", "QUEUED"),
    ("CREATED", "SCHEDULED"),
    ("SCHEDULED", "QUEUED"),
    ("QUEUED", "QUEUED"),
    ("QUEUED", "RUNNING"),
    ("RUNNING", "RUNNING"),
    ("RUNNING", "COMPLETED"),
    ("RUNNING", "RETRYING"),
    ("RUNNING", "DEAD"),
    ("RUNNING", "QUEUED"),
    ("RETRYING", "QUEUED"),
    ("QUEUED", "CANCELLED"),
    ("SCHEDULED", "CANCELLED"),
    ("RETRYING", "CANCELLED"),
}


@dataclass(frozen=True)
class JobPlan:
    index: int
    kind: str
    job_type: str
    payload: dict[str, Any]
    expected_status: str
    priority: int = 5
    max_retries: int = 0
    timeout_seconds: int | None = None

    @property
    def expects_progress_events(self) -> bool:
        return self.kind == "progress"

    @property
    def expects_timeout_events(self) -> bool:
        return self.kind == "timeout"


@dataclass(frozen=True)
class SubmittedJob:
    plan: JobPlan
    job_id: str
    idempotency_key: str


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def build_payload_for_complete_job(job_type: str, index: int) -> dict[str, Any]:
    if job_type == "sum_numbers":
        return {"numbers": [index, index + 1, index + 2]}
    if job_type == "progress_demo":
        return {"steps": 3}
    if job_type == "async_sleep":
        return {"duration_seconds": 0.2}
    if job_type == "sleep":
        return {"duration_seconds": 1}
    if job_type == "fail_randomly":
        return {"failure_probability": 0.0}
    if job_type == "generate_report":
        return {"rows": 10}

    raise ValueError(
        "Unsupported --concurrency-limit-job-type for this synthetic load test: "
        f"{job_type!r}."
    )


def build_job_plans(total_jobs: int, concurrency_limit_job_type: str) -> list[JobPlan]:
    plans: list[JobPlan] = []

    for index in range(total_jobs):
        bucket = index % 6

        if bucket == 0:
            plans.append(
                JobPlan(
                    index=index,
                    kind="sum_numbers",
                    job_type="sum_numbers",
                    payload={"numbers": [index, index + 1, index + 2]},
                    expected_status="COMPLETED",
                    priority=6,
                )
            )
        elif bucket == 1:
            plans.append(
                JobPlan(
                    index=index,
                    kind="progress",
                    job_type="progress_demo",
                    payload={"steps": 4},
                    expected_status="COMPLETED",
                    priority=5,
                )
            )
        elif bucket == 2:
            plans.append(
                JobPlan(
                    index=index,
                    kind="async_sleep",
                    job_type="async_sleep",
                    payload={"duration_seconds": 0.05},
                    expected_status="COMPLETED",
                    priority=5,
                )
            )
        elif bucket == 3:
            plans.append(
                JobPlan(
                    index=index,
                    kind="timeout",
                    job_type="async_sleep",
                    payload={"duration_seconds": 2.0},
                    expected_status="DEAD",
                    priority=4,
                    timeout_seconds=1,
                )
            )
        elif bucket == 4:
            plans.append(
                JobPlan(
                    index=index,
                    kind="intentional_failure",
                    job_type="fail_randomly",
                    payload={"failure_probability": 1.0},
                    expected_status="DEAD",
                    priority=4,
                )
            )
        else:
            plans.append(
                JobPlan(
                    index=index,
                    kind="concurrency_limited",
                    job_type=concurrency_limit_job_type,
                    payload=build_payload_for_complete_job(
                        concurrency_limit_job_type,
                        index,
                    ),
                    expected_status="COMPLETED",
                    priority=7,
                )
            )

    return plans


def submit_job(
    client: httpx.Client,
    api_url: str,
    run_id: str,
    plan: JobPlan,
) -> SubmittedJob:
    idempotency_key = f"load-test-{run_id}-{plan.index:05d}-{plan.kind}"
    request_body: dict[str, Any] = {
        "job_type": plan.job_type,
        "payload": plan.payload,
        "priority": plan.priority,
        "max_retries": plan.max_retries,
    }

    if plan.timeout_seconds is not None:
        request_body["timeout_seconds"] = plan.timeout_seconds

    response = client.post(
        f"{api_url}/jobs",
        headers={"Idempotency-Key": idempotency_key},
        json=request_body,
    )
    response.raise_for_status()

    return SubmittedJob(
        plan=plan,
        job_id=response.json()["id"],
        idempotency_key=idempotency_key,
    )


def get_workers(client: httpx.Client, api_url: str) -> list[dict[str, Any]]:
    response = client.get(f"{api_url}/workers")
    response.raise_for_status()
    return list(response.json())


def poll_jobs_until_terminal(
    client: httpx.Client,
    api_url: str,
    submitted_jobs: list[SubmittedJob],
    *,
    timeout_seconds: int,
    poll_interval_seconds: float,
    verbose: bool,
) -> dict[str, dict[str, Any]]:
    deadline = time.monotonic() + timeout_seconds
    latest_by_job_id: dict[str, dict[str, Any]] = {}
    last_summary: Counter[str] | None = None

    while time.monotonic() < deadline:
        status_counts: Counter[str] = Counter()

        for submitted in submitted_jobs:
            response = client.get(f"{api_url}/jobs/{submitted.job_id}")
            response.raise_for_status()

            job = response.json()
            latest_by_job_id[submitted.job_id] = job
            status_counts[job["status"]] += 1

        if verbose and status_counts != last_summary:
            summary = ", ".join(
                f"{status}={count}" for status, count in sorted(status_counts.items())
            )
            print(f"Poll status: {summary}")
            last_summary = status_counts.copy()

        if all(
            latest_by_job_id[submitted.job_id]["status"] in TERMINAL_STATUSES
            for submitted in submitted_jobs
        ):
            return latest_by_job_id

        time.sleep(poll_interval_seconds)

    unfinished = [
        (
            submitted.job_id,
            latest_by_job_id.get(submitted.job_id, {}).get("status", "UNKNOWN"),
        )
        for submitted in submitted_jobs
        if latest_by_job_id.get(submitted.job_id, {}).get("status")
        not in TERMINAL_STATUSES
    ]
    unfinished_preview = ", ".join(
        f"{job_id}:{status}" for job_id, status in unfinished[:10]
    )
    raise TimeoutError(
        f"Timed out waiting for terminal job states. Unfinished: {unfinished_preview}"
    )


def verify_final_statuses(
    submitted_jobs: list[SubmittedJob],
    jobs_by_id: dict[str, dict[str, Any]],
) -> list[CheckResult]:
    failures = []

    for submitted in submitted_jobs:
        actual = jobs_by_id[submitted.job_id]["status"]
        expected = submitted.plan.expected_status

        if actual != expected:
            failures.append(
                f"{submitted.job_id} kind={submitted.plan.kind} "
                f"expected={expected} actual={actual}"
            )

    return [
        CheckResult(
            name="expected final statuses",
            passed=not failures,
            detail=(
                "all jobs reached expected statuses"
                if not failures
                else "; ".join(failures[:10])
            ),
        )
    ]


def verify_workers(
    workers: list[dict[str, Any]],
    *,
    expected_workers: int,
) -> list[CheckResult]:
    active_workers = [
        worker for worker in workers if worker.get("status") in ACTIVE_WORKER_STATUSES
    ]
    worker_count = len(workers)
    active_count = len(active_workers)

    results = [
        CheckResult(
            name="at least one active worker",
            passed=active_count > 0,
            detail=f"observed {active_count} active workers out of {worker_count}",
        )
    ]

    if worker_count < expected_workers:
        results.append(
            CheckResult(
                name="expected worker count warning",
                passed=True,
                detail=(
                    f"warning: observed {worker_count} registered workers, "
                    f"expected {expected_workers}"
                ),
            )
        )
    else:
        results.append(
            CheckResult(
                name="expected worker count",
                passed=True,
                detail=f"observed {worker_count} registered workers",
            )
        )

    return results


def verify_http_event_checks(
    client: httpx.Client,
    api_url: str,
    submitted_jobs: list[SubmittedJob],
) -> list[CheckResult]:
    job_ids_by_kind: dict[str, list[str]] = defaultdict(list)

    for submitted in submitted_jobs:
        job_ids_by_kind[submitted.plan.kind].append(submitted.job_id)

    progress_job_ids = job_ids_by_kind.get("progress", [])
    timeout_job_ids = job_ids_by_kind.get("timeout", [])
    event_count_by_type: Counter[str] = Counter()
    events_by_job_id: dict[str, list[dict[str, Any]]] = {}

    for submitted in submitted_jobs:
        response = client.get(f"{api_url}/jobs/{submitted.job_id}/events")
        response.raise_for_status()

        events = list(response.json())
        events_by_job_id[submitted.job_id] = events

        for event in events:
            event_count_by_type[event["event_type"]] += 1

    progress_jobs_with_events = [
        job_id
        for job_id in progress_job_ids
        if any(
            event["event_type"] == "JOB_PROGRESS_UPDATED"
            for event in events_by_job_id.get(job_id, [])
        )
    ]
    timeout_jobs_with_events = [
        job_id
        for job_id in timeout_job_ids
        if any(
            event["event_type"] == "JOB_TIMED_OUT"
            for event in events_by_job_id.get(job_id, [])
        )
    ]

    progress_passed = len(progress_jobs_with_events) == len(progress_job_ids)
    timeout_passed = len(timeout_jobs_with_events) == len(timeout_job_ids)

    if not progress_passed or not timeout_passed:
        print("Event verification debug:")
        print(
            "  event rows found for submitted jobs: "
            f"{sum(len(events) for events in events_by_job_id.values())}"
        )
        print(
            "  event counts by event_type: "
            f"{dict(sorted(event_count_by_type.items()))}"
        )
        print(f"  progress job IDs checked: {len(progress_job_ids)}")
        print(f"  timeout job IDs checked: {len(timeout_job_ids)}")

    return [
        CheckResult(
            name="progress events",
            passed=progress_passed,
            detail=(
                f"observed JOB_PROGRESS_UPDATED for "
                f"{len(progress_jobs_with_events)}/{len(progress_job_ids)} "
                "progress jobs"
            ),
        ),
        CheckResult(
            name="timeout events",
            passed=timeout_passed,
            detail=(
                f"observed JOB_TIMED_OUT for "
                f"{len(timeout_jobs_with_events)}/{len(timeout_job_ids)} timeout jobs"
            ),
        ),
    ]


def verify_database_checks(submitted_jobs: list[SubmittedJob]) -> list[CheckResult]:
    try:
        from sqlalchemy import func, select

        from app.core.database import SessionLocal
        from app.models.job_attempt import JobAttempt
        from app.models.job_event import JobEvent
    except Exception as exc:
        return [
            CheckResult(
                name="database verification setup",
                passed=False,
                detail=f"could not import database utilities: {exc}",
            )
        ]

    job_ids = [uuid.UUID(submitted.job_id) for submitted in submitted_jobs]
    job_ids_by_kind: dict[str, list[uuid.UUID]] = defaultdict(list)

    for submitted in submitted_jobs:
        job_ids_by_kind[submitted.plan.kind].append(uuid.UUID(submitted.job_id))

    try:
        with SessionLocal() as db:
            duplicate_attempt_rows = db.execute(
                select(
                    JobAttempt.job_id,
                    JobAttempt.attempt_number,
                    func.count(JobAttempt.id),
                )
                .where(JobAttempt.job_id.in_(job_ids))
                .group_by(JobAttempt.job_id, JobAttempt.attempt_number)
                .having(func.count(JobAttempt.id) > 1)
            ).all()

            concurrency_job_ids = job_ids_by_kind.get("concurrency_limited", [])
            concurrency_deferred_count = 0
            if concurrency_job_ids:
                concurrency_deferred_count = int(
                    db.scalar(
                        select(func.count(JobEvent.id))
                        .where(JobEvent.job_id.in_(concurrency_job_ids))
                        .where(JobEvent.event_type == "JOB_CONCURRENCY_DEFERRED")
                    )
                    or 0
                )

            transition_failures = []
            events = list(
                db.scalars(
                    select(JobEvent)
                    .where(JobEvent.job_id.in_(job_ids))
                    .order_by(JobEvent.job_id.asc(), JobEvent.created_at.asc())
                ).all()
            )

            for event in events:
                transition = (event.old_status, event.new_status)

                if (
                    event.old_status is not None
                    and event.old_status not in JOB_STATUSES
                ):
                    transition_failures.append(
                        f"{event.job_id} has unknown old_status={event.old_status}"
                    )
                    continue

                if (
                    event.new_status is not None
                    and event.new_status not in JOB_STATUSES
                ):
                    transition_failures.append(
                        f"{event.job_id} has unknown new_status={event.new_status}"
                    )
                    continue

                if (
                    event.new_status is not None
                    and transition not in ALLOWED_STATUS_TRANSITIONS
                ):
                    transition_failures.append(
                        f"{event.job_id} has unexpected transition "
                        f"{event.old_status}->{event.new_status}"
                    )

    except Exception as exc:
        return [
            CheckResult(
                name="database verification query",
                passed=False,
                detail=f"database verification failed: {exc}",
            )
        ]

    results = [
        CheckResult(
            name="no duplicate execution attempts",
            passed=not duplicate_attempt_rows,
            detail=(
                "no duplicate (job_id, attempt_number) rows"
                if not duplicate_attempt_rows
                else f"found {len(duplicate_attempt_rows)} duplicate attempt groups"
            ),
        ),
        CheckResult(
            name="state transition sanity",
            passed=not transition_failures,
            detail=(
                "no impossible status transitions found"
                if not transition_failures
                else "; ".join(transition_failures[:10])
            ),
        ),
    ]

    if concurrency_job_ids:
        results.append(
            CheckResult(
                name="concurrency deferred events",
                passed=True,
                detail=(
                    f"observed {concurrency_deferred_count} deferred events for "
                    f"{len(concurrency_job_ids)} concurrency-target jobs; "
                    "zero is acceptable if the running stack has no matching "
                    "JOB_TYPE_CONCURRENCY_LIMITS or workers did not saturate it"
                ),
            )
        )

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Submit a synthetic TaskForge workload through the public API and "
            "verify distributed correctness with optional database reads."
        )
    )
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--jobs", type=int, default=100)
    parser.add_argument("--expected-workers", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--poll-interval-seconds", type=float, default=1)
    parser.add_argument("--concurrency-limit-job-type", default="async_sleep")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.jobs < 1:
        print("--jobs must be at least 1", file=sys.stderr)
        return 2

    run_id = f"{int(time.time())}-{uuid.uuid4().hex[:8]}"
    plans = build_job_plans(args.jobs, args.concurrency_limit_job_type)
    started_at = time.monotonic()

    print(f"TaskForge synthetic load test run_id={run_id}")
    print(f"API URL: {args.api_url}")
    print(f"Jobs: {args.jobs}")
    print(f"Expected workers: {args.expected_workers}")

    try:
        with httpx.Client(timeout=10.0) as client:
            workers_before = get_workers(client, args.api_url)
            worker_checks = verify_workers(
                workers_before,
                expected_workers=args.expected_workers,
            )

            for check in worker_checks:
                prefix = "PASS" if check.passed else "FAIL"
                print(f"{prefix}: {check.name}: {check.detail}")

            if any(not check.passed for check in worker_checks):
                print("No active worker is available; aborting before submission.")
                return 1

            submitted_jobs = []
            for plan in plans:
                submitted = submit_job(client, args.api_url, run_id, plan)
                submitted_jobs.append(submitted)

                if args.verbose:
                    print(
                        "Submitted "
                        f"{submitted.job_id} kind={plan.kind} "
                        f"expected={plan.expected_status}"
                    )
                elif len(submitted_jobs) % 10 == 0 or len(submitted_jobs) == len(plans):
                    print(f"Submitted {len(submitted_jobs)}/{len(plans)} jobs")

            jobs_by_id = poll_jobs_until_terminal(
                client,
                args.api_url,
                submitted_jobs,
                timeout_seconds=args.timeout_seconds,
                poll_interval_seconds=args.poll_interval_seconds,
                verbose=args.verbose,
            )

            workers_after = get_workers(client, args.api_url)
            event_checks = verify_http_event_checks(
                client, args.api_url, submitted_jobs
            )

    except TimeoutError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    except httpx.HTTPError as exc:
        print(f"FAIL: HTTP request failed: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 2

    status_checks = verify_final_statuses(submitted_jobs, jobs_by_id)
    db_checks = verify_database_checks(submitted_jobs)
    checks = status_checks + event_checks + db_checks

    duration_seconds = time.monotonic() - started_at
    status_counts = Counter(job["status"] for job in jobs_by_id.values())
    kind_counts = Counter(submitted.plan.kind for submitted in submitted_jobs)
    failed_checks = [check for check in checks if not check.passed]
    observed_worker_count = len(workers_after)

    print()
    print("Job mix:")
    for kind, count in sorted(kind_counts.items()):
        print(f"  {kind}: {count}")

    print()
    print("Correctness checks:")
    for check in checks:
        prefix = "PASS" if check.passed else "FAIL"
        print(f"  {prefix}: {check.name}: {check.detail}")

    print()
    print("Summary:")
    print(f"  run_id: {run_id}")
    print(f"  jobs_submitted: {len(submitted_jobs)}")
    print(f"  completed_count: {status_counts.get('COMPLETED', 0)}")
    print(f"  dead_count: {status_counts.get('DEAD', 0)}")
    print(f"  failed_verification_count: {len(failed_checks)}")
    print(f"  duration_seconds: {duration_seconds:.2f}")
    print(
        f"  approximate_jobs_per_second: {len(submitted_jobs) / duration_seconds:.2f}"
    )
    print(f"  worker_count_observed: {observed_worker_count}")

    if failed_checks:
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
