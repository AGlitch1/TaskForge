import pytest
from sqlalchemy import func, select

from app.core.config import get_settings
from app.core.enums import JobEventType, JobStatus
from app.core.time import utc_now
from app.models.job_attempt import JobAttempt
from app.models.job_event import JobEvent
from app.schemas.jobs import JobCreateRequest
from app.services.concurrency_service import (
    build_slot_key,
    release_concurrency_slot,
    try_acquire_concurrency_slot,
)
from app.services.job_service import create_job
from app.services.queue_service import CONCURRENCY_DEFERRED_JOBS_KEY
from app.services.worker_service import register_worker
from worker.runner import run_one_job


@pytest.fixture(autouse=True)
def clear_settings_between_tests():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def set_concurrency_limits(monkeypatch, limits: str, defer_seconds: int = 1) -> None:
    monkeypatch.setenv("JOB_TYPE_CONCURRENCY_LIMITS", limits)
    monkeypatch.setenv("CONCURRENCY_DEFER_SECONDS", str(defer_seconds))
    get_settings.cache_clear()


def test_limited_job_runs_when_slot_available_and_releases_slot(
    db_session,
    redis_client,
    monkeypatch,
):
    set_concurrency_limits(monkeypatch, '{"async_sleep":1}')

    worker = register_worker(
        db_session,
        worker_id="test-worker-concurrency-success",
        hostname="test-host",
        process_id=201,
    )

    job = create_job(
        db_session,
        request=JobCreateRequest(
            job_type="async_sleep",
            payload={"duration_seconds": 0.01},
            priority=5,
            scheduled_at=None,
            max_retries=0,
        ),
        idempotency_key="integration-concurrency-success",
    )

    did_work = run_one_job(db_session, worker_id=worker.id)

    assert did_work is True

    db_session.refresh(job)

    assert job.status == JobStatus.COMPLETED.value
    assert redis_client.zcard(build_slot_key("async_sleep")) == 0


def test_capacity_deferred_job_stays_queued_without_attempt_then_runs_later(
    db_session,
    redis_client,
    monkeypatch,
):
    set_concurrency_limits(monkeypatch, '{"async_sleep":1}')

    worker = register_worker(
        db_session,
        worker_id="test-worker-concurrency-deferred",
        hostname="test-host",
        process_id=202,
    )

    acquired = try_acquire_concurrency_slot(
        redis_client,
        job_type="async_sleep",
        job_id="already-running-job",
        limit=1,
        ttl_seconds=30,
    )
    assert acquired is True

    job = create_job(
        db_session,
        request=JobCreateRequest(
            job_type="async_sleep",
            payload={"duration_seconds": 0.01},
            priority=5,
            scheduled_at=None,
            max_retries=0,
        ),
        idempotency_key="integration-concurrency-deferred",
    )

    did_work = run_one_job(db_session, worker_id=worker.id)

    assert did_work is False

    db_session.refresh(job)

    assert job.status == JobStatus.QUEUED.value
    assert job.retry_count == 0
    assert redis_client.zscore("ready_jobs", str(job.id)) is None
    assert redis_client.zscore(CONCURRENCY_DEFERRED_JOBS_KEY, str(job.id)) is not None

    attempt_count = db_session.scalar(
        select(func.count()).select_from(JobAttempt).where(JobAttempt.job_id == job.id)
    )
    assert attempt_count == 0

    deferred_event = db_session.scalar(
        select(JobEvent)
        .where(JobEvent.job_id == job.id)
        .where(JobEvent.event_type == JobEventType.JOB_CONCURRENCY_DEFERRED.value)
    )

    assert deferred_event is not None
    assert deferred_event.event_metadata["job_type"] == "async_sleep"
    assert deferred_event.event_metadata["limit"] == 1
    assert deferred_event.event_metadata["defer_seconds"] == 1
    assert deferred_event.event_metadata["deferred_until"] is not None

    release_concurrency_slot(
        redis_client,
        job_type="async_sleep",
        job_id="already-running-job",
    )
    redis_client.zadd(
        CONCURRENCY_DEFERRED_JOBS_KEY,
        {str(job.id): utc_now().timestamp() - 1},
    )

    did_work = run_one_job(db_session, worker_id=worker.id)

    assert did_work is True

    db_session.refresh(job)

    assert job.status == JobStatus.COMPLETED.value
    assert redis_client.zcard(build_slot_key("async_sleep")) == 0


def test_limited_job_releases_slot_after_handler_failure(
    db_session,
    redis_client,
    monkeypatch,
):
    set_concurrency_limits(monkeypatch, '{"fail_randomly":1}')

    worker = register_worker(
        db_session,
        worker_id="test-worker-concurrency-failure",
        hostname="test-host",
        process_id=203,
    )

    job = create_job(
        db_session,
        request=JobCreateRequest(
            job_type="fail_randomly",
            payload={"failure_probability": 1.0},
            priority=5,
            scheduled_at=None,
            max_retries=0,
        ),
        idempotency_key="integration-concurrency-failure",
    )

    did_work = run_one_job(db_session, worker_id=worker.id)

    assert did_work is True

    db_session.refresh(job)

    assert job.status == JobStatus.DEAD.value
    assert redis_client.zcard(build_slot_key("fail_randomly")) == 0


def test_limited_job_releases_slot_after_timeout(
    db_session,
    redis_client,
    monkeypatch,
):
    set_concurrency_limits(monkeypatch, '{"async_sleep":1}')

    worker = register_worker(
        db_session,
        worker_id="test-worker-concurrency-timeout",
        hostname="test-host",
        process_id=204,
    )

    job = create_job(
        db_session,
        request=JobCreateRequest(
            job_type="async_sleep",
            payload={"duration_seconds": 2.0},
            priority=5,
            scheduled_at=None,
            max_retries=0,
            timeout_seconds=1,
        ),
        idempotency_key="integration-concurrency-timeout",
    )

    did_work = run_one_job(db_session, worker_id=worker.id)

    assert did_work is True

    db_session.refresh(job)

    assert job.status == JobStatus.DEAD.value
    assert redis_client.zcard(build_slot_key("async_sleep")) == 0


def test_unlisted_job_type_is_not_affected_by_limited_job_type_slot(
    db_session,
    redis_client,
    monkeypatch,
):
    set_concurrency_limits(monkeypatch, '{"async_sleep":1}')

    worker = register_worker(
        db_session,
        worker_id="test-worker-concurrency-unlisted",
        hostname="test-host",
        process_id=205,
    )

    acquired = try_acquire_concurrency_slot(
        redis_client,
        job_type="async_sleep",
        job_id="already-running-job",
        limit=1,
        ttl_seconds=30,
    )
    assert acquired is True

    job = create_job(
        db_session,
        request=JobCreateRequest(
            job_type="sum_numbers",
            payload={"numbers": [1, 2, 3]},
            priority=5,
            scheduled_at=None,
            max_retries=0,
        ),
        idempotency_key="integration-concurrency-unlisted",
    )

    did_work = run_one_job(db_session, worker_id=worker.id)

    assert did_work is True

    db_session.refresh(job)

    assert job.status == JobStatus.COMPLETED.value
    assert job.result == {"sum": 6.0, "count": 3}
