from sqlalchemy import select

from app.core.enums import JobEventType, JobStatus
from app.models.job_event import JobEvent
from app.schemas.jobs import JobCreateRequest
from app.services.job_service import create_job
from app.services.worker_service import register_worker
from worker.runner import run_one_job


def test_worker_completes_sum_numbers_job(db_session, redis_client):
    worker = register_worker(
        db_session,
        worker_id="test-worker-1",
        hostname="test-host",
        process_id=123,
    )

    request = JobCreateRequest(
        job_type="sum_numbers",
        payload={"numbers": [1, 2, 3]},
        priority=5,
        scheduled_at=None,
        max_retries=3,
    )

    job = create_job(
        db_session,
        request=request,
        idempotency_key="integration-sum-1",
    )

    assert job.status == JobStatus.QUEUED.value

    did_work = run_one_job(
        db_session,
        worker_id=worker.id,
    )

    assert did_work is True

    db_session.refresh(job)

    assert job.status == JobStatus.COMPLETED.value
    assert job.result == {
        "sum": 6.0,
        "count": 3,
    }


def test_worker_marks_failing_job_dead_when_no_retries(db_session, redis_client):
    worker = register_worker(
        db_session,
        worker_id="test-worker-2",
        hostname="test-host",
        process_id=124,
    )

    request = JobCreateRequest(
        job_type="fail_randomly",
        payload={"failure_probability": 1.0},
        priority=5,
        scheduled_at=None,
        max_retries=0,
    )

    job = create_job(
        db_session,
        request=request,
        idempotency_key="integration-fail-1",
    )

    assert job.status == JobStatus.QUEUED.value

    did_work = run_one_job(
        db_session,
        worker_id=worker.id,
    )

    assert did_work is True

    db_session.refresh(job)

    assert job.status == JobStatus.DEAD.value
    assert job.retry_count == 1
    assert job.error_message is not None


def test_worker_records_progress_updates_from_context_handler(db_session, redis_client):
    worker = register_worker(
        db_session,
        worker_id="test-worker-progress-1",
        hostname="test-host",
        process_id=125,
    )

    request = JobCreateRequest(
        job_type="progress_demo",
        payload={"steps": 3},
        priority=5,
        scheduled_at=None,
        max_retries=3,
    )

    job = create_job(
        db_session,
        request=request,
        idempotency_key="integration-progress-1",
    )

    assert job.status == JobStatus.QUEUED.value

    did_work = run_one_job(
        db_session,
        worker_id=worker.id,
    )

    assert did_work is True

    db_session.refresh(job)

    assert job.status == JobStatus.COMPLETED.value
    assert job.progress_percent == 100
    assert job.progress_message == "Job completed successfully."
    assert job.result == {
        "steps": 3,
        "progress_updates": 3,
    }

    progress_events = list(
        db_session.scalars(
            select(JobEvent)
            .where(JobEvent.job_id == job.id)
            .where(JobEvent.event_type == JobEventType.JOB_PROGRESS_UPDATED.value)
            .order_by(JobEvent.created_at.asc())
        ).all()
    )

    assert len(progress_events) == 3

    assert progress_events[0].event_metadata["progress_percent"] == 33
    assert progress_events[1].event_metadata["progress_percent"] == 66
    assert progress_events[2].event_metadata["progress_percent"] == 100

    for event in progress_events:
        assert event.worker_id == worker.id
        assert event.event_metadata["attempt_number"] == 1
        assert event.event_metadata["attempt_id"] is not None
