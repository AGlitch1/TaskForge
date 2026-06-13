from app.core.enums import JobStatus
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