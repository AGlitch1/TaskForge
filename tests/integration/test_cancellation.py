import pytest
from fastapi import HTTPException

from app.core.enums import JobStatus
from app.schemas.jobs import JobCreateRequest
from app.services.job_service import cancel_job, create_job
from app.services.queue_service import get_ready_queue_depth


def test_cancel_queued_job_removes_it_from_redis(db_session, redis_client):
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
        idempotency_key="integration-cancel-queued-1",
    )

    assert job.status == JobStatus.QUEUED.value
    assert get_ready_queue_depth(redis_client) == 1

    cancelled_job = cancel_job(
        db_session,
        job_id=job.id,
    )

    assert cancelled_job.status == JobStatus.CANCELLED.value
    assert get_ready_queue_depth(redis_client) == 0


def test_cancel_scheduled_job_sets_cancelled(db_session, redis_client):
    from datetime import timedelta
    from app.core.time import utc_now

    request = JobCreateRequest(
        job_type="sum_numbers",
        payload={"numbers": [4, 5, 6]},
        priority=5,
        scheduled_at=utc_now() + timedelta(hours=1),
        max_retries=3,
    )

    job = create_job(
        db_session,
        request=request,
        idempotency_key="integration-cancel-scheduled-1",
    )

    assert job.status == JobStatus.SCHEDULED.value
    assert get_ready_queue_depth(redis_client) == 0

    cancelled_job = cancel_job(
        db_session,
        job_id=job.id,
    )

    assert cancelled_job.status == JobStatus.CANCELLED.value
    assert get_ready_queue_depth(redis_client) == 0


def test_cannot_cancel_completed_job(db_session, redis_client):
    request = JobCreateRequest(
        job_type="sum_numbers",
        payload={"numbers": [7, 8, 9]},
        priority=5,
        scheduled_at=None,
        max_retries=3,
    )

    job = create_job(
        db_session,
        request=request,
        idempotency_key="integration-cancel-completed-1",
    )

    job.status = JobStatus.COMPLETED.value
    db_session.commit()
    db_session.refresh(job)

    with pytest.raises(HTTPException) as exc_info:
        cancel_job(
            db_session,
            job_id=job.id,
        )

    assert exc_info.value.status_code == 409
    assert "Cannot cancel job with status COMPLETED" in exc_info.value.detail