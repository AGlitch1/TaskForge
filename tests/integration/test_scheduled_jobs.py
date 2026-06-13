from datetime import timedelta

from app.core.enums import JobStatus
from app.core.time import utc_now
from app.schemas.jobs import JobCreateRequest
from app.services.job_service import create_job
from app.services.queue_service import get_ready_queue_depth
from scheduler.scheduled_jobs import release_due_scheduled_jobs

def test_due_scheduled_job_is_released_to_queue(db_session, redis_client):
    scheduled_at = utc_now() + timedelta(hours=1)

    request = JobCreateRequest(
        job_type="sum_numbers",
        payload={"numbers": [1, 2, 3]},
        priority=5,
        scheduled_at=scheduled_at,
        max_retries=3,
    )

    job = create_job(
        db_session,
        request=request,
        idempotency_key="integration-scheduled-1",
    )

    assert job.status == JobStatus.SCHEDULED.value
    assert get_ready_queue_depth(redis_client) == 0

    job.scheduled_at = utc_now() - timedelta(seconds=1)
    db_session.commit()
    db_session.refresh(job)

    released_count = release_due_scheduled_jobs(db_session)

    assert released_count == 1

    db_session.refresh(job)

    assert job.status == JobStatus.QUEUED.value
    assert get_ready_queue_depth(redis_client) == 1

    redis_job_ids = redis_client.zrange("ready_jobs", 0, -1)
    assert str(job.id) in redis_job_ids

def test_future_scheduled_job_is_not_released(db_session, redis_client):
    scheduled_at = utc_now() + timedelta(hours=1)

    request = JobCreateRequest(
        job_type="sum_numbers",
        payload={"numbers": [4, 5, 6]},
        priority=5,
        scheduled_at=scheduled_at,
        max_retries=3,
    )

    job = create_job(
        db_session,
        request=request,
        idempotency_key="integration-scheduled-future-1",
    )

    assert job.status == JobStatus.SCHEDULED.value

    released_count = release_due_scheduled_jobs(db_session)

    assert released_count == 0

    db_session.refresh(job)

    assert job.status == JobStatus.SCHEDULED.value
    assert get_ready_queue_depth(redis_client) == 0