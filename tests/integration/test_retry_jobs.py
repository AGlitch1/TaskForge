from datetime import timedelta

from app.core.enums import JobStatus
from app.core.time import utc_now
from app.models.job import Job
from app.services.queue_service import get_ready_queue_depth
from scheduler.retry_jobs import release_due_retrying_jobs


def test_due_retrying_job_is_released_to_queue(db_session, redis_client):
    job = Job(
        job_type="sum_numbers",
        payload={"numbers": [1, 2, 3]},
        status=JobStatus.RETRYING.value,
        priority=5,
        max_retries=3,
        retry_count=1,
        next_run_at=utc_now() - timedelta(seconds=1),
        progress_percent=0,
        progress_message="Waiting to retry.",
    )

    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)

    assert get_ready_queue_depth(redis_client) == 0

    released_count = release_due_retrying_jobs(db_session)

    assert released_count == 1

    db_session.refresh(job)

    assert job.status == JobStatus.QUEUED.value
    assert get_ready_queue_depth(redis_client) == 1

    redis_job_ids = redis_client.zrange("ready_jobs", 0, -1)
    assert str(job.id) in redis_job_ids


def test_future_retrying_job_is_not_released(db_session, redis_client):
    job = Job(
        job_type="sum_numbers",
        payload={"numbers": [4, 5, 6]},
        status=JobStatus.RETRYING.value,
        priority=5,
        max_retries=3,
        retry_count=1,
        next_run_at=utc_now() + timedelta(hours=1),
        progress_percent=0,
        progress_message="Waiting to retry.",
    )

    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)

    released_count = release_due_retrying_jobs(db_session)

    assert released_count == 0

    db_session.refresh(job)

    assert job.status == JobStatus.RETRYING.value
    assert get_ready_queue_depth(redis_client) == 0