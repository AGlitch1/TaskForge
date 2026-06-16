import pytest
from fastapi import HTTPException

from app.core.enums import JobStatus
from app.models.job import Job
from app.services.job_service import replay_dead_job
from app.services.queue_service import get_ready_queue_depth


def test_replay_dead_job_creates_new_queued_job(db_session, redis_client):
    original_job = Job(
        job_type="sum_numbers",
        payload={"numbers": [1, 2, 3]},
        status=JobStatus.DEAD.value,
        priority=5,
        max_retries=3,
        timeout_seconds=120,
        retry_count=3,
        error_message="Original job failed.",
        progress_percent=0,
        progress_message="Dead job.",
    )

    db_session.add(original_job)
    db_session.commit()
    db_session.refresh(original_job)

    assert get_ready_queue_depth(redis_client) == 0

    replayed_job = replay_dead_job(
        db_session,
        job_id=original_job.id,
    )

    assert replayed_job.id != original_job.id
    assert replayed_job.status == JobStatus.QUEUED.value
    assert replayed_job.job_type == original_job.job_type
    assert replayed_job.payload == original_job.payload
    assert replayed_job.priority == original_job.priority
    assert replayed_job.max_retries == original_job.max_retries
    assert replayed_job.timeout_seconds == original_job.timeout_seconds
    assert replayed_job.retry_count == 0
    assert replayed_job.replayed_from_job_id == original_job.id
    assert replayed_job.error_message is None
    assert replayed_job.result is None

    assert get_ready_queue_depth(redis_client) == 1

    redis_job_ids = redis_client.zrange("ready_jobs", 0, -1)
    assert str(replayed_job.id) in redis_job_ids


def test_cannot_replay_non_dead_job(db_session, redis_client):
    job = Job(
        job_type="sum_numbers",
        payload={"numbers": [1, 2, 3]},
        status=JobStatus.COMPLETED.value,
        priority=5,
        max_retries=3,
        retry_count=0,
        result={"sum": 6.0, "count": 3},
        progress_percent=100,
        progress_message="Completed.",
    )

    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)

    with pytest.raises(HTTPException) as exc_info:
        replay_dead_job(
            db_session,
            job_id=job.id,
        )

    assert exc_info.value.status_code == 409
    assert "Only DEAD jobs can be replayed" in exc_info.value.detail
