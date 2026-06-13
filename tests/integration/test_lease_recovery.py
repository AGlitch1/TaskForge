from datetime import timedelta

from app.core.enums import AttemptStatus, JobStatus
from app.core.time import utc_now
from app.models.job import Job
from app.services.attempt_service import start_job_attempt
from app.services.queue_service import get_ready_queue_depth
from app.services.worker_service import register_worker
from scheduler.lease_recovery import recover_expired_leases


def test_expired_running_job_is_recovered_to_queue(db_session, redis_client):
    worker = register_worker(
        db_session,
        worker_id="test-worker-lease-1",
        hostname="test-host",
        process_id=999,
    )

    job = Job(
        job_type="sleep",
        payload={"duration_seconds": 60},
        status=JobStatus.RUNNING.value,
        priority=5,
        max_retries=3,
        retry_count=0,
        leased_by=worker.id,
        lease_expires_at=utc_now() - timedelta(seconds=1),
        started_at=utc_now() - timedelta(seconds=30),
        progress_percent=0,
        progress_message="Running.",
    )

    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)

    attempt = start_job_attempt(
        db_session,
        job_id=job.id,
        worker_id=worker.id,
    )
    db_session.commit()
    db_session.refresh(attempt)

    assert job.status == JobStatus.RUNNING.value
    assert attempt.status == AttemptStatus.RUNNING.value
    assert get_ready_queue_depth(redis_client) == 0

    recovered_count = recover_expired_leases(db_session)

    assert recovered_count == 1

    db_session.refresh(job)
    db_session.refresh(attempt)

    assert job.status == JobStatus.QUEUED.value
    assert job.leased_by is None
    assert job.lease_expires_at is None
    assert get_ready_queue_depth(redis_client) == 1

    assert attempt.status == AttemptStatus.FAILED.value
    assert attempt.error_message == "Job attempt abandoned because the worker lease expired."

    redis_job_ids = redis_client.zrange("ready_jobs", 0, -1)
    assert str(job.id) in redis_job_ids


def test_non_expired_running_job_is_not_recovered(db_session, redis_client):
    worker = register_worker(
        db_session,
        worker_id="test-worker-lease-2",
        hostname="test-host",
        process_id=1000,
    )

    job = Job(
        job_type="sleep",
        payload={"duration_seconds": 60},
        status=JobStatus.RUNNING.value,
        priority=5,
        max_retries=3,
        retry_count=0,
        leased_by=worker.id,
        lease_expires_at=utc_now() + timedelta(minutes=5),
        started_at=utc_now(),
        progress_percent=0,
        progress_message="Running.",
    )

    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)

    recovered_count = recover_expired_leases(db_session)

    assert recovered_count == 0

    db_session.refresh(job)

    assert job.status == JobStatus.RUNNING.value
    assert job.leased_by == worker.id
    assert get_ready_queue_depth(redis_client) == 0