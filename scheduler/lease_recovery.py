from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.enums import AttemptStatus, JobEventType, JobStatus
from app.core.redis import get_redis_client
from app.core.time import utc_now
from app.models.job import Job
from app.models.job_attempt import JobAttempt
from app.services.event_service import create_job_event
from app.services.queue_service import enqueue_ready_job


def fail_running_attempt_for_recovered_job(
    db: Session,
    *,
    job: Job,
    error_message: str,
) -> None:
    """
    Mark the latest RUNNING attempt for this job as FAILED.

    This happens when a worker crashes after starting an attempt,
    and the scheduler later recovers the expired job lease.
    """

    now = utc_now()

    query = (
        select(JobAttempt)
        .where(JobAttempt.job_id == job.id)
        .where(JobAttempt.status == AttemptStatus.RUNNING.value)
        .order_by(JobAttempt.attempt_number.desc())
        .limit(1)
    )

    attempt = db.scalar(query)

    if attempt is None:
        return

    attempt.status = AttemptStatus.FAILED.value
    attempt.finished_at = now
    attempt.error_message = error_message

    if attempt.started_at is not None:
        duration = now - attempt.started_at
        attempt.duration_ms = int(duration.total_seconds() * 1000)


def recover_expired_leases(db: Session) -> int:
    """
    Recover RUNNING jobs whose leases have expired.

    This handles the case where a worker claimed a job and then crashed.
    The job is moved back to QUEUED and pushed into Redis so another worker
    can retry it.
    """

    now = utc_now()
    redis_client = get_redis_client()

    query = (
        select(Job)
        .where(Job.status == JobStatus.RUNNING.value)
        .where(Job.lease_expires_at.is_not(None))
        .where(Job.lease_expires_at <= now)
        .order_by(Job.lease_expires_at.asc())
        .limit(100)
    )

    jobs = list(db.scalars(query).all())

    recovered_count = 0

    for job in jobs:
        old_worker_id = job.leased_by
        old_lease_expires_at = job.lease_expires_at

        fail_running_attempt_for_recovered_job(
            db,
            job=job,
            error_message=(
                "Job attempt abandoned because the worker lease expired."
            ),
        )

        job.status = JobStatus.QUEUED.value
        job.leased_by = None
        job.lease_expires_at = None
        job.started_at = None
        job.updated_at = now
        job.progress_message = "Job lease expired and job was requeued."

        enqueue_ready_job(
            redis_client,
            job_id=job.id,
            priority=job.priority,
            queued_at=now,
        )

        create_job_event(
            db,
            job_id=job.id,
            event_type=JobEventType.JOB_QUEUED.value,
            old_status=JobStatus.RUNNING.value,
            new_status=JobStatus.QUEUED.value,
            message="Expired lease recovered and job was requeued.",
            worker_id=old_worker_id,
            metadata={
                "old_worker_id": old_worker_id,
                "old_lease_expires_at": old_lease_expires_at.isoformat()
                if old_lease_expires_at
                else None,
                "recovered_at": now.isoformat(),
                "abandoned_attempt_marked_failed": True,
            },
        )

        recovered_count += 1

    db.commit()

    return recovered_count