from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.enums import JobEventType, JobStatus
from app.core.redis import get_redis_client
from app.core.time import utc_now
from app.models.job import Job
from app.services.event_service import create_job_event
from app.services.queue_service import enqueue_ready_job


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
            },
        )

        recovered_count += 1

    db.commit()

    return recovered_count