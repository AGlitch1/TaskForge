from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.enums import JobEventType, JobStatus
from app.core.redis import get_redis_client
from app.core.time import utc_now
from app.models.job import Job
from app.services.event_service import create_job_event
from app.services.queue_service import enqueue_ready_job


def release_due_scheduled_jobs(db: Session) -> int:
    """
    Move SCHEDULED jobs whose scheduled_at has arrived to QUEUED
    and enqueue them into Redis.
    """

    now = utc_now()
    redis_client = get_redis_client()

    query = (
        select(Job)
        .where(Job.status == JobStatus.SCHEDULED.value)
        .where(Job.scheduled_at.is_not(None))
        .where(Job.scheduled_at <= now)
        .order_by(Job.scheduled_at.asc())
        .limit(100)
    )

    jobs = list(db.scalars(query).all())

    released_count = 0

    for job in jobs:
        old_status = job.status

        job.status = JobStatus.QUEUED.value
        job.updated_at = now
        job.progress_message = "Scheduled time arrived and job has been queued."

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
            old_status=old_status,
            new_status=JobStatus.QUEUED.value,
            message="Scheduled job became due and was queued.",
            metadata={
                "scheduled_at": job.scheduled_at.isoformat()
                if job.scheduled_at
                else None,
            },
        )

        released_count += 1

    db.commit()

    return released_count