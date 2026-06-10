from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.enums import JobEventType, JobStatus
from app.core.redis import get_redis_client
from app.core.time import utc_now
from app.models.job import Job
from app.services.event_service import create_job_event
from app.services.queue_service import enqueue_ready_job


def release_due_retrying_jobs(db: Session) -> int:
    """
    Move RETRYING jobs whose next_run_at has arrived back to QUEUED
    and enqueue them into Redis.
    """

    now = utc_now()
    redis_client = get_redis_client()

    query = (
        select(Job)
        .where(Job.status == JobStatus.RETRYING.value)
        .where(Job.next_run_at.is_not(None))
        .where(Job.next_run_at <= now)
        .order_by(Job.next_run_at.asc())
        .limit(100)
    )

    jobs = list(db.scalars(query).all())

    released_count = 0

    for job in jobs:
        old_status = job.status

        job.status = JobStatus.QUEUED.value
        job.updated_at = now
        job.progress_message = "Retry is ready and job has been requeued."

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
            message="Retry became due and job was requeued.",
            metadata={
                "retry_count": job.retry_count,
                "next_run_at": job.next_run_at.isoformat()
                if job.next_run_at
                else None,
            },
        )

        released_count += 1

    db.commit()

    return released_count