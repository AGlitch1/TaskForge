from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.enums import JobEventType, JobStatus
from app.core.redis import get_redis_client
from app.core.time import utc_now
from app.models.job import Job
from app.services.event_service import create_job_event
from app.services.queue_service import enqueue_ready_job, is_job_concurrency_deferred


def reconcile_queued_jobs(db: Session) -> int:
    """
    Repair Redis ready queue from PostgreSQL.

    PostgreSQL is the source of truth. If a job is QUEUED in Postgres
    but missing from Redis, put it back into Redis.
    """

    redis_client = get_redis_client()

    query = (
        select(Job)
        .where(Job.status == JobStatus.QUEUED.value)
        .order_by(Job.priority.desc(), Job.created_at.asc())
        .limit(100)
    )

    jobs = list(db.scalars(query).all())

    repaired_count = 0

    for job in jobs:
        job_id_string = str(job.id)

        if is_job_concurrency_deferred(redis_client, job_id=job.id):
            continue

        score = redis_client.zscore("ready_jobs", job_id_string)

        if score is not None:
            continue

        enqueue_ready_job(
            redis_client,
            job_id=job.id,
            priority=job.priority,
            queued_at=job.updated_at or job.created_at or utc_now(),
        )

        create_job_event(
            db,
            job_id=job.id,
            event_type=JobEventType.JOB_QUEUED.value,
            old_status=JobStatus.QUEUED.value,
            new_status=JobStatus.QUEUED.value,
            message="Queue reconciliation restored missing job to Redis.",
            metadata={
                "reconciled_at": utc_now().isoformat(),
                "reason": "QUEUED job was missing from Redis ready queue.",
            },
        )

        repaired_count += 1

    db.commit()

    return repaired_count
