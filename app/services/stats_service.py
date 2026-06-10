from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.enums import JobStatus
from app.core.redis import get_redis_client
from app.models.job import Job
from app.services.queue_service import get_ready_queue_depth, peek_ready_jobs


def _count_jobs_by_status(db: Session, status: JobStatus) -> int:
    count = db.scalar(
        select(func.count()).select_from(Job).where(Job.status == status.value)
    )
    return int(count or 0)


def get_queue_stats(db: Session) -> dict:
    redis_client = get_redis_client()

    return {
        "ready_queue_depth": get_ready_queue_depth(redis_client),
        "queued_jobs": _count_jobs_by_status(db, JobStatus.QUEUED),
        "scheduled_jobs": _count_jobs_by_status(db, JobStatus.SCHEDULED),
        "retrying_jobs": _count_jobs_by_status(db, JobStatus.RETRYING),
        "running_jobs": _count_jobs_by_status(db, JobStatus.RUNNING),
        "completed_jobs": _count_jobs_by_status(db, JobStatus.COMPLETED),
        "dead_jobs": _count_jobs_by_status(db, JobStatus.DEAD),
        "cancelled_jobs": _count_jobs_by_status(db, JobStatus.CANCELLED),
        "ready_job_ids_preview": peek_ready_jobs(redis_client, limit=10),
    }