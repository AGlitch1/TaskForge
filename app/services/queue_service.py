import uuid
from datetime import datetime

from redis import Redis
from sqlalchemy.orm import Session

from app.core.enums import JobStatus
from app.core.time import utc_now
from app.models.job import Job

READY_JOBS_KEY = "ready_jobs"
CONCURRENCY_DEFERRED_JOBS_KEY = "concurrency_deferred_jobs"


def calculate_ready_job_score(
    *,
    queued_at: datetime,
    priority: int,
) -> float:
    """
    Lower Redis sorted-set score means the job is picked earlier.

    We use:
        score = queued_at_timestamp - priority_boost

    Higher priority gets a larger subtraction, so it appears earlier.
    """

    priority_boost = priority * 1_000_000
    return queued_at.timestamp() - priority_boost


def enqueue_ready_job(
    redis_client: Redis,
    *,
    job_id: uuid.UUID,
    priority: int,
    queued_at: datetime,
) -> None:
    score = calculate_ready_job_score(
        queued_at=queued_at,
        priority=priority,
    )

    redis_client.zadd(
        READY_JOBS_KEY,
        {str(job_id): score},
    )


def remove_ready_job(
    redis_client: Redis,
    *,
    job_id: uuid.UUID,
) -> None:
    redis_client.zrem(READY_JOBS_KEY, str(job_id))


def get_ready_queue_depth(redis_client: Redis) -> int:
    return int(redis_client.zcard(READY_JOBS_KEY))


def peek_ready_jobs(
    redis_client: Redis,
    *,
    limit: int = 10,
) -> list[str]:
    return list(redis_client.zrange(READY_JOBS_KEY, 0, limit - 1))


def clear_ready_queue(redis_client: Redis) -> None:
    redis_client.delete(READY_JOBS_KEY)


def pop_ready_job_candidate(redis_client: Redis) -> str | None:
    """
    Get the highest-priority ready job candidate.

    This only reads the candidate from Redis.
    PostgreSQL still decides whether the claim is valid.
    """

    job_ids = redis_client.zrange(READY_JOBS_KEY, 0, 0)

    if not job_ids:
        return None

    return job_ids[0]


def remove_ready_job_by_id_string(
    redis_client: Redis,
    *,
    job_id: str,
) -> None:
    redis_client.zrem(READY_JOBS_KEY, job_id)


def defer_ready_job_for_concurrency(
    redis_client: Redis,
    *,
    job_id: uuid.UUID,
    ready_at: datetime,
) -> None:
    job_id_string = str(job_id)

    redis_client.zrem(READY_JOBS_KEY, job_id_string)
    redis_client.zadd(
        CONCURRENCY_DEFERRED_JOBS_KEY,
        {job_id_string: ready_at.timestamp()},
    )


def is_job_concurrency_deferred(
    redis_client: Redis,
    *,
    job_id: uuid.UUID,
) -> bool:
    return redis_client.zscore(CONCURRENCY_DEFERRED_JOBS_KEY, str(job_id)) is not None


def release_due_concurrency_deferred_jobs(
    db: Session,
    *,
    redis_client: Redis,
    limit: int = 100,
) -> int:
    now = utc_now()

    job_ids = redis_client.zrangebyscore(
        CONCURRENCY_DEFERRED_JOBS_KEY,
        "-inf",
        now.timestamp(),
        start=0,
        num=limit,
    )

    released_count = 0

    for job_id_string in job_ids:
        redis_client.zrem(CONCURRENCY_DEFERRED_JOBS_KEY, job_id_string)

        try:
            job_id = uuid.UUID(job_id_string)
        except ValueError:
            continue

        job = db.get(Job, job_id)

        if job is None or job.status != JobStatus.QUEUED.value:
            continue

        enqueue_ready_job(
            redis_client,
            job_id=job.id,
            priority=job.priority,
            queued_at=now,
        )

        released_count += 1

    db.commit()

    return released_count
