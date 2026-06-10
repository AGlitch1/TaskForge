import uuid
from datetime import datetime

from redis import Redis

READY_JOBS_KEY = "ready_jobs"


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