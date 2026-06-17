from redis import Redis

from app.core.config import get_settings
from app.core.time import utc_now


ACQUIRE_CONCURRENCY_SLOT_LUA = """
local key = KEYS[1]
local job_id = ARGV[1]
local limit = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local expires_at = tonumber(ARGV[4])

redis.call("ZREMRANGEBYSCORE", key, "-inf", now)

if redis.call("ZSCORE", key, job_id) then
    redis.call("ZADD", key, expires_at, job_id)
    return 1
end

if redis.call("ZCARD", key) < limit then
    redis.call("ZADD", key, expires_at, job_id)
    return 1
end

return 0
"""

REFRESH_CONCURRENCY_SLOT_LUA = """
local key = KEYS[1]
local job_id = ARGV[1]
local now = tonumber(ARGV[2])
local expires_at = tonumber(ARGV[3])

redis.call("ZREMRANGEBYSCORE", key, "-inf", now)

if redis.call("ZSCORE", key, job_id) then
    redis.call("ZADD", key, expires_at, job_id)
end

return 0
"""


def get_concurrency_limit(job_type: str) -> int | None:
    settings = get_settings()
    return settings.job_type_concurrency_limits.get(job_type)


def is_job_type_limited(job_type: str) -> bool:
    return get_concurrency_limit(job_type) is not None


def build_slot_key(job_type: str) -> str:
    return f"job_type_slots:{job_type}"


def try_acquire_concurrency_slot(
    redis_client: Redis,
    *,
    job_type: str,
    job_id: str,
    limit: int,
    ttl_seconds: int,
) -> bool:
    now = utc_now().timestamp()
    expires_at = now + ttl_seconds

    acquired = redis_client.eval(
        ACQUIRE_CONCURRENCY_SLOT_LUA,
        1,
        build_slot_key(job_type),
        str(job_id),
        limit,
        now,
        expires_at,
    )

    return bool(acquired)


def release_concurrency_slot(
    redis_client: Redis,
    *,
    job_type: str,
    job_id: str,
) -> None:
    redis_client.zrem(build_slot_key(job_type), str(job_id))


def refresh_concurrency_slot(
    redis_client: Redis,
    *,
    job_type: str,
    job_id: str,
    ttl_seconds: int,
) -> None:
    now = utc_now().timestamp()
    expires_at = now + ttl_seconds

    redis_client.eval(
        REFRESH_CONCURRENCY_SLOT_LUA,
        1,
        build_slot_key(job_type),
        str(job_id),
        now,
        expires_at,
    )
