from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "TaskForge"
    app_env: str = "local"
    debug: bool = True

    database_url: str
    redis_url: str

    test_database_url: str | None = None
    test_redis_url: str | None = None

    worker_poll_interval_seconds: float = 1.0
    worker_heartbeat_interval_seconds: int = 5
    dead_worker_timeout_seconds: int = 30

    job_lease_seconds: int = 30
    lease_renew_interval_seconds: int = 10

    scheduler_interval_seconds: int = 2
    scheduler_lock_ttl_seconds: int = 10

    max_retries_default: int = 3
    default_job_timeout_seconds: int = 300
    max_job_timeout_seconds: int = 86_400
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
