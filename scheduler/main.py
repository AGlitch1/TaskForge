import time

from app.core.config import get_settings
from app.core.database import SessionLocal
from scheduler.retry_jobs import release_due_retrying_jobs
from scheduler.scheduled_jobs import release_due_scheduled_jobs


def run_scheduler_cycle() -> None:
    with SessionLocal() as db:
        scheduled_count = release_due_scheduled_jobs(db)
        retrying_count = release_due_retrying_jobs(db)

    if scheduled_count or retrying_count:
        print(
            "[scheduler] released jobs "
            f"scheduled={scheduled_count} retrying={retrying_count}"
        )


def main() -> None:
    settings = get_settings()

    print("[scheduler] starting scheduler")

    while True:
        run_scheduler_cycle()
        time.sleep(settings.scheduler_interval_seconds)


if __name__ == "__main__":
    main()