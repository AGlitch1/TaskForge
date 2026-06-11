import time

from app.core.config import get_settings
from app.core.database import SessionLocal
from scheduler.dead_workers import mark_stale_workers_dead
from scheduler.lease_recovery import recover_expired_leases
from scheduler.retry_jobs import release_due_retrying_jobs
from scheduler.scheduled_jobs import release_due_scheduled_jobs


def run_scheduler_cycle() -> None:
    with SessionLocal() as db:
        dead_workers_count = mark_stale_workers_dead(db)
        recovered_leases_count = recover_expired_leases(db)
        scheduled_count = release_due_scheduled_jobs(db)
        retrying_count = release_due_retrying_jobs(db)

    if dead_workers_count or recovered_leases_count or scheduled_count or retrying_count:
        print(
            "[scheduler] cycle result "
            f"dead_workers={dead_workers_count} "
            f"recovered_leases={recovered_leases_count} "
            f"scheduled={scheduled_count} "
            f"retrying={retrying_count}"
        )


def main() -> None:
    settings = get_settings()

    print("[scheduler] starting scheduler")

    while True:
        run_scheduler_cycle()
        time.sleep(settings.scheduler_interval_seconds)


if __name__ == "__main__":
    main()