import time

from app.core.config import get_settings
from app.core.database import SessionLocal
from app.core.logging import configure_logging, get_logger, log_extra
from scheduler.dead_workers import mark_stale_workers_dead
from scheduler.lease_recovery import recover_expired_leases
from scheduler.queue_reconciliation import reconcile_queued_jobs
from scheduler.retry_jobs import release_due_retrying_jobs
from scheduler.scheduled_jobs import release_due_scheduled_jobs


logger = get_logger("taskforge.scheduler")


def run_scheduler_cycle() -> None:
    with SessionLocal() as db:
        dead_workers_count = mark_stale_workers_dead(db)
        recovered_leases_count = recover_expired_leases(db)
        scheduled_count = release_due_scheduled_jobs(db)
        retrying_count = release_due_retrying_jobs(db)
        reconciled_count = reconcile_queued_jobs(db)

    if (
        dead_workers_count
        or recovered_leases_count
        or scheduled_count
        or retrying_count
        or reconciled_count
    ):
        logger.info(
            "scheduler_cycle_result",
            extra=log_extra(
                service="scheduler",
                event="scheduler_cycle_result",
                dead_workers=dead_workers_count,
                recovered_leases=recovered_leases_count,
                scheduled=scheduled_count,
                retrying=retrying_count,
                reconciled=reconciled_count,
            ),
        )


def main() -> None:
    configure_logging()

    settings = get_settings()

    logger.info(
        "scheduler_started",
        extra=log_extra(
            service="scheduler",
            event="scheduler_started",
            interval_seconds=settings.scheduler_interval_seconds,
        ),
    )

    while True:
        run_scheduler_cycle()
        time.sleep(settings.scheduler_interval_seconds)


if __name__ == "__main__":
    main()