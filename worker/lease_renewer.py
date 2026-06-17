import threading
from datetime import timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.database import SessionLocal
from app.core.enums import JobStatus, WorkerStatus
from app.core.logging import get_logger, log_extra
from app.core.redis import get_redis_client
from app.core.time import utc_now
from app.models.job import Job
from app.models.worker import Worker
from app.services.concurrency_service import (
    is_job_type_limited,
    refresh_concurrency_slot,
)

logger = get_logger("taskforge.worker.lease_renewer")


def renew_current_job_lease(
    db: Session,
    *,
    worker_id: str,
) -> None:
    settings = get_settings()
    now = utc_now()

    worker = db.get(Worker, worker_id)

    if worker is None:
        return

    if worker.status != WorkerStatus.BUSY.value:
        return

    if worker.current_job_id is None:
        return

    job_id = UUID(str(worker.current_job_id))

    query = (
        select(Job)
        .where(Job.id == job_id)
        .where(Job.status == JobStatus.RUNNING.value)
        .where(Job.leased_by == worker_id)
    )

    job = db.scalar(query)

    if job is None:
        return

    job.lease_expires_at = now + timedelta(seconds=settings.job_lease_seconds)
    job.updated_at = now

    db.commit()

    if not is_job_type_limited(job.job_type):
        return

    try:
        refresh_concurrency_slot(
            get_redis_client(),
            job_type=job.job_type,
            job_id=str(job.id),
            ttl_seconds=settings.job_lease_seconds,
        )
    except Exception as exc:
        logger.warning(
            "job_concurrency_slot_refresh_failed",
            extra=log_extra(
                service="worker",
                event="job_concurrency_slot_refresh_failed",
                worker_id=worker_id,
                job_id=str(job.id),
                job_type=job.job_type,
                error_message=str(exc),
            ),
        )


def lease_renewal_loop(
    *,
    worker_id: str,
    stop_event: threading.Event,
) -> None:
    settings = get_settings()

    renew_interval_seconds = max(1, settings.job_lease_seconds // 3)

    while not stop_event.is_set():
        with SessionLocal() as db:
            renew_current_job_lease(db, worker_id=worker_id)

        stop_event.wait(renew_interval_seconds)
