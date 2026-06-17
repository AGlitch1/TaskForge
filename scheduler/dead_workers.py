from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.enums import WorkerStatus
from app.core.time import utc_now
from app.models.worker import Worker


def mark_stale_workers_dead(db: Session) -> int:
    """
    Mark workers as DEAD if their heartbeat is too old.

    This only updates worker state. Expired RUNNING job recovery is handled
    separately by lease recovery.
    """

    settings = get_settings()
    now = utc_now()
    cutoff = now - timedelta(seconds=settings.dead_worker_timeout_seconds)

    query = (
        select(Worker)
        .where(Worker.status.in_([WorkerStatus.IDLE.value, WorkerStatus.BUSY.value]))
        .where(Worker.last_heartbeat_at < cutoff)
    )

    workers = list(db.scalars(query).all())

    for worker in workers:
        worker.status = WorkerStatus.DEAD.value
        worker.stopped_at = now
        worker.updated_at = now

    db.commit()

    return len(workers)
