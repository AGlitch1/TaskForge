from sqlalchemy.orm import Session

from app.core.enums import WorkerStatus
from app.core.time import utc_now
from app.models.worker import Worker


def register_worker(
    db: Session,
    *,
    worker_id: str,
    hostname: str,
    process_id: int | None,
) -> Worker:
    now = utc_now()

    worker = db.get(Worker, worker_id)

    if worker is None:
        worker = Worker(
            id=worker_id,
            hostname=hostname,
            process_id=process_id,
            status=WorkerStatus.IDLE.value,
            current_job_id=None,
            last_heartbeat_at=now,
            started_at=now,
            stopped_at=None,
        )
        db.add(worker)
    else:
        worker.hostname = hostname
        worker.process_id = process_id
        worker.status = WorkerStatus.IDLE.value
        worker.current_job_id = None
        worker.last_heartbeat_at = now
        worker.started_at = now
        worker.stopped_at = None
        worker.updated_at = now

    db.commit()
    db.refresh(worker)

    return worker