import uuid
from typing import Any

from sqlalchemy.orm import Session

from app.models.job_event import JobEvent


def create_job_event(
    db: Session,
    *,
    job_id: uuid.UUID,
    event_type: str,
    old_status: str | None = None,
    new_status: str | None = None,
    message: str | None = None,
    worker_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> JobEvent:
    event = JobEvent(
        job_id=job_id,
        event_type=event_type,
        old_status=old_status,
        new_status=new_status,
        message=message,
        worker_id=worker_id,
        event_metadata=metadata,
    )

    db.add(event)
    return event