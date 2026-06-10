import uuid

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.enums import AttemptStatus
from app.core.time import utc_now
from app.models.job_attempt import JobAttempt


def get_next_attempt_number(
    db: Session,
    *,
    job_id: uuid.UUID,
) -> int:
    max_attempt_number = db.scalar(
        select(func.max(JobAttempt.attempt_number)).where(JobAttempt.job_id == job_id)
    )

    return int(max_attempt_number or 0) + 1


def start_job_attempt(
    db: Session,
    *,
    job_id: uuid.UUID,
    worker_id: str,
) -> JobAttempt:
    attempt = JobAttempt(
        job_id=job_id,
        worker_id=worker_id,
        attempt_number=get_next_attempt_number(db, job_id=job_id),
        status=AttemptStatus.RUNNING.value,
        started_at=utc_now(),
    )

    db.add(attempt)
    db.flush()

    return attempt


def complete_job_attempt(
    db: Session,
    *,
    attempt: JobAttempt,
) -> JobAttempt:
    now = utc_now()

    attempt.status = AttemptStatus.COMPLETED.value
    attempt.finished_at = now

    duration = now - attempt.started_at
    attempt.duration_ms = int(duration.total_seconds() * 1000)

    db.flush()

    return attempt


def list_attempts_for_job(
    db: Session,
    *,
    job_id: uuid.UUID,
) -> list[JobAttempt]:
    query = (
        select(JobAttempt)
        .where(JobAttempt.job_id == job_id)
        .order_by(JobAttempt.attempt_number.asc())
    )

    return list(db.scalars(query).all())