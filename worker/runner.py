import uuid
from datetime import timedelta

from sqlalchemy import update
from sqlalchemy.orm import Session

from app.core.enums import JobEventType, JobStatus
from app.core.redis import get_redis_client
from app.core.time import utc_now
from app.job_handlers.registry import get_job_handler
from app.models.job import Job
from app.schemas.payloads import validate_payload_for_job_type
from app.services.event_service import create_job_event
from app.services.queue_service import (
    pop_ready_job_candidate,
    remove_ready_job,
    remove_ready_job_by_id_string,
)
from app.models.job_attempt import JobAttempt
from app.services.attempt_service import complete_job_attempt, start_job_attempt


def claim_job(
    db: Session,
    *,
    job_id: uuid.UUID,
    worker_id: str,
) -> Job | None:
    now = utc_now()

    statement = (
        update(Job)
        .where(Job.id == job_id)
        .where(Job.status == JobStatus.QUEUED.value)
        .values(
            status=JobStatus.RUNNING.value,
            leased_by=worker_id,
            lease_expires_at=now + timedelta(seconds=30),
            started_at=now,
            updated_at=now,
        )
        .returning(Job)
    )

    claimed_job = db.execute(statement).scalar_one_or_none()

    if claimed_job is None:
        db.rollback()
        return None

    create_job_event(
        db,
        job_id=claimed_job.id,
        event_type=JobEventType.JOB_CLAIMED.value,
        old_status=JobStatus.QUEUED.value,
        new_status=JobStatus.RUNNING.value,
        message=f"Job claimed by worker {worker_id}.",
        worker_id=worker_id,
    )

    create_job_event(
        db,
        job_id=claimed_job.id,
        event_type=JobEventType.JOB_STARTED.value,
        old_status=JobStatus.QUEUED.value,
        new_status=JobStatus.RUNNING.value,
        message="Job execution started.",
        worker_id=worker_id,
    )

    db.commit()
    db.refresh(claimed_job)

    return claimed_job

def complete_job(
    db: Session,
    *,
    job: Job,
    worker_id: str,
    result: dict,
    attempt: JobAttempt,
) -> Job:
    now = utc_now()

    complete_job_attempt(db, attempt=attempt)

    job.status = JobStatus.COMPLETED.value
    job.result = result
    job.progress_percent = 100
    job.progress_message = "Job completed successfully."
    job.completed_at = now
    job.updated_at = now
    job.leased_by = None
    job.lease_expires_at = None

    create_job_event(
        db,
        job_id=job.id,
        event_type=JobEventType.JOB_COMPLETED.value,
        old_status=JobStatus.RUNNING.value,
        new_status=JobStatus.COMPLETED.value,
        message="Job completed successfully.",
        worker_id=worker_id,
        metadata={
            "result": result,
            "attempt_number": attempt.attempt_number,
        },
    )

    db.commit()
    db.refresh(job)

    return job

def run_one_job(db: Session, *, worker_id: str) -> bool:
    redis_client = get_redis_client()

    candidate_job_id = pop_ready_job_candidate(redis_client)

    if candidate_job_id is None:
        return False

    try:
        job_uuid = uuid.UUID(candidate_job_id)
    except ValueError:
        remove_ready_job_by_id_string(redis_client, job_id=candidate_job_id)
        return False

    job = claim_job(
        db,
        job_id=job_uuid,
        worker_id=worker_id,
    )

    if job is None:
        remove_ready_job(redis_client, job_id=job_uuid)
        return False

    remove_ready_job(redis_client, job_id=job.id)

    attempt = start_job_attempt(
        db,
        job_id=job.id,
        worker_id=worker_id,
    )
    db.commit()
    db.refresh(attempt)

    validated_payload = validate_payload_for_job_type(
        job.job_type,
        job.payload,
    )

    handler = get_job_handler(job.job_type)
    result = handler(validated_payload)

    complete_job(
        db,
        job=job,
        worker_id=worker_id,
        result=result,
        attempt=attempt,
    )

    return True