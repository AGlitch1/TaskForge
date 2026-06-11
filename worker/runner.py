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
from app.services.attempt_service import (
    complete_job_attempt,
    fail_job_attempt,
    start_job_attempt,
)
from app.services.worker_service import mark_worker_busy, mark_worker_idle
from app.core.config import get_settings

def calculate_retry_delay_seconds(retry_count: int) -> int:
    """
    retry_count is the number of failures after this attempt.

    retry_count = 1 -> 5 seconds
    retry_count = 2 -> 30 seconds
    retry_count = 3 -> 120 seconds
    """
    retry_delays = {
        1: 5,
        2: 30,
        3: 120,
    }

    return retry_delays.get(retry_count, 120)

def claim_job(
    db: Session,
    *,
    job_id: uuid.UUID,
    worker_id: str,
) -> Job | None:
    now = utc_now()
    
    settings = get_settings()

    statement = (
        update(Job)
        .where(Job.id == job_id)
        .where(Job.status == JobStatus.QUEUED.value)
        .values(
            status=JobStatus.RUNNING.value,
            leased_by=worker_id,
            lease_expires_at=now + timedelta(seconds=settings.job_lease_seconds),
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
    
    mark_worker_idle(db, worker_id=worker_id)

    db.commit()
    db.refresh(job)

    return job

def mark_job_failed_or_retrying(
    db: Session,
    *,
    job: Job,
    worker_id: str,
    attempt: JobAttempt,
    error_message: str,
) -> Job:
    now = utc_now()

    fail_job_attempt(
        db,
        attempt=attempt,
        error_message=error_message,
    )

    job.retry_count += 1
    job.error_message = error_message
    job.leased_by = None
    job.lease_expires_at = None
    job.updated_at = now

    if job.retry_count <= job.max_retries:
        delay_seconds = calculate_retry_delay_seconds(job.retry_count)
        next_run_at = now + timedelta(seconds=delay_seconds)

        job.status = JobStatus.RETRYING.value
        job.next_run_at = next_run_at
        job.progress_message = (
            f"Job failed. Retry {job.retry_count}/{job.max_retries} "
            f"scheduled in {delay_seconds} seconds."
        )

        create_job_event(
            db,
            job_id=job.id,
            event_type=JobEventType.JOB_ATTEMPT_FAILED.value,
            old_status=JobStatus.RUNNING.value,
            new_status=JobStatus.RETRYING.value,
            message=error_message,
            worker_id=worker_id,
            metadata={
                "attempt_number": attempt.attempt_number,
                "retry_count": job.retry_count,
            },
        )

        create_job_event(
            db,
            job_id=job.id,
            event_type=JobEventType.JOB_RETRY_SCHEDULED.value,
            old_status=JobStatus.RUNNING.value,
            new_status=JobStatus.RETRYING.value,
            message=f"Next retry scheduled at {next_run_at.isoformat()}.",
            worker_id=worker_id,
            metadata={
                "attempt_number": attempt.attempt_number,
                "retry_count": job.retry_count,
                "next_run_at": next_run_at.isoformat(),
                "delay_seconds": delay_seconds,
            },
        )

    else:
        job.status = JobStatus.DEAD.value
        job.next_run_at = None
        job.progress_message = "Job exhausted all retries and was marked DEAD."

        create_job_event(
            db,
            job_id=job.id,
            event_type=JobEventType.JOB_ATTEMPT_FAILED.value,
            old_status=JobStatus.RUNNING.value,
            new_status=JobStatus.DEAD.value,
            message=error_message,
            worker_id=worker_id,
            metadata={
                "attempt_number": attempt.attempt_number,
                "retry_count": job.retry_count,
            },
        )

        create_job_event(
            db,
            job_id=job.id,
            event_type=JobEventType.JOB_DEAD.value,
            old_status=JobStatus.RUNNING.value,
            new_status=JobStatus.DEAD.value,
            message="Job exhausted all retries.",
            worker_id=worker_id,
            metadata={
                "attempt_number": attempt.attempt_number,
                "retry_count": job.retry_count,
                "max_retries": job.max_retries,
            },
        )

    mark_worker_idle(db, worker_id=worker_id)
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

    mark_worker_busy(
        db,
        worker_id=worker_id,
        job_id=str(job.id),
    )
    db.commit()

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

    try:
        result = handler(validated_payload)
    except Exception as exc:
        mark_job_failed_or_retrying(
            db,
            job=job,
            worker_id=worker_id,
            attempt=attempt,
            error_message=str(exc),
        )
        return True

    complete_job(
        db,
        job=job,
        worker_id=worker_id,
        result=result,
        attempt=attempt,
    )

    return True