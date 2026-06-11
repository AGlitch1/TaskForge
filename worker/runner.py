from datetime import timedelta
from uuid import UUID

from sqlalchemy import update
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.enums import JobEventType, JobStatus
from app.core.logging import get_logger, log_extra
from app.core.redis import get_redis_client
from app.core.time import utc_now
from app.models.job import Job
from app.models.job_attempt import JobAttempt
from app.services.attempt_service import (
    complete_job_attempt,
    fail_job_attempt,
    start_job_attempt,
)
from app.services.event_service import create_job_event
from app.services.queue_service import (
    pop_ready_job_candidate,
    remove_ready_job,
)
from app.services.worker_service import mark_worker_busy, mark_worker_idle
from app.schemas.payloads import validate_payload_for_job_type
from app.job_handlers.registry import get_job_handler


logger = get_logger("taskforge.worker.runner")


def calculate_retry_delay_seconds(retry_count: int) -> int:
    retry_delays = {
        1: 5,
        2: 30,
        3: 120,
    }

    return retry_delays.get(retry_count, 120)


def claim_job(
    db: Session,
    *,
    job_id: UUID,
    worker_id: str,
) -> Job | None:
    settings = get_settings()
    now = utc_now()

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
            progress_message="Job claimed by worker.",
        )
        .returning(Job)
    )

    result = db.execute(statement)
    claimed_job = result.scalar_one_or_none()

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
    attempt: JobAttempt,
    result: dict,
) -> Job:
    now = utc_now()

    complete_job_attempt(db, attempt=attempt)

    job.status = JobStatus.COMPLETED.value
    job.result = result
    job.error_message = None
    job.progress_percent = 100
    job.progress_message = "Job completed successfully."
    job.completed_at = now
    job.lease_expires_at = None
    job.leased_by = None
    job.updated_at = now

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

    old_status = job.status
    job.retry_count += 1
    job.error_message = error_message
    job.lease_expires_at = None
    job.leased_by = None
    job.updated_at = now

    create_job_event(
        db,
        job_id=job.id,
        event_type=JobEventType.JOB_ATTEMPT_FAILED.value,
        old_status=old_status,
        new_status=old_status,
        message="Job attempt failed.",
        worker_id=worker_id,
        metadata={
            "attempt_number": attempt.attempt_number,
            "error_message": error_message,
            "retry_count": job.retry_count,
            "max_retries": job.max_retries,
        },
    )

    if job.retry_count <= job.max_retries:
        delay_seconds = calculate_retry_delay_seconds(job.retry_count)
        next_run_at = now + timedelta(seconds=delay_seconds)

        job.status = JobStatus.RETRYING.value
        job.next_run_at = next_run_at
        job.progress_message = (
            f"Job failed and will retry in {delay_seconds} seconds."
        )

        create_job_event(
            db,
            job_id=job.id,
            event_type=JobEventType.JOB_RETRY_SCHEDULED.value,
            old_status=old_status,
            new_status=JobStatus.RETRYING.value,
            message="Job retry scheduled.",
            worker_id=worker_id,
            metadata={
                "retry_count": job.retry_count,
                "max_retries": job.max_retries,
                "next_run_at": next_run_at.isoformat(),
                "delay_seconds": delay_seconds,
            },
        )
    else:
        job.status = JobStatus.DEAD.value
        job.next_run_at = None
        job.progress_message = "Job exhausted all retries and is now dead."

        create_job_event(
            db,
            job_id=job.id,
            event_type=JobEventType.JOB_DEAD.value,
            old_status=old_status,
            new_status=JobStatus.DEAD.value,
            message="Job exhausted all retries and is now dead.",
            worker_id=worker_id,
            metadata={
                "retry_count": job.retry_count,
                "max_retries": job.max_retries,
                "error_message": error_message,
            },
        )

    mark_worker_idle(db, worker_id=worker_id)

    db.commit()
    db.refresh(job)

    return job


def run_one_job(
    db: Session,
    *,
    worker_id: str,
) -> bool:
    redis_client = get_redis_client()

    candidate_job_id = pop_ready_job_candidate(redis_client)

    if candidate_job_id is None:
        return False

    logger.info(
        "job_candidate_found",
        extra=log_extra(
            service="worker",
            event="job_candidate_found",
            worker_id=worker_id,
            job_id=candidate_job_id,
        ),
    )

    try:
        job_uuid = UUID(candidate_job_id)
    except ValueError:
        logger.warning(
            "invalid_job_id_in_redis",
            extra=log_extra(
                service="worker",
                event="invalid_job_id_in_redis",
                worker_id=worker_id,
                job_id=candidate_job_id,
            ),
        )
        return False

    job = claim_job(
        db,
        job_id=job_uuid,
        worker_id=worker_id,
    )

    if job is None:
        remove_ready_job(redis_client, job_id=job_uuid)

        logger.info(
            "job_claim_skipped",
            extra=log_extra(
                service="worker",
                event="job_claim_skipped",
                worker_id=worker_id,
                job_id=str(job_uuid),
                reason="Job was not QUEUED when worker tried to claim it.",
            ),
        )

        return False

    remove_ready_job(redis_client, job_id=job.id)

    logger.info(
        "job_claimed",
        extra=log_extra(
            service="worker",
            event="job_claimed",
            worker_id=worker_id,
            job_id=str(job.id),
            job_type=job.job_type,
        ),
    )

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

    logger.info(
        "job_started",
        extra=log_extra(
            service="worker",
            event="job_started",
            worker_id=worker_id,
            job_id=str(job.id),
            job_type=job.job_type,
            attempt_number=attempt.attempt_number,
        ),
    )

    try:
        validated_payload = validate_payload_for_job_type(
            job_type=job.job_type,
            payload=job.payload,
        )

        handler = get_job_handler(job.job_type)
        result = handler(validated_payload)

    except Exception as exc:
        logger.exception(
            "job_failed",
            extra=log_extra(
                service="worker",
                event="job_failed",
                worker_id=worker_id,
                job_id=str(job.id),
                job_type=job.job_type,
                attempt_number=attempt.attempt_number,
                error_message=str(exc),
            ),
        )

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
        attempt=attempt,
        result=result,
    )

    logger.info(
        "job_completed",
        extra=log_extra(
            service="worker",
            event="job_completed",
            worker_id=worker_id,
            job_id=str(job.id),
            job_type=job.job_type,
            attempt_number=attempt.attempt_number,
        ),
    )

    return True