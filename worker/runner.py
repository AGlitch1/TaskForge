import asyncio
import inspect
from collections.abc import Callable
from typing import Any

from datetime import timedelta
from uuid import UUID

from sqlalchemy import update
from sqlalchemy.orm import Session, sessionmaker

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
from app.services.concurrency_service import (
    get_concurrency_limit,
    release_concurrency_slot,
    try_acquire_concurrency_slot,
)
from app.services.event_service import create_job_event
from app.services.queue_service import (
    defer_ready_job_for_concurrency,
    pop_ready_job_candidate,
    release_due_concurrency_deferred_jobs,
    remove_ready_job,
    remove_ready_job_by_id_string,
)
from app.services.worker_service import mark_worker_busy, mark_worker_idle
from app.schemas.payloads import validate_payload_for_job_type
from app.job_handlers.registry import get_job_handler
from worker.context import JobContext

logger = get_logger("taskforge.worker.runner")


class JobTimeoutError(RuntimeError):
    pass


def calculate_retry_delay_seconds(retry_count: int) -> int:
    retry_delays = {
        1: 5,
        2: 30,
        3: 120,
    }

    return retry_delays.get(retry_count, 120)


def create_context_session_factory(db: Session) -> Callable[[], Session]:
    return sessionmaker(
        bind=db.get_bind(),
        autocommit=False,
        autoflush=False,
    )


def handler_accepts_context(handler: Callable[..., Any]) -> bool:
    signature = inspect.signature(handler)
    return len(signature.parameters) >= 2


def execute_handler(
    handler: Callable[..., Any],
    payload: dict[str, Any],
    context: JobContext,
    timeout_seconds: int,
) -> dict[str, Any]:
    if handler_accepts_context(handler):
        result = handler(payload, context)
    else:
        result = handler(payload)

    if inspect.isawaitable(result):
        try:
            result = asyncio.run(asyncio.wait_for(result, timeout=timeout_seconds))
        except TimeoutError as exc:
            raise JobTimeoutError(
                f"Job timed out after {timeout_seconds} seconds."
            ) from exc

    if not isinstance(result, dict):
        raise TypeError("Job handler must return a dictionary result.")

    return result


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
            progress_percent=0,
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


def defer_job_for_concurrency(
    db: Session,
    *,
    redis_client,
    job: Job,
    worker_id: str,
    limit: int,
) -> None:
    settings = get_settings()
    deferred_until = utc_now() + timedelta(seconds=settings.concurrency_defer_seconds)

    defer_ready_job_for_concurrency(
        redis_client,
        job_id=job.id,
        ready_at=deferred_until,
    )

    create_job_event(
        db,
        job_id=job.id,
        event_type=JobEventType.JOB_CONCURRENCY_DEFERRED.value,
        old_status=JobStatus.QUEUED.value,
        new_status=JobStatus.QUEUED.value,
        message="Job deferred because its job type is at concurrency capacity.",
        worker_id=worker_id,
        metadata={
            "job_type": job.job_type,
            "limit": limit,
            "defer_seconds": settings.concurrency_defer_seconds,
            "deferred_until": deferred_until.isoformat(),
        },
    )

    db.commit()


def record_concurrency_acquire_failed(
    db: Session,
    *,
    job: Job,
    worker_id: str,
    limit: int,
    error_message: str,
) -> None:
    create_job_event(
        db,
        job_id=job.id,
        event_type=JobEventType.JOB_CONCURRENCY_ACQUIRE_FAILED.value,
        old_status=JobStatus.QUEUED.value,
        new_status=JobStatus.QUEUED.value,
        message="Job concurrency slot acquisition failed.",
        worker_id=worker_id,
        metadata={
            "job_type": job.job_type,
            "limit": limit,
            "error_message": error_message,
        },
    )

    db.commit()


def release_job_concurrency_slot(
    *,
    redis_client,
    job: Job,
) -> None:
    try:
        release_concurrency_slot(
            redis_client,
            job_type=job.job_type,
            job_id=str(job.id),
        )
    except Exception as exc:
        logger.warning(
            "job_concurrency_slot_release_failed",
            extra=log_extra(
                service="worker",
                event="job_concurrency_slot_release_failed",
                job_id=str(job.id),
                job_type=job.job_type,
                error_message=str(exc),
            ),
        )


def run_one_job(
    db: Session,
    *,
    worker_id: str,
) -> bool:
    settings = get_settings()
    redis_client = get_redis_client()

    release_due_concurrency_deferred_jobs(db, redis_client=redis_client)

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
        remove_ready_job_by_id_string(redis_client, job_id=candidate_job_id)
        return False

    queued_job = db.get(Job, job_uuid)

    if queued_job is None or queued_job.status != JobStatus.QUEUED.value:
        remove_ready_job(redis_client, job_id=job_uuid)

        logger.info(
            "job_candidate_skipped",
            extra=log_extra(
                service="worker",
                event="job_candidate_skipped",
                worker_id=worker_id,
                job_id=str(job_uuid),
                reason="Job does not exist or is not QUEUED.",
            ),
        )

        return False

    concurrency_limit = get_concurrency_limit(queued_job.job_type)
    slot_acquired = False

    if concurrency_limit is not None:
        try:
            slot_acquired = try_acquire_concurrency_slot(
                redis_client,
                job_type=queued_job.job_type,
                job_id=str(queued_job.id),
                limit=concurrency_limit,
                ttl_seconds=settings.job_lease_seconds,
            )
        except Exception as exc:
            logger.exception(
                "job_concurrency_slot_acquire_failed",
                extra=log_extra(
                    service="worker",
                    event="job_concurrency_slot_acquire_failed",
                    worker_id=worker_id,
                    job_id=str(queued_job.id),
                    job_type=queued_job.job_type,
                    error_message=str(exc),
                ),
            )

            record_concurrency_acquire_failed(
                db,
                job=queued_job,
                worker_id=worker_id,
                limit=concurrency_limit,
                error_message=str(exc),
            )

            return False

        if not slot_acquired:
            defer_job_for_concurrency(
                db,
                redis_client=redis_client,
                job=queued_job,
                worker_id=worker_id,
                limit=concurrency_limit,
            )

            logger.info(
                "job_concurrency_deferred",
                extra=log_extra(
                    service="worker",
                    event="job_concurrency_deferred",
                    worker_id=worker_id,
                    job_id=str(queued_job.id),
                    job_type=queued_job.job_type,
                    concurrency_limit=concurrency_limit,
                ),
            )

            return False

    job = claim_job(
        db,
        job_id=job_uuid,
        worker_id=worker_id,
    )

    if job is None:
        if slot_acquired:
            release_job_concurrency_slot(
                redis_client=redis_client,
                job=queued_job,
            )

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

    try:
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
            timeout_seconds = (
                job.timeout_seconds
                if job.timeout_seconds is not None
                else settings.default_job_timeout_seconds
            )

            context = JobContext(
                job_id=job.id,
                worker_id=worker_id,
                attempt_id=attempt.id,
                attempt_number=attempt.attempt_number,
                session_factory=create_context_session_factory(db),
            )

            result = execute_handler(
                handler=handler,
                payload=validated_payload,
                context=context,
                timeout_seconds=timeout_seconds,
            )

        except JobTimeoutError as exc:
            error_message = str(exc)

            logger.exception(
                "job_timed_out",
                extra=log_extra(
                    service="worker",
                    event="job_timed_out",
                    worker_id=worker_id,
                    job_id=str(job.id),
                    job_type=job.job_type,
                    attempt_number=attempt.attempt_number,
                    error_message=error_message,
                ),
            )

            create_job_event(
                db,
                job_id=job.id,
                event_type=JobEventType.JOB_TIMED_OUT.value,
                old_status=JobStatus.RUNNING.value,
                new_status=JobStatus.RUNNING.value,
                message=error_message,
                worker_id=worker_id,
                metadata={
                    "attempt_id": str(attempt.id),
                    "attempt_number": attempt.attempt_number,
                    "timeout_seconds": timeout_seconds,
                },
            )

            mark_job_failed_or_retrying(
                db,
                job=job,
                worker_id=worker_id,
                attempt=attempt,
                error_message=error_message,
            )

            return True

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
    finally:
        if slot_acquired:
            release_job_concurrency_slot(
                redis_client=redis_client,
                job=job,
            )
