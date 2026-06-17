import uuid
from datetime import datetime
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from uuid import UUID
from app.core.config import get_settings
from app.core.enums import JobEventType, JobStatus
from app.core.redis import get_redis_client
from app.core.time import utc_now
from app.models.job import Job
from app.models.job_attempt import JobAttempt
from app.models.job_event import JobEvent
from app.schemas.jobs import JobCreateRequest
from app.schemas.payloads import validate_payload_for_job_type
from app.services.attempt_service import list_attempts_for_job
from app.services.event_service import create_job_event
from app.services.fingerprint_service import create_request_fingerprint
from app.services.queue_service import enqueue_ready_job, remove_ready_job


def create_job(
    db: Session,
    *,
    request: JobCreateRequest,
    idempotency_key: str | None,
) -> Job:
    settings = get_settings()

    if (
        request.timeout_seconds is not None
        and request.timeout_seconds > settings.max_job_timeout_seconds
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "timeout_seconds must be less than or equal to "
                f"{settings.max_job_timeout_seconds}."
            ),
        )

    try:
        validated_payload = validate_payload_for_job_type(
            request.job_type,
            request.payload,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    fingerprint = None

    if idempotency_key:
        fingerprint = create_request_fingerprint(
            job_type=request.job_type,
            payload=validated_payload,
            priority=request.priority,
            scheduled_at=request.scheduled_at,
            max_retries=request.max_retries,
            timeout_seconds=request.timeout_seconds,
        )

        existing_job = db.scalar(
            select(Job).where(Job.idempotency_key == idempotency_key)
        )

        if existing_job:
            if existing_job.request_fingerprint != fingerprint:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Idempotency key was reused with a different request payload.",
                )

            return existing_job

    now = utc_now()

    initial_status = JobStatus.QUEUED
    if request.scheduled_at and request.scheduled_at > now:
        initial_status = JobStatus.SCHEDULED

    job = Job(
        job_type=request.job_type,
        payload=validated_payload,
        status=initial_status.value,
        priority=request.priority,
        scheduled_at=request.scheduled_at,
        max_retries=request.max_retries,
        timeout_seconds=request.timeout_seconds,
        retry_count=0,
        progress_percent=0,
        idempotency_key=idempotency_key,
        request_fingerprint=fingerprint,
    )

    db.add(job)
    db.flush()

    create_job_event(
        db,
        job_id=job.id,
        event_type=JobEventType.JOB_CREATED.value,
        old_status=None,
        new_status=JobStatus.CREATED.value,
        message="Job created through API.",
    )

    if initial_status == JobStatus.QUEUED:
        redis_client = get_redis_client()
        enqueue_ready_job(
            redis_client,
            job_id=job.id,
            priority=job.priority,
            queued_at=now,
        )

        create_job_event(
            db,
            job_id=job.id,
            event_type=JobEventType.JOB_QUEUED.value,
            old_status=JobStatus.CREATED.value,
            new_status=JobStatus.QUEUED.value,
            message="Job queued for immediate execution.",
        )
    else:
        create_job_event(
            db,
            job_id=job.id,
            event_type=JobEventType.JOB_SCHEDULED.value,
            old_status=JobStatus.CREATED.value,
            new_status=JobStatus.SCHEDULED.value,
            message="Job scheduled for future execution.",
        )

    db.commit()
    db.refresh(job)

    return job


def get_job_or_404(db: Session, job_id: uuid.UUID) -> Job:
    job = db.get(Job, job_id)

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found.",
        )

    return job


def list_jobs(
    db: Session,
    *,
    status_filter: JobStatus | None = None,
    job_type: str | None = None,
    priority: int | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[Job], int]:
    query = select(Job)
    count_query = select(func.count()).select_from(Job)

    conditions = []

    if status_filter is not None:
        conditions.append(Job.status == status_filter.value)

    if job_type is not None:
        conditions.append(Job.job_type == job_type)

    if priority is not None:
        conditions.append(Job.priority == priority)

    if created_after is not None:
        conditions.append(Job.created_at >= created_after)

    if created_before is not None:
        conditions.append(Job.created_at <= created_before)

    for condition in conditions:
        query = query.where(condition)
        count_query = count_query.where(condition)

    query = query.order_by(Job.created_at.desc()).limit(limit).offset(offset)

    jobs = list(db.scalars(query).all())
    total = db.scalar(count_query) or 0

    return jobs, total

def list_job_events(
    db: Session,
    *,
    job_id: uuid.UUID,
) -> list[JobEvent]:
    get_job_or_404(db, job_id)

    query = (
        select(JobEvent)
        .where(JobEvent.job_id == job_id)
        .order_by(JobEvent.created_at.asc())
    )

    return list(db.scalars(query).all())

def list_job_attempts(
    db: Session,
    *,
    job_id: uuid.UUID,
) -> list[JobAttempt]:
    get_job_or_404(db, job_id)
    return list_attempts_for_job(db, job_id=job_id)

def cancel_job(
    db: Session,
    *,
    job_id: UUID,
) -> Job:
    job = get_job_or_404(db, job_id)

    cancellable_statuses = {
        JobStatus.QUEUED.value,
        JobStatus.SCHEDULED.value,
        JobStatus.RETRYING.value,
    }

    if job.status == JobStatus.CANCELLED.value:
        return job

    if job.status not in cancellable_statuses:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel job with status {job.status}.",
        )

    old_status = job.status
    now = utc_now()

    if job.status == JobStatus.QUEUED.value:
        redis_client = get_redis_client()
        remove_ready_job(redis_client, job_id=job.id)

    job.status = JobStatus.CANCELLED.value
    job.cancelled_at = now
    job.updated_at = now
    job.progress_message = "Job was cancelled before execution."

    create_job_event(
        db,
        job_id=job.id,
        event_type=JobEventType.JOB_CANCELLED.value,
        old_status=old_status,
        new_status=JobStatus.CANCELLED.value,
        message="Job was cancelled before execution.",
        metadata={
            "cancelled_at": now.isoformat(),
        },
    )

    db.commit()
    db.refresh(job)

    return job

def replay_dead_job(
    db: Session,
    *,
    job_id: UUID,
) -> Job:
    original_job = get_job_or_404(db, job_id)

    if original_job.status != JobStatus.DEAD.value:
        raise HTTPException(
            status_code=409,
            detail=f"Only DEAD jobs can be replayed. Current status is {original_job.status}.",
        )

    now = utc_now()

    new_job = Job(
        job_type=original_job.job_type,
        payload=original_job.payload,
        priority=original_job.priority,
        max_retries=original_job.max_retries,
        timeout_seconds=original_job.timeout_seconds,
        retry_count=0,
        status=JobStatus.QUEUED.value,
        scheduled_at=None,
        next_run_at=None,
        result=None,
        error_message=None,
        progress_percent=0,
        progress_message="Replayed from dead job.",
        replayed_from_job_id=original_job.id,
    )

    db.add(new_job)
    db.flush()

    create_job_event(
        db,
        job_id=new_job.id,
        event_type=JobEventType.JOB_CREATED.value,
        old_status=None,
        new_status=JobStatus.CREATED.value,
        message="Job replayed from dead job.",
        metadata={
            "replayed_from_job_id": str(original_job.id),
        },
    )

    create_job_event(
        db,
        job_id=new_job.id,
        event_type=JobEventType.JOB_QUEUED.value,
        old_status=JobStatus.CREATED.value,
        new_status=JobStatus.QUEUED.value,
        message="Replayed job queued for execution.",
        metadata={
            "replayed_from_job_id": str(original_job.id),
        },
    )

    redis_client = get_redis_client()

    enqueue_ready_job(
        redis_client,
        job_id=new_job.id,
        priority=new_job.priority,
        queued_at=now,
    )

    db.commit()
    db.refresh(new_job)

    return new_job
