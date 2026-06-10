import uuid
from datetime import datetime
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.enums import JobEventType, JobStatus
from app.core.time import utc_now
from app.models.job import Job
from app.schemas.jobs import JobCreateRequest
from app.schemas.payloads import validate_payload_for_job_type
from app.services.event_service import create_job_event
from app.services.fingerprint_service import create_request_fingerprint
from app.models.job_event import JobEvent

def create_job(
    db: Session,
    *,
    request: JobCreateRequest,
    idempotency_key: str | None,
) -> Job:
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