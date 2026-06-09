import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, Header, Query, status
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.enums import JobStatus
from app.schemas.jobs import JobCreateRequest, JobListResponse, JobResponse
from app.services.job_service import create_job, get_job_or_404, list_jobs


router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_job_endpoint(
    request: JobCreateRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    db: Session = Depends(get_db),
) -> JobResponse:
    job = create_job(
        db,
        request=request,
        idempotency_key=idempotency_key,
    )

    return JobResponse.model_validate(job)


@router.get(
    "/{job_id}",
    response_model=JobResponse,
)
def get_job_endpoint(
    job_id: uuid.UUID,
    db: Session = Depends(get_db),
) -> JobResponse:
    job = get_job_or_404(db, job_id)
    return JobResponse.model_validate(job)


@router.get(
    "",
    response_model=JobListResponse,
)
def list_jobs_endpoint(
    status_filter: JobStatus | None = Query(default=None, alias="status"),
    job_type: str | None = None,
    priority: int | None = Query(default=None, ge=1, le=10),
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> JobListResponse:
    jobs, total = list_jobs(
        db,
        status_filter=status_filter,
        job_type=job_type,
        priority=priority,
        created_after=created_after,
        created_before=created_before,
        limit=limit,
        offset=offset,
    )

    return JobListResponse(
        items=[JobResponse.model_validate(job) for job in jobs],
        limit=limit,
        offset=offset,
        total=total,
    )