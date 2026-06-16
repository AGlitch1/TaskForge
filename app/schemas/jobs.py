import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.core.enums import JobStatus


class JobCreateRequest(BaseModel):
    job_type: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any]
    priority: int = Field(default=5, ge=1, le=10)
    scheduled_at: datetime | None = None
    max_retries: int = Field(default=3, ge=0, le=10)
    timeout_seconds: int | None = Field(default=None, gt=0)


class JobResponse(BaseModel):
    id: uuid.UUID
    job_type: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int
    scheduled_at: datetime | None
    next_run_at: datetime | None
    max_retries: int
    timeout_seconds: int | None
    retry_count: int
    result: dict[str, Any] | None
    error_message: str | None
    progress_percent: int
    progress_message: str | None
    idempotency_key: str | None
    request_fingerprint: str | None
    replayed_from_job_id: uuid.UUID | None
    leased_by: str | None
    lease_expires_at: datetime | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    cancelled_at: datetime | None

    model_config = {
        "from_attributes": True
    }


class JobListResponse(BaseModel):
    items: list[JobResponse]
    limit: int
    offset: int
    total: int

class JobEventResponse(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    event_type: str
    old_status: str | None
    new_status: str | None
    message: str | None
    worker_id: str | None
    event_metadata: dict[str, Any] | None
    created_at: datetime

    model_config = {
        "from_attributes": True
    }

class JobAttemptResponse(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    worker_id: str | None
    attempt_number: int
    status: str
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int | None
    error_message: str | None
    created_at: datetime

    model_config = {
        "from_attributes": True
    }
