import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base
from app.core.enums import JobStatus
from app.core.time import utc_now


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    job_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default=JobStatus.CREATED.value,
        index=True,
    )

    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=5)

    scheduled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    next_run_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    progress_percent: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    progress_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    idempotency_key: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        unique=True,
        index=True,
    )
    request_fingerprint: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )

    replayed_from_job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="SET NULL"),
        nullable=True,
    )

    leased_by: Mapped[str | None] = mapped_column(
        String(255),
        ForeignKey("workers.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now,
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    cancelled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    attempts = relationship(
        "JobAttempt",
        back_populates="job",
        cascade="all, delete-orphan",
        foreign_keys="JobAttempt.job_id",
    )

    events = relationship(
        "JobEvent",
        back_populates="job",
        cascade="all, delete-orphan",
        foreign_keys="JobEvent.job_id",
    )

    replayed_from = relationship(
        "Job",
        remote_side=[id],
        foreign_keys=[replayed_from_job_id],
    )

    leasing_worker = relationship(
        "Worker",
        foreign_keys=[leased_by],
        back_populates="leased_jobs",
    )