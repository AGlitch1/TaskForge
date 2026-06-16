import uuid
from collections.abc import Callable
from typing import Any

from sqlalchemy.orm import Session

from app.core.enums import AttemptStatus, JobEventType, JobStatus
from app.core.time import utc_now
from app.models.job import Job
from app.models.job_attempt import JobAttempt
from app.services.event_service import create_job_event


SessionFactory = Callable[[], Session]


class JobProgressError(RuntimeError):
    pass


class JobContext:
    def __init__(
        self,
        *,
        job_id: uuid.UUID,
        worker_id: str,
        attempt_id: uuid.UUID,
        attempt_number: int,
        session_factory: SessionFactory,
    ) -> None:
        self.job_id = job_id
        self.worker_id = worker_id
        self.attempt_id = attempt_id
        self.attempt_number = attempt_number
        self._session_factory = session_factory

    def update_progress(
        self,
        percent: int,
        message: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        if percent < 0 or percent > 100:
            raise JobProgressError("Progress percent must be between 0 and 100.")

        with self._session_factory() as db:
            try:
                job = db.get(Job, self.job_id)

                if job is None:
                    raise JobProgressError(f"Job {self.job_id} no longer exists.")

                if job.status != JobStatus.RUNNING.value:
                    raise JobProgressError(
                        f"Cannot update progress for job {self.job_id} "
                        f"because status is {job.status}, not RUNNING."
                    )

                if job.leased_by != self.worker_id:
                    raise JobProgressError(
                        f"Cannot update progress for job {self.job_id} "
                        "because worker ownership changed."
                    )

                attempt = db.get(JobAttempt, self.attempt_id)

                if attempt is None:
                    raise JobProgressError(
                        f"Attempt {self.attempt_id} no longer exists."
                    )

                if attempt.job_id != self.job_id:
                    raise JobProgressError("Attempt does not belong to this job.")

                if attempt.worker_id != self.worker_id:
                    raise JobProgressError(
                        "Cannot update progress because attempt worker changed."
                    )

                if attempt.attempt_number != self.attempt_number:
                    raise JobProgressError(
                        "Cannot update progress because attempt number changed."
                    )

                if attempt.status != AttemptStatus.RUNNING.value:
                    raise JobProgressError(
                        f"Cannot update progress because attempt status is "
                        f"{attempt.status}, not RUNNING."
                    )

                if percent < job.progress_percent:
                    raise JobProgressError(
                        "Progress percent cannot decrease within the same attempt."
                    )

                now = utc_now()

                job.progress_percent = percent

                if message is not None:
                    job.progress_message = message

                job.updated_at = now

                event_metadata: dict[str, Any] = {
                    "attempt_id": str(self.attempt_id),
                    "attempt_number": self.attempt_number,
                    "progress_percent": percent,
                }

                if message is not None:
                    event_metadata["progress_message"] = message

                if meta is not None:
                    event_metadata["handler_metadata"] = meta

                create_job_event(
                    db,
                    job_id=self.job_id,
                    event_type=JobEventType.JOB_PROGRESS_UPDATED.value,
                    old_status=JobStatus.RUNNING.value,
                    new_status=JobStatus.RUNNING.value,
                    message=message,
                    worker_id=self.worker_id,
                    metadata=event_metadata,
                )

                db.commit()

            except Exception:
                db.rollback()
                raise