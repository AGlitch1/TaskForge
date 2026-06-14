from fastapi import APIRouter, Depends, Response
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.redis import get_redis_client
from app.models.job import Job
from app.models.worker import Worker
from app.services.queue_service import get_ready_queue_depth

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/metrics")
def get_metrics(db: Session = Depends(get_db)):
    redis_client = get_redis_client()

    job_status_rows = db.execute(
        select(Job.status, func.count(Job.id)).group_by(Job.status)
    ).all()

    worker_status_rows = db.execute(
        select(Worker.status, func.count(Worker.id)).group_by(Worker.status)
    ).all()

    job_counts = {status: count for status, count in job_status_rows}
    worker_counts = {status: count for status, count in worker_status_rows}

    ready_queue_depth = get_ready_queue_depth(redis_client)

    lines = []

    all_job_statuses = [
        "QUEUED",
        "SCHEDULED",
        "RUNNING",
        "RETRYING",
        "COMPLETED",
        "DEAD",
        "CANCELLED",
    ]

    for status in all_job_statuses:
        metric_name = f"taskforge_jobs_{status.lower()}"
        lines.append(f"{metric_name} {job_counts.get(status, 0)}")

    all_worker_statuses = [
        "IDLE",
        "BUSY",
        "STOPPED",
        "DEAD",
    ]

    for status in all_worker_statuses:
        metric_name = f"taskforge_workers_{status.lower()}"
        lines.append(f"{metric_name} {worker_counts.get(status, 0)}")

    lines.append(f"taskforge_ready_queue_depth {ready_queue_depth}")

    metrics_text = "\n".join(lines) + "\n"

    return Response(
        content=metrics_text,
        media_type="text/plain",
    )