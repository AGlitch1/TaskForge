from pydantic import BaseModel


class QueueStatsResponse(BaseModel):
    ready_queue_depth: int
    queued_jobs: int
    scheduled_jobs: int
    retrying_jobs: int
    running_jobs: int
    completed_jobs: int
    dead_jobs: int
    cancelled_jobs: int
    ready_job_ids_preview: list[str]