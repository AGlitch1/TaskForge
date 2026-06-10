from datetime import datetime

from pydantic import BaseModel


class WorkerResponse(BaseModel):
    id: str
    hostname: str
    process_id: int | None
    status: str
    current_job_id: str | None
    last_heartbeat_at: datetime | None
    started_at: datetime
    stopped_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = {
        "from_attributes": True
    }