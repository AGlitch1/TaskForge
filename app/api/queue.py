from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.schemas.queue import QueueStatsResponse
from app.services.stats_service import get_queue_stats


router = APIRouter(prefix="/queue", tags=["queue"])


@router.get(
    "/stats",
    response_model=QueueStatsResponse,
)
def get_queue_stats_endpoint(
    db: Session = Depends(get_db),
) -> QueueStatsResponse:
    stats = get_queue_stats(db)
    return QueueStatsResponse(**stats)