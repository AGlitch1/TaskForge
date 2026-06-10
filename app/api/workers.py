from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.schemas.workers import WorkerResponse
from app.services.worker_service import list_workers


router = APIRouter(prefix="/workers", tags=["workers"])


@router.get(
    "",
    response_model=list[WorkerResponse],
)
def list_workers_endpoint(
    db: Session = Depends(get_db),
) -> list[WorkerResponse]:
    workers = list_workers(db)
    return [WorkerResponse.model_validate(worker) for worker in workers]