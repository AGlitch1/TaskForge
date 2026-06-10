import threading

from app.core.config import get_settings
from app.core.database import SessionLocal
from app.services.worker_service import update_worker_heartbeat


def heartbeat_loop(
    *,
    worker_id: str,
    stop_event: threading.Event,
) -> None:
    settings = get_settings()

    while not stop_event.is_set():
        with SessionLocal() as db:
            update_worker_heartbeat(db, worker_id=worker_id)

        stop_event.wait(settings.worker_heartbeat_interval_seconds)