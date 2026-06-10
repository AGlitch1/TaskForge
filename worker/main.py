import os
import socket
import time
import uuid

from app.core.config import get_settings
from app.core.database import SessionLocal
from app.services.worker_service import register_worker
from worker.runner import run_one_job


def build_worker_id() -> str:
    hostname = socket.gethostname()
    short_uuid = str(uuid.uuid4())[:8]
    return f"worker-{hostname}-{short_uuid}"


def main() -> None:
    settings = get_settings()

    hostname = socket.gethostname()
    process_id = os.getpid()
    worker_id = os.getenv("WORKER_ID") or build_worker_id()

    print(f"[worker] starting worker_id={worker_id}")

    with SessionLocal() as db:
        register_worker(
            db,
            worker_id=worker_id,
            hostname=hostname,
            process_id=process_id,
        )

    print(f"[worker] registered worker_id={worker_id}")

    while True:
        with SessionLocal() as db:
            did_work = run_one_job(db, worker_id=worker_id)

        if not did_work:
            time.sleep(settings.worker_poll_interval_seconds)


if __name__ == "__main__":
    main()