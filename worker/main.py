import os
import signal
import socket
import threading
import time
import uuid

from app.core.config import get_settings
from app.core.database import SessionLocal
from app.services.worker_service import mark_worker_stopped, register_worker
from worker.heartbeat import heartbeat_loop
from worker.lease_renewer import lease_renewal_loop
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

    stop_event = threading.Event()
    shutdown_requested = threading.Event()

    def handle_shutdown_signal(signum, frame) -> None:
        if not shutdown_requested.is_set():
            print(
                "[worker] shutdown requested; "
                "will finish current job and stop polling"
            )
            shutdown_requested.set()
        else:
            print("[worker] shutdown already requested; waiting for current job")

    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

    print(f"[worker] starting worker_id={worker_id}")

    with SessionLocal() as db:
        register_worker(
            db,
            worker_id=worker_id,
            hostname=hostname,
            process_id=process_id,
        )

    print(f"[worker] registered worker_id={worker_id}")

    heartbeat_thread = threading.Thread(
        target=heartbeat_loop,
        kwargs={
            "worker_id": worker_id,
            "stop_event": stop_event,
        },
        daemon=True,
    )
    heartbeat_thread.start()

    lease_renewal_thread = threading.Thread(
        target=lease_renewal_loop,
        kwargs={
            "worker_id": worker_id,
            "stop_event": stop_event,
        },
        daemon=True,
    )
    lease_renewal_thread.start()

    try:
        while not shutdown_requested.is_set():
            with SessionLocal() as db:
                did_work = run_one_job(db, worker_id=worker_id)

            if not did_work:
                stop_event.wait(settings.worker_poll_interval_seconds)

    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=2)
        lease_renewal_thread.join(timeout=2)

        with SessionLocal() as db:
            mark_worker_stopped(db, worker_id=worker_id)

        print(f"[worker] stopped worker_id={worker_id}")


if __name__ == "__main__":
    main()