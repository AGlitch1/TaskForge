import hashlib
import json
from datetime import datetime
from typing import Any


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()

    return str(value)


def create_request_fingerprint(
    *,
    job_type: str,
    payload: dict[str, Any],
    priority: int,
    scheduled_at: datetime | None,
    max_retries: int,
    timeout_seconds: int | None,
) -> str:
    normalized = {
        "job_type": job_type,
        "payload": payload,
        "priority": priority,
        "scheduled_at": scheduled_at,
        "max_retries": max_retries,
        "timeout_seconds": timeout_seconds,
    }

    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")

    return hashlib.sha256(encoded).hexdigest()
