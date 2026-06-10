import time
from typing import Any


def run(payload: dict[str, Any]) -> dict[str, Any]:
    duration_seconds = payload["duration_seconds"]

    time.sleep(duration_seconds)

    return {
        "slept_seconds": duration_seconds,
    }