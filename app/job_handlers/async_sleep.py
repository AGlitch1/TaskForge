import asyncio
from typing import Any


async def run(payload: dict[str, Any]) -> dict[str, Any]:
    duration_seconds = payload["duration_seconds"]

    await asyncio.sleep(duration_seconds)

    return {
        "slept_seconds": duration_seconds,
    }
