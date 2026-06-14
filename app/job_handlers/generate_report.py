import time
import uuid
from typing import Any


def run(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload["rows"]

    # Simulate report generation work.
    # Keep this short so tests and demos do not become slow.
    time.sleep(1)

    return {
        "report_id": str(uuid.uuid4()),
        "rows_processed": rows,
        "status": "generated",
    }