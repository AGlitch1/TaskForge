from typing import Any


def run(payload: dict[str, Any]) -> dict[str, Any]:
    numbers = payload["numbers"]
    total = sum(numbers)

    return {
        "sum": total,
        "count": len(numbers),
    }