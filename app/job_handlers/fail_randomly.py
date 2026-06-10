import random
from typing import Any


def run(payload: dict[str, Any]) -> dict[str, Any]:
    failure_probability = payload["failure_probability"]

    random_value = random.random()

    if random_value < failure_probability:
        raise RuntimeError(
            f"Random failure triggered. random_value={random_value:.4f}, "
            f"failure_probability={failure_probability}"
        )

    return {
        "success": True,
        "random_value": random_value,
        "failure_probability": failure_probability,
    }