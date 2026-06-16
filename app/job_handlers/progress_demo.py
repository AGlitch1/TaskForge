from typing import Any


def run(payload: dict[str, Any], ctx: Any) -> dict[str, Any]:
    steps = payload["steps"]

    for step in range(1, steps + 1):
        percent = int((step / steps) * 100)

        ctx.update_progress(
            percent,
            f"Completed progress step {step}/{steps}.",
            {
                "step": step,
                "steps": steps,
            },
        )

    return {
        "steps": steps,
        "progress_updates": steps,
    }
