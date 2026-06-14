from collections.abc import Callable
from typing import Any

from app.job_handlers import fail_randomly, sleep, sum_numbers,generate_report,webhook


JobHandler = Callable[[dict[str, Any]], dict[str, Any]]


JOB_HANDLERS: dict[str, JobHandler] = {
    "sleep": sleep.run,
    "sum_numbers": sum_numbers.run,
    "fail_randomly": fail_randomly.run,
    "generate_report": generate_report.run,
    "webhook": webhook.run,
}

def get_job_handler(job_type: str) -> JobHandler:
    handler = JOB_HANDLERS.get(job_type)

    if handler is None:
        supported = ", ".join(sorted(JOB_HANDLERS))
        raise ValueError(
            f"No handler registered for job_type '{job_type}'. "
            f"Supported handlers: {supported}"
        )

    return handler