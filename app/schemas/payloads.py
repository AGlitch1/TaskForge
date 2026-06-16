from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl


class SleepPayload(BaseModel):
    duration_seconds: int = Field(ge=1, le=300)


class AsyncSleepPayload(BaseModel):
    duration_seconds: float = Field(gt=0, le=300)


class SumNumbersPayload(BaseModel):
    numbers: list[float] = Field(min_length=1)


class FailRandomlyPayload(BaseModel):
    failure_probability: float = Field(ge=0.0, le=1.0)


class WebhookPayload(BaseModel):
    url: HttpUrl
    method: Literal["GET", "POST"] = "POST"
    body: dict[str, Any] | None = None


class GenerateReportPayload(BaseModel):
    rows: int = Field(ge=1, le=100_000)


class ProgressDemoPayload(BaseModel):
    steps: int = Field(default=3, ge=1, le=20)


PAYLOAD_SCHEMA_BY_JOB_TYPE = {
    "sleep": SleepPayload,
    "async_sleep": AsyncSleepPayload,
    "sum_numbers": SumNumbersPayload,
    "fail_randomly": FailRandomlyPayload,
    "webhook": WebhookPayload,
    "generate_report": GenerateReportPayload,
    "progress_demo": ProgressDemoPayload,
}


def validate_payload_for_job_type(job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    schema_class = PAYLOAD_SCHEMA_BY_JOB_TYPE.get(job_type)

    if schema_class is None:
        supported_types = ", ".join(sorted(PAYLOAD_SCHEMA_BY_JOB_TYPE))
        raise ValueError(
            f"Unsupported job_type '{job_type}'. Supported job types: {supported_types}"
        )

    validated_payload = schema_class.model_validate(payload)
    return validated_payload.model_dump(mode="json")
