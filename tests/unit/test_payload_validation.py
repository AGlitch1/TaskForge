import pytest
from pydantic import ValidationError

from app.schemas.payloads import validate_payload_for_job_type


def test_sum_numbers_valid_payload():
    payload = validate_payload_for_job_type(
        job_type="sum_numbers",
        payload={"numbers": [1, 2, 3]},
    )

    assert payload["numbers"] == [1.0, 2.0, 3.0]


def test_sum_numbers_rejects_empty_list():
    with pytest.raises(ValidationError):
        validate_payload_for_job_type(
            job_type="sum_numbers",
            payload={"numbers": []},
        )


def test_sleep_valid_payload():
    payload = validate_payload_for_job_type(
        job_type="sleep",
        payload={"duration_seconds": 5},
    )

    assert payload["duration_seconds"] == 5


def test_sleep_rejects_zero_seconds():
    with pytest.raises(ValidationError):
        validate_payload_for_job_type(
            job_type="sleep",
            payload={"duration_seconds": 0},
        )


def test_fail_randomly_valid_payload():
    payload = validate_payload_for_job_type(
        job_type="fail_randomly",
        payload={"failure_probability": 0.5},
    )

    assert payload["failure_probability"] == 0.5


def test_fail_randomly_rejects_probability_above_one():
    with pytest.raises(ValidationError):
        validate_payload_for_job_type(
            job_type="fail_randomly",
            payload={"failure_probability": 1.5},
        )


def test_unknown_job_type_is_rejected():
    with pytest.raises(ValueError):
        validate_payload_for_job_type(
            job_type="unknown_job",
            payload={},
        )