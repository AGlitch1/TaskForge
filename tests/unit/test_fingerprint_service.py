from datetime import datetime, timezone

from app.services.fingerprint_service import create_request_fingerprint


def test_same_request_has_same_fingerprint():
    scheduled_at = datetime(2026, 1, 1, tzinfo=timezone.utc)

    first = create_request_fingerprint(
        job_type="sum_numbers",
        payload={"numbers": [1, 2, 3]},
        priority=5,
        scheduled_at=scheduled_at,
        max_retries=3,
        timeout_seconds=None,
    )

    second = create_request_fingerprint(
        job_type="sum_numbers",
        payload={"numbers": [1, 2, 3]},
        priority=5,
        scheduled_at=scheduled_at,
        max_retries=3,
        timeout_seconds=None,
    )

    assert first == second


def test_payload_key_order_does_not_change_fingerprint():
    first = create_request_fingerprint(
        job_type="webhook",
        payload={"url": "https://example.com", "method": "POST", "body": {"a": 1, "b": 2}},
        priority=5,
        scheduled_at=None,
        max_retries=3,
        timeout_seconds=None,
    )

    second = create_request_fingerprint(
        job_type="webhook",
        payload={"method": "POST", "body": {"b": 2, "a": 1}, "url": "https://example.com"},
        priority=5,
        scheduled_at=None,
        max_retries=3,
        timeout_seconds=None,
    )

    assert first == second


def test_different_payload_changes_fingerprint():
    first = create_request_fingerprint(
        job_type="sum_numbers",
        payload={"numbers": [1, 2, 3]},
        priority=5,
        scheduled_at=None,
        max_retries=3,
        timeout_seconds=None,
    )

    second = create_request_fingerprint(
        job_type="sum_numbers",
        payload={"numbers": [1, 2, 4]},
        priority=5,
        scheduled_at=None,
        max_retries=3,
        timeout_seconds=None,
    )

    assert first != second


def test_different_timeout_seconds_changes_fingerprint():
    first = create_request_fingerprint(
        job_type="sum_numbers",
        payload={"numbers": [1, 2, 3]},
        priority=5,
        scheduled_at=None,
        max_retries=3,
        timeout_seconds=300,
    )

    second = create_request_fingerprint(
        job_type="sum_numbers",
        payload={"numbers": [1, 2, 3]},
        priority=5,
        scheduled_at=None,
        max_retries=3,
        timeout_seconds=600,
    )

    assert first != second
