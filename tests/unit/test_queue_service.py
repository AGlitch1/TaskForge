from datetime import datetime, timezone

from app.services.queue_service import calculate_ready_job_score


def test_higher_priority_gets_lower_score():
    queued_at = datetime(2026, 1, 1, tzinfo=timezone.utc)

    low_priority_score = calculate_ready_job_score(
        queued_at=queued_at,
        priority=1,
    )

    high_priority_score = calculate_ready_job_score(
        queued_at=queued_at,
        priority=10,
    )

    assert high_priority_score < low_priority_score


def test_older_job_with_same_priority_gets_lower_score():
    older = datetime(2026, 1, 1, tzinfo=timezone.utc)
    newer = datetime(2026, 1, 2, tzinfo=timezone.utc)

    older_score = calculate_ready_job_score(
        queued_at=older,
        priority=5,
    )

    newer_score = calculate_ready_job_score(
        queued_at=newer,
        priority=5,
    )

    assert older_score < newer_score