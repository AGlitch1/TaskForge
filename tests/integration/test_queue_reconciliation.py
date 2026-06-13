from app.core.enums import JobStatus
from app.schemas.jobs import JobCreateRequest
from app.services.job_service import create_job
from app.services.queue_service import get_ready_queue_depth
from scheduler.queue_reconciliation import reconcile_queued_jobs


def test_queue_reconciliation_restores_missing_queued_job(
    db_session,
    redis_client,
):
    request = JobCreateRequest(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
        priority=5,
        scheduled_at=None,
        max_retries=3,
    )

    job = create_job(
        db_session,
        request=request,
        idempotency_key="integration-reconcile-1",
    )

    assert job.status == JobStatus.QUEUED.value
    assert get_ready_queue_depth(redis_client) == 1

    redis_client.delete("ready_jobs")

    assert get_ready_queue_depth(redis_client) == 0

    repaired_count = reconcile_queued_jobs(db_session)

    assert repaired_count == 1
    assert get_ready_queue_depth(redis_client) == 1

    redis_job_ids = redis_client.zrange("ready_jobs", 0, -1)

    assert str(job.id) in redis_job_ids