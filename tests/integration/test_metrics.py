from fastapi.testclient import TestClient

from app.main import app


def test_metrics_endpoint_returns_plain_text_metrics(db_session, redis_client):
    client = TestClient(app)

    response = client.get("/admin/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")

    body = response.text

    assert "taskforge_jobs_queued" in body
    assert "taskforge_jobs_scheduled" in body
    assert "taskforge_jobs_running" in body
    assert "taskforge_jobs_retrying" in body
    assert "taskforge_jobs_completed" in body
    assert "taskforge_jobs_dead" in body
    assert "taskforge_jobs_cancelled" in body

    assert "taskforge_workers_idle" in body
    assert "taskforge_workers_busy" in body
    assert "taskforge_workers_stopped" in body
    assert "taskforge_workers_dead" in body

    assert "taskforge_ready_queue_depth" in body