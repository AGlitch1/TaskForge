import pytest

from app.job_handlers.fail_randomly import run as run_fail_randomly
from app.job_handlers.sum_numbers import run as run_sum_numbers
from app.job_handlers.generate_report import run as run_generate_report
from app.job_handlers.webhook import run as run_webhook

def test_sum_numbers_handler():
    result = run_sum_numbers({"numbers": [1, 2, 3]})

    assert result == {
        "sum": 6,
        "count": 3,
    }


def test_fail_randomly_with_zero_probability_succeeds():
    result = run_fail_randomly({"failure_probability": 0.0})

    assert result["success"] is True
    assert result["failure_probability"] == 0.0


def test_fail_randomly_with_one_probability_fails():
    with pytest.raises(RuntimeError):
        run_fail_randomly({"failure_probability": 1.0})

def test_generate_report_handler():
    result = run_generate_report({"rows": 100})

    assert result["rows_processed"] == 100
    assert result["status"] == "generated"
    assert "report_id" in result

def test_webhook_handler_rejects_unsupported_method():
    with pytest.raises(ValueError):
        run_webhook(
            {
                "url": "https://example.com",
                "method": "PUT",
                "body": {"hello": "world"},
            }
        )