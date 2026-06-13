from worker.runner import calculate_retry_delay_seconds


def test_retry_delay_first_retry():
    assert calculate_retry_delay_seconds(1) == 5


def test_retry_delay_second_retry():
    assert calculate_retry_delay_seconds(2) == 30


def test_retry_delay_third_retry():
    assert calculate_retry_delay_seconds(3) == 120


def test_retry_delay_after_known_values_defaults_to_120():
    assert calculate_retry_delay_seconds(99) == 120