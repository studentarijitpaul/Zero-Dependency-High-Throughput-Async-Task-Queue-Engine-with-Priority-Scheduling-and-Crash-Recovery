import pytest

from async_task_queue.retry.policy import RetryPolicy


def test_default_policy() -> None:
    policy = RetryPolicy()

    assert policy.max_retries == 0
    assert policy.base_delay == 1.0
    assert policy.max_delay == 30.0
    assert policy.jitter is False


def test_should_retry_retryable_exception() -> None:
    policy = RetryPolicy(
        max_retries=3,
        retryable_exceptions=(TimeoutError,),
    )

    assert policy.should_retry(
        TimeoutError(),
        retry_count=0,
    )


def test_should_not_retry_non_retryable_exception() -> None:
    policy = RetryPolicy(
        max_retries=3,
        retryable_exceptions=(TimeoutError,),
    )

    assert not policy.should_retry(
        ValueError(),
        retry_count=0,
    )


def test_should_not_retry_when_retries_exhausted() -> None:
    policy = RetryPolicy(
        max_retries=3,
        retryable_exceptions=(TimeoutError,),
    )

    assert not policy.should_retry(
        TimeoutError(),
        retry_count=3,
    )


def test_exponential_backoff() -> None:
    policy = RetryPolicy(
        max_retries=5,
        base_delay=1.0,
        max_delay=30.0,
    )

    assert policy.get_delay(1) == 1.0
    assert policy.get_delay(2) == 2.0
    assert policy.get_delay(3) == 4.0
    assert policy.get_delay(4) == 8.0
    assert policy.get_delay(5) == 16.0


def test_backoff_is_capped() -> None:
    policy = RetryPolicy(
        max_retries=10,
        base_delay=1.0,
        max_delay=5.0,
    )

    assert policy.get_delay(1) == 1.0
    assert policy.get_delay(2) == 2.0
    assert policy.get_delay(3) == 4.0
    assert policy.get_delay(4) == 5.0
    assert policy.get_delay(5) == 5.0


def test_jitter_changes_delay() -> None:
    policy = RetryPolicy(
        max_retries=3,
        base_delay=10.0,
        max_delay=30.0,
        jitter=True,
    )

    delay = policy.get_delay(2)

    assert 0.0 <= delay <= 20.0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_retries": -1},
        {"base_delay": -1.0},
        {"max_delay": -1.0},
        {"base_delay": 10.0, "max_delay": 5.0},
    ],
)
def test_invalid_configuration(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize("retry_number", [0, -1])
def test_invalid_retry_number(retry_number: int) -> None:
    policy = RetryPolicy()

    with pytest.raises(ValueError):
        policy.get_delay(retry_number)