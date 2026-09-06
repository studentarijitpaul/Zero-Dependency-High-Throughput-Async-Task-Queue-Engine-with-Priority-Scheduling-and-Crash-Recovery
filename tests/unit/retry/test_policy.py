from typing import cast

import pytest

from async_task_queue.retry.policy import RetryPolicy


def test_default_policy() -> None:
    policy = RetryPolicy()

    assert policy.base_delay == 1.0
    assert policy.max_delay == 30.0
    assert policy.retryable_exceptions == (Exception,)
    assert policy.jitter is False


def test_should_retry_retryable_exception() -> None:
    policy = RetryPolicy(
        retryable_exceptions=(TimeoutError,),
    )

    assert policy.should_retry(TimeoutError("temporary")) is True


def test_should_not_retry_non_retryable_exception() -> None:
    policy = RetryPolicy(
        retryable_exceptions=(TimeoutError,),
    )

    assert policy.should_retry(ValueError("permanent")) is False


def test_should_retry_does_not_consider_retry_count() -> None:
    policy = RetryPolicy(
        retryable_exceptions=(TimeoutError,),
    )

    exception = TimeoutError("temporary")

    assert policy.should_retry(exception) is True


def test_exponential_backoff() -> None:
    policy = RetryPolicy(
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
        base_delay=10.0,
        max_delay=30.0,
        jitter=True,
    )

    delays = {policy.get_delay(2) for _ in range(20)}

    assert all(0.0 <= delay <= 20.0 for delay in delays)

    # With multiple samples, jitter should normally produce
    # more than one distinct value.
    assert len(delays) > 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"base_delay": -1.0},
        {"max_delay": -1.0},
        {
            "base_delay": 10.0,
            "max_delay": 5.0,
        },
    ],
)
def test_invalid_configuration(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**cast(dict[str, object], kwargs))


@pytest.mark.parametrize(
    "retry_number",
    [0, -1],
)
def test_invalid_retry_number(
    retry_number: int,
) -> None:
    policy = RetryPolicy()

    with pytest.raises(
        ValueError,
        match="retry_number must be >= 1",
    ):
        policy.get_delay(retry_number)
