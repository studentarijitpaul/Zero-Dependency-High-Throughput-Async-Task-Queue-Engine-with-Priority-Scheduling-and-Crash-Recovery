import pytest

from async_task_queue.queue.queue import TaskQueue
from async_task_queue.retry.policy import RetryPolicy
from async_task_queue.task.registry import TaskRegistry
from async_task_queue.task.state import TaskStatus


@pytest.mark.asyncio
async def test_retry_flow_eventually_completes_task() -> None:
    registry = TaskRegistry()

    attempts = 0

    async def handler(payload: dict[str, object]) -> None:
        nonlocal attempts

        attempts += 1

        if attempts < 3:
            raise TimeoutError("temporary failure")

    registry.register("unstable_task", handler)

    queue = TaskQueue(
        registry,
        retry_policy=RetryPolicy(
            base_delay=0.01,
            max_delay=0.01,
            retryable_exceptions=(TimeoutError,),
        ),
    )

    await queue.start()

    task_id = await queue.submit(
        "unstable_task",
        {},
        max_retries=2,
    )

    task = await queue.wait(task_id)

    await queue.stop()

    assert task.status == TaskStatus.COMPLETED
    assert task.retry_count == 2
    assert attempts == 3


@pytest.mark.asyncio
async def test_retry_flow_fails_after_task_retries_are_exhausted() -> None:
    registry = TaskRegistry()

    attempts = 0

    async def handler(payload: dict[str, object]) -> None:
        nonlocal attempts

        attempts += 1
        raise TimeoutError("permanent temporary failure")

    registry.register("unstable_task", handler)

    queue = TaskQueue(
        registry,
        retry_policy=RetryPolicy(
            base_delay=0.01,
            max_delay=0.01,
            retryable_exceptions=(TimeoutError,),
        ),
    )

    await queue.start()

    task_id = await queue.submit(
        "unstable_task",
        {},
        max_retries=2,
    )

    task = await queue.wait(task_id)

    await queue.stop()

    assert task.status == TaskStatus.FAILED
    assert task.retry_count == 2
    assert attempts == 3


@pytest.mark.asyncio
async def test_non_retryable_exception_fails_immediately() -> None:
    registry = TaskRegistry()

    attempts = 0

    async def handler(payload: dict[str, object]) -> None:
        nonlocal attempts

        attempts += 1
        raise ValueError("permanent failure")

    registry.register("task", handler)

    queue = TaskQueue(
        registry,
        retry_policy=RetryPolicy(
            base_delay=0.01,
            max_delay=0.01,
            retryable_exceptions=(TimeoutError,),
        ),
    )

    await queue.start()

    task_id = await queue.submit(
        "task",
        {},
        max_retries=5,
    )

    task = await queue.wait(task_id)

    await queue.stop()

    assert task.status == TaskStatus.FAILED
    assert task.retry_count == 0
    assert attempts == 1


@pytest.mark.asyncio
async def test_different_tasks_can_have_different_retry_limits() -> None:
    registry = TaskRegistry()

    attempts: dict[str, int] = {
        "no_retry": 0,
        "two_retries": 0,
    }

    async def handler(payload: dict[str, object]) -> None:
        name = payload["name"]
        assert isinstance(name, str)

        attempts[name] += 1
        raise TimeoutError("temporary failure")

    registry.register("unstable_task", handler)

    queue = TaskQueue(
        registry,
        worker_count=2,
        retry_policy=RetryPolicy(
            base_delay=0.01,
            max_delay=0.01,
            retryable_exceptions=(TimeoutError,),
        ),
    )

    await queue.start()

    no_retry_id = await queue.submit(
        "unstable_task",
        {"name": "no_retry"},
        max_retries=0,
    )

    two_retries_id = await queue.submit(
        "unstable_task",
        {"name": "two_retries"},
        max_retries=2,
    )

    no_retry = await queue.wait(no_retry_id)
    two_retries = await queue.wait(two_retries_id)

    await queue.stop()

    assert no_retry.status == TaskStatus.FAILED
    assert no_retry.retry_count == 0
    assert attempts["no_retry"] == 1

    assert two_retries.status == TaskStatus.FAILED
    assert two_retries.retry_count == 2
    assert attempts["two_retries"] == 3
