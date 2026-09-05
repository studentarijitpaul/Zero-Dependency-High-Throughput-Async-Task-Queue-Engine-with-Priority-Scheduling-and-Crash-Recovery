import pytest

from async_task_queue.retry.policy import RetryPolicy
from async_task_queue.task.model import Task
from async_task_queue.task.registry import TaskRegistry
from async_task_queue.task.state import TaskStatus
from async_task_queue.worker.worker import Worker


@pytest.mark.asyncio
async def test_worker_executes_handler() -> None:
    registry = TaskRegistry()
    executed_payload: list[dict[str, object]] = []

    async def handler(payload: dict[str, object]) -> None:
        executed_payload.append(payload)

    registry.register("test_task", handler)

    task = Task(
        task_name="test_task",
        payload={"value": 42},
    )

    worker = Worker(
        registry,
        RetryPolicy(),
    )

    await worker.execute(task)

    assert executed_payload == [{"value": 42}]
    assert task.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_worker_marks_task_running_before_execution() -> None:
    registry = TaskRegistry()
    observed_status: list[TaskStatus] = []

    async def handler(payload: dict[str, object]) -> None:
        observed_status.append(task.status)

    registry.register("test_task", handler)

    task = Task(
        task_name="test_task",
        payload={},
    )

    worker = Worker(
        registry,
        RetryPolicy(),
    )

    await worker.execute(task)

    assert observed_status == [TaskStatus.RUNNING]


@pytest.mark.asyncio
async def test_worker_propagates_handler_exception() -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        raise ValueError("something went wrong")

    registry.register("test_task", handler)

    task = Task(
        task_name="test_task",
        payload={},
    )

    worker = Worker(
        registry,
        RetryPolicy(),
    )

    with pytest.raises(ValueError, match="something went wrong"):
        await worker.execute(task)

    assert task.status == TaskStatus.FAILED


@pytest.mark.asyncio
async def test_worker_moves_failed_task_to_retry_wait() -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        raise TimeoutError("temporary failure")

    registry.register("test_task", handler)

    task = Task(
        task_name="test_task",
        payload={},
    )

    policy = RetryPolicy(
        max_retries=3,
        retryable_exceptions=(TimeoutError,),
    )

    worker = Worker(registry, policy)

    await worker.execute(task)

    assert task.status == TaskStatus.RETRY_WAIT
    assert task.retry_count == 1


@pytest.mark.asyncio
async def test_worker_marks_task_failed_when_retries_exhausted() -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        raise TimeoutError("failure")

    registry.register("test_task", handler)

    task = Task(
        task_name="test_task",
        payload={},
        max_retries=1,
        retry_count=1,
    )

    policy = RetryPolicy(
        max_retries=1,
        retryable_exceptions=(TimeoutError,),
    )

    worker = Worker(registry, policy)

    with pytest.raises(TimeoutError):
        await worker.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.retry_count == 1


@pytest.mark.asyncio
async def test_worker_fails_non_retryable_exception() -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        raise ValueError("permanent failure")

    registry.register("test_task", handler)

    task = Task(
        task_name="test_task",
        payload={},
    )

    policy = RetryPolicy(
        max_retries=3,
        retryable_exceptions=(TimeoutError,),
    )

    worker = Worker(registry, policy)

    with pytest.raises(ValueError):
        await worker.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.retry_count == 0