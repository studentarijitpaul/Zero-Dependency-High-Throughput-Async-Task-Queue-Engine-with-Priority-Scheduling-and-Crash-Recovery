import pytest

from async_task_queue.queue.queue import TaskQueue
from async_task_queue.task.model import Task
from async_task_queue.task.registry import TaskRegistry


@pytest.mark.asyncio
async def test_queue_starts() -> None:
    registry = TaskRegistry()
    queue = TaskQueue(registry)

    await queue.start()

    assert queue.started is True


@pytest.mark.asyncio
async def test_start_is_idempotent() -> None:
    registry = TaskRegistry()
    queue = TaskQueue(registry)

    await queue.start()
    await queue.start()

    assert queue.started is True


@pytest.mark.asyncio
async def test_stop_is_idempotent() -> None:
    registry = TaskRegistry()
    queue = TaskQueue(registry)

    await queue.start()

    await queue.stop()
    await queue.stop()

    assert queue.started is False


@pytest.mark.asyncio
async def test_submit_task() -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        pass

    registry.register("test_task", handler)

    queue = TaskQueue(registry)

    await queue.start()

    task_id = await queue.submit(
        "test_task",
        {"value": 42},
        priority=1,
        max_retries=3,
    )

    task = queue.get(task_id)

    assert isinstance(task, Task)
    assert task.id == task_id
    assert task.task_name == "test_task"
    assert task.payload == {"value": 42}
    assert task.priority == 1
    assert task.max_retries == 3


@pytest.mark.asyncio
async def test_submit_requires_running_queue() -> None:
    registry = TaskRegistry()
    queue = TaskQueue(registry)

    with pytest.raises(
        RuntimeError,
        match="Task queue is not running",
    ):
        await queue.submit("test_task", {})


@pytest.mark.asyncio
async def test_submit_requires_registered_handler() -> None:
    registry = TaskRegistry()
    queue = TaskQueue(registry)

    await queue.start()

    with pytest.raises(Exception):
        await queue.submit("missing_task", {})


def test_get_missing_task_raises() -> None:
    registry = TaskRegistry()
    queue = TaskQueue(registry)

    with pytest.raises(Exception):
        queue.get(__import__("uuid").uuid4())


def test_invalid_worker_count() -> None:
    registry = TaskRegistry()

    with pytest.raises(ValueError):
        TaskQueue(registry, worker_count=0)