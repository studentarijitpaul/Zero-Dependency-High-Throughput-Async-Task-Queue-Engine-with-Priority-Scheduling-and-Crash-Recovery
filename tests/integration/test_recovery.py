from pathlib import Path

import pytest

from async_task_queue.persistence.json_store import JSONTaskStore
from async_task_queue.queue.queue import TaskQueue
from async_task_queue.task.model import Task
from async_task_queue.task.registry import TaskRegistry
from async_task_queue.task.state import TaskStatus


@pytest.mark.asyncio
async def test_pending_task_is_recovered_after_restart(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    executed: list[dict[str, object]] = []

    async def handler(payload: dict[str, object]) -> None:
        executed.append(payload)

    registry.register("test_task", handler)

    store = JSONTaskStore(
        tmp_path / "tasks.json",
    )

    first_queue = TaskQueue(
        registry,
        store=store,
    )

    task = Task(
        task_name="test_task",
        payload={"value": 42},
    )

    store.save(task)

    recovered_queue = TaskQueue(
        registry,
        store=store,
    )

    await recovered_queue.start()

    recovered_task = await recovered_queue.wait(task.id)

    await recovered_queue.stop()

    assert recovered_task.id == task.id
    assert recovered_task.status == TaskStatus.COMPLETED
    assert executed == [{"value": 42}]

    # Keep the first queue unused but make the intent explicit:
    assert first_queue.started is False


@pytest.mark.asyncio
async def test_running_task_is_recovered_as_pending(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        return

    registry.register("test_task", handler)

    store = JSONTaskStore(
        tmp_path / "tasks.json",
    )

    task = Task(
        task_name="test_task",
        payload={},
        status=TaskStatus.RUNNING,
    )

    store.save(task)

    queue = TaskQueue(
        registry,
        store=store,
    )

    await queue.start()

    recovered_task = await queue.wait(task.id)

    await queue.stop()

    assert recovered_task.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_completed_task_is_not_executed_again_after_restart(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    executed_count = 0

    async def handler(payload: dict[str, object]) -> None:
        nonlocal executed_count
        executed_count += 1

    registry.register("test_task", handler)

    store = JSONTaskStore(
        tmp_path / "tasks.json",
    )

    completed_task = Task(
        task_name="test_task",
        payload={},
        status=TaskStatus.COMPLETED,
    )

    store.save(completed_task)

    queue = TaskQueue(
        registry,
        store=store,
    )

    await queue.start()

    recovered_task = queue.get(
        completed_task.id,
    )

    await queue.stop()

    assert recovered_task.status == TaskStatus.COMPLETED
    assert executed_count == 0


@pytest.mark.asyncio
async def test_cancelled_task_is_not_executed_after_restart(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    executed_count = 0

    async def handler(payload: dict[str, object]) -> None:
        nonlocal executed_count
        executed_count += 1

    registry.register("test_task", handler)

    store = JSONTaskStore(
        tmp_path / "tasks.json",
    )

    cancelled_task = Task(
        task_name="test_task",
        payload={},
        status=TaskStatus.CANCELLED,
    )

    store.save(cancelled_task)

    queue = TaskQueue(
        registry,
        store=store,
    )

    await queue.start()

    recovered_task = queue.get(
        cancelled_task.id,
    )

    await queue.stop()

    assert recovered_task.status == TaskStatus.CANCELLED
    assert executed_count == 0


@pytest.mark.asyncio
async def test_failed_task_is_not_automatically_retried_after_restart(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    executed_count = 0

    async def handler(payload: dict[str, object]) -> None:
        nonlocal executed_count
        executed_count += 1

    registry.register("test_task", handler)

    store = JSONTaskStore(
        tmp_path / "tasks.json",
    )

    failed_task = Task(
        task_name="test_task",
        payload={},
        status=TaskStatus.FAILED,
        max_retries=3,
        retry_count=3,
    )

    store.save(failed_task)

    queue = TaskQueue(
        registry,
        store=store,
    )

    await queue.start()

    recovered_task = queue.get(
        failed_task.id,
    )

    await queue.stop()

    assert recovered_task.status == TaskStatus.FAILED
    assert executed_count == 0