from pathlib import Path

import pytest

from async_task_queue.persistence.json_store import JSONTaskStore
from async_task_queue.queue.queue import TaskQueue
from async_task_queue.task.registry import TaskRegistry
from async_task_queue.task.state import TaskStatus


@pytest.mark.asyncio
async def test_submitted_task_is_persisted(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        return

    registry.register("test_task", handler)

    store = JSONTaskStore(
        tmp_path / "tasks.json",
    )

    queue = TaskQueue(
        registry,
        store=store,
    )

    task_id = await queue.submit(
        "test_task",
        {"value": 42},
    )

    persisted = store.load_all()

    assert len(persisted) == 1
    assert persisted[0].id == task_id
    assert persisted[0].status == TaskStatus.PENDING
    assert persisted[0].payload == {"value": 42}


@pytest.mark.asyncio
async def test_completed_task_is_persisted(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        return

    registry.register("test_task", handler)

    store = JSONTaskStore(
        tmp_path / "tasks.json",
    )

    queue = TaskQueue(
        registry,
        store=store,
    )

    await queue.start()

    task_id = await queue.submit(
        "test_task",
        {"value": 42},
    )

    task = await queue.wait(task_id)

    await queue.stop()

    persisted = store.load_all()

    assert len(persisted) == 1
    assert persisted[0].id == task_id
    assert persisted[0].status == TaskStatus.COMPLETED
    assert persisted[0].payload == {"value": 42}
    assert task.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_retry_state_is_persisted(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        raise TimeoutError("temporary failure")

    registry.register("test_task", handler)

    store = JSONTaskStore(
        tmp_path / "tasks.json",
    )

    queue = TaskQueue(
        registry,
        store=store,
    )

    await queue.start()

    task_id = await queue.submit(
        "test_task",
        {},
        max_retries=2,
    )

    await queue.stop()

    persisted = store.load_all()

    persisted_task = next(task for task in persisted if task.id == task_id)

    assert persisted_task.id == task_id
    assert persisted_task.task_name == "test_task"


@pytest.mark.asyncio
async def test_queue_and_store_remain_consistent(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        return

    registry.register("test_task", handler)

    store = JSONTaskStore(
        tmp_path / "tasks.json",
    )

    queue = TaskQueue(
        registry,
        store=store,
    )

    await queue.start()

    task_ids = [
        await queue.submit(
            "test_task",
            {"value": value},
        )
        for value in range(3)
    ]

    for task_id in task_ids:
        await queue.wait(task_id)

    await queue.stop()

    persisted = store.load_all()

    persisted_by_id = {task.id: task for task in persisted}

    assert set(persisted_by_id) == set(task_ids)

    for task_id in task_ids:
        assert persisted_by_id[task_id].status == TaskStatus.COMPLETED
