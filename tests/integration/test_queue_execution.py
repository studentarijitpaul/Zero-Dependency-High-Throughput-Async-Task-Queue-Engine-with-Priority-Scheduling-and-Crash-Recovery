import asyncio

import pytest

from async_task_queue.queue.queue import TaskQueue
from async_task_queue.task.registry import TaskRegistry
from async_task_queue.task.state import TaskStatus


@pytest.mark.asyncio
async def test_task_flows_from_submission_to_completion() -> None:
    registry = TaskRegistry()

    executed_payloads: list[dict[str, object]] = []

    async def handler(payload: dict[str, object]) -> None:
        executed_payloads.append(payload)

    registry.register("process_data", handler)

    queue = TaskQueue(
        registry,
        worker_count=2,
    )

    await queue.start()

    task_id = await queue.submit(
        "process_data",
        {"value": 42},
    )

    task = await queue.wait(task_id)

    await queue.stop()

    assert task.id == task_id
    assert task.status == TaskStatus.COMPLETED
    assert task.payload == {"value": 42}
    assert executed_payloads == [{"value": 42}]


@pytest.mark.asyncio
async def test_multiple_tasks_are_executed() -> None:
    registry = TaskRegistry()

    executed: list[int] = []

    async def handler(payload: dict[str, object]) -> None:
        value = payload["value"]
        assert isinstance(value, int)

        await asyncio.sleep(0.01)

        executed.append(value)

    registry.register("process", handler)

    queue = TaskQueue(
        registry,
        worker_count=3,
    )

    await queue.start()

    task_ids = [
        await queue.submit(
            "process",
            {"value": value},
        )
        for value in range(5)
    ]

    tasks = [
        await queue.wait(task_id)
        for task_id in task_ids
    ]

    await queue.stop()

    assert len(executed) == 5
    assert all(
        task.status == TaskStatus.COMPLETED
        for task in tasks
    )
    assert sorted(executed) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_priority_is_respected_when_tasks_are_waiting() -> None:
    registry = TaskRegistry()

    executed: list[str] = []

    async def handler(payload: dict[str, object]) -> None:
        name = payload["name"]
        assert isinstance(name, str)

        executed.append(name)

    registry.register("process", handler)

    queue = TaskQueue(
        registry,
        worker_count=1,
    )

    await queue.start()

    low_id = await queue.submit(
        "process",
        {"name": "low"},
        priority=10,
    )

    high_id = await queue.submit(
        "process",
        {"name": "high"},
        priority=0,
    )

    medium_id = await queue.submit(
        "process",
        {"name": "medium"},
        priority=5,
    )

    await queue.wait(low_id)
    await queue.wait(high_id)
    await queue.wait(medium_id)

    await queue.stop()

    assert executed == [
        "high",
        "medium",
        "low",
    ]