import asyncio
from pathlib import Path

import pytest

from async_task_queue.persistence.json_store import JSONTaskStore
from async_task_queue.queue.queue import TaskQueue
from async_task_queue.task.registry import TaskRegistry
from async_task_queue.task.state import TaskStatus


@pytest.mark.asyncio
async def test_shutdown_allows_running_task_to_finish() -> None:
    registry = TaskRegistry()

    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(payload: dict[str, object]) -> None:
        started.set()
        await release.wait()

    registry.register("blocking_task", handler)

    queue = TaskQueue(
        registry,
        worker_count=1,
    )

    await queue.start()

    task_id = await queue.submit(
        "blocking_task",
        {},
    )

    await asyncio.wait_for(
        started.wait(),
        timeout=1.0,
    )

    stop_task = asyncio.create_task(
        queue.stop()
    )

    await asyncio.sleep(0.01)

    assert queue.started is True
    assert queue.accepting_tasks is False

    release.set()

    await stop_task

    task = queue.get(task_id)

    assert task.status == TaskStatus.COMPLETED
    assert queue.started is False


@pytest.mark.asyncio
async def test_shutdown_preserves_pending_tasks(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(payload: dict[str, object]) -> None:
        if payload.get("block") is True:
            started.set()
            await release.wait()

    registry.register("test_task", handler)

    store = JSONTaskStore(
        tmp_path / "tasks.json",
    )

    queue = TaskQueue(
        registry,
        worker_count=1,
        store=store,
    )

    await queue.start()

    running_id = await queue.submit(
        "test_task",
        {"block": True},
    )

    pending_id = await queue.submit(
        "test_task",
        {"block": False},
    )

    await asyncio.wait_for(
        started.wait(),
        timeout=1.0,
    )

    stop_task = asyncio.create_task(
        queue.stop()
    )

    await asyncio.sleep(0.01)

    release.set()

    await stop_task

    persisted = store.load_all()

    persisted_by_id = {
        task.id: task
        for task in persisted
    }

    assert (
        persisted_by_id[running_id].status
        == TaskStatus.COMPLETED
    )

    assert (
        persisted_by_id[pending_id].status
        == TaskStatus.PENDING
    )


@pytest.mark.asyncio
async def test_shutdown_rejects_new_tasks() -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        return

    registry.register("test_task", handler)

    queue = TaskQueue(registry)

    await queue.start()

    stop_task = asyncio.create_task(
        queue.stop()
    )

    await asyncio.sleep(0)

    with pytest.raises(
        RuntimeError,
        match="not accepting tasks",
    ):
        await queue.submit(
            "test_task",
            {},
        )

    await stop_task


@pytest.mark.asyncio
async def test_shutdown_cancels_retry_timer() -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        raise TimeoutError("temporary failure")

    registry.register("unstable_task", handler)

    queue = TaskQueue(
        registry,
        retry_policy=__import__(
            "async_task_queue.retry.policy",
            fromlist=["RetryPolicy"],
        ).RetryPolicy(
            base_delay=10.0,
            max_delay=10.0,
            retryable_exceptions=(TimeoutError,),
        ),
    )

    await queue.start()

    task_id = await queue.submit(
        "unstable_task",
        {},
        max_retries=1,
    )

    await asyncio.sleep(0.01)

    task = queue.get(task_id)

    assert task.status == TaskStatus.RETRY_WAIT

    await queue.stop()

    assert task.status == TaskStatus.RETRY_WAIT
    assert queue.started is False