import asyncio
from pathlib import Path
from uuid import uuid4

import pytest

from async_task_queue.exceptions import (
    TaskCancellationError,
    TaskNotFoundError,
)
from async_task_queue.persistence.json_store import JSONTaskStore
from async_task_queue.queue.queue import TaskQueue
from async_task_queue.retry.policy import RetryPolicy
from async_task_queue.task.model import Task
from async_task_queue.task.registry import TaskRegistry
from async_task_queue.task.state import TaskStatus


@pytest.mark.asyncio
async def test_queue_starts() -> None:
    registry = TaskRegistry()
    queue = TaskQueue(registry)

    await queue.start()

    assert queue.started is True

    await queue.stop()


@pytest.mark.asyncio
async def test_start_is_idempotent() -> None:
    registry = TaskRegistry()
    queue = TaskQueue(registry)

    await queue.start()
    await queue.start()

    assert queue.started is True

    await queue.stop()


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
async def test_submit_before_start_executes_after_start() -> None:
    registry = TaskRegistry()
    completed = asyncio.Event()

    async def handler(payload: dict[str, object]) -> None:
        completed.set()

    registry.register("test_task", handler)

    queue = TaskQueue(registry)

    task_id = await queue.submit(
        "test_task",
        {},
    )

    assert queue.get(task_id).status == TaskStatus.PENDING

    await queue.start()

    await asyncio.wait_for(
        completed.wait(),
        timeout=1.0,
    )

    task = await queue.wait(task_id)

    assert task.status == TaskStatus.COMPLETED

    await queue.stop()


@pytest.mark.asyncio
async def test_submit_requires_registered_handler() -> None:
    registry = TaskRegistry()
    queue = TaskQueue(registry)

    with pytest.raises(TaskNotFoundError):
        await queue.submit(
            "missing_task",
            {},
        )


def test_get_missing_task_raises() -> None:
    registry = TaskRegistry()
    queue = TaskQueue(registry)

    with pytest.raises(TaskNotFoundError):
        queue.get(uuid4())


def test_invalid_worker_count() -> None:
    registry = TaskRegistry()

    with pytest.raises(ValueError):
        TaskQueue(
            registry,
            worker_count=0,
        )


@pytest.mark.asyncio
async def test_queue_executes_submitted_task() -> None:
    registry = TaskRegistry()
    completed = asyncio.Event()

    async def handler(payload: dict[str, object]) -> None:
        completed.set()

    registry.register("test_task", handler)

    queue = TaskQueue(registry)

    await queue.start()

    task_id = await queue.submit(
        "test_task",
        {},
    )

    await asyncio.wait_for(
        completed.wait(),
        timeout=1.0,
    )

    task = queue.get(task_id)

    assert task.status == TaskStatus.COMPLETED

    await queue.stop()


@pytest.mark.asyncio
async def test_queue_supports_multiple_workers() -> None:
    registry = TaskRegistry()

    started = 0
    lock = asyncio.Lock()
    all_started = asyncio.Event()

    async def handler(payload: dict[str, object]) -> None:
        nonlocal started

        async with lock:
            started += 1

            if started == 2:
                all_started.set()

        await asyncio.sleep(0.05)

    registry.register("test_task", handler)

    queue = TaskQueue(
        registry,
        worker_count=2,
    )

    await queue.start()

    await queue.submit(
        "test_task",
        {},
    )
    await queue.submit(
        "test_task",
        {},
    )

    await asyncio.wait_for(
        all_started.wait(),
        timeout=1.0,
    )

    await queue.stop()

    assert started == 2


@pytest.mark.asyncio
async def test_wait_returns_completed_task() -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        pass

    registry.register("test_task", handler)

    queue = TaskQueue(registry)

    await queue.start()

    task_id = await queue.submit(
        "test_task",
        {},
    )

    task = await queue.wait(task_id)

    assert task.id == task_id
    assert task.status == TaskStatus.COMPLETED

    await queue.stop()


@pytest.mark.asyncio
async def test_queue_retries_failed_task() -> None:
    registry = TaskRegistry()

    attempts = 0

    async def handler(payload: dict[str, object]) -> None:
        nonlocal attempts

        attempts += 1

        if attempts == 1:
            raise TimeoutError("temporary failure")

    registry.register("test_task", handler)

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
        "test_task",
        {},
        max_retries=1,
    )

    task = await queue.wait(task_id)

    await queue.stop()

    assert task.status == TaskStatus.COMPLETED
    assert attempts == 2
    assert task.retry_count == 1


@pytest.mark.asyncio
async def test_queue_persists_submitted_task(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        pass

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
        priority=2,
        max_retries=3,
    )

    persisted = store.load_all()

    assert len(persisted) == 1
    assert persisted[0].id == task_id
    assert persisted[0].task_name == "test_task"
    assert persisted[0].payload == {"value": 42}
    assert persisted[0].priority == 2
    assert persisted[0].max_retries == 3


@pytest.mark.asyncio
async def test_queue_persists_completed_task(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        pass

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
    )

    await queue.wait(task_id)

    persisted = store.load_all()

    assert len(persisted) == 1
    assert persisted[0].status == TaskStatus.COMPLETED

    await queue.stop()


@pytest.mark.asyncio
async def test_queue_recovers_pending_task(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()
    executed = asyncio.Event()

    async def handler(payload: dict[str, object]) -> None:
        executed.set()

    registry.register("test_task", handler)

    store = JSONTaskStore(
        tmp_path / "tasks.json",
    )

    task = Task(
        task_name="test_task",
        payload={"value": 42},
        status=TaskStatus.PENDING,
    )

    store.save(task)

    queue = TaskQueue(
        registry,
        store=store,
    )

    await queue.start()

    await asyncio.wait_for(
        executed.wait(),
        timeout=1.0,
    )

    recovered = queue.get(task.id)

    assert recovered.status == TaskStatus.COMPLETED

    await queue.stop()


@pytest.mark.asyncio
async def test_queue_recovers_running_task_as_pending(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()
    executed = asyncio.Event()

    async def handler(payload: dict[str, object]) -> None:
        executed.set()

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

    await asyncio.wait_for(
        executed.wait(),
        timeout=1.0,
    )

    recovered = queue.get(task.id)

    assert recovered.status == TaskStatus.COMPLETED

    await queue.stop()


@pytest.mark.asyncio
async def test_queue_ignores_completed_tasks_on_recovery(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    executed = False

    async def handler(payload: dict[str, object]) -> None:
        nonlocal executed
        executed = True

    registry.register("test_task", handler)

    store = JSONTaskStore(
        tmp_path / "tasks.json",
    )

    task = Task(
        task_name="test_task",
        payload={},
        status=TaskStatus.COMPLETED,
    )

    store.save(task)

    queue = TaskQueue(
        registry,
        store=store,
    )

    await queue.start()

    await asyncio.sleep(0.05)

    assert executed is False
    assert queue.get(task.id).status == TaskStatus.COMPLETED

    await queue.stop()


@pytest.mark.asyncio
async def test_cancel_pending_task() -> None:
    registry = TaskRegistry()

    started = asyncio.Event()

    async def handler(payload: dict[str, object]) -> None:
        started.set()

    registry.register("test_task", handler)

    queue = TaskQueue(registry)

    await queue.start()

    first_id = await queue.submit(
        "test_task",
        {},
    )

    second_id = await queue.submit(
        "test_task",
        {},
    )

    await queue.cancel(second_id)

    assert queue.get(second_id).status == TaskStatus.CANCELLED

    await queue.wait(first_id)

    await asyncio.sleep(0.02)

    assert started.is_set()
    assert queue.get(second_id).status == TaskStatus.CANCELLED

    await queue.stop()


@pytest.mark.asyncio
async def test_cancel_running_task() -> None:
    registry = TaskRegistry()

    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handler(payload: dict[str, object]) -> None:
        started.set()

        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    registry.register("test_task", handler)

    queue = TaskQueue(registry)

    await queue.start()

    task_id = await queue.submit(
        "test_task",
        {},
    )

    await asyncio.wait_for(
        started.wait(),
        timeout=1.0,
    )

    await queue.cancel(task_id)

    task = queue.get(task_id)

    assert cancelled.is_set()
    assert task.status == TaskStatus.CANCELLED

    await queue.stop()


@pytest.mark.asyncio
async def test_cancel_completed_task_fails() -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        pass

    registry.register("test_task", handler)

    queue = TaskQueue(registry)

    await queue.start()

    task_id = await queue.submit(
        "test_task",
        {},
    )

    await queue.wait(task_id)

    with pytest.raises(
        TaskCancellationError,
        match="cannot be cancelled",
    ):
        await queue.cancel(task_id)

    await queue.stop()


@pytest.mark.asyncio
async def test_stop_stops_accepting_tasks() -> None:
    registry = TaskRegistry()

    async def handler(payload: dict[str, object]) -> None:
        pass

    registry.register("test_task", handler)

    queue = TaskQueue(registry)

    await queue.start()
    await queue.stop()

    assert queue.started is False
    assert queue.accepting_tasks is False

    with pytest.raises(
        RuntimeError,
        match="not accepting tasks",
    ):
        await queue.submit(
            "test_task",
            {},
        )


@pytest.mark.asyncio
async def test_stop_allows_running_task_to_finish() -> None:
    registry = TaskRegistry()

    started = asyncio.Event()
    finished = asyncio.Event()

    async def handler(payload: dict[str, object]) -> None:
        started.set()
        await asyncio.sleep(0.05)
        finished.set()

    registry.register("test_task", handler)

    queue = TaskQueue(registry)

    await queue.start()

    task_id = await queue.submit(
        "test_task",
        {},
    )

    await asyncio.wait_for(
        started.wait(),
        timeout=1.0,
    )

    await queue.stop()

    assert finished.is_set()
    assert queue.get(task_id).status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_stop_persists_pending_tasks(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry()

    blocker_started = asyncio.Event()
    release_blocker = asyncio.Event()

    async def handler(payload: dict[str, object]) -> None:
        if payload.get("block") is True:
            blocker_started.set()
            await release_blocker.wait()

    registry.register("test_task", handler)

    store = JSONTaskStore(
        tmp_path / "tasks.json",
    )

    queue = TaskQueue(
        registry,
        store=store,
    )

    await queue.start()

    blocker_id = await queue.submit(
        "test_task",
        {"block": True},
    )

    pending_id = await queue.submit(
        "test_task",
        {"block": False},
    )

    await asyncio.wait_for(
        blocker_started.wait(),
        timeout=1.0,
    )

    stop_task = asyncio.create_task(queue.stop())

    await asyncio.sleep(0.01)

    assert queue.accepting_tasks is False

    release_blocker.set()

    await stop_task

    persisted = store.load_all()

    persisted_by_id = {task.id: task for task in persisted}

    assert persisted_by_id[blocker_id].status == TaskStatus.COMPLETED

    assert persisted_by_id[pending_id].status == TaskStatus.PENDING


@pytest.mark.asyncio
async def test_cancelling_running_task_does_not_kill_worker() -> None:
    registry = TaskRegistry()

    started = asyncio.Event()
    cancelled = asyncio.Event()
    second_completed = asyncio.Event()

    async def handler(payload: dict[str, object]) -> None:
        if payload["name"] == "first":
            started.set()

            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled.set()
                raise
        else:
            second_completed.set()

    registry.register("test_task", handler)

    queue = TaskQueue(
        registry,
        worker_count=1,
    )

    await queue.start()

    first_id = await queue.submit(
        "test_task",
        {"name": "first"},
    )

    await asyncio.wait_for(
        started.wait(),
        timeout=1.0,
    )

    await queue.cancel(first_id)

    assert cancelled.is_set()
    assert queue.get(first_id).status == TaskStatus.CANCELLED

    second_id = await queue.submit(
        "test_task",
        {"name": "second"},
    )

    await asyncio.wait_for(
        second_completed.wait(),
        timeout=1.0,
    )

    assert queue.get(second_id).status == TaskStatus.COMPLETED

    await queue.stop()
