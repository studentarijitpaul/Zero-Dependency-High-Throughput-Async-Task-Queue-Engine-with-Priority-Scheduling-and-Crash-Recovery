import asyncio

import pytest

from async_task_queue.queue.scheduler import TaskScheduler
from async_task_queue.task.model import Task


def make_task(name: str, priority: int) -> Task:
    return Task(
        task_name=name,
        payload={},
        priority=priority,
    )


@pytest.mark.asyncio
async def test_scheduler_starts_empty() -> None:
    scheduler = TaskScheduler()

    assert scheduler.empty()
    assert len(scheduler) == 0
    assert scheduler.closed is False


@pytest.mark.asyncio
async def test_scheduler_orders_by_priority() -> None:
    scheduler = TaskScheduler()

    low = make_task("low", priority=5)
    high = make_task("high", priority=0)
    medium = make_task("medium", priority=2)

    await scheduler.add(low)
    await scheduler.add(high)
    await scheduler.add(medium)

    assert await scheduler.get_next() is high
    assert await scheduler.get_next() is medium
    assert await scheduler.get_next() is low


@pytest.mark.asyncio
async def test_scheduler_preserves_fifo_for_equal_priority() -> None:
    scheduler = TaskScheduler()

    first = make_task("first", priority=1)
    second = make_task("second", priority=1)
    third = make_task("third", priority=1)

    await scheduler.add(first)
    await scheduler.add(second)
    await scheduler.add(third)

    assert await scheduler.get_next() is first
    assert await scheduler.get_next() is second
    assert await scheduler.get_next() is third


@pytest.mark.asyncio
async def test_priority_takes_precedence_over_fifo() -> None:
    scheduler = TaskScheduler()

    first = make_task("first", priority=5)
    second = make_task("second", priority=0)

    await scheduler.add(first)
    await scheduler.add(second)

    assert await scheduler.get_next() is second
    assert await scheduler.get_next() is first


@pytest.mark.asyncio
async def test_length_decreases_when_tasks_are_removed() -> None:
    scheduler = TaskScheduler()

    await scheduler.add(make_task("one", priority=1))
    await scheduler.add(make_task("two", priority=2))

    assert len(scheduler) == 2

    await scheduler.get_next()

    assert len(scheduler) == 1

    await scheduler.get_next()

    assert scheduler.empty()


@pytest.mark.asyncio
async def test_get_next_waits_until_task_is_added() -> None:
    scheduler = TaskScheduler()
    task = make_task("delayed", priority=1)

    async def add_later() -> None:
        await asyncio.sleep(0.01)
        await scheduler.add(task)

    producer = asyncio.create_task(add_later())

    result = await scheduler.get_next()

    await producer

    assert result is task


@pytest.mark.asyncio
async def test_cancelled_task_is_skipped() -> None:
    scheduler = TaskScheduler()

    cancelled = make_task(
        "cancelled",
        priority=0,
    )
    normal = make_task(
        "normal",
        priority=1,
    )

    await scheduler.add(cancelled)
    await scheduler.add(normal)

    await scheduler.cancel(cancelled.id)

    result = await scheduler.get_next()

    assert result is normal


@pytest.mark.asyncio
async def test_close_wakes_waiting_worker() -> None:
    scheduler = TaskScheduler()

    worker = asyncio.create_task(scheduler.get_next())

    await asyncio.sleep(0)

    await scheduler.close()

    with pytest.raises(
        RuntimeError,
        match="Scheduler is closed",
    ):
        await worker

    assert scheduler.closed is True


@pytest.mark.asyncio
async def test_add_after_close_fails() -> None:
    scheduler = TaskScheduler()

    await scheduler.close()

    with pytest.raises(
        RuntimeError,
        match="Scheduler is closed",
    ):
        await scheduler.add(make_task("test", priority=0))
