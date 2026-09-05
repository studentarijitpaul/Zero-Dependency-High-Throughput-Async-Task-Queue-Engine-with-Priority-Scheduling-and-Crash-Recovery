from async_task_queue.queue.scheduler import TaskScheduler
from async_task_queue.task.model import Task


def make_task(
    name: str,
    priority: int,
) -> Task:
    return Task(
        task_name=name,
        payload={},
        priority=priority,
    )


def test_scheduler_starts_empty() -> None:
    scheduler = TaskScheduler()

    assert scheduler.empty()
    assert len(scheduler) == 0


def test_scheduler_orders_by_priority() -> None:
    scheduler = TaskScheduler()

    low = make_task("low", priority=5)
    high = make_task("high", priority=0)
    medium = make_task("medium", priority=2)

    scheduler.add(low)
    scheduler.add(high)
    scheduler.add(medium)

    assert scheduler.get_next() is high
    assert scheduler.get_next() is medium
    assert scheduler.get_next() is low


def test_scheduler_preserves_fifo_for_equal_priority() -> None:
    scheduler = TaskScheduler()

    first = make_task("first", priority=1)
    second = make_task("second", priority=1)
    third = make_task("third", priority=1)

    scheduler.add(first)
    scheduler.add(second)
    scheduler.add(third)

    assert scheduler.get_next() is first
    assert scheduler.get_next() is second
    assert scheduler.get_next() is third


def test_priority_takes_precedence_over_fifo() -> None:
    scheduler = TaskScheduler()

    first = make_task("first", priority=5)
    second = make_task("second", priority=0)

    scheduler.add(first)
    scheduler.add(second)

    assert scheduler.get_next() is second
    assert scheduler.get_next() is first


def test_length_decreases_when_tasks_are_removed() -> None:
    scheduler = TaskScheduler()

    scheduler.add(make_task("one", priority=1))
    scheduler.add(make_task("two", priority=2))

    assert len(scheduler) == 2

    scheduler.get_next()

    assert len(scheduler) == 1

    scheduler.get_next()

    assert scheduler.empty()