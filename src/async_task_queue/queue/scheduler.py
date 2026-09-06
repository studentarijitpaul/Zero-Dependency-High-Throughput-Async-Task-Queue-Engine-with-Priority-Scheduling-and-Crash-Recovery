import asyncio
import heapq
from dataclasses import dataclass, field
from uuid import UUID

from async_task_queue.task.model import Task


@dataclass(order=True, slots=True)
class _ScheduledTask:
    """Internal heap entry used by the scheduler."""

    priority: int
    sequence: int
    task_id: UUID = field(compare=True)
    task: Task = field(compare=False)


class TaskScheduler:
    """Schedules tasks according to priority and FIFO order."""

    def __init__(self) -> None:
        self._heap: list[_ScheduledTask] = []
        self._sequence = 0
        self._cancelled: set[UUID] = set()
        self._closed = False
        self._condition = asyncio.Condition()

    async def add(self, task: Task) -> None:
        """Add a task and wake a waiting worker."""
        async with self._condition:
            if self._closed:
                raise RuntimeError("Scheduler is closed")

            entry = _ScheduledTask(
                priority=task.priority,
                sequence=self._sequence,
                task_id=task.id,
                task=task,
            )

            self._sequence += 1
            heapq.heappush(self._heap, entry)
            self._condition.notify()

    async def get_next(self) -> Task:
        """Wait for and return the highest-priority task."""
        async with self._condition:
            while True:
                await self._condition.wait_for(lambda: bool(self._heap) or self._closed)

                if not self._heap:
                    raise RuntimeError("Scheduler is closed")

                entry = heapq.heappop(self._heap)

                if entry.task_id in self._cancelled:
                    self._cancelled.remove(entry.task_id)
                    continue

                return entry.task

    async def cancel(self, task_id: UUID) -> None:
        """Mark a scheduled task as cancelled."""
        async with self._condition:
            self._cancelled.add(task_id)
            self._condition.notify_all()

    async def close(self) -> None:
        """Close the scheduler and wake waiting workers."""
        async with self._condition:
            self._closed = True
            self._condition.notify_all()

    async def shutdown(self) -> list[Task]:
        """Close the scheduler and atomically drain waiting tasks."""
        async with self._condition:
            self._closed = True

            tasks: list[Task] = []

            while self._heap:
                entry = heapq.heappop(self._heap)

                if entry.task_id in self._cancelled:
                    self._cancelled.remove(entry.task_id)
                    continue

                tasks.append(entry.task)

            self._condition.notify_all()

            return tasks

    def __len__(self) -> int:
        """Return the number of scheduled tasks."""
        return len(self._heap)

    def empty(self) -> bool:
        """Return whether the scheduler has no tasks."""
        return not self._heap

    @property
    def closed(self) -> bool:
        """Return whether the scheduler is closed."""
        return self._closed
