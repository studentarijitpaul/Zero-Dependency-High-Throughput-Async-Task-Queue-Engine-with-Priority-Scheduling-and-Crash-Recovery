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
        self._condition = asyncio.Condition()

    async def add(self, task: Task) -> None:
        """Add a task and wake a waiting worker."""
        async with self._condition:
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
            await self._condition.wait_for(
                lambda: bool(self._heap)
            )

            entry = heapq.heappop(self._heap)
            return entry.task

    def __len__(self) -> int:
        """Return the number of scheduled tasks."""
        return len(self._heap)

    def empty(self) -> bool:
        """Return whether the scheduler has no tasks."""
        return not self._heap