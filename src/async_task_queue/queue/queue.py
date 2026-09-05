from uuid import UUID, uuid4

from async_task_queue.exceptions import TaskNotFoundError
from async_task_queue.task.model import Task
from async_task_queue.task.registry import TaskRegistry
from async_task_queue.task.state import TaskStatus
from async_task_queue.types import JSONPayload


class TaskQueue:
    """Coordinates task submission and execution."""

    def __init__(
        self,
        registry: TaskRegistry,
        worker_count: int = 1,
    ) -> None:
        if worker_count < 1:
            raise ValueError("worker_count must be >= 1")

        self._registry = registry
        self._worker_count = worker_count
        self._tasks: dict[UUID, Task] = {}
        self._started = False
        self._accepting_tasks = False

    async def start(self) -> None:
        """Start the task queue."""
        if self._started:
            return

        self._started = True
        self._accepting_tasks = True

    async def stop(self) -> None:
        """Stop accepting new tasks."""
        if not self._started:
            return

        self._accepting_tasks = False
        self._started = False

    async def submit(
        self,
        task_name: str,
        payload: JSONPayload,
        priority: int = 0,
        max_retries: int = 0,
    ) -> UUID:
        """Submit a new task."""
        if not self._accepting_tasks:
            raise RuntimeError("Task queue is not running")

        # Validate that a handler exists before accepting the task.
        self._registry.get(task_name)

        task = Task(
            task_name=task_name,
            payload=payload,
            priority=priority,
            max_retries=max_retries,
        )

        self._tasks[task.id] = task

        return task.id

    def get(self, task_id: UUID) -> Task:
        """Return a task by ID."""
        try:
            return self._tasks[task_id]
        except KeyError as exc:
            raise TaskNotFoundError(
                f"Task not found: {task_id}"
            ) from exc

    @property
    def worker_count(self) -> int:
        """Return the configured worker count."""
        return self._worker_count

    @property
    def started(self) -> bool:
        """Return whether the queue is running."""
        return self._started