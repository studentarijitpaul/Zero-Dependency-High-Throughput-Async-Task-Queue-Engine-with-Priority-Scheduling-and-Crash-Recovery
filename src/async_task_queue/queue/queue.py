import asyncio
from datetime import datetime, timezone
from uuid import UUID

from async_task_queue.exceptions import TaskNotFoundError
from async_task_queue.persistence.protocol import TaskStore
from async_task_queue.queue.scheduler import TaskScheduler
from async_task_queue.retry.policy import RetryPolicy
from async_task_queue.task.model import Task
from async_task_queue.task.registry import TaskRegistry
from async_task_queue.task.state import TaskStatus
from async_task_queue.types import JSONPayload
from async_task_queue.worker.worker import Worker


class TaskQueue:
    """Coordinates task submission and asynchronous execution."""

    def __init__(
        self,
        registry: TaskRegistry,
        worker_count: int = 1,
        retry_policy: RetryPolicy | None = None,
        store: TaskStore | None = None,
    ) -> None:
        if worker_count < 1:
            raise ValueError("worker_count must be >= 1")

        self._registry = registry
        self._worker_count = worker_count
        self._retry_policy = retry_policy or RetryPolicy()
        self._store = store

        self._scheduler = TaskScheduler()
        self._worker = Worker(
            registry,
            self._retry_policy,
        )

        self._tasks: dict[UUID, Task] = {}
        self._worker_tasks: list[asyncio.Task[None]] = []

        self._started = False
        self._accepting_tasks = False

    async def start(self) -> None:
        """Start the task queue and recover persisted tasks."""
        if self._started:
            return

        self._started = True
        self._accepting_tasks = True

        await self._recover_tasks()

        self._worker_tasks = [
            asyncio.create_task(self._worker_loop())
            for _ in range(self._worker_count)
        ]

        await self._schedule_recovered_tasks()

    async def stop(self) -> None:
        """Stop workers gracefully."""
        if not self._started:
            return

        self._accepting_tasks = False
        self._started = False

        for worker_task in self._worker_tasks:
            worker_task.cancel()

        await asyncio.gather(
            *self._worker_tasks,
            return_exceptions=True,
        )

        self._worker_tasks.clear()

    async def submit(
        self,
        task_name: str,
        payload: JSONPayload,
        priority: int = 0,
        max_retries: int = 0,
    ) -> UUID:
        """Submit a new task for execution.

        Tasks may be submitted before the queue is started. They are
        kept in memory and scheduled when start() is called.
        """
        self._registry.get(task_name)

        task = Task(
            task_name=task_name,
            payload=payload,
            priority=priority,
            max_retries=max_retries,
        )

        self._tasks[task.id] = task

        if self._store is not None:
            self._store.save(task)

        if self._started:
            await self._scheduler.add(task)

        return task.id

    async def wait(self, task_id: UUID) -> Task:
        """Wait until a task reaches a terminal state."""
        task = self.get(task_id)

        while task.status not in {
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }:
            await asyncio.sleep(0.01)

        return task

    async def _worker_loop(self) -> None:
        """Continuously retrieve and execute scheduled tasks."""
        while True:
            task = await self._scheduler.get_next()

            try:
                await self._worker.execute(task)

            except Exception:
                self._persist(task)
                continue

            self._persist(task)

            if task.status == TaskStatus.RETRY_WAIT:
                await self._schedule_retry(task)

    async def _schedule_retry(self, task: Task) -> None:
        """Wait for a retry delay and re-schedule the task."""
        if task.next_retry_at is None:
            return

        delay = max(
            0.0,
            (
                task.next_retry_at
                - datetime.now(timezone.utc)
            ).total_seconds(),
        )

        await asyncio.sleep(delay)

        task.transition_to(TaskStatus.PENDING)
        self._persist(task)

        await self._scheduler.add(task)

    async def _recover_tasks(self) -> None:
        """Load and recover tasks from persistent storage."""
        if self._store is None:
            return

        persisted_tasks = self._store.load_all()

        for task in persisted_tasks:
            task.recover()
            self._tasks[task.id] = task

            if task.status == TaskStatus.PENDING:
                self._persist(task)

    async def _schedule_recovered_tasks(self) -> None:
        """Schedule persisted tasks that need execution."""
        for task in self._tasks.values():
            if task.status == TaskStatus.PENDING:
                await self._scheduler.add(task)

            elif task.status == TaskStatus.RETRY_WAIT:
                asyncio.create_task(
                    self._schedule_retry(task)
                )

    def _persist(self, task: Task) -> None:
        """Persist a task if a store is configured."""
        if self._store is not None:
            self._store.save(task)

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