import asyncio
from datetime import datetime, timezone
from uuid import UUID

from async_task_queue.exceptions import (
    TaskCancellationError,
    TaskNotFoundError,
)
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
        self._running_tasks: dict[UUID, asyncio.Task[None]] = {}
        self._retry_tasks: dict[UUID, asyncio.Task[None]] = {}

        self._started = False
        self._accepting_tasks = True
        self._stopping = False

    async def start(self) -> None:
        """Start the task queue and recover persisted tasks."""
        if self._started:
            return

        self._scheduler = TaskScheduler()
        self._started = True
        self._accepting_tasks = True
        self._stopping = False

        await self._recover_tasks()

        self._worker_tasks = [
            asyncio.create_task(self._worker_loop())
            for _ in range(self._worker_count)
        ]

        await self._schedule_recovered_tasks()

        for task in self._tasks.values():
            if task.status == TaskStatus.PENDING:
                await self._scheduler.add(task)

    async def stop(self) -> None:
        """Gracefully stop the task queue."""
        if not self._started:
            self._accepting_tasks = False
            self._stopping = False
            return

        self._accepting_tasks = False
        self._stopping = True

        pending_tasks = await self._scheduler.shutdown()

        for task in pending_tasks:
            self._persist(task)

        retry_tasks = list(self._retry_tasks.values())

        for retry_task in retry_tasks:
            retry_task.cancel()

        if retry_tasks:
            await asyncio.gather(
                *retry_tasks,
                return_exceptions=True,
            )

        self._retry_tasks.clear()

        if self._running_tasks:
            await asyncio.gather(
                *self._running_tasks.values(),
                return_exceptions=True,
            )

        for task in self._tasks.values():
            self._persist(task)

        await asyncio.gather(
            *self._worker_tasks,
            return_exceptions=True,
        )

        self._worker_tasks.clear()
        self._running_tasks.clear()
        self._started = False
        self._stopping = False

    async def submit(
        self,
        task_name: str,
        payload: JSONPayload,
        priority: int = 0,
        max_retries: int = 0,
    ) -> UUID:
        """Submit a new task for execution."""
        if not self._accepting_tasks:
            raise RuntimeError(
                "Task queue is not accepting tasks"
            )

        self._registry.get(task_name)

        if max_retries < 0:
            raise ValueError(
                "max_retries must be >= 0"
            )

        task = Task(
            task_name=task_name,
            payload=payload,
            priority=priority,
            max_retries=max_retries,
        )

        self._tasks[task.id] = task
        self._persist(task)

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

    async def cancel(self, task_id: UUID) -> None:
        """Cancel a pending, retrying, or running task."""
        task = self.get(task_id)

        if task.status == TaskStatus.PENDING:
            task.transition_to(TaskStatus.CANCELLED)
            await self._scheduler.cancel(task.id)
            self._persist(task)
            return

        if task.status == TaskStatus.RETRY_WAIT:
            task.transition_to(TaskStatus.CANCELLED)

            retry_task = self._retry_tasks.pop(
                task.id,
                None,
            )

            if retry_task is not None:
                retry_task.cancel()

                try:
                    await retry_task
                except asyncio.CancelledError:
                    pass

            self._persist(task)
            return

        if task.status == TaskStatus.RUNNING:
            execution = self._running_tasks.get(task.id)

            if execution is None:
                raise TaskCancellationError(
                    f"Task {task.id} is running but "
                    "cannot be cancelled."
                )

            execution.cancel()

            try:
                await execution
            except asyncio.CancelledError:
                pass

            self._persist(task)
            return

        raise TaskCancellationError(
            f"Task {task.id} cannot be cancelled from "
            f"state {task.status.value!r}."
        )

    async def _worker_loop(self) -> None:
        """Continuously retrieve and execute scheduled tasks."""
        while True:
            try:
                task = await self._scheduler.get_next()
            except RuntimeError as exc:
                if str(exc) == "Scheduler is closed":
                    return
                raise

            if self._stopping:
                self._persist(task)
                continue

            execution = asyncio.create_task(
                self._execute_task(task)
            )

            self._running_tasks[task.id] = execution

            try:
                await execution
            except asyncio.CancelledError:
                # Execution task cancellation must not kill worker.
                pass
            finally:
                self._running_tasks.pop(
                    task.id,
                    None,
                )

    async def _execute_task(self, task: Task) -> None:
        """Execute one task and handle its result."""
        try:
            await self._worker.execute(task)
        except asyncio.CancelledError:
            self._persist(task)
            raise
        except Exception:
            self._persist(task)
            return

        self._persist(task)

        if task.status == TaskStatus.RETRY_WAIT:
            self._start_retry_task(task)

    def _start_retry_task(self, task: Task) -> None:
        """Start a background retry timer."""
        retry_task = asyncio.create_task(
            self._schedule_retry(task)
        )

        self._retry_tasks[task.id] = retry_task

        retry_task.add_done_callback(
            lambda _: self._retry_tasks.pop(
                task.id,
                None,
            )
        )

    async def _schedule_retry(self, task: Task) -> None:
        """Wait for retry delay and re-schedule the task."""
        if task.next_retry_at is None:
            return

        delay = max(
            0.0,
            (
                task.next_retry_at
                - datetime.now(timezone.utc)
            ).total_seconds(),
        )

        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return

        if not self._started:
            return

        if task.status != TaskStatus.RETRY_WAIT:
            return

        task.transition_to(TaskStatus.PENDING)
        self._persist(task)

        if self._accepting_tasks:
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
        """Schedule persisted tasks that need retry handling."""
        for task in self._tasks.values():
            if task.status == TaskStatus.RETRY_WAIT:
                self._start_retry_task(task)

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

    @property
    def accepting_tasks(self) -> bool:
        """Return whether the queue accepts new tasks."""
        return self._accepting_tasks