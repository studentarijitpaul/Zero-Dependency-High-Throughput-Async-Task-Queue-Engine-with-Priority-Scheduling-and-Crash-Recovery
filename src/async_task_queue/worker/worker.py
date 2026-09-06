import asyncio
from datetime import datetime, timedelta, timezone

from async_task_queue.retry.policy import RetryPolicy
from async_task_queue.task.model import Task
from async_task_queue.task.registry import TaskRegistry
from async_task_queue.task.state import TaskStatus


class Worker:
    """Executes tasks using registered handlers."""

    def __init__(
        self,
        registry: TaskRegistry,
        retry_policy: RetryPolicy,
    ) -> None:
        self._registry = registry
        self._retry_policy = retry_policy

    async def execute(self, task: Task) -> None:
        """Execute a task and update its lifecycle state."""
        task.transition_to(TaskStatus.RUNNING)

        handler = self._registry.get(task.task_name)

        try:
            await handler(task.payload)

        except asyncio.CancelledError:
            task.transition_to(TaskStatus.CANCELLED)
            raise

        except Exception as exception:
            if self._should_retry(task, exception):
                self._schedule_retry(task)
                return

            task.transition_to(TaskStatus.FAILED)
            raise

        task.next_retry_at = None
        task.transition_to(TaskStatus.COMPLETED)

    def _should_retry(
        self,
        task: Task,
        exception: Exception,
    ) -> bool:
        """Return whether the task should be retried."""
        if task.retry_count >= task.max_retries:
            return False

        return self._retry_policy.should_retry(exception)

    def _schedule_retry(self, task: Task) -> None:
        """Move a failed task into retry-wait state."""
        task.increment_retry_count()

        delay = self._retry_policy.get_delay(task.retry_count)

        task.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay)

        task.transition_to(TaskStatus.RETRY_WAIT)
