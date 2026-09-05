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
        """Execute a single task."""
        handler = self._registry.get(task.task_name)

        task.transition_to(TaskStatus.RUNNING)

        try:
            await handler(task.payload)
        except Exception as exc:
            if self._retry_policy.should_retry(
                exc,
                task.retry_count,
            ):
                task.increment_retry_count()
                task.transition_to(TaskStatus.RETRY_WAIT)
            else:
                task.transition_to(TaskStatus.FAILED)

                raise
        else:
            task.transition_to(TaskStatus.COMPLETED)