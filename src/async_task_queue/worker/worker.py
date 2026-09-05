from async_task_queue.task.model import Task
from async_task_queue.task.registry import TaskRegistry
from async_task_queue.task.state import TaskStatus


class Worker:
    """Executes tasks using registered handlers."""

    def __init__(self, registry: TaskRegistry) -> None:
        self._registry = registry

    async def execute(self, task: Task) -> None:
        """Execute a single task."""
        handler = self._registry.get(task.task_name)

        if not handler:
            raise ValueError(f"Handler for task '{task.task_name}' not found.")

        task.transition_to(TaskStatus.RUNNING)

        await handler(task.payload)

        task.transition_to(TaskStatus.COMPLETED)