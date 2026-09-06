from async_task_queue.exceptions import TaskAlreadyExistsError, TaskNotFoundError
from async_task_queue.types import TaskHandler


class TaskRegistry:
    """Maps task names to their execution handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(self, name: str, handler: TaskHandler) -> None:
        """Register a task handler for a given task name."""
        if name in self._handlers:
            raise TaskAlreadyExistsError(f"Task handler already registered :{name!r}")
        self._handlers[name] = handler

    def get(self, name: str) -> TaskHandler:
        """Return the Handler registered under aa task name ."""
        try:
            return self._handlers[name]
        except KeyError:
            raise TaskNotFoundError(f"no task handler registered for : {name!r}")

    def unregister(self, name: str) -> None:
        """Remove a registered task handler"""
        try:
            del self._handlers[name]
        except KeyError as exc:
            raise TaskNotFoundError(
                f"no task handler registered for : {name!r}"
            ) from exc

    def contains(self, name: str) -> bool:
        """Return whether a task handler is registered"""

        return name in self._handlers
