from collections.abc import Sequence
from typing import Protocol

from async_task_queue.task.model import Task

class TaskStore(Protocol):
    """Interface for persisting tasks."""

    def save(self, task:Task)-> None:
        """persist a task"""
        ...

    def load_all(self)-> Sequence[Task]:
        """load all persistence tasks"""
        ...
    def delete(self, task: Task)-> None:
        """delete a Persisted task"""

        