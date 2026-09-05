from datetime import datetime
from typing import Any

from async_task_queue.task.model import Task
from async_task_queue.task.state import TaskStatus


class TaskSerializer:
    """Serialize and deserialize Task objects."""

    @staticmethod
    def serialize(task: Task) -> dict[str, Any]:
        """Convert a Task into JSON-compatible data."""
        return {
            "id": str(task.id),
            "task_name": task.task_name,
            "payload": task.payload,
            "priority": task.priority,
            "max_retries": task.max_retries,
            "status": task.status.value,
            "retry_count": task.retry_count,
            "created_at": task.created_at.isoformat(),
            "updated_at": task.updated_at.isoformat(),
            "next_retry_at": (
                task.next_retry_at.isoformat()
                if task.next_retry_at is not None
                else None
            ),
        }

    @staticmethod
    def deserialize(data: dict[str, Any]) -> Task:
        """Reconstruct a Task from serialized data."""
        next_retry_at = data.get("next_retry_at")

        return Task(
            id=__import__("uuid").UUID(data["id"]),
            task_name=data["task_name"],
            payload=data["payload"],
            priority=data["priority"],
            max_retries=data["max_retries"],
            status=TaskStatus(data["status"]),
            retry_count=data["retry_count"],
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            next_retry_at=(
                datetime.fromisoformat(next_retry_at)
                if next_retry_at is not None
                else None
            ),
        )