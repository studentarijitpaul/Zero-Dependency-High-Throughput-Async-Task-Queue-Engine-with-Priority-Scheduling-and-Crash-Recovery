from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import UUID, uuid4

from async_task_queue.exceptions import InvalidTaskStateError
from async_task_queue.task.state import TaskStatus, validate_transition
from async_task_queue.types import JSONPayload


@dataclass(slots=True)
class Task:
    """Represents a unit of work managed by the task queue."""

    task_name: str
    payload: JSONPayload
    priority: int = 0
    max_retries: int = 0

    id: UUID = field(default_factory=uuid4)
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0

    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    next_retry_at: datetime | None = None

    def transition_to(self, target: TaskStatus) -> None:
        """Move the task to a valid target state."""
        try:
            validate_transition(self.status, target)
        except InvalidTaskStateError:
            raise

        self.status = target
        self.updated_at = datetime.now(timezone.utc)

    def increment_retry_count(self) -> None:
        """Increment the number of retries consumed by this task."""
        self.retry_count += 1
        self.updated_at = datetime.now(timezone.utc)