from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import UUID, uuid4

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
        validate_transition(self.status, target)
        self.status = target
        self.updated_at = datetime.now(timezone.utc)

    def increment_retry_count(self) -> None:
        """Increment the number of retries consumed by this task."""
        self.retry_count += 1
        self.updated_at = datetime.now(timezone.utc)

    def recover(self) -> None:
        """Recover a task after a process restart.

        A task that was RUNNING when the process crashed may have
        completed its side effect but not persisted its completion.
        It must therefore be returned to PENDING for at-least-once
        execution.

        This is intentionally separate from transition_to() because
        RUNNING -> PENDING is not a normal runtime transition.
        """
        if self.status != TaskStatus.RUNNING:
            return

        self.status = TaskStatus.PENDING
        self.updated_at = datetime.now(timezone.utc)