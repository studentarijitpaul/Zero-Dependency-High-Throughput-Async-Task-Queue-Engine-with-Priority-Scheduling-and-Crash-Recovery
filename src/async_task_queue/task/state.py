from enum import StrEnum

from async_task_queue.exceptions import InvalidTaskStateError


class TaskStatus(StrEnum):
    """Lifecycle states for a task."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    RETRY_WAIT = "retry_wait"
    FAILED = "failed"
    CANCELLED = "cancelled"


_ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset(
        {
            TaskStatus.RUNNING,
            TaskStatus.CANCELLED,
        }
    ),
    TaskStatus.RUNNING: frozenset(
        {
            TaskStatus.COMPLETED,
            TaskStatus.RETRY_WAIT,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }
    ),
    TaskStatus.RETRY_WAIT: frozenset(
        {
            TaskStatus.PENDING,
            TaskStatus.CANCELLED,
        }
    ),
    TaskStatus.COMPLETED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
}


def can_transition(
    current: TaskStatus,
    target: TaskStatus,
) -> bool:
    """Return whether a task can move from current to target state."""
    return target in _ALLOWED_TRANSITIONS[current]


def validate_transition(
    current: TaskStatus,
    target: TaskStatus,
) -> None:
    """Raise an error if a state transition is not allowed."""
    if not can_transition(current, target):
        raise InvalidTaskStateError(
            f"Invalid task state transition: "
            f"{current.value} -> {target.value}"
        )