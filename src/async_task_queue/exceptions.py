class TaskQueueError(Exception):
    """Base exception for all task queue errors."""


class TaskNotFoundError(TaskQueueError):
    """Raised when a requested task does not exist."""


class TaskRegistrationError(TaskQueueError):
    """Raised when task registration fails."""


class TaskAlreadyExistsError(TaskRegistrationError):
    """Raised when a task name is already registered."""


class InvalidTaskStateError(TaskQueueError):
    """Raised when an invalid task state transition is attempted."""


class TaskCancellationError(TaskQueueError):
    """Raised when a task cannot be cancelled."""


class PersistenceError(TaskQueueError):
    """Raised when task persistence or recovery fails."""