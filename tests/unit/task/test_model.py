from datetime import datetime, timezone
from uuid import uuid4

import pytest

from async_task_queue.task.model import Task
from async_task_queue.task.state import TaskStatus


def test_task_defaults() -> None:
    task = Task(
        task_name="test_task",
        payload={"value": 42},
    )

    assert task.task_name == "test_task"
    assert task.payload == {"value": 42}
    assert task.priority == 0
    assert task.max_retries == 0
    assert task.status == TaskStatus.PENDING
    assert task.retry_count == 0
    assert task.next_retry_at is None


def test_task_can_transition() -> None:
    task = Task(
        task_name="test_task",
        payload={},
    )

    before = task.updated_at

    task.transition_to(TaskStatus.RUNNING)

    assert task.status == TaskStatus.RUNNING
    assert task.updated_at >= before


def test_task_can_increment_retry_count() -> None:
    task = Task(
        task_name="test_task",
        payload={},
    )

    before = task.updated_at

    task.increment_retry_count()

    assert task.retry_count == 1
    assert task.updated_at >= before


def test_task_recover_changes_running_to_pending() -> None:
    task = Task(
        task_name="test_task",
        payload={},
        status=TaskStatus.RUNNING,
    )

    before = task.updated_at

    task.recover()

    assert task.status == TaskStatus.PENDING
    assert task.updated_at >= before


@pytest.mark.parametrize(
    "status",
    [
        TaskStatus.PENDING,
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.RETRY_WAIT,
    ],
)
def test_task_recover_does_nothing_for_non_running(
    status: TaskStatus,
) -> None:
    task = Task(
        task_name="test_task",
        payload={},
        status=status,
    )

    original_updated_at = task.updated_at

    task.recover()

    assert task.status == status
    assert task.updated_at == original_updated_at


def test_task_supports_custom_values() -> None:
    task_id = uuid4()
    created_at = datetime.now(timezone.utc)

    task = Task(
        task_name="send_email",
        payload={"to": "alice@example.com"},
        priority=5,
        max_retries=3,
        id=task_id,
        created_at=created_at,
        updated_at=created_at,
    )

    assert task.id == task_id
    assert task.priority == 5
    assert task.max_retries == 3
    assert task.created_at == created_at