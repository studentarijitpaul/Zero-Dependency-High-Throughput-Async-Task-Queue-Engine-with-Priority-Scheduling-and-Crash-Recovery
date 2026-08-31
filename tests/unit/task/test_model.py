from datetime import datetime, timezone

import pytest

from async_task_queue.exceptions import InvalidTaskStateError
from async_task_queue.task.model import Task
from async_task_queue.task.state import TaskStatus


def test_task_has_expected_defaults() -> None:
    task = Task(
        task_name="send_email",
        payload={"to": "alice@example.com"},
    )

    assert task.task_name == "send_email"
    assert task.payload == {"to": "alice@example.com"}
    assert task.priority == 0
    assert task.max_retries == 0
    assert task.status is TaskStatus.PENDING
    assert task.retry_count == 0
    assert task.next_retry_at is None
    assert task.id is not None


def test_task_id_is_unique() -> None:
    task_a = Task(
        task_name="task_a",
        payload={},
    )
    task_b = Task(
        task_name="task_b",
        payload={},
    )

    assert task_a.id != task_b.id


def test_task_timestamps_are_utc_aware() -> None:
    task = Task(
        task_name="example",
        payload={},
    )

    assert task.created_at.tzinfo is timezone.utc
    assert task.updated_at.tzinfo is timezone.utc


def test_task_can_transition_to_valid_state() -> None:
    task = Task(
        task_name="example",
        payload={},
    )

    original_updated_at = task.updated_at

    task.transition_to(TaskStatus.RUNNING)

    assert task.status is TaskStatus.RUNNING
    assert task.updated_at >= original_updated_at


def test_task_rejects_invalid_transition() -> None:
    task = Task(
        task_name="example",
        payload={},
    )

    with pytest.raises(InvalidTaskStateError):
        task.transition_to(TaskStatus.COMPLETED)


def test_task_retry_count_can_be_incremented() -> None:
    task = Task(
        task_name="example",
        payload={},
    )

    original_updated_at = task.updated_at

    task.increment_retry_count()

    assert task.retry_count == 1
    assert task.updated_at >= original_updated_at


def test_task_can_use_custom_priority_and_retry_limit() -> None:
    task = Task(
        task_name="important_task",
        payload={"value": 42},
        priority=-1,
        max_retries=3,
    )

    assert task.priority == -1
    assert task.max_retries == 3