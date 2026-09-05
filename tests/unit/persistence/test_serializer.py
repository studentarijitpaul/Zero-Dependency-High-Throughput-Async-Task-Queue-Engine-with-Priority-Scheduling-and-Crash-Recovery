from datetime import datetime, timezone
from uuid import uuid4

from async_task_queue.persistence.serializer import TaskSerializer
from async_task_queue.task.model import Task
from async_task_queue.task.state import TaskStatus


def test_serialize_task() -> None:
    task_id = uuid4()
    created_at = datetime.now(timezone.utc)
    updated_at = datetime.now(timezone.utc)

    task = Task(
        id=task_id,
        task_name="send_email",
        payload={"to": "alice@example.com"},
        priority=1,
        max_retries=3,
        status=TaskStatus.PENDING,
        retry_count=0,
        created_at=created_at,
        updated_at=updated_at,
    )

    data = TaskSerializer.serialize(task)

    assert data["id"] == str(task_id)
    assert data["task_name"] == "send_email"
    assert data["payload"] == {"to": "alice@example.com"}
    assert data["priority"] == 1
    assert data["max_retries"] == 3
    assert data["status"] == "pending"
    assert data["retry_count"] == 0
    assert data["created_at"] == created_at.isoformat()
    assert data["updated_at"] == updated_at.isoformat()
    assert data["next_retry_at"] is None


def test_serialize_task_with_retry_time() -> None:
    retry_at = datetime.now(timezone.utc)

    task = Task(
        task_name="send_email",
        payload={"to": "alice@example.com"},
        next_retry_at=retry_at,
    )

    data = TaskSerializer.serialize(task)

    assert data["next_retry_at"] == retry_at.isoformat()


def test_deserialize_task() -> None:
    task_id = uuid4()
    created_at = datetime.now(timezone.utc)
    updated_at = datetime.now(timezone.utc)

    data = {
        "id": str(task_id),
        "task_name": "send_email",
        "payload": {"to": "alice@example.com"},
        "priority": 2,
        "max_retries": 3,
        "status": "retry_wait",
        "retry_count": 1,
        "created_at": created_at.isoformat(),
        "updated_at": updated_at.isoformat(),
        "next_retry_at": updated_at.isoformat(),
    }

    task = TaskSerializer.deserialize(data)

    assert task.id == task_id
    assert task.task_name == "send_email"
    assert task.payload == {"to": "alice@example.com"}
    assert task.priority == 2
    assert task.max_retries == 3
    assert task.status == TaskStatus.RETRY_WAIT
    assert task.retry_count == 1
    assert task.created_at == created_at
    assert task.updated_at == updated_at
    assert task.next_retry_at == updated_at


def test_serialize_deserialize_round_trip() -> None:
    task = Task(
        task_name="process_data",
        payload={"value": 42},
        priority=5,
        max_retries=3,
        retry_count=1,
    )

    restored = TaskSerializer.deserialize(
        TaskSerializer.serialize(task)
    )

    assert restored.id == task.id
    assert restored.task_name == task.task_name
    assert restored.payload == task.payload
    assert restored.priority == task.priority
    assert restored.max_retries == task.max_retries
    assert restored.status == task.status
    assert restored.retry_count == task.retry_count
    assert restored.created_at == task.created_at
    assert restored.updated_at == task.updated_at
    assert restored.next_retry_at == task.next_retry_at