import json
from uuid import uuid4

import pytest

from async_task_queue.exceptions import PersistenceError
from async_task_queue.persistence.json_store import JSONTaskStore
from async_task_queue.task.model import Task


def test_load_empty_store(tmp_path) -> None:
    store = JSONTaskStore(tmp_path / "tasks.json")

    assert store.load_all() == []


def test_save_and_load_task(tmp_path) -> None:
    store = JSONTaskStore(tmp_path / "tasks.json")

    task = Task(
        task_name="send_email",
        payload={"to": "alice@example.com"},
        priority=1,
        max_retries=3,
    )

    store.save(task)

    loaded = store.load_all()

    assert len(loaded) == 1
    assert loaded[0].id == task.id
    assert loaded[0].task_name == task.task_name
    assert loaded[0].payload == task.payload


def test_save_updates_existing_task(tmp_path) -> None:
    store = JSONTaskStore(tmp_path / "tasks.json")

    task = Task(
        task_name="send_email",
        payload={"to": "alice@example.com"},
    )

    store.save(task)

    task.priority = 10
    store.save(task)

    loaded = store.load_all()

    assert len(loaded) == 1
    assert loaded[0].priority == 10


def test_delete_task(tmp_path) -> None:
    store = JSONTaskStore(tmp_path / "tasks.json")

    task = Task(
        task_name="send_email",
        payload={"to": "alice@example.com"},
    )

    store.save(task)
    store.delete(str(task.id))

    assert store.load_all() == []


def test_store_contains_version(tmp_path) -> None:
    path = tmp_path / "tasks.json"
    store = JSONTaskStore(path)

    store.save(
        Task(
            task_name="test",
            payload={},
        )
    )

    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    assert data["version"] == 1
    assert isinstance(data["tasks"], list)


def test_corrupt_json_raises_persistence_error(tmp_path) -> None:
    path = tmp_path / "tasks.json"
    path.write_text(
        '{"version": 1, "tasks": [',
        encoding="utf-8",
    )

    store = JSONTaskStore(path)

    with pytest.raises(PersistenceError):
        store.load_all()


def test_invalid_root_raises_persistence_error(tmp_path) -> None:
    path = tmp_path / "tasks.json"
    path.write_text(
        "[]",
        encoding="utf-8",
    )

    store = JSONTaskStore(path)

    with pytest.raises(PersistenceError):
        store.load_all()


def test_invalid_version_raises_persistence_error(tmp_path) -> None:
    path = tmp_path / "tasks.json"

    path.write_text(
        json.dumps(
            {
                "version": 999,
                "tasks": [],
            }
        ),
        encoding="utf-8",
    )

    store = JSONTaskStore(path)

    with pytest.raises(PersistenceError):
        store.load_all()


def test_invalid_tasks_value_raises_persistence_error(tmp_path) -> None:
    path = tmp_path / "tasks.json"

    path.write_text(
        json.dumps(
            {
                "version": 1,
                "tasks": {},
            }
        ),
        encoding="utf-8",
    )

    store = JSONTaskStore(path)

    with pytest.raises(PersistenceError):
        store.load_all()


def test_invalid_task_data_raises_persistence_error(tmp_path) -> None:
    path = tmp_path / "tasks.json"

    path.write_text(
        json.dumps(
            {
                "version": 1,
                "tasks": [
                    {
                        "id": str(uuid4()),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    store = JSONTaskStore(path)

    with pytest.raises(PersistenceError):
        store.load_all()


def test_delete_invalid_task_id_raises_persistence_error(tmp_path) -> None:
    store = JSONTaskStore(tmp_path / "tasks.json")

    with pytest.raises(PersistenceError):
        store.delete("not-a-uuid")
