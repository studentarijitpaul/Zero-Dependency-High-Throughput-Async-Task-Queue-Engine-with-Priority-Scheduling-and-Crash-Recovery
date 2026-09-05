import json
import os
from pathlib import Path
from uuid import UUID

from async_task_queue.exceptions import PersistenceError
from async_task_queue.persistence.serializer import TaskSerializer
from async_task_queue.task.model import Task


class JSONTaskStore:
    """Persist tasks in a JSON file."""

    _VERSION = 1

    def __init__(self, path: Path) -> None:
        self._path = path

    def save(self, task: Task) -> None:
        """Save or update a task."""
        tasks = list(self.load_all())

        for index, existing in enumerate(tasks):
            if existing.id == task.id:
                tasks[index] = task
                break
        else:
            tasks.append(task)

        self._write(tasks)

    def load_all(self) -> list[Task]:
        """Load all tasks from disk."""
        if not self._path.exists():
            return []

        try:
            with self._path.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            raise PersistenceError(
                f"Failed to read task store: {self._path}"
            ) from exc

        if not isinstance(data, dict):
            raise PersistenceError("Task store root must be an object.")

        if data.get("version") != self._VERSION:
            raise PersistenceError(
                f"Unsupported task store version: "
                f"{data.get('version')!r}"
            )

        tasks_data = data.get("tasks")

        if not isinstance(tasks_data, list):
            raise PersistenceError(
                "Task store 'tasks' must be a list."
            )

        try:
            return [
                TaskSerializer.deserialize(task_data)
                for task_data in tasks_data
            ]
        except (KeyError, TypeError, ValueError) as exc:
            raise PersistenceError(
                "Task store contains invalid task data."
            ) from exc

    def delete(self, task_id: str) -> None:
        """Delete a task from the store."""
        try:
            parsed_id = UUID(task_id)
        except ValueError as exc:
            raise PersistenceError(
                f"Invalid task ID: {task_id!r}"
            ) from exc

        tasks = [
            task
            for task in self.load_all()
            if task.id != parsed_id
        ]

        self._write(tasks)

    def _write(self, tasks: list[Task]) -> None:
        """Atomically write tasks to disk."""
        self._path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        data = {
            "version": self._VERSION,
            "tasks": [
                TaskSerializer.serialize(task)
                for task in tasks
            ],
        }

        temporary_path = self._path.with_suffix(
            self._path.suffix + ".tmp"
        )

        try:
            with temporary_path.open(
                "w",
                encoding="utf-8",
            ) as file:
                json.dump(
                    data,
                    file,
                    indent=2,
                )
                file.flush()
                os.fsync(file.fileno())

            os.replace(temporary_path, self._path)

        except OSError as exc:
            raise PersistenceError(
                f"Failed to write task store: {self._path}"
            ) from exc

        finally:
            if temporary_path.exists():
                temporary_path.unlink(missing_ok=True)