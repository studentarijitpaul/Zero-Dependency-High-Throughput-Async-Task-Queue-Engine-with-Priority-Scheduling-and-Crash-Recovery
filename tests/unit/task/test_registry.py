import pytest

from async_task_queue.exceptions import (
    TaskAlreadyExistsError,
    TaskNotFoundError,
)
from async_task_queue.task.registry import TaskRegistry
from async_task_queue.types import JSONPayload


async def send_email(payload: JSONPayload) -> None:
    print(payload)


async def resize_image(payload: JSONPayload) -> None:
    print(payload)


def test_register_and_get_handler() -> None:
    registry = TaskRegistry()

    registry.register("send_email", send_email)

    assert registry.get("send_email") is send_email


def test_register_multiple_handlers() -> None:
    registry = TaskRegistry()

    registry.register("send_email", send_email)
    registry.register("resize_image", resize_image)

    assert registry.get("send_email") is send_email
    assert registry.get("resize_image") is resize_image


def test_duplicate_registration_raises_error() -> None:
    registry = TaskRegistry()

    registry.register("send_email", send_email)

    with pytest.raises(TaskAlreadyExistsError):
        registry.register("send_email", resize_image)


def test_get_missing_handler_raises_error() -> None:
    registry = TaskRegistry()

    with pytest.raises(TaskNotFoundError):
        registry.get("does_not_exist")


def test_unregister_handler() -> None:
    registry = TaskRegistry()

    registry.register("send_email", send_email)

    registry.unregister("send_email")

    assert not registry.contains("send_email")


def test_unregister_missing_handler_raises_error() -> None:
    registry = TaskRegistry()

    with pytest.raises(TaskNotFoundError):
        registry.unregister("does_not_exist")


def test_contains_returns_true_for_registered_handler() -> None:
    registry = TaskRegistry()

    registry.register("send_email", send_email)

    assert registry.contains("send_email")


def test_contains_returns_false_for_unknown_handler() -> None:
    registry = TaskRegistry()

    assert not registry.contains("does_not_exist")