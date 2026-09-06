from collections.abc import Awaitable, Callable
from typing import TypeAlias

JSONValue: TypeAlias = (
    None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]
)

JSONPayload: TypeAlias = dict[str, JSONValue]

TaskHandler: TypeAlias = Callable[[JSONPayload], Awaitable[None]]
