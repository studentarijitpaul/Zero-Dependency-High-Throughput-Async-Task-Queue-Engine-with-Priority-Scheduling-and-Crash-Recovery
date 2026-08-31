from typing import TypeAlias


Jsonprimitive: TypeAlias = str | int|float|bool|None

JSONPayload: TypeAlias =(
    Jsonprimitive
    | list['JSONValue']
    | dict[str, 'JSONValue']
)

JSONValue: TypeAlias = Jsonprimitive | JSONPayload