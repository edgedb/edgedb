from typing import *

import enum
import uuid


class ToJson:
    def to_json(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}


def json_hook(value: Any):
    if isinstance(value, ToJson):
        return value.to_json()
    elif isinstance(value, uuid.UUID):
        return str(value)
    elif isinstance(value, enum.Enum):
        return value.value
    elif isinstance(value, (frozenset, set)):
        return list(value)
    raise TypeError(f"Cannot serialize {value!r}")
