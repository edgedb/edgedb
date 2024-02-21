#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from typing import Any

import enum
import uuid


class ToJson:
    def to_json(self) -> Any:
        return {k: v for k, v in self.__dict__.items() if v is not None}


def json_hook(value: Any) -> Any:
    if isinstance(value, ToJson):
        return value.to_json()
    elif isinstance(value, uuid.UUID):
        return str(value)
    elif isinstance(value, enum.Enum):
        return value.value
    elif isinstance(value, (frozenset, set)):
        return list(value)
    raise TypeError(f"Cannot serialize {value!r}")
