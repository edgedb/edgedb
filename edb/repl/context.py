#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations

import dataclasses
import enum
from typing import *
import uuid


class QueryMode(enum.IntEnum):

    Normal = 0
    JSON = 1

    def cycle(self) -> QueryMode:
        return QueryMode((int(self) + 1) % 2)


@dataclasses.dataclass
class ReplContext:

    use_colors: bool = False
    show_implicit_fields: bool = False
    introspect_types: bool = False
    query_mode: QueryMode = QueryMode.Normal
    typenames: Optional[Dict[uuid.UUID, str]] = None
    last_exception: Optional[Exception] = None
    implicit_limit: int = 100

    def toggle_query_mode(self) -> None:
        self.query_mode = self.query_mode.cycle()

    def toggle_implicit(self) -> None:
        self.show_implicit_fields = not self.show_implicit_fields

    def toggle_introspect_types(self) -> None:
        self.introspect_types = not self.introspect_types
