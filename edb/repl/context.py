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


import dataclasses
import enum


class QueryMode(enum.IntEnum):

    Normal = 0
    JSON = 1
    GraphQL = 2

    def cycle(self):
        return QueryMode((int(self) + 1) % 3)


@dataclasses.dataclass
class ReplContext:

    show_implicit_fields: bool = False
    query_mode: QueryMode = QueryMode.Normal

    def toggle_query_mode(self):
        self.query_mode = self.query_mode.cycle()

    def toggle_implicit(self):
        self.show_implicit_fields = not self.show_implicit_fields
