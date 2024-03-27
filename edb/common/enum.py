#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

import enum
import functools


class StrEnum(str, enum.Enum):
    """A version of string enum with reasonable __str__."""
    def __str__(self):
        return self._value_


@functools.total_ordering
class OrderedEnumMixin():
    @classmethod
    @functools.lru_cache(None)
    def _index_of(cls, value):
        return list(cls).index(value)

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self._index_of(self) < self._index_of(other)
        return NotImplemented
