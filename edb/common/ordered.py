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
from typing import (
    Any,
    Optional,
    TypeVar,
    Hashable,
    Iterable,
    Iterator,
    MutableSet,
    Dict,
)

import collections
import collections.abc


K = TypeVar("K", bound=Hashable)


class OrderedSet(MutableSet[K]):

    map: Dict[K, None]

    def __init__(self, iterable: Optional[Iterable[K]] = None) -> None:
        if iterable is not None:
            self.map = {v: None for v in iterable}
        else:
            self.map = {}

    def add(self, item: K) -> None:
        self.map[item] = None

    def discard(self, item: K) -> None:
        self.map.pop(item, None)

    def update(self, iterable: Iterable[K]) -> None:
        for item in iterable:
            self.map[item] = None

    def replace(self, existing: K, new: K) -> None:
        if existing not in self.map:
            raise LookupError(f'{existing!r} is not in set')
        self.map[existing] = None

    difference_update = collections.abc.MutableSet.__isub__
    symmetric_difference_update = collections.abc.MutableSet.__ixor__
    intersection_update = collections.abc.MutableSet.__iand__

    def __len__(self) -> int:
        return len(self.map)

    def __contains__(self, item: Any) -> bool:
        return item in self.map

    def __iter__(self) -> Iterator[K]:
        return iter(self.map)

    def __reversed__(self) -> Iterator[K]:
        return reversed(self.map.keys())

    def __repr__(self) -> str:
        if not self:
            return '%s()' % (self.__class__.__name__, )
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, self.__class__):
            return len(self) == len(other) and self.map == other.map
        elif other is None:
            return False
        else:
            return not self.isdisjoint(other)

    def copy(self) -> OrderedSet[K]:
        return self.__class__(self)

    def clear(self) -> None:
        self.map.clear()
