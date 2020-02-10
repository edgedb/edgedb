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
from typing import *

import collections
import collections.abc


K = TypeVar("K", bound=Hashable)


class OrderedSet(MutableSet[K]):
    def __init__(self, iterable: Optional[Iterable[K]] = None) -> None:
        self.map: collections.OrderedDict[K, K] = collections.OrderedDict()
        if iterable is not None:
            # The ignore below is because typing of collections.abc.MutableSet
            # inherits a limitation of the built-in set that disallows |= with
            # iterables that are not sets themselves.  However, the mixin
            # *does* allow this and OrderedSet depends on this.
            self.update(iterable)

    def add(self, item: K, *, last: Optional[bool] = None) -> None:
        self.map[item] = item
        if last is not None:
            self.map.move_to_end(item, last=last)

    def discard(self, item: K) -> None:
        self.map.pop(item, item)

    def popitem(self, last: bool = True) -> K:
        key, item = self.map.popitem(last)
        return item

    def update(self, iterable: Iterable[K]) -> None:
        for item in iterable:
            self.add(item)

    difference_update = collections.abc.MutableSet.__isub__
    symmetric_difference_update = collections.abc.MutableSet.__ixor__
    intersection_update = collections.abc.MutableSet.__iand__

    def __len__(self) -> int:
        return len(self.map)

    def __contains__(self, item: Any) -> bool:
        return item in self.map

    def __iter__(self) -> Iterator[K]:
        return iter(list(self.map.values()))

    def __reversed__(self) -> Iterator[K]:
        return reversed(self.map.values())

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
