#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
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

"""
This stub file is needed so that __and__, __or__, __sub__, __xor__, and
so on properly return the instance of the *current class*, not the abstract
versions.
"""
from __future__ import annotations

from typing import (
    AbstractSet,
    Any,
    Hashable,
    Iterable,
    Iterator,
    MutableSet,
    Optional,
    TypeVar,
    Union,
)

_S = TypeVar("_S")
_T = TypeVar("_T")
_H = TypeVar("_H", bound=Hashable)

class OrderedSet(MutableSet[_H]):
    def __init__(self, iterable: Optional[Iterable[_H]] = None) -> None: ...
    def __and__(self, s: AbstractSet[Any]) -> OrderedSet[_H]: ...
    def __or__(self, s: AbstractSet[_T]) -> OrderedSet[Union[_H, _T]]: ...
    def __sub__(self, s: AbstractSet[Any]) -> OrderedSet[_H]: ...
    def __xor__(self, s: AbstractSet[_T]) -> OrderedSet[Union[_H, _T]]: ...
    def __ior__(self, s: AbstractSet[_S]) -> OrderedSet[Union[_T, _S]]: ...
    def __iand__(self, s: AbstractSet[Any]) -> OrderedSet[_T]: ...
    def __ixor__(self, s: AbstractSet[_S]) -> OrderedSet[Union[_T, _S]]: ...
    def __isub__(self, s: AbstractSet[Any]) -> OrderedSet[_T]: ...
    difference_update = MutableSet.__isub__
    symmetric_difference_update = MutableSet.__ixor__
    intersection_update = MutableSet.__iand__
    def add(self, item: _H) -> None: ...
    def discard(self, item: _H) -> None: ...
    def update(self, s: Iterable[_H]) -> None: ...
    def replace(self, existing: _H, new: _H) -> None: ...
    def __len__(self) -> int: ...
    def __contains__(self, item: Any) -> bool: ...
    def __iter__(self) -> Iterator[_H]: ...
    def __reversed__(self) -> Iterator[_H]: ...
    def copy(self) -> OrderedSet[_H]: ...
    def clear(self) -> None: ...
