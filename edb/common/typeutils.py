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


from __future__ import annotations

from typing import Any, Callable, Optional, Sequence, Type, TypeVar

import collections.abc
import functools

T = TypeVar('T')
TC = TypeVar('TC', bound=Callable)


def chain_decorators(
    funcs: Sequence[Callable[[TC], TC]]
) -> Callable[[TC], TC]:
    def f(func: TC) -> TC:
        for dec in reversed(funcs):
            func = dec(func)
        return func

    return f


def downcast(typ: Type[T], x: Any) -> T:
    assert isinstance(x, typ)
    return x


def not_none(x: Optional[T]) -> T:
    assert x is not None
    return x


@functools.lru_cache(1024)
def _is_container_type(cls):
    return (
        issubclass(cls, (collections.abc.Container))
        and not issubclass(cls, (str, bytes, bytearray, memoryview))
        # not namedtuple, either
        and not (issubclass(cls, tuple) and hasattr(cls, '_fields'))
    )


@functools.lru_cache(1024)
def _is_iterable_type(cls):
    return (
        issubclass(cls, collections.abc.Iterable)
    )


def is_container(obj):
    cls = obj.__class__
    return _is_container_type(cls) and _is_iterable_type(cls)


def is_container_type(type_):
    return isinstance(type_, type) and _is_container_type(type_)
