#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Any, Callable, Generic, Protocol, TypeVar, Iterable

import functools
import inspect
import types


_T = TypeVar("_T")


class _ValueDispatchCallable(Generic[_T], Protocol):
    registry: types.MappingProxyType[Any, Callable[..., _T]]

    def register(
        self,
        val: Any,
    ) -> Callable[[Callable[..., _T]], Callable[..., _T]]:
        ...

    def register_for_all(
        self,
        val: Iterable[Any],
    ) -> Callable[[Callable[..., _T]], Callable[..., _T]]:
        ...

    def __call__(__self, *args: Any, **kwargs: Any) -> _T: ...


def value_dispatch(func: Callable[..., _T]) -> _ValueDispatchCallable[_T]:
    """Like singledispatch() but dispatches by value of the first arg.

    Example:

      @value_dispatch
      def eat(fruit):
          return f"I don't want a {fruit}..."

      @eat.register('apple')
      def _eat_apple(fruit):
          return "I love apples!"

      @eat.register('eggplant')
      @eat.register('squash')
      def _eat_what(fruit):
          return f"I didn't know {fruit} is a fruit!"

    An alternative to applying multuple `register` decorators is to
    use the `register_for_all` helper:

      @eat.register_for_all({'eggplant', 'squash'})
      def _eat_what(fruit):
          return f"I didn't know {fruit} is a fruit!"
    """

    registry: dict[Any, Callable[..., _T]] = {}

    @functools.wraps(func)
    def wrapper(arg0: Any, *args: Any, **kwargs: Any) -> _T:
        try:
            delegate = registry[arg0]
        except KeyError:
            pass
        else:
            return delegate(arg0, *args, **kwargs)

        return func(arg0, *args, **kwargs)

    def register(
        value: Any,
    ) -> Callable[[Callable[..., _T]], Callable[..., _T]]:
        if inspect.isfunction(value):
            raise TypeError(
                "value_dispatch.register() decorator requires a value")

        def wrap(func: Callable[..., _T]) -> Callable[..., _T]:
            if value in registry:
                raise ValueError(
                    f'@value_dispatch: there is already a handler '
                    f'registered for {value!r}'
                )
            registry[value] = func
            return func
        return wrap

    def register_for_all(
        values: Iterable[Any],
    ) -> Callable[[Callable[..., _T]], Callable[..., _T]]:
        def wrap(func: Callable[..., _T]) -> Callable[..., _T]:
            for value in values:
                if value in registry:
                    raise ValueError(
                        f'@value_dispatch: there is already a handler '
                        f'registered for {value!r}'
                    )
                registry[value] = func
            return func
        return wrap

    wrapper.register = register  # type: ignore [attr-defined]
    wrapper.register_for_all = register_for_all  # type: ignore [attr-defined]
    return wrapper  # type: ignore [return-value]
