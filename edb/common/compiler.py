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
import re


ContextLevel_T = TypeVar('ContextLevel_T', bound='ContextLevel')


class ContextLevel:
    _stack: CompilerContext[ContextLevel]

    def __init__(self, prevlevel: Optional[ContextLevel], mode: Any) -> None:
        pass

    def on_pop(
        self: ContextLevel_T,
        prevlevel: Optional[ContextLevel_T],
    ) -> None:
        pass

    def new(
        self: ContextLevel_T,
        mode: Any=None,
    ) -> CompilerContextManager[ContextLevel_T]:
        stack = cast(CompilerContext[ContextLevel_T], self._stack)
        return stack.new(mode, self)


class CompilerContextManager(ContextManager[ContextLevel_T]):
    def __init__(
        self,
        context: CompilerContext[ContextLevel_T],
        mode: Any,
        prevlevel: Optional[ContextLevel_T],
    ) -> None:
        self.context = context
        self.mode = mode
        self.prevlevel = prevlevel

    def __enter__(self) -> ContextLevel_T:
        return self.context.push(self.mode, self.prevlevel)

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.context.pop()


class CompilerContext(Generic[ContextLevel_T]):
    stack: List[ContextLevel_T]
    ContextLevelClass: Type[ContextLevel_T]
    default_mode: Any

    def __init__(self, initial: ContextLevel_T) -> None:
        self.stack = []
        self._push(None, initial=initial)

    def push(
        self,
        mode: Any,
        prevlevel: Optional[ContextLevel_T] = None,
    ) -> ContextLevel_T:
        return self._push(mode, prevlevel)

    def _push(
        self,
        mode: Any,
        prevlevel: Optional[ContextLevel_T] = None,
        *,
        initial: Optional[ContextLevel_T] = None,
    ) -> ContextLevel_T:
        if initial is not None:
            level = initial
        else:
            prevlevel = self.current
            level = self.ContextLevelClass(prevlevel, mode)
        # XXX: typing fu
        level._stack = cast(CompilerContext[ContextLevel], self)
        self.stack.append(level)
        return level

    def pop(self) -> None:
        level = self.stack.pop()
        level.on_pop(self.stack[-1] if self.stack else None)

    def new(
        self,
        mode: Any = None,
        prevlevel: Optional[ContextLevel_T] = None,
    ) -> CompilerContextManager[ContextLevel_T]:
        if mode is None:
            mode = self.default_mode
        return CompilerContextManager(self, mode, prevlevel)

    @property
    def current(self) -> ContextLevel_T:
        return self.stack[-1]


class SimpleCounter:
    counts: DefaultDict[str, int]

    def __init__(self) -> None:
        self.counts = collections.defaultdict(int)

    def nextval(self, name: str = 'default') -> int:
        self.counts[name] += 1
        return self.counts[name]


class AliasGenerator(SimpleCounter):
    def get(self, hint: str = '') -> str:
        if not hint:
            hint = 'v'
        m = re.search(r'~\d+$', hint)
        if m:
            hint = hint[:m.start()]

        idx = self.nextval(hint)
        alias = f'{hint}~{idx}'

        return alias
