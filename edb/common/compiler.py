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

import collections
import re


class ContextLevel:
    def on_pop(self, prevlevel):
        pass

    def new(self, mode=None):
        return self._stack.new(mode, self)


class CompilerContextManager:
    def __init__(self, context, mode, prevlevel):
        self.context = context
        self.mode = mode
        self.prevlevel = prevlevel

    def __enter__(self):
        self.context.push(self.mode, self.prevlevel)
        return self.context.current

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.pop()


class CompilerContext:
    def __init__(self):
        self.stack = []
        self.push(None)

    def push(self, mode, prevlevel=None):
        if prevlevel is None:
            prevlevel = self.current
        level = self.ContextLevelClass(prevlevel, mode)
        level._stack = self
        self.stack.append(level)
        return level

    def pop(self):
        level = self.stack.pop()
        level.on_pop(self.stack[-1] if self.stack else None)

    def new(self, mode=None, prevlevel=None):
        if mode is None:
            mode = self.default_mode
        return CompilerContextManager(self, mode, prevlevel)

    def _current(self):
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return None

    current = property(_current)


class Counter:
    def __init__(self):
        self.counts = collections.defaultdict(int)

    def nextval(self, name='default'):
        self.counts[name] += 1
        return self.counts[name]


class AliasGenerator(Counter):
    def get(self, hint=None):
        if hint is None:
            hint = 'v'
        m = re.search(r'~\d+$', hint)
        if m:
            hint = hint[:m.start()]

        idx = self.nextval(hint)
        alias = f'{hint}~{idx}'

        return alias
