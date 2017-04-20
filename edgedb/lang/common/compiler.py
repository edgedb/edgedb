##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re


class ContextLevel:
    def on_pop(self, prevlevel):
        pass

    def new(self, mode=None):
        return self._stack.new(mode)


class CompilerContextManager:
    def __init__(self, context, mode):
        self.context = context
        self.mode = mode

    def __enter__(self):
        self.context.push(self.mode)
        return self.context.current

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.pop()


class CompilerContext:
    def __init__(self):
        self.stack = []
        self.push(None)

    def push(self, mode):
        level = self.ContextLevelClass(self.current, mode)
        level._stack = self
        self.stack.append(level)
        return level

    def pop(self):
        level = self.stack.pop()
        level.on_pop(self.stack[-1] if self.stack else None)

    def new(self, mode=None):
        if mode is None:
            mode = self.default_mode
        return CompilerContextManager(self, mode)

    def _current(self):
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return None

    current = property(_current)


class AliasGenerator:
    def __init__(self):
        self.aliascnt = {}

    def get(self, hint=None):
        if hint is None:
            hint = 'v'
        m = re.search(r'~\d+$', hint)
        if m:
            hint = hint[:m.start()]

        if hint not in self.aliascnt:
            self.aliascnt[hint] = 1
        else:
            self.aliascnt[hint] += 1

        alias = hint + '~' + str(self.aliascnt[hint])

        return alias
