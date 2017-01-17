##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import re

from edgedb.lang.common.ordered import OrderedSet

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common


class Alias(str):
    def __new__(cls, value=''):
        return super(Alias, cls).__new__(
            cls, common.edgedb_name_to_pg_name(value))

    def __add__(self, other):
        return Alias(super().__add__(other))

    __iadd__ = __add__


class TransformerContextLevel:
    def __init__(self, prevlevel=None, mode=None):
        self._mode = mode

        if prevlevel is None:
            self.backend = None
            self.schema = None

            self.memo = {}

            stmt = pgast.SelectStmt()
            self.toplevel_stmt = None
            self.stmt = stmt
            self.query = stmt
            self.rel = stmt

            self.clause = None
            self.scope_cutoff = False
            self.in_exists = False
            self.in_aggregate = False
            self.in_set_expr = False
            self.in_shape = False
            self.expr_exposed = False
            self.lax_paths = False

            self.aliascnt = {}
            self.argmap = OrderedSet()
            self.ctemap = {}
            self.setscope = {}

            self.subquery_map = collections.defaultdict(dict)
            self.output_format = None
            self.rel_overlays = collections.defaultdict(list)

        else:
            self.backend = prevlevel.backend
            self.schema = prevlevel.schema

            self.memo = {}

            self.toplevel_stmt = prevlevel.toplevel_stmt
            self.stmt = prevlevel.stmt
            self.query = prevlevel.query
            self.rel = prevlevel.rel

            self.clause = prevlevel.clause
            self.scope_cutoff = False
            self.in_exists = prevlevel.in_exists
            self.in_aggregate = prevlevel.in_aggregate
            self.in_set_expr = prevlevel.in_set_expr
            self.in_shape = prevlevel.in_shape
            self.expr_exposed = prevlevel.expr_exposed
            self.lax_paths = prevlevel.lax_paths

            self.aliascnt = prevlevel.aliascnt
            self.argmap = prevlevel.argmap
            self.ctemap = prevlevel.ctemap
            self.setscope = prevlevel.setscope

            self.subquery_map = prevlevel.subquery_map
            self.output_format = prevlevel.output_format
            self.rel_overlays = prevlevel.rel_overlays

            if mode in {TransformerContext.SUBQUERY,
                        TransformerContext.SUBSTMT}:
                self.query = pgast.SelectStmt()
                self.rel = self.query

                self.clause = None
                self.in_aggregate = False
                self.in_set_expr = False
                self.in_shape = False
                self.in_exists = False
                self.lax_paths = False

                self.ctemap = prevlevel.ctemap.copy()
                self.setscope = {}

                self.subquery_map = collections.defaultdict(dict)

            if mode == TransformerContext.SUBSTMT:
                self.stmt = self.query

            if mode == TransformerContext.SETSCOPE:
                self.setscope = {}

    def genalias(self, hint):
        m = re.search(r'~\d+$', hint)
        if m:
            hint = hint[:m.start()]

        if hint not in self.aliascnt:
            self.aliascnt[hint] = 1
        else:
            self.aliascnt[hint] += 1

        alias = hint + '~' + str(self.aliascnt[hint])

        return Alias(alias)

    def on_pop(self, prevlevel):
        if self._mode == TransformerContext.SETSCOPE and prevlevel:
            for ir_set, lax in self.setscope.items():
                if lax or ir_set not in prevlevel.setscope:
                    prevlevel.setscope[ir_set] = lax


class TransformerContext:
    TRANSPARENT, SETSCOPE, SUBQUERY, SUBSTMT = range(0, 4)

    def __init__(self):
        self.stack = []
        self.push(None)

    def push(self, mode):
        level = TransformerContextLevel(self.current, mode)
        self.stack.append(level)
        return level

    def pop(self):
        level = self.stack.pop()
        level.on_pop(self.stack[-1] if self.stack else None)

    def new(self, mode=None):
        if mode is None:
            mode = TransformerContext.TRANSPARENT
        return TransformerContextWrapper(self, mode)

    def newsetscope(self):
        return self.new(TransformerContext.SETSCOPE)

    def subquery(self):
        return self.new(TransformerContext.SUBQUERY)

    def substmt(self):
        return self.new(TransformerContext.SUBSTMT)

    def _current(self):
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return None

    current = property(_current)


class TransformerContextWrapper:
    def __init__(self, context, mode):
        self.context = context
        self.mode = mode

    def __enter__(self):
        self.context.push(self.mode)
        return self.context.current

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.pop()
