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

    def __radd__(self, other):
        return Alias(str(other) + str(self))

    __iadd__ = __add__


class TransformerContextLevel:
    def __init__(self, prevlevel=None, mode=None):
        if prevlevel is None:
            self.stmt = None
            self.query = pgast.SelectStmt()
            self.rel = self.query
            self.aliascnt = {}
            self.ctemap = {}
            self.argmap = OrderedSet()
            self.location = 'query'
            self.in_aggregate = False
            self.backend = None
            self.schema = None
            self.subquery_map = collections.defaultdict(dict)
            self.output_format = None
            self.memo = {}
            self.clsref_as_id = False
            self.rel_overlays = collections.defaultdict(list)

        else:
            self.stmt = prevlevel.stmt
            self.query = prevlevel.query
            self.rel = prevlevel.rel
            self.argmap = prevlevel.argmap
            self.in_aggregate = prevlevel.in_aggregate
            self.backend = prevlevel.backend
            self.schema = prevlevel.schema
            self.aliascnt = prevlevel.aliascnt
            self.output_format = prevlevel.output_format
            self.memo = prevlevel.memo.copy()
            self.location = prevlevel.location
            self.ctemap = prevlevel.ctemap
            self.subquery_map = prevlevel.subquery_map
            self.clsref_as_id = prevlevel.clsref_as_id
            self.rel_overlays = prevlevel.rel_overlays

            if mode in {TransformerContext.SUBQUERY,
                        TransformerContext.SUBSTMT}:
                self.query = pgast.SelectStmt()
                self.rel = self.query
                self.location = 'query'
                self.ctemap = prevlevel.ctemap.copy()
                self.in_aggregate = False
                self.subquery_map = collections.defaultdict(dict)

            if mode == TransformerContext.SUBSTMT:
                self.stmt = self.query

    def genalias(self, hint=None):
        if hint is None:
            hint = 'a'

        m = re.search(r'~\d+$', hint)
        if m:
            hint = hint[:m.start()]

        if hint not in self.aliascnt:
            self.aliascnt[hint] = 1
        else:
            self.aliascnt[hint] += 1

        alias = hint + '~' + str(self.aliascnt[hint])

        return Alias(alias)


class TransformerContext:
    TRANSPARENT, SUBQUERY, SUBSTMT = range(0, 3)

    def __init__(self):
        self.stack = []
        self.push()

    def push(self, mode=None):
        level = TransformerContextLevel(self.current, mode)
        self.stack.append(level)
        return level

    def pop(self):
        self.stack.pop()

    def new(self, mode=None):
        if not mode:
            mode = TransformerContext.TRANSPARENT
        return TransformerContextWrapper(self, mode)

    def subquery(self):
        return self.new(TransformerContext.SUBQUERY)

    def substmt(self):
        return self.new(TransformerContext.SUBSTMT)

    def _current(self):
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return None

    def __getitem__(self, idx):
        return self.stack[idx]

    def __len__(self):
        return len(self.stack)

    current = property(_current)


class TransformerContextWrapper:
    def __init__(self, context, mode):
        self.context = context
        self.mode = mode

    def __enter__(self):
        self.context.push(self.mode)
        return self.context

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.pop()
