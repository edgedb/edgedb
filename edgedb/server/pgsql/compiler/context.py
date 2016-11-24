##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from edgedb.lang.common.datastructures import OrderedSet

from edgedb.server.pgsql import ast2 as pgast
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
            self.aliascnt = {}
            self.ctemap = {}
            self.explicit_cte_map = {}
            self.computable_map = {}
            self.argmap = OrderedSet()
            self.location = 'query'
            self.in_aggregate = False
            self.query = pgast.SelectStmt()
            self.rel = self.query
            self.backend = None
            self.schema = None
            self.subquery_map = collections.defaultdict(dict)
            self.set_refs = set()
            self.record_info = {}
            self.output_format = None
            self.filter_null_records = True
            self.memo = {}
            self.entityref_as_id = False
            self.rel_overlays = collections.defaultdict(list)

        else:
            self.argmap = prevlevel.argmap
            self.in_aggregate = prevlevel.in_aggregate
            self.query = prevlevel.query
            self.rel = prevlevel.rel
            self.backend = prevlevel.backend
            self.schema = prevlevel.schema
            self.aliascnt = prevlevel.aliascnt
            self.record_info = prevlevel.record_info
            self.output_format = prevlevel.output_format
            self.memo = prevlevel.memo.copy()
            self.location = prevlevel.location
            self.ctemap = prevlevel.ctemap
            self.explicit_cte_map = prevlevel.explicit_cte_map
            self.computable_map = prevlevel.computable_map
            self.subquery_map = prevlevel.subquery_map
            self.set_refs = prevlevel.set_refs
            self.filter_null_records = prevlevel.filter_null_records
            self.entityref_as_id = prevlevel.entityref_as_id
            self.rel_overlays = prevlevel.rel_overlays

            if mode == TransformerContext.SUBQUERY:
                self.location = 'query'
                self.ctemap = prevlevel.ctemap.copy()
                self.explicit_cte_map = prevlevel.explicit_cte_map.copy()
                self.computable_map = prevlevel.computable_map.copy()
                self.in_aggregate = False
                self.query = pgast.SelectStmt()
                self.rel = self.query
                self.subquery_map = collections.defaultdict(dict)
                self.set_refs = set()
                self.filter_null_records = True

    def genalias(self, hint=None):
        if hint is None:
            hint = 'a'

        if hint not in self.aliascnt:
            self.aliascnt[hint] = 1
        else:
            self.aliascnt[hint] += 1

        alias = hint + '_' + str(self.aliascnt[hint])

        return Alias(alias)


class TransformerContext:
    TRANSPARENT, SUBQUERY = range(0, 2)

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
