##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from edgedb.lang.common.datastructures import OrderedSet

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
        if prevlevel is not None:
            self.argmap = prevlevel.argmap
            self.location = 'query'
            self.append_graphs = False
            self.ignore_cardinality = prevlevel.ignore_cardinality
            self.in_aggregate = prevlevel.in_aggregate
            self.query = prevlevel.query
            self.rel = prevlevel.rel
            self.backend = prevlevel.backend
            self.schema = prevlevel.schema
            self.unwind_rlinks = prevlevel.unwind_rlinks
            self.aliascnt = prevlevel.aliascnt
            self.record_info = prevlevel.record_info
            self.output_format = prevlevel.output_format
            self.in_subquery = prevlevel.in_subquery
            self.global_ctes = prevlevel.global_ctes
            self.local_atom_expr_source = prevlevel.local_atom_expr_source
            self.search_path = prevlevel.search_path
            self.entityref_as_id = prevlevel.entityref_as_id
            self.memo = prevlevel.memo.copy()

            if mode == TransformerContext.NEW_TRANSPARENT:
                self.location = prevlevel.location
                self.vars = prevlevel.vars
                self.ctes = prevlevel.ctes
                self.ctemap = prevlevel.ctemap
                self.explicit_cte_map = prevlevel.explicit_cte_map
                self.ir_set_field_map = prevlevel.ir_set_field_map
                self.computable_map = prevlevel.computable_map
                self.link_node_map = prevlevel.link_node_map
                self.subquery_map = prevlevel.subquery_map
                self.direct_subquery_ref = prevlevel.direct_subquery_ref
                self.node_callbacks = prevlevel.node_callbacks
                self.filter_null_records = prevlevel.filter_null_records

            elif mode == TransformerContext.SUBQUERY:
                self.vars = {}
                self.ctes = prevlevel.ctes.copy()
                self.ctemap = prevlevel.ctemap.copy()
                self.explicit_cte_map = prevlevel.explicit_cte_map.copy()
                self.ir_set_field_map = prevlevel.ir_set_field_map.copy()
                self.computable_map = prevlevel.computable_map.copy()
                self.link_node_map = prevlevel.link_node_map.copy()

                if prevlevel.ignore_cardinality != 'recursive':
                    self.ignore_cardinality = False

                self.in_aggregate = False
                self.query = pgast.SelectQueryNode()
                self.rel = self.query
                self.subquery_map = collections.defaultdict(dict)
                self.direct_subquery_ref = False
                self.node_callbacks = {}

                self.in_subquery = True
                self.filter_null_records = True

            else:
                self.vars = prevlevel.vars.copy()
                self.ctes = prevlevel.ctes.copy()
                self.ctemap = prevlevel.ctemap.copy()
                self.explicit_cte_map = prevlevel.explicit_cte_map.copy()
                self.ir_set_field_map = prevlevel.ir_set_field_map.copy()
                self.computable_map = prevlevel.computable_map.copy()
                self.link_node_map = prevlevel.link_node_map.copy()
                self.subquery_map = prevlevel.subquery_map
                self.direct_subquery_ref = False
                self.node_callbacks = prevlevel.node_callbacks.copy()
                self.filter_null_records = True

        else:
            self.vars = {}
            self.ctes = {}
            self.global_ctes = {}
            self.aliascnt = {}
            self.ctemap = {}
            self.explicit_cte_map = {}
            self.ir_set_field_map = {}
            self.computable_map = {}
            self.link_node_map = {}
            self.argmap = OrderedSet()
            self.location = 'query'
            self.append_graphs = False
            self.ignore_cardinality = False
            self.in_aggregate = False
            self.query = pgast.SelectQueryNode()
            self.rel = self.query
            self.backend = None
            self.schema = None
            self.subquery_map = collections.defaultdict(dict)
            self.direct_subquery_ref = False
            self.node_callbacks = {}
            self.unwind_rlinks = True
            self.record_info = {}
            self.output_format = None
            self.in_subquery = False
            self.local_atom_expr_source = None
            self.search_path = []
            self.entityref_as_id = False
            self.filter_null_records = True
            self.memo = {}

    def genalias(self, hint=None):
        if hint is None:
            hint = 'a'

        if hint not in self.aliascnt:
            self.aliascnt[hint] = 1
        else:
            self.aliascnt[hint] += 1

        alias = hint + '_' + str(self.aliascnt[hint])

        return Alias(alias)


class TransformerContext(object):
    CURRENT, ALTERNATE, NEW, NEW_TRANSPARENT, SUBQUERY = range(0, 5)

    def __init__(self):
        self.stack = []
        self.push()

    def push(self, mode=None):
        level = TransformerContextLevel(self.current, mode)

        if mode == TransformerContext.ALTERNATE:
            pass

        self.stack.append(level)

        return level

    def pop(self):
        self.stack.pop()

    def __call__(self, mode=None):
        if not mode:
            mode = TransformerContext.CURRENT
        return TransformerContextWrapper(self, mode)

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


class TransformerContextWrapper(object):
    def __init__(self, context, mode):
        self.context = context
        self.mode = mode

    def __enter__(self):
        if self.mode == TransformerContext.CURRENT:
            return self.context
        else:
            self.context.push(self.mode)
            return self.context

    def __exit__(self, exc_type, exc_value, traceback):
        if self.mode != TransformerContext.CURRENT:
            self.context.pop()
