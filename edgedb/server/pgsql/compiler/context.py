##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""IR compiler context."""

import collections
import enum
import re

from edgedb.lang.common import compiler
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


class ContextSwitchMode(enum.Enum):
    TRANSPARENT = enum.auto()
    SETSCOPE = enum.auto()
    SUBQUERY = enum.auto()
    SUBSTMT = enum.auto()


class ShapeFormat(enum.Enum):
    SERIALIZED = enum.auto()
    FLAT = enum.auto()


class CompilerContextLevel(compiler.ContextLevel):
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
            self.in_member_test = False
            self.in_set_expr = False
            self.in_shape = False
            self.expr_exposed = None
            self.lax_paths = False

            self.aliascnt = {}
            self.argmap = OrderedSet()
            self.ctemap = {}
            self.setscope = {}

            self.output_format = None
            self.shape_format = ShapeFormat.SERIALIZED

            self.subquery_map = collections.defaultdict(dict)
            self.rel_overlays = collections.defaultdict(list)
            self.computed_node_rels = {}
            self.path_id_aliases = {}

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
            self.in_member_test = prevlevel.in_member_test
            self.in_set_expr = prevlevel.in_set_expr
            self.in_shape = prevlevel.in_shape
            self.expr_exposed = prevlevel.expr_exposed
            self.lax_paths = prevlevel.lax_paths

            self.aliascnt = prevlevel.aliascnt
            self.argmap = prevlevel.argmap
            self.ctemap = prevlevel.ctemap
            self.setscope = prevlevel.setscope

            self.output_format = prevlevel.output_format
            self.shape_format = prevlevel.shape_format

            self.subquery_map = prevlevel.subquery_map
            self.rel_overlays = prevlevel.rel_overlays
            self.computed_node_rels = prevlevel.computed_node_rels
            self.path_id_aliases = prevlevel.path_id_aliases

            if mode in {ContextSwitchMode.SUBQUERY,
                        ContextSwitchMode.SUBSTMT}:
                self.query = pgast.SelectStmt()
                self.rel = self.query

                self.clause = None
                self.in_aggregate = False
                self.in_member_test = False
                self.in_set_expr = False
                self.in_shape = False
                self.in_exists = False
                self.lax_paths = False

                self.ctemap = prevlevel.ctemap.copy()
                self.setscope = {}

                self.subquery_map = collections.defaultdict(dict)

            if mode == ContextSwitchMode.SUBSTMT:
                self.stmt = self.query

            if mode == ContextSwitchMode.SETSCOPE:
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
        if self._mode == ContextSwitchMode.SETSCOPE and prevlevel:
            for ir_set, lax in self.setscope.items():
                if lax or ir_set not in prevlevel.setscope:
                    prevlevel.setscope[ir_set] = lax


class CompilerContext(compiler.CompilerContext):
    ContextLevelClass = CompilerContextLevel
    default_mode = ContextSwitchMode.TRANSPARENT

    def newsetscope(self):
        return self.new(ContextSwitchMode.SETSCOPE)

    def subquery(self):
        return self.new(ContextSwitchMode.SUBQUERY)

    def substmt(self):
        return self.new(ContextSwitchMode.SUBSTMT)
