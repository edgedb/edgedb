##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""IR compiler context."""

import collections
import enum

from edgedb.lang.common import compiler

from edgedb.server.pgsql import ast as pgast

from . import aliases


class ContextSwitchMode(enum.Enum):
    TRANSPARENT = enum.auto()
    SUBQUERY = enum.auto()
    SUBSTMT = enum.auto()


class ShapeFormat(enum.Enum):
    SERIALIZED = enum.auto()
    FLAT = enum.auto()


class OutputFormat(enum.Enum):
    NATIVE = enum.auto()
    JSON = enum.auto()


class CompilerContextLevel(compiler.ContextLevel):
    def __init__(self, prevlevel=None, mode=None):
        self._mode = mode

        if prevlevel is None:
            self.backend = None
            self.schema = None
            self.singleton_mode = False

            self.memo = {}

            stmt = pgast.SelectStmt()
            self.toplevel_stmt = None
            self.stmt = stmt
            self.query = stmt
            self.rel = stmt
            self.stmt_hierarchy = {}

            self.clause = None
            self.in_exists = False
            self.in_aggregate = False
            self.aggregated_scope = set()
            self.in_member_test = False
            self.in_set_expr = False
            self.in_shape = False
            self.expr_exposed = None
            self.lax_paths = 0
            self.weak_path_bond_regime = False
            self.correct_set_assumed = False
            self.expr_injected_path_bond = None
            self.view_path_id_map = {}

            self.env = None
            self.argmap = collections.OrderedDict()
            self.ctemap = {}
            self.ctemap_by_stmt = collections.defaultdict(dict)
            self.stmtmap = {}
            self.setscope = {}

            self.shape_format = ShapeFormat.SERIALIZED

            self.subquery_map = collections.defaultdict(dict)
            self.computed_node_rels = {}
            self.parent_var_scope = {}
            self.path_bonds = {}
            self.path_bonds_by_stmt = collections.defaultdict(dict)
            self.parent_path_bonds = {}
            self.stmt_path_scope = dict()
            self.stmt_specific_path_scope = set()
            self.parent_stmt_path_scope = dict()

        else:
            self.backend = prevlevel.backend
            self.schema = prevlevel.schema
            self.singleton_mode = prevlevel.singleton_mode

            self.memo = {}

            self.toplevel_stmt = prevlevel.toplevel_stmt
            self.stmt = prevlevel.stmt
            self.query = prevlevel.query
            self.rel = prevlevel.rel
            self.stmt_hierarchy = prevlevel.stmt_hierarchy

            self.clause = prevlevel.clause
            self.in_exists = prevlevel.in_exists
            self.in_aggregate = prevlevel.in_aggregate
            self.aggregated_scope = prevlevel.aggregated_scope
            self.in_member_test = prevlevel.in_member_test
            self.in_set_expr = prevlevel.in_set_expr
            self.in_shape = prevlevel.in_shape
            self.expr_exposed = prevlevel.expr_exposed
            self.lax_paths = prevlevel.lax_paths
            self.weak_path_bond_regime = prevlevel.weak_path_bond_regime
            self.correct_set_assumed = prevlevel.correct_set_assumed
            self.expr_injected_path_bond = prevlevel.expr_injected_path_bond
            self.view_path_id_map = prevlevel.view_path_id_map

            self.env = prevlevel.env
            self.argmap = prevlevel.argmap
            self.ctemap = prevlevel.ctemap
            self.ctemap_by_stmt = prevlevel.ctemap_by_stmt
            self.stmtmap = prevlevel.stmtmap
            self.setscope = prevlevel.setscope

            self.shape_format = prevlevel.shape_format

            self.subquery_map = prevlevel.subquery_map
            self.computed_node_rels = prevlevel.computed_node_rels
            self.parent_var_scope = prevlevel.parent_var_scope
            self.path_bonds = prevlevel.path_bonds
            self.path_bonds_by_stmt = prevlevel.path_bonds_by_stmt
            self.parent_path_bonds = prevlevel.parent_path_bonds
            self.stmt_path_scope = prevlevel.stmt_path_scope
            self.stmt_specific_path_scope = prevlevel.stmt_specific_path_scope
            self.parent_stmt_path_scope = prevlevel.parent_stmt_path_scope

            if mode in {ContextSwitchMode.SUBQUERY,
                        ContextSwitchMode.SUBSTMT}:
                self.query = pgast.SelectStmt()
                self.rel = self.query

                self.clause = 'result'
                self.in_aggregate = False
                self.in_member_test = False
                self.in_set_expr = False
                self.in_shape = False
                self.in_exists = False
                self.lax_paths = (
                    prevlevel.lax_paths - 1 if prevlevel.lax_paths else 0)
                self.correct_set_assumed = False
                self.view_path_id_map = {}

                self.ctemap = prevlevel.ctemap.copy()
                self.setscope = {}

                self.subquery_map = collections.defaultdict(dict)
                self.path_bonds = prevlevel.path_bonds.copy()

            if mode == ContextSwitchMode.SUBSTMT:
                self.stmt = self.query
                self.parent_path_bonds = prevlevel.path_bonds
                self.computed_node_rels = prevlevel.computed_node_rels.copy()
                self.parent_var_scope = prevlevel.parent_var_scope.copy()
                self.stmt_specific_path_scope = set()

    def genalias(self, hint=None):
        return self.env.aliases.get(hint)

    def subquery(self):
        return self.new(ContextSwitchMode.SUBQUERY)

    def substmt(self):
        return self.new(ContextSwitchMode.SUBSTMT)


class CompilerContext(compiler.CompilerContext):
    ContextLevelClass = CompilerContextLevel
    default_mode = ContextSwitchMode.TRANSPARENT

    def subquery(self):
        return self.new(ContextSwitchMode.SUBQUERY)

    def substmt(self):
        return self.new(ContextSwitchMode.SUBSTMT)


class Environment:
    """Static compilation environment."""

    def __init__(self, schema, output_format=OutputFormat.NATIVE):
        self.schema = schema
        self.aliases = aliases.AliasGenerator()
        self.root_rels = set()
        self.rel_overlays = collections.defaultdict(list)
        self.output_format = output_format
