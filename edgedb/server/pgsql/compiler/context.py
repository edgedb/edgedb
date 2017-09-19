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

            stmt = pgast.SelectStmt()
            self.toplevel_stmt = None
            self.stmt = stmt
            self.query = stmt
            self.rel = stmt
            self.stmt_hierarchy = {}

            self.clause = None
            self.expr_as_isolated_set = False
            self.expr_as_value = False
            self.expr_exposed = None
            self.lax_paths = 0
            self.correct_set_assumed = False
            self.unique_set_assumed = False
            self.expr_injected_path_bond = None
            self.view_path_id_map = {}

            self.env = None
            self.argmap = collections.OrderedDict()
            self.ctemap = {}
            self.ctemap_by_stmt = collections.defaultdict(dict)
            self.stmtmap = {}
            self.viewmap = {}

            self.shape_format = ShapeFormat.SERIALIZED

            self.subquery_map = collections.defaultdict(dict)
            self.computed_node_rels = {}
            self.path_scope_refs = {}
            self.path_scope_refs_by_stmt = collections.defaultdict(dict)
            self.parent_path_scope_refs = {}
            self.path_scope = frozenset()

        else:
            self.backend = prevlevel.backend
            self.schema = prevlevel.schema
            self.singleton_mode = prevlevel.singleton_mode

            self.toplevel_stmt = prevlevel.toplevel_stmt
            self.stmt = prevlevel.stmt
            self.query = prevlevel.query
            self.rel = prevlevel.rel
            self.stmt_hierarchy = prevlevel.stmt_hierarchy

            self.clause = prevlevel.clause
            self.expr_as_isolated_set = prevlevel.expr_as_isolated_set
            self.expr_as_value = prevlevel.expr_as_value
            self.expr_exposed = prevlevel.expr_exposed
            self.lax_paths = prevlevel.lax_paths
            self.correct_set_assumed = prevlevel.correct_set_assumed
            self.unique_set_assumed = prevlevel.unique_set_assumed
            self.expr_injected_path_bond = prevlevel.expr_injected_path_bond
            self.view_path_id_map = prevlevel.view_path_id_map

            self.env = prevlevel.env
            self.argmap = prevlevel.argmap
            self.ctemap = prevlevel.ctemap
            self.ctemap_by_stmt = prevlevel.ctemap_by_stmt
            self.stmtmap = prevlevel.stmtmap
            self.viewmap = prevlevel.viewmap

            self.shape_format = prevlevel.shape_format

            self.subquery_map = prevlevel.subquery_map
            self.computed_node_rels = prevlevel.computed_node_rels
            self.path_scope_refs = prevlevel.path_scope_refs
            self.path_scope_refs_by_stmt = prevlevel.path_scope_refs_by_stmt
            self.parent_path_scope_refs = prevlevel.parent_path_scope_refs
            self.path_scope = prevlevel.path_scope

            if mode in {ContextSwitchMode.SUBQUERY,
                        ContextSwitchMode.SUBSTMT}:
                self.query = pgast.SelectStmt()
                self.rel = self.query

                self.clause = 'result'
                self.expr_as_isolated_set = False
                self.expr_as_value = False
                self.lax_paths = (
                    prevlevel.lax_paths - 1 if prevlevel.lax_paths else 0)
                self.correct_set_assumed = False
                self.unique_set_assumed = False
                self.view_path_id_map = {}

                self.ctemap = prevlevel.ctemap.copy()

                self.subquery_map = collections.defaultdict(dict)
                self.path_scope_refs = prevlevel.path_scope_refs.copy()

            if mode == ContextSwitchMode.SUBSTMT:
                self.stmt = self.query
                self.parent_path_scope_refs = prevlevel.path_scope_refs
                self.computed_node_rels = prevlevel.computed_node_rels.copy()

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
