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
    SUBREL = enum.auto()
    SUBSTMT = enum.auto()
    NEWSCOPE = enum.auto()


class ShapeFormat(enum.Enum):
    SERIALIZED = enum.auto()
    FLAT = enum.auto()


class OutputFormat(enum.Enum):
    NATIVE = enum.auto()
    JSON = enum.auto()


class CompilerContextLevel(compiler.ContextLevel):
    def __init__(self, prevlevel, mode):
        if prevlevel is None:
            self.env = None
            self.argmap = collections.OrderedDict()

            stmt = pgast.SelectStmt()
            self.toplevel_stmt = None
            self.stmt = stmt
            self.rel = stmt
            self.rel_hierarchy = {}
            self.pending_query = None

            self.clause = None
            self.expr_exposed = None
            self.volatility_ref = None
            self.group_by_rels = {}

            self.shape_format = ShapeFormat.SERIALIZED
            self.disable_semi_join = set()
            self.unique_paths = set()

            self.path_scope = collections.ChainMap()
            self.scope_tree = None

        else:
            self.env = prevlevel.env
            self.argmap = prevlevel.argmap

            self.toplevel_stmt = prevlevel.toplevel_stmt
            self.stmt = prevlevel.stmt
            self.rel = prevlevel.rel
            self.rel_hierarchy = prevlevel.rel_hierarchy
            self.pending_query = prevlevel.pending_query

            self.clause = prevlevel.clause
            self.expr_exposed = prevlevel.expr_exposed
            self.volatility_ref = prevlevel.volatility_ref
            self.group_by_rels = prevlevel.group_by_rels

            self.shape_format = prevlevel.shape_format
            self.disable_semi_join = prevlevel.disable_semi_join.copy()
            self.unique_paths = prevlevel.unique_paths.copy()

            self.path_scope = prevlevel.path_scope
            self.scope_tree = prevlevel.scope_tree

            if mode in {ContextSwitchMode.SUBREL, ContextSwitchMode.SUBSTMT}:
                if self.pending_query and mode == ContextSwitchMode.SUBSTMT:
                    self.rel = self.pending_query
                else:
                    self.rel = pgast.SelectStmt()
                    self.rel_hierarchy[self.rel] = prevlevel.rel

                self.pending_query = None
                self.clause = 'result'

            if mode == ContextSwitchMode.SUBSTMT:
                self.stmt = self.rel

            if mode == ContextSwitchMode.NEWSCOPE:
                self.path_scope = prevlevel.path_scope.new_child()

    def subrel(self):
        return self.new(ContextSwitchMode.SUBREL)

    def substmt(self):
        return self.new(ContextSwitchMode.SUBSTMT)

    def newscope(self):
        return self.new(ContextSwitchMode.NEWSCOPE)


class CompilerContext(compiler.CompilerContext):
    ContextLevelClass = CompilerContextLevel
    default_mode = ContextSwitchMode.TRANSPARENT


class Environment:
    """Static compilation environment."""

    def __init__(self, *, schema, output_format, backend, singleton_mode):
        self.schema = schema
        self.backend = backend
        self.singleton_mode = singleton_mode
        self.aliases = aliases.AliasGenerator()
        self.root_rels = set()
        self.rel_overlays = collections.defaultdict(list)
        self.output_format = output_format
