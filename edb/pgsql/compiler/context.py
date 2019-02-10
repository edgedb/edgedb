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


"""IR compiler context."""

import collections
import enum

from edb.common import compiler

from edb.pgsql import ast as pgast

from . import aliases


class ContextSwitchMode(enum.Enum):
    TRANSPARENT = enum.auto()
    SUBREL = enum.auto()
    NEWREL = enum.auto()
    SUBSTMT = enum.auto()
    NEWSCOPE = enum.auto()


class ShapeFormat(enum.Enum):
    SERIALIZED = enum.auto()
    FLAT = enum.auto()


class OutputFormat(enum.Enum):
    NATIVE = enum.auto()
    JSON = enum.auto()


NO_VOLATILITY = object()


class CompilerContextLevel(compiler.ContextLevel):
    def __init__(self, prevlevel, mode):
        if prevlevel is None:
            self.env = None
            self.argmap = collections.OrderedDict()

            self.toplevel_stmt = None
            self.stmt = None
            self.rel = None
            self.rel_hierarchy = {}
            self.pending_query = None

            self.clause = None
            self.toplevel_clause = None
            self.expr_exposed = None
            self.volatility_ref = None
            self.group_by_rels = {}

            self.disable_semi_join = set()
            self.unique_paths = set()
            self.force_optional = set()
            self.join_target_type_filter = {}

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
            self.toplevel_clause = prevlevel.toplevel_clause
            self.expr_exposed = prevlevel.expr_exposed
            self.volatility_ref = prevlevel.volatility_ref
            self.group_by_rels = prevlevel.group_by_rels

            self.disable_semi_join = prevlevel.disable_semi_join.copy()
            self.unique_paths = prevlevel.unique_paths.copy()
            self.force_optional = prevlevel.force_optional.copy()
            self.join_target_type_filter = prevlevel.join_target_type_filter

            self.path_scope = prevlevel.path_scope
            self.scope_tree = prevlevel.scope_tree

            if mode in {ContextSwitchMode.SUBREL, ContextSwitchMode.NEWREL,
                        ContextSwitchMode.SUBSTMT}:
                if self.pending_query and mode == ContextSwitchMode.SUBSTMT:
                    self.rel = self.pending_query
                else:
                    self.rel = pgast.SelectStmt()
                    if mode != ContextSwitchMode.NEWREL:
                        self.rel_hierarchy[self.rel] = prevlevel.rel

                self.pending_query = None
                self.clause = 'result'

            if mode == ContextSwitchMode.SUBSTMT:
                self.stmt = self.rel

            if mode == ContextSwitchMode.NEWSCOPE:
                self.path_scope = prevlevel.path_scope.new_child()

    def subrel(self):
        return self.new(ContextSwitchMode.SUBREL)

    def newrel(self):
        return self.new(ContextSwitchMode.NEWREL)

    def substmt(self):
        return self.new(ContextSwitchMode.SUBSTMT)

    def newscope(self):
        return self.new(ContextSwitchMode.NEWSCOPE)


class CompilerContext(compiler.CompilerContext):
    ContextLevelClass = CompilerContextLevel
    default_mode = ContextSwitchMode.TRANSPARENT


class Environment:
    """Static compilation environment."""

    def __init__(self, *, output_format, singleton_mode, use_named_params):
        self.singleton_mode = singleton_mode
        self.aliases = aliases.AliasGenerator()
        self.root_rels = set()
        self.rel_overlays = collections.defaultdict(list)
        self.output_format = output_format
        self.tuple_formats = {}
        self.use_named_params = use_named_params
