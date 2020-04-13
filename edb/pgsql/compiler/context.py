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

from __future__ import annotations
from typing import *

import collections
import enum

from edb.common import compiler

from edb.pgsql import ast as pgast

from . import aliases

if TYPE_CHECKING:
    from edb.ir import ast as irast


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
    JSONB = enum.auto()
    JSON_ELEMENTS = enum.auto()


class NoVolatilitySentinel:
    pass


NO_VOLATILITY = NoVolatilitySentinel()
NO_STMT = pgast.SelectStmt()


class CompilerContextLevel(compiler.ContextLevel):
    #: static compilation environment
    env: Environment

    #: mapping of named args to position
    argmap: Dict[str, int]

    #: whether compiling in singleton expression mode
    singleton_mode: bool

    #: the top-level SQL statement
    toplevel_stmt: pgast.Query

    #: Record of DML CTEs generated for the corresponding IR DML.
    dml_stmts: Dict[irast.MutatingStmt, pgast.CommonTableExpr]

    #: SQL statement corresponding to the IR statement
    #: currently being compiled.
    stmt: pgast.SelectStmt

    #: Current SQL subquery
    rel: pgast.SelectStmt

    #: SQL query hierarchy
    rel_hierarchy: Dict[pgast.Query, pgast.Query]

    #: The logical parent of the current query in the
    #: query hierarchy
    parent_rel: Optional[pgast.Query]

    #: Query to become current in the next SUBSTMT switch.
    pending_query: Optional[pgast.SelectStmt]

    #: Whether the expression currently being processed is
    #: directly exposed to the output of the statement.
    expr_exposed: Optional[bool]

    #: Expression to use to force SQL expression volatility in this context
    volatility_ref: Optional[Union[pgast.BaseExpr, NoVolatilitySentinel]]

    group_by_rels: Dict[
        Tuple[irast.PathId, irast.PathId],
        Union[pgast.BaseRelation, pgast.CommonTableExpr]
    ]

    #: Paths, for which semi-join is banned in this context.
    disable_semi_join: Set[irast.PathId]

    #: Paths, which need to be explicitly wrapped into SQL
    #: optionality scaffolding.
    force_optional: Set[irast.PathId]

    #: ir.TypeRef used to narrow the joined relation representing
    #: the mapping key.
    join_target_type_filter: Dict[irast.Set, irast.TypeRef]

    #: Which SQL query holds the SQL scope for the given PathId
    path_scope: ChainMap[irast.PathId, pgast.SelectStmt]

    #: Relevant IR scope for this context.
    scope_tree: irast.ScopeTreeNode

    #: Relations used to "overlay" the main table for
    #: the type.  Mostly used with DML statements.
    type_rel_overlays: DefaultDict[
        Tuple[str, Optional[irast.MutatingStmt]],
        List[
            Tuple[
                str,
                Union[pgast.BaseRelation, pgast.CommonTableExpr],
                irast.PathId,
            ]
        ]
    ]

    #: Relations used to "overlay" the main table for
    #: the pointer.  Mostly used with DML statements.
    ptr_rel_overlays: DefaultDict[
        Tuple[str, Optional[irast.MutatingStmt]],
        List[
            Tuple[
                str,
                Union[pgast.BaseRelation, pgast.CommonTableExpr],
            ]
        ]
    ]

    def __init__(
        self,
        prevlevel: Optional[CompilerContextLevel],
        mode: ContextSwitchMode,
        *,
        env: Optional[Environment] = None,
        scope_tree: Optional[irast.ScopeTreeNode] = None,
    ) -> None:
        if prevlevel is None:
            assert env is not None
            assert scope_tree is not None

            self.env = env
            self.argmap = collections.OrderedDict()

            self.singleton_mode = False

            self.toplevel_stmt = NO_STMT
            self.stmt = NO_STMT
            self.rel = NO_STMT
            self.rel_hierarchy = {}
            self.dml_stmts = {}
            self.parent_rel = None
            self.pending_query = None

            self.expr_exposed = None
            self.volatility_ref = None
            self.group_by_rels = {}

            self.disable_semi_join = set()
            self.force_optional = set()
            self.join_target_type_filter = {}

            self.path_scope = collections.ChainMap()
            self.scope_tree = scope_tree
            self.type_rel_overlays = collections.defaultdict(list)
            self.ptr_rel_overlays = collections.defaultdict(list)

        else:
            self.env = prevlevel.env
            self.argmap = prevlevel.argmap

            self.singleton_mode = prevlevel.singleton_mode

            self.toplevel_stmt = prevlevel.toplevel_stmt
            self.stmt = prevlevel.stmt
            self.rel = prevlevel.rel
            self.rel_hierarchy = prevlevel.rel_hierarchy
            self.dml_stmts = prevlevel.dml_stmts
            self.parent_rel = prevlevel.parent_rel
            self.pending_query = prevlevel.pending_query

            self.expr_exposed = prevlevel.expr_exposed
            self.volatility_ref = prevlevel.volatility_ref
            self.group_by_rels = prevlevel.group_by_rels

            self.disable_semi_join = prevlevel.disable_semi_join.copy()
            self.force_optional = prevlevel.force_optional.copy()
            self.join_target_type_filter = prevlevel.join_target_type_filter

            self.path_scope = prevlevel.path_scope
            self.scope_tree = prevlevel.scope_tree
            self.type_rel_overlays = prevlevel.type_rel_overlays
            self.ptr_rel_overlays = prevlevel.ptr_rel_overlays

            if mode in {ContextSwitchMode.SUBREL, ContextSwitchMode.NEWREL,
                        ContextSwitchMode.SUBSTMT}:
                if self.pending_query and mode == ContextSwitchMode.SUBSTMT:
                    self.rel = self.pending_query
                else:
                    self.rel = pgast.SelectStmt()
                    if mode != ContextSwitchMode.NEWREL:
                        if prevlevel.parent_rel is not None:
                            parent_rel = prevlevel.parent_rel
                        else:
                            parent_rel = prevlevel.rel
                        self.rel_hierarchy[self.rel] = parent_rel

                self.pending_query = None
                self.parent_rel = None

            if mode == ContextSwitchMode.SUBSTMT:
                self.stmt = self.rel

            if mode == ContextSwitchMode.NEWSCOPE:
                self.path_scope = prevlevel.path_scope.new_child()

    def subrel(
        self,
    ) -> compiler.CompilerContextManager[CompilerContextLevel]:
        return self.new(ContextSwitchMode.SUBREL)

    def newrel(
        self,
    ) -> compiler.CompilerContextManager[CompilerContextLevel]:
        return self.new(ContextSwitchMode.NEWREL)

    def substmt(
        self,
    ) -> compiler.CompilerContextManager[CompilerContextLevel]:
        return self.new(ContextSwitchMode.SUBSTMT)

    def newscope(
        self,
    ) -> compiler.CompilerContextManager[CompilerContextLevel]:
        return self.new(ContextSwitchMode.NEWSCOPE)


class CompilerContext(compiler.CompilerContext[CompilerContextLevel]):
    ContextLevelClass = CompilerContextLevel
    default_mode = ContextSwitchMode.TRANSPARENT


class Environment:
    """Static compilation environment."""

    aliases: aliases.AliasGenerator
    output_format: Optional[OutputFormat]
    use_named_params: bool
    ptrref_source_visibility: Dict[irast.BasePointerRef, bool]
    expected_cardinality_one: bool
    ignore_object_shapes: bool
    explicit_top_cast: Optional[irast.TypeRef]
    singleton_mode: bool
    query_params: Dict[str, irast.TypeRef]

    def __init__(
        self,
        *,
        output_format: Optional[OutputFormat],
        use_named_params: bool,
        expected_cardinality_one: bool,
        ignore_object_shapes: bool,
        singleton_mode: bool,
        explicit_top_cast: Optional[irast.TypeRef],
        query_params: Dict[str, irast.TypeRef],
    ) -> None:
        self.aliases = aliases.AliasGenerator()
        self.output_format = output_format
        self.use_named_params = use_named_params
        self.ptrref_source_visibility = {}
        self.expected_cardinality_one = expected_cardinality_one
        self.ignore_object_shapes = ignore_object_shapes
        self.singleton_mode = singleton_mode
        self.explicit_top_cast = explicit_top_cast
        self.query_params = query_params
