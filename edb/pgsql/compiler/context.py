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
import contextlib
import enum
import uuid

from edb.common import compiler

from edb.pgsql import ast as pgast
from edb.pgsql import params as pgparams

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
    #: Result data output in PostgreSQL format.
    NATIVE = enum.auto()
    #: Result data output as a single JSON string.
    JSON = enum.auto()
    #: Result data output as a single PostgreSQL JSONB type value.
    JSONB = enum.auto()
    #: Result data output as a JSON string for each element in returned set.
    JSON_ELEMENTS = enum.auto()
    #: None mode: query result not returned, cardinality of result set
    #: is returned instead.
    NONE = enum.auto()
    #: Like NATIVE, but objects without an explicit shape are serialized
    #: as UUIDs.
    NATIVE_INTERNAL = enum.auto()


NO_STMT = pgast.SelectStmt()


class CompilerContextLevel(compiler.ContextLevel):
    #: static compilation environment
    env: Environment

    #: mapping of named args to position
    argmap: Dict[str, pgast.Param]

    #: whether compiling in singleton expression mode
    singleton_mode: bool

    #: the top-level SQL statement
    toplevel_stmt: pgast.Query

    #: Record of DML CTEs generated for the corresponding IR DML.
    #: CTEs generated for DML-containing FOR statements are keyed
    #: by their iterator set.
    dml_stmts: Dict[Union[irast.MutatingStmt, irast.Set],
                    pgast.CommonTableExpr]

    #: SQL statement corresponding to the IR statement
    #: currently being compiled.
    stmt: pgast.SelectStmt

    #: Current SQL subquery
    rel: pgast.SelectStmt

    #: SQL query hierarchy
    rel_hierarchy: Dict[pgast.Query, pgast.Query]

    #: CTEs representing decoded parameters
    param_ctes: Dict[str, pgast.CommonTableExpr]

    #: CTEs representing schema types, when rewritten based on access policy
    type_ctes: Dict[RewriteKey, pgast.CommonTableExpr]

    #: A set of type CTEs currently being generated
    pending_type_ctes: Set[RewriteKey]

    #: The logical parent of the current query in the
    #: query hierarchy
    parent_rel: Optional[pgast.Query]

    #: Query to become current in the next SUBSTMT switch.
    pending_query: Optional[pgast.SelectStmt]

    #: Sets currently being materialized
    materializing: FrozenSet[irast.Stmt]

    #: Whether the expression currently being processed is
    #: directly exposed to the output of the statement.
    expr_exposed: Optional[bool]

    #: A hack that indicates a tuple element that should be treated as
    #: exposed. This enables us to treat 'bar' in (foo, bar).1 as exposed,
    #: which eta-expansion and some casts rely on.
    expr_exposed_tuple_cheat: Optional[irast.TupleElement]

    #: Expression to use to force SQL expression volatility in this context
    #: (Delayed with a lambda to avoid inserting it when not used.)
    volatility_ref: Tuple[
        Callable[[pgast.SelectStmt, CompilerContextLevel],
                 Optional[pgast.BaseExpr]], ...]

    # Current path_id we are INSERTing, so that we can avoid creating
    # a bogus volatility ref to it...
    current_insert_path_id: Optional[irast.PathId]

    #: Paths, for which semi-join is banned in this context.
    disable_semi_join: FrozenSet[irast.PathId]

    #: Paths, which need to be explicitly wrapped into SQL
    #: optionality scaffolding.
    force_optional: FrozenSet[irast.PathId]

    #: Paths that can be ignored when they appear as the source of a
    # computable. This is key to optimizing away free object sources in
    # group by aggregates.
    skippable_sources: FrozenSet[irast.PathId]

    #: Specifies that references to a specific Set must be narrowed
    #: by only selecting instances of type specified by the mapping value.
    intersection_narrowing: Dict[irast.Set, irast.Set]

    #: Which SQL query holds the SQL scope for the given PathId
    path_scope: ChainMap[irast.PathId, Optional[pgast.SelectStmt]]

    #: Relevant IR scope for this context.
    scope_tree: irast.ScopeTreeNode

    #: A stack of dml statements currently being compiled. Used for
    #: figuring out what to record in type_rel_overlays.
    dml_stmt_stack: List[irast.MutatingStmt]

    #: Relations used to "overlay" the main table for
    #: the type.  Mostly used with DML statements.
    type_rel_overlays: DefaultDict[
        Optional[irast.MutatingStmt],
        DefaultDict[
            uuid.UUID,
            List[
                Tuple[
                    str,
                    Union[pgast.BaseRelation, pgast.CommonTableExpr],
                    irast.PathId,
                ]
            ],
        ],
    ]

    #: Relations used to "overlay" the main table for
    #: the pointer.  Mostly used with DML statements.
    ptr_rel_overlays: DefaultDict[
        Optional[irast.MutatingStmt],
        DefaultDict[
            Tuple[uuid.UUID, str],
            List[
                Tuple[
                    str,
                    Union[pgast.BaseRelation, pgast.CommonTableExpr],
                    irast.PathId,
                ]
            ],
        ],
    ]

    #: The CTE and some metadata of any enclosing iterator-like
    #: construct (which includes iterators, insert/update, and INSERT
    #: ELSE select clauses) currently being compiled.
    enclosing_cte_iterator: Optional[pgast.IteratorCTE]

    #: Sets to force shape compilation on, because the values are
    #: needed by DML.
    shapes_needed_by_dml: Set[irast.Set]

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
            self.param_ctes = {}
            self.type_ctes = {}
            self.pending_type_ctes = set()
            self.dml_stmts = {}
            self.parent_rel = None
            self.pending_query = None
            self.materializing = frozenset()

            self.expr_exposed = None
            self.expr_exposed_tuple_cheat = None
            self.volatility_ref = ()
            self.current_insert_path_id = None

            self.disable_semi_join = frozenset()
            self.force_optional = frozenset()
            self.skippable_sources = frozenset()
            self.intersection_narrowing = {}

            self.path_scope = collections.ChainMap()
            self.scope_tree = scope_tree
            self.dml_stmt_stack = []
            self.type_rel_overlays = collections.defaultdict(
                lambda: collections.defaultdict(list))
            self.ptr_rel_overlays = collections.defaultdict(
                lambda: collections.defaultdict(list))
            self.enclosing_cte_iterator = None
            self.shapes_needed_by_dml = set()

        else:
            self.env = prevlevel.env
            self.argmap = prevlevel.argmap

            self.singleton_mode = prevlevel.singleton_mode

            self.toplevel_stmt = prevlevel.toplevel_stmt
            self.stmt = prevlevel.stmt
            self.rel = prevlevel.rel
            self.rel_hierarchy = prevlevel.rel_hierarchy
            self.param_ctes = prevlevel.param_ctes
            self.type_ctes = prevlevel.type_ctes
            self.pending_type_ctes = prevlevel.pending_type_ctes
            self.dml_stmts = prevlevel.dml_stmts
            self.parent_rel = prevlevel.parent_rel
            self.pending_query = prevlevel.pending_query
            self.materializing = prevlevel.materializing

            self.expr_exposed = prevlevel.expr_exposed
            self.expr_exposed_tuple_cheat = prevlevel.expr_exposed_tuple_cheat
            self.volatility_ref = prevlevel.volatility_ref
            self.current_insert_path_id = prevlevel.current_insert_path_id

            self.disable_semi_join = prevlevel.disable_semi_join
            self.force_optional = prevlevel.force_optional
            self.skippable_sources = prevlevel.skippable_sources
            self.intersection_narrowing = prevlevel.intersection_narrowing

            self.path_scope = prevlevel.path_scope
            self.scope_tree = prevlevel.scope_tree
            self.dml_stmt_stack = prevlevel.dml_stmt_stack
            self.type_rel_overlays = prevlevel.type_rel_overlays
            self.ptr_rel_overlays = prevlevel.ptr_rel_overlays
            self.enclosing_cte_iterator = prevlevel.enclosing_cte_iterator
            self.shapes_needed_by_dml = prevlevel.shapes_needed_by_dml

            if mode is ContextSwitchMode.SUBSTMT:
                if self.pending_query is not None:
                    self.rel = self.pending_query
                else:
                    self.rel = pgast.SelectStmt()
                    if prevlevel.parent_rel is not None:
                        parent_rel = prevlevel.parent_rel
                    else:
                        parent_rel = prevlevel.rel
                    self.rel_hierarchy[self.rel] = parent_rel

                self.stmt = self.rel
                self.pending_query = None
                self.parent_rel = None

            elif mode is ContextSwitchMode.SUBREL:
                self.rel = pgast.SelectStmt()
                if prevlevel.parent_rel is not None:
                    parent_rel = prevlevel.parent_rel
                else:
                    parent_rel = prevlevel.rel
                self.rel_hierarchy[self.rel] = parent_rel
                self.pending_query = None
                self.parent_rel = None

            elif mode is ContextSwitchMode.NEWREL:
                self.rel = pgast.SelectStmt()
                self.pending_query = None
                self.parent_rel = None
                self.path_scope = collections.ChainMap()
                self.rel_hierarchy = {}
                self.scope_tree = prevlevel.scope_tree.root

                self.disable_semi_join = frozenset()
                self.force_optional = frozenset()
                self.intersection_narrowing = {}
                self.pending_type_ctes = set(prevlevel.pending_type_ctes)

            elif mode == ContextSwitchMode.NEWSCOPE:
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

    def up_hierarchy(
        self,
        n: int, q: Optional[pgast.Query]=None
    ) -> Optional[pgast.Query]:
        # mostly intended as a debugging helper
        q = q or self.rel
        for _ in range(n):
            if q:
                q = self.rel_hierarchy.get(q)
        return q


class CompilerContext(compiler.CompilerContext[CompilerContextLevel]):
    ContextLevelClass = CompilerContextLevel
    default_mode = ContextSwitchMode.TRANSPARENT


RewriteKey = Tuple[uuid.UUID, bool]


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
    query_params: List[irast.Param]
    type_rewrites: Dict[RewriteKey, irast.Set]
    scope_tree_nodes: Dict[int, irast.ScopeTreeNode]
    external_rvars: Mapping[Tuple[irast.PathId, str], pgast.PathRangeVar]
    external_rels: Mapping[irast.PathId, pgast.BaseRelation]
    materialized_views: Dict[uuid.UUID, irast.Set]
    backend_runtime_params: pgparams.BackendRuntimeParams

    #: A list of CTEs that implement constraint validation at the
    #: query level.
    check_ctes: List[pgast.CommonTableExpr]

    def __init__(
        self,
        *,
        output_format: Optional[OutputFormat],
        use_named_params: bool,
        expected_cardinality_one: bool,
        ignore_object_shapes: bool,
        singleton_mode: bool,
        expand_inhviews: bool,
        explicit_top_cast: Optional[irast.TypeRef],
        query_params: List[irast.Param],
        type_rewrites: Dict[RewriteKey, irast.Set],
        scope_tree_nodes: Dict[int, irast.ScopeTreeNode],
        external_rvars: Optional[
            Mapping[Tuple[irast.PathId, str], pgast.PathRangeVar]
        ] = None,
        external_rels: Optional[
            Mapping[irast.PathId, pgast.BaseRelation]
        ] = None,
        backend_runtime_params: pgparams.BackendRuntimeParams,
    ) -> None:
        self.aliases = aliases.AliasGenerator()
        self.output_format = output_format
        self.use_named_params = use_named_params
        self.ptrref_source_visibility = {}
        self.expected_cardinality_one = expected_cardinality_one
        self.ignore_object_shapes = ignore_object_shapes
        self.singleton_mode = singleton_mode
        self.expand_inhviews = expand_inhviews
        self.explicit_top_cast = explicit_top_cast
        self.query_params = query_params
        self.type_rewrites = type_rewrites
        self.scope_tree_nodes = scope_tree_nodes
        self.external_rvars = external_rvars or {}
        self.external_rels = external_rels or {}
        self.materialized_views = {}
        self.check_ctes = []
        self.backend_runtime_params = backend_runtime_params


# XXX: this context hack is necessary until pathctx is converted
#      to use context levels instead of using env directly.
@contextlib.contextmanager
def output_format(
    ctx: CompilerContextLevel,
    output_format: OutputFormat,
) -> Generator[None, None, None]:
    original_output_format = ctx.env.output_format
    ctx.env.output_format = output_format
    try:
        yield
    finally:
        ctx.env.output_format = original_output_format
