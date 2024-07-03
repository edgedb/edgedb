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
from typing import (
    Callable,
    Optional,
    Tuple,
    Union,
    Mapping,
    ChainMap,
    Dict,
    List,
    Set,
    FrozenSet,
    Generator,
    TYPE_CHECKING,
)

import collections
import contextlib
import dataclasses
import enum
import uuid

import immutables as immu

from edb.common import compiler
from edb.common import enum as s_enum

from edb.pgsql import ast as pgast
from edb.pgsql import params as pgparams

from . import aliases as pg_aliases

if TYPE_CHECKING:
    from edb.ir import ast as irast
    from . import enums as pgce


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


class OverlayOp(s_enum.StrEnum):
    UNION = 'union'
    REPLACE = 'replace'
    FILTER = 'filter'
    EXCEPT = 'except'


OverlayEntry = tuple[
    OverlayOp,
    Union[pgast.BaseRelation, pgast.CommonTableExpr],
    'irast.PathId',
]


@dataclasses.dataclass(kw_only=True)
class RelOverlays:
    """Container for relation overlays.

    These track "overlays" that can be registered for different types,
    in the context of DML.

    Consider the query:
      with X := (
        insert Person {
          name := "Sully",
          notes := assert_distinct({
            (insert Note {name := "1"}),
            (select Note filter .name = "2"),
          }),
        }),
      select X { name, notes: {name} };

    When we go to select X, we find the source of that set without any
    trouble (it's the result of the actual insert statement, more or
    less; in any case, it's in a CTE that we then include).

    Handling the notes are trickier, though:
      * The links aren't in the link table yet, but only in a CTE.
        (In similar update cases, with things like +=, they might be mixed
        between both.)
      * Some of the actual Note objects aren't in the table yet, just an insert
        CTE. But some *are*, so we need to union them.

    We solve these problems using overlays:
      * Whenever we do DML (or reference WITH-bound DML),
        we register overlays describing the changes done
        to *all of the enclosing DML*. So here, the Note insert's overlays
        get registered both for the Note insert and for the Person insert.
      * When we try to compile a root set or pointer, we see if it is connected
        to a DML statement, and if so we apply the overlays.

    The overlay itself is simply a sequence of operations on relations
    and CTEs that mix in the new data. In the obvious insert cases,
    these consist of unioning the new data in.

    This system works decently well but is also a little broken: I
    think that both the "all of the enclosing DML" and the "see if it
    is connected to a DML statement" have dangers; see Issue #3030.

    In relctx, see range_for_material_objtype, range_for_ptrref, and
    range_from_queryset (which those two call) for details on how
    overlays are applied.
    Overlays are added to with relctx.add_type_rel_overlay
    and relctx.add_ptr_rel_overlay.


    ===== NOTE ON MUTABILITY:
    In typical use, the overlays are mutable: nested DML adds overlays
    that are then consumed by code in enclosing contexts.

    In some places, however, we need to temporarily customize the
    overlay environment (during policy and trigger compilation, for
    example).

    The original version of overlays were implemented as a dict of
    dicts of lists. Doing temporary customizations required doing
    at least some copying. Doing a full deep copy always felt excessive
    but doing anything short of that left me constantly terrified.

    So instead we represent the overlays as a mutable object that
    contains immutable maps. When we add overlays, we update the maps
    and then reassign their values.

    When we want to do a temporary adjustment, we can cheaply make a
    fresh RelOverlays object and then modify that without touching the
    original.
    """

    #: Relations used to "overlay" the main table for
    #: the type.  Mostly used with DML statements.
    type: immu.Map[
        Optional[irast.MutatingLikeStmt],
        immu.Map[
            uuid.UUID,
            tuple[OverlayEntry, ...],
        ],
    ] = immu.Map()

    #: Relations used to "overlay" the main table for
    #: the pointer.  Mostly used with DML statements.
    ptr: immu.Map[
        Optional[irast.MutatingLikeStmt],
        immu.Map[
            Tuple[uuid.UUID, str],
            Tuple[
                Tuple[
                    OverlayOp,
                    Union[pgast.BaseRelation, pgast.CommonTableExpr],
                    irast.PathId,
                ], ...
            ],
        ],
    ] = immu.Map()

    def copy(self) -> RelOverlays:
        return RelOverlays(type=self.type, ptr=self.ptr)


class CompilerContextLevel(compiler.ContextLevel):
    #: static compilation environment
    env: Environment

    #: mapping of named args to position
    argmap: Dict[str, pgast.Param]

    #: whether compiling in singleton expression mode
    singleton_mode: bool

    #: whether compiling a trigger
    trigger_mode: bool

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

    #: CTEs representing pointers and their inherited pointers
    ptr_inheritance_ctes: Dict[uuid.UUID, pgast.CommonTableExpr]

    #: CTEs representing types, when rewritten based on access policy
    type_rewrite_ctes: Dict[FullRewriteKey, pgast.CommonTableExpr]

    #: A set of type CTEs currently being generated
    pending_type_rewrite_ctes: Set[RewriteKey]

    #: CTEs representing types and their inherited types
    type_inheritance_ctes: Dict[uuid.UUID, pgast.CommonTableExpr]

    # Type and type inheriance CTEs in creation order. This ensures type CTEs
    # referring to other CTEs are in the correct order.
    ordered_type_ctes: list[pgast.CommonTableExpr]

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
    dml_stmt_stack: List[irast.MutatingLikeStmt]

    #: Relations used to "overlay" the main table for
    #: the type.  Mostly used with DML statements.
    rel_overlays: RelOverlays

    #: Mapping from path ids to "external" rels given by a particular relation
    external_rels: Mapping[
        irast.PathId,
        Tuple[
            pgast.BaseRelation | pgast.CommonTableExpr,
            Tuple[pgce.PathAspect, ...]
        ]
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
            self.ptr_inheritance_ctes = {}
            self.type_rewrite_ctes = {}
            self.pending_type_rewrite_ctes = set()
            self.type_inheritance_ctes = {}
            self.ordered_type_ctes = []
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
            self.rel_overlays = RelOverlays()

            self.external_rels = {}
            self.enclosing_cte_iterator = None
            self.shapes_needed_by_dml = set()

            self.trigger_mode = False

        else:
            self.env = prevlevel.env
            self.argmap = prevlevel.argmap

            self.singleton_mode = prevlevel.singleton_mode

            self.toplevel_stmt = prevlevel.toplevel_stmt
            self.stmt = prevlevel.stmt
            self.rel = prevlevel.rel
            self.rel_hierarchy = prevlevel.rel_hierarchy
            self.param_ctes = prevlevel.param_ctes
            self.ptr_inheritance_ctes = prevlevel.ptr_inheritance_ctes
            self.type_rewrite_ctes = prevlevel.type_rewrite_ctes
            self.pending_type_rewrite_ctes = prevlevel.pending_type_rewrite_ctes
            self.type_inheritance_ctes = prevlevel.type_inheritance_ctes
            self.ordered_type_ctes = prevlevel.ordered_type_ctes
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
            self.rel_overlays = prevlevel.rel_overlays
            self.enclosing_cte_iterator = prevlevel.enclosing_cte_iterator
            self.shapes_needed_by_dml = prevlevel.shapes_needed_by_dml
            self.external_rels = prevlevel.external_rels

            self.trigger_mode = prevlevel.trigger_mode

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
                self.volatility_ref = ()

                self.disable_semi_join = frozenset()
                self.force_optional = frozenset()
                self.intersection_narrowing = {}
                self.pending_type_rewrite_ctes = set(
                    prevlevel.pending_type_rewrite_ctes
                )

            elif mode == ContextSwitchMode.NEWSCOPE:
                self.path_scope = prevlevel.path_scope.new_child()

    def get_current_dml_stmt(self) -> Optional[irast.MutatingLikeStmt]:
        if len(self.dml_stmt_stack) == 0:
            return None
        return self.dml_stmt_stack[-1]

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
FullRewriteKey = Tuple[
    uuid.UUID, bool, Optional[frozenset['irast.MutatingLikeStmt']]]


class Environment:
    """Static compilation environment."""

    aliases: pg_aliases.AliasGenerator
    output_format: Optional[OutputFormat]
    named_param_prefix: Optional[tuple[str, ...]]
    ptrref_source_visibility: Dict[irast.BasePointerRef, bool]
    expected_cardinality_one: bool
    ignore_object_shapes: bool
    explicit_top_cast: Optional[irast.TypeRef]
    singleton_mode: bool
    query_params: List[irast.Param]
    type_rewrites: Dict[RewriteKey, irast.Set]
    scope_tree_nodes: Dict[int, irast.ScopeTreeNode]
    external_rvars: Mapping[
        Tuple[irast.PathId, pgce.PathAspect], pgast.PathRangeVar
    ]
    materialized_views: Dict[uuid.UUID, irast.Set]
    backend_runtime_params: pgparams.BackendRuntimeParams
    versioned_stdlib: bool

    #: A list of CTEs that implement constraint validation at the
    #: query level.
    check_ctes: List[pgast.CommonTableExpr]

    def __init__(
        self,
        *,
        alias_generator: Optional[pg_aliases.AliasGenerator] = None,
        output_format: Optional[OutputFormat],
        named_param_prefix: Optional[tuple[str, ...]],
        expected_cardinality_one: bool,
        ignore_object_shapes: bool,
        singleton_mode: bool,
        is_explain: bool,
        explicit_top_cast: Optional[irast.TypeRef],
        query_params: List[irast.Param],
        type_rewrites: Dict[RewriteKey, irast.Set],
        scope_tree_nodes: Dict[int, irast.ScopeTreeNode],
        external_rvars: Optional[
            Mapping[Tuple[irast.PathId, pgce.PathAspect], pgast.PathRangeVar]
        ] = None,
        backend_runtime_params: pgparams.BackendRuntimeParams,
        # XXX: TRAMPOLINE: THIS IS WRONG
        versioned_stdlib: bool = True,
    ) -> None:
        self.aliases = alias_generator or pg_aliases.AliasGenerator()
        self.output_format = output_format
        self.named_param_prefix = named_param_prefix
        self.ptrref_source_visibility = {}
        self.expected_cardinality_one = expected_cardinality_one
        self.ignore_object_shapes = ignore_object_shapes
        self.singleton_mode = singleton_mode
        self.is_explain = is_explain
        self.explicit_top_cast = explicit_top_cast
        self.query_params = query_params
        self.type_rewrites = type_rewrites
        self.scope_tree_nodes = scope_tree_nodes
        self.external_rvars = external_rvars or {}
        self.materialized_views = {}
        self.check_ctes = []
        self.backend_runtime_params = backend_runtime_params
        self.versioned_stdlib = versioned_stdlib


# XXX: this context hack is necessary until pathctx is converted
#      to use context levels instead of using env directly.
@contextlib.contextmanager
def output_format(
    ctx: CompilerContextLevel,
    output_format: OutputFormat,
) -> Generator[None, None, None]:
    original_output_format = ctx.env.output_format
    original_ignore_object_shapes = ctx.env.ignore_object_shapes
    ctx.env.output_format = output_format
    ctx.env.ignore_object_shapes = False
    try:
        yield
    finally:
        ctx.env.output_format = original_output_format
        ctx.env.ignore_object_shapes = original_ignore_object_shapes
