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


"""EdgeQL to IR compiler context."""

from __future__ import annotations
from typing import *  # NoQA
from typing_extensions import Protocol  # type: ignore

import collections
import dataclasses
import enum
import uuid

from edb.common import compiler
from edb.common import parsing

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.ir import ast as irast

from edb.schema import functions as s_func
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import pointers as s_pointers
from edb.schema import schema as s_schema
from edb.schema import types as s_types

if TYPE_CHECKING:
    from edb.schema import objtypes as s_objtypes
    from edb.schema import sources as s_sources


class ContextSwitchMode(enum.Enum):
    NEW = enum.auto()
    SUBQUERY = enum.auto()
    NEWSCOPE = enum.auto()
    NEWSCOPE_TEMP = enum.auto()
    NEWFENCE = enum.auto()
    NEWFENCE_TEMP = enum.auto()
    DETACHED = enum.auto()


class ViewRPtr:
    def __init__(
        self,
        source: s_sources.Source,
        *,
        ptrcls: Optional[s_pointers.Pointer],
        ptrcls_name: Optional[s_name.Name] = None,
        base_ptrcls: Optional[s_pointers.Pointer] = None,
        ptrcls_is_linkprop: bool = False,
        ptrcls_is_alias: bool = False,
        rptr: Optional[irast.Pointer] = None,
        is_insert: bool = False,
        is_update: bool = False,
    ) -> None:
        self.source = source
        self.ptrcls = ptrcls
        self.base_ptrcls = base_ptrcls
        self.ptrcls_name = ptrcls_name
        self.ptrcls_is_linkprop = ptrcls_is_linkprop
        self.ptrcls_is_alias = ptrcls_is_alias
        self.rptr = rptr
        self.is_insert = is_insert
        self.is_update = is_update


@dataclasses.dataclass
class StatementMetadata:
    is_unnest_fence: bool = False


class CompletionWorkCallback(Protocol):

    def __call__(
        self,
        *,
        ctx: ContextLevel,
    ) -> None:
        ...


class PointerCardinalityCallback(Protocol):

    def __call__(
        self,
        ptrcls: s_pointers.PointerLike,
        *,
        ctx: ContextLevel,
    ) -> None:
        ...


class PendingCardinality(NamedTuple):

    specified_cardinality: Optional[qltypes.Cardinality]
    source_ctx: Optional[parsing.ParserContext]
    callbacks: List[PointerCardinalityCallback]


class Environment:
    """Compilation environment."""

    schema: s_schema.Schema
    """A Schema instance to use for class resolution."""

    parent_object_type: Optional[s_obj.ObjectMeta]
    """The type of a schema object, if the expression is part of its def."""

    path_scope: irast.ScopeTreeNode
    """Overrall expression path scope tree."""

    schema_view_cache: Dict[s_types.Type, s_types.Type]
    """Type cache used by schema-level views."""

    query_parameters: Dict[str, s_types.Type]
    """A mapping of query parameters to their types.  Gets populated during
    the compilation."""

    schema_view_mode: bool
    """Use material types for pointer targets in schema views."""

    set_types: Dict[irast.Set, s_types.Type]
    """A dictionary of all Set instances and their schema types."""

    type_origins: Dict[s_types.Type, parsing.ParserContext]
    """A dictionary of notable types and their source origins.

    This is used to trace where a particular type instance originated in
    order to provide useful diagnostics for type errors.
    """

    inferred_types: Dict[irast.Base, s_types.Type]
    """A dictionary of all expressions and their inferred schema types."""

    inferred_cardinality: Dict[
        Tuple[irast.Base, irast.ScopeTreeNode],
        qltypes.Cardinality]
    """A dictionary of all expressions and their inferred cardinality."""

    constant_folding: bool
    """Enables constant folding optimization (enabled by default)."""

    view_shapes: Dict[
        Union[s_types.Type, s_pointers.PointerLike],
        List[s_pointers.Pointer]
    ]
    """Object output or modification shapes."""

    view_shapes_metadata: Dict[s_types.Type, irast.ViewShapeMetadata]

    func_params: Optional[s_func.ParameterLikeList]
    """If compiling a function body, a list of function parameters."""

    json_parameters: bool
    """Force types of all parameters to std::json"""

    session_mode: bool
    """Whether there is a specific session."""

    schema_refs: Set[s_obj.Object]
    """A set of all schema objects referenced by an expression."""

    created_schema_objects: Set[s_obj.Object]
    """A set of all schema objects derived by this compilation."""

    allow_generic_type_output: bool
    """Whether to allow the expression to be of a generic type."""

    def __init__(
        self,
        *,
        schema: s_schema.Schema,
        path_scope: irast.ScopeTreeNode,
        parent_object_type: Optional[s_obj.ObjectMeta]=None,
        schema_view_mode: bool=False,
        constant_folding: bool=True,
        json_parameters: bool=False,
        session_mode: bool=False,
        allow_generic_type_output: bool=False,
        func_params: Optional[s_func.ParameterLikeList]=None,
    ) -> None:
        self.schema = schema
        self.path_scope = path_scope
        self.schema_view_cache = {}
        self.query_parameters = {}
        self.schema_view_mode = schema_view_mode
        self.set_types = {}
        self.type_origins = {}
        self.inferred_types = {}
        self.inferred_cardinality = {}
        self.constant_folding = constant_folding
        self.view_shapes = collections.defaultdict(list)
        self.view_shapes_metadata = collections.defaultdict(
            irast.ViewShapeMetadata)
        self.json_parameters = json_parameters
        self.session_mode = session_mode
        self.allow_generic_type_output = allow_generic_type_output
        self.schema_refs = set()
        self.created_schema_objects = set()
        self.func_params = func_params
        self.parent_object_type = parent_object_type

    def get_track_schema_object(
        self,
        name: str,
        *,
        modaliases: Optional[Mapping[Optional[str], str]] = None,
        type: Tuple[s_obj.ObjectMeta, ...] = (),
        default: Union[s_obj.Object, s_obj.NoDefaultT, None] = s_obj.NoDefault,
        label: Optional[str] = None,
        condition: Optional[Callable[[s_obj.Object], bool]] = None,
    ) -> s_obj.Object:
        sobj = self.schema.get(name, module_aliases=modaliases, type=type,
                               condition=condition, label=label,
                               default=default)
        if sobj is not default:
            self.schema_refs.add(sobj)

        return sobj

    def get_track_schema_type(
        self,
        name: str,
        *,
        modaliases: Optional[Mapping[Optional[str], str]] = None,
        default: Union[None, s_obj.Object, s_obj.NoDefaultT] = s_obj.NoDefault,
        label: Optional[str]=None,
        condition: Optional[Callable[[s_obj.Object], bool]]=None,
    ) -> s_types.Type:

        stype = self.get_track_schema_object(
            name, modaliases=modaliases, default=default, label=label,
            condition=condition, type=(s_types.Type,),
        )

        return cast(s_types.Type, stype)


class ContextLevel(compiler.ContextLevel):

    env: Environment
    """Compilation environment common for all context levels."""

    derived_target_module: Optional[str]
    """The name of the module for classes derived by views."""

    anchors: Dict[
        Union[str, Type[qlast.SpecialAnchor]],
        irast.Set,
    ]
    """A mapping of anchor variables (aliases to path expressions passed
    to the compiler programmatically).
    """

    modaliases: Dict[Optional[str], str]
    """A combined list of module name aliases declared in the WITH block,
    or passed to the compiler programmatically.
    """

    func: Optional[s_func.Function]
    """Schema function object required when compiling functions bodies."""

    stmt_metadata: Dict[qlast.Statement, StatementMetadata]
    """Extra statement metadata needed by the compiler, but not in AST."""

    source_map: Dict[
        s_pointers.PointerLike,
        Tuple[
            qlast.Expr,
            ContextLevel,
            irast.PathId,
            Optional[irast.WeakNamespace],
        ],
    ]
    """A mapping of computable pointers to QL source AST and context."""

    view_nodes: Dict[s_name.SchemaName, s_types.Type]
    """A dictionary of newly derived Node classes representing views."""

    view_sets: Dict[s_types.Type, irast.Set]
    """A dictionary of IR expressions for views declared in the query."""

    aliased_views: ChainMap[str, Optional[s_types.Type]]
    """A dictionary of views aliased in a statement body."""

    must_use_views: Dict[s_types.Type, Tuple[str, parsing.ParserContext]]
    """A set of views that *must* be used in an expression."""

    expr_view_cache: Dict[Tuple[qlast.Base, str], irast.Set]
    """Type cache used by expression-level views."""

    shape_type_cache: Dict[
        Tuple[
            s_objtypes.ObjectType,
            Tuple[qlast.ShapeElement, ...],
        ],
        s_objtypes.ObjectType,
    ]
    """Type cache for shape expressions."""

    class_view_overrides: Dict[uuid.UUID, s_types.Type]
    """Object mapping used by implicit view override in SELECT."""

    clause: Optional[str]
    """Statement clause the compiler is currently in."""

    toplevel_stmt: Optional[irast.Stmt]
    """Top-level statement."""

    stmt: Optional[irast.Stmt]
    """Statement node currently being built."""

    path_id_namespace: FrozenSet[str]
    """A namespace to use for all path ids."""

    pending_stmt_own_path_id_namespace: FrozenSet[str]
    """A path id namespace to add to the fence of the next statement."""

    pending_stmt_full_path_id_namespace: FrozenSet[str]
    """A set of path id namespaces to use in path ids in the next statement."""

    banned_paths: Set[irast.PathId]
    """A set of path ids that are considered invalid in this context."""

    view_map: ChainMap[irast.PathId, irast.Set]
    """Set translation map.  Used for views."""

    completion_work: List[CompletionWorkCallback]
    """A list of callbacks to execute when the whole query has been seen."""

    pending_cardinality: Dict[
        s_pointers.PointerLike,
        PendingCardinality,
    ]
    """A set of derived pointers for which the cardinality is not yet known."""

    pointer_derivation_map: Dict[
        s_pointers.Pointer,
        List[s_pointers.Pointer],
    ]
    """A parent: children mapping of derived pointer classes."""

    path_scope: irast.ScopeTreeNode
    """Path scope tree, with per-lexical-scope levels."""

    path_scope_map: Dict[irast.Set, irast.ScopeTreeNode]
    """A forest of scope trees used for views."""

    scope_id_ctr: compiler.SimpleCounter
    """Path scope id counter."""

    view_rptr: Optional[ViewRPtr]
    """Pointer information for the top-level view of the substatement."""

    view_scls: Optional[s_types.Type]
    """Schema class for the top-level set of the substatement."""

    toplevel_result_view_name: Optional[s_name.SchemaName]
    """The name to use for the view that is the result of the top statement."""

    partial_path_prefix: Optional[irast.Set]
    """The set used as a prefix for partial paths."""

    implicit_id_in_shapes: bool
    """Whether to include the id property in object shapes implicitly."""

    implicit_tid_in_shapes: bool
    """Whether to include the type id property in object shapes implicitly."""

    special_computables_in_mutation_shape: FrozenSet[str]
    """A set of "special" compiutable pointers allowed in mutation shape."""

    empty_result_type_hint: Optional[s_types.Type]
    """Type to use if the statement result expression is an empty set ctor."""

    defining_view: bool
    """Whether a view is currently being defined (as opposed to be compiled)"""

    def __init__(
        self,
        prevlevel: Optional[ContextLevel],
        mode: ContextSwitchMode,
        *,
        env: Optional[Environment] = None,
    ) -> None:
        self.mode = mode

        if prevlevel is None:
            assert env is not None
            self.env = env
            self.derived_target_module = None
            self.aliases = compiler.AliasGenerator()
            self.anchors = {}
            self.modaliases = {}
            self.stmt_metadata = {}
            self.completion_work = []
            self.pending_cardinality = {}
            self.pointer_derivation_map = collections.defaultdict(list)

            self.source_map = {}
            self.view_nodes = {}
            self.view_sets = {}
            self.aliased_views = collections.ChainMap()
            self.must_use_views = {}
            self.expr_view_cache = {}
            self.shape_type_cache = {}
            self.class_view_overrides = {}

            self.toplevel_stmt = None
            self.stmt = None
            self.path_id_namespace = frozenset()
            self.pending_stmt_own_path_id_namespace = frozenset()
            self.pending_stmt_full_path_id_namespace = frozenset()
            self.banned_paths = set()
            self.view_map = collections.ChainMap()
            self.path_scope = irast.new_scope_tree()
            self.path_scope_map = {}
            self.scope_id_ctr = compiler.SimpleCounter()
            self.view_scls = None
            self.expr_exposed = False

            self.partial_path_prefix = None

            self.view_rptr = None
            self.toplevel_result_view_name = None
            self.implicit_id_in_shapes = False
            self.implicit_tid_in_shapes = False
            self.special_computables_in_mutation_shape = frozenset()
            self.empty_result_type_hint = None
            self.defining_view = False

        else:
            self.env = prevlevel.env
            self.derived_target_module = prevlevel.derived_target_module
            self.aliases = prevlevel.aliases
            self.stmt_metadata = prevlevel.stmt_metadata
            self.completion_work = prevlevel.completion_work
            self.pending_cardinality = prevlevel.pending_cardinality
            self.pointer_derivation_map = prevlevel.pointer_derivation_map

            self.source_map = prevlevel.source_map
            self.view_nodes = prevlevel.view_nodes
            self.view_sets = prevlevel.view_sets
            self.must_use_views = prevlevel.must_use_views
            self.expr_view_cache = prevlevel.expr_view_cache
            self.shape_type_cache = prevlevel.shape_type_cache

            self.path_id_namespace = prevlevel.path_id_namespace
            self.pending_stmt_own_path_id_namespace = \
                prevlevel.pending_stmt_own_path_id_namespace
            self.pending_stmt_full_path_id_namespace = \
                prevlevel.pending_stmt_full_path_id_namespace
            self.banned_paths = prevlevel.banned_paths
            self.view_map = prevlevel.view_map
            self.path_scope = prevlevel.path_scope
            self.path_scope_map = prevlevel.path_scope_map
            self.scope_id_ctr = prevlevel.scope_id_ctr
            self.view_scls = prevlevel.view_scls
            self.expr_exposed = prevlevel.expr_exposed
            self.partial_path_prefix = prevlevel.partial_path_prefix
            self.toplevel_stmt = prevlevel.toplevel_stmt
            self.implicit_id_in_shapes = prevlevel.implicit_id_in_shapes
            self.implicit_tid_in_shapes = prevlevel.implicit_tid_in_shapes
            self.special_computables_in_mutation_shape = \
                prevlevel.special_computables_in_mutation_shape
            self.empty_result_type_hint = prevlevel.empty_result_type_hint
            self.defining_view = prevlevel.defining_view

            if mode == ContextSwitchMode.SUBQUERY:
                self.anchors = prevlevel.anchors.copy()
                self.modaliases = prevlevel.modaliases.copy()
                self.aliased_views = prevlevel.aliased_views.new_child()
                self.class_view_overrides = \
                    prevlevel.class_view_overrides.copy()

                self.pending_stmt_own_path_id_namespace = frozenset()
                self.pending_stmt_full_path_id_namespace = frozenset()
                self.banned_paths = prevlevel.banned_paths.copy()

                self.view_rptr = None
                self.view_scls = None
                self.stmt = None

                self.view_rptr = None
                self.toplevel_result_view_name = None

            elif mode == ContextSwitchMode.DETACHED:
                self.anchors = prevlevel.anchors.copy()
                self.modaliases = prevlevel.modaliases.copy()
                self.aliased_views = collections.ChainMap()
                self.class_view_overrides = {}
                self.expr_exposed = prevlevel.expr_exposed

                self.view_nodes = {}
                self.view_sets = {}
                self.path_id_namespace = frozenset({self.aliases.get('ns')})
                self.pending_stmt_own_path_id_namespace = frozenset()
                self.pending_stmt_full_path_id_namespace = frozenset()
                self.banned_paths = set()

                self.view_rptr = None
                self.view_scls = None
                self.stmt = prevlevel.stmt

                self.partial_path_prefix = None

                self.view_rptr = None
                self.toplevel_result_view_name = None
            else:
                self.anchors = prevlevel.anchors
                self.modaliases = prevlevel.modaliases
                self.aliased_views = prevlevel.aliased_views
                self.class_view_overrides = prevlevel.class_view_overrides

                self.stmt = prevlevel.stmt

                self.view_rptr = prevlevel.view_rptr
                self.toplevel_result_view_name = \
                    prevlevel.toplevel_result_view_name

            if mode in {ContextSwitchMode.NEWFENCE_TEMP,
                        ContextSwitchMode.NEWSCOPE_TEMP}:
                if prevlevel.path_scope is None:
                    prevlevel.path_scope = self.env.path_scope

                self.path_scope = prevlevel.path_scope.copy()

            if mode in {ContextSwitchMode.NEWFENCE,
                        ContextSwitchMode.NEWFENCE_TEMP}:
                if prevlevel.path_scope is None:
                    prevlevel.path_scope = self.env.path_scope

                self.path_scope = prevlevel.path_scope.attach_fence()

            if mode in {ContextSwitchMode.NEWSCOPE,
                        ContextSwitchMode.NEWSCOPE_TEMP}:
                if prevlevel.path_scope is None:
                    prevlevel.path_scope = self.env.path_scope

                self.path_scope = prevlevel.path_scope.attach_branch()

    def on_pop(self, prevlevel: Optional[ContextLevel]) -> None:
        if (prevlevel is not None
                and self.mode in {ContextSwitchMode.NEWFENCE_TEMP,
                                  ContextSwitchMode.NEWSCOPE_TEMP}):
            prevlevel.path_scope.remove_subtree(self.path_scope)

    def subquery(self) -> compiler.CompilerContextManager[ContextLevel]:
        return self.new(ContextSwitchMode.SUBQUERY)

    def newscope(
        self,
        *,
        temporary: bool = False,
        fenced: bool = False,
    ) -> compiler.CompilerContextManager[ContextLevel]:
        if temporary and fenced:
            mode = ContextSwitchMode.NEWFENCE_TEMP
        elif temporary:
            mode = ContextSwitchMode.NEWSCOPE_TEMP
        elif fenced:
            mode = ContextSwitchMode.NEWFENCE
        else:
            mode = ContextSwitchMode.NEWSCOPE

        return self.new(mode)

    def detached(self) -> compiler.CompilerContextManager[ContextLevel]:
        return self.new(ContextSwitchMode.DETACHED)


class CompilerContext(compiler.CompilerContext[ContextLevel]):
    ContextLevelClass = ContextLevel
    default_mode = ContextSwitchMode.NEW
