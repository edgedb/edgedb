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
from typing import (
    Callable,
    Literal,
    Optional,
    Tuple,
    Type,
    Union,
    Mapping,
    MutableMapping,
    Sequence,
    ChainMap,
    Dict,
    List,
    Set,
    FrozenSet,
    NamedTuple,
    cast,
    overload,
    TYPE_CHECKING,
)

import collections
import dataclasses
import enum
import uuid
import weakref

from edb.common import compiler
from edb.common import ordered
from edb.common import parsing

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.ir import ast as irast
from edb.ir import utils as irutils
from edb.ir import typeutils as irtyputils

from edb.schema import expraliases as s_aliases
from edb.schema import futures as s_futures
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import pointers as s_pointers
from edb.schema import schema as s_schema
from edb.schema import types as s_types

from .options import GlobalCompilerOptions

if TYPE_CHECKING:
    from edb.schema import objtypes as s_objtypes
    from edb.schema import sources as s_sources


class Exposure(enum.IntEnum):
    UNEXPOSED = 0
    BINDING = 1
    EXPOSED = 2

    def __bool__(self) -> bool:
        return self == Exposure.EXPOSED


class ContextSwitchMode(enum.Enum):
    NEW = enum.auto()
    SUBQUERY = enum.auto()
    NEWSCOPE = enum.auto()
    NEWFENCE = enum.auto()
    DETACHED = enum.auto()


@dataclasses.dataclass(kw_only=True)
class ViewRPtr:
    source: s_sources.Source
    ptrcls: Optional[s_pointers.Pointer]
    ptrcls_name: Optional[s_name.QualName] = None
    base_ptrcls: Optional[s_pointers.Pointer] = None
    ptrcls_is_linkprop: bool = False
    ptrcls_is_alias: bool = False
    exprtype: s_types.ExprType = s_types.ExprType.Select
    rptr_dir: Optional[s_pointers.PointerDirection] = None


@dataclasses.dataclass
class ScopeInfo:
    path_scope: irast.ScopeTreeNode
    binding_kind: Optional[irast.BindingKind]
    pinned_path_id_ns: Optional[FrozenSet[str]] = None


class PointerRefCache(Dict[irtyputils.PtrRefCacheKey, irast.BasePointerRef]):

    _rcache: Dict[irast.BasePointerRef, s_pointers.PointerLike]

    def __init__(self) -> None:
        super().__init__()
        self._rcache = {}

    def __setitem__(
        self,
        key: irtyputils.PtrRefCacheKey,
        val: irast.BasePointerRef,
    ) -> None:
        super().__setitem__(key, val)
        self._rcache[val] = key

    def get_ptrcls_for_ref(
        self,
        ref: irast.BasePointerRef,
    ) -> Optional[s_pointers.PointerLike]:
        return self._rcache.get(ref)


# Volatility inference computes two volatility results:
# A basic one, and one for consumption by materialization
InferredVolatility = Union[
    qltypes.Volatility, Tuple[qltypes.Volatility, qltypes.Volatility]]


class Environment:
    """Compilation environment."""

    schema: s_schema.Schema
    """A Schema instance to use for class resolution."""

    orig_schema: s_schema.Schema
    """A Schema as it was at the start of the compilation."""

    options: GlobalCompilerOptions
    """Compiler options."""

    path_scope: irast.ScopeTreeNode
    """Overrall expression path scope tree."""

    schema_view_cache: Dict[
        tuple[s_types.Type, object],
        tuple[s_types.Type, irast.Set],
    ]
    """Type cache used by schema-level views."""

    query_parameters: Dict[str, irast.Param]
    """A mapping of query parameters to their types.  Gets populated during
    the compilation."""

    query_globals: Dict[s_name.QualName, irast.Global]
    """A mapping of query globals.  Gets populated during
    the compilation."""

    set_types: Dict[irast.Set, s_types.Type]
    """A dictionary of all Set instances and their schema types."""

    type_origins: Dict[s_types.Type, Optional[parsing.Span]]
    """A dictionary of notable types and their source origins.

    This is used to trace where a particular type instance originated in
    order to provide useful diagnostics for type errors.
    """

    inferred_volatility: Dict[
        irast.Base,
        InferredVolatility]
    """A dictionary of expressions and their inferred volatility."""

    view_shapes: Dict[
        Union[s_types.Type, s_pointers.PointerLike],
        List[Tuple[s_pointers.Pointer, qlast.ShapeOp]]
    ]
    """Object output or modification shapes."""

    pointer_derivation_map: Dict[
        s_pointers.Pointer,
        List[s_pointers.Pointer],
    ]
    """A parent: children mapping of derived pointer classes."""

    pointer_specified_info: Dict[
        s_pointers.Pointer,
        Tuple[
            Optional[qltypes.SchemaCardinality],
            Optional[bool],
            Optional[parsing.Span],
        ],
    ]
    """Cardinality/source context for pointers with unclear cardinality."""

    view_shapes_metadata: Dict[s_types.Type, irast.ViewShapeMetadata]

    schema_refs: Set[s_obj.Object]
    """A set of all schema objects referenced by an expression."""

    schema_ref_exprs: Optional[Dict[s_obj.Object, Set[qlast.Base]]]
    """Map from all schema objects referenced to the ast referants.

    This is used for rewriting expressions in the schema after a rename. """

    # Caches for costly operations in edb.ir.typeutils
    ptr_ref_cache: PointerRefCache
    type_ref_cache: Dict[irtyputils.TypeRefCacheKey, irast.TypeRef]

    dml_exprs: List[qlast.Base]
    """A list of DML expressions (statements and DML-containing
    functions) that appear in a function body.
    """

    dml_stmts: list[irast.MutatingStmt]
    """A list of DML statements in the query"""

    #: A list of bindings that should be assumed to be singletons.
    singletons: List[irast.PathId]

    scope_tree_nodes: MutableMapping[int, irast.ScopeTreeNode]
    """Map from unique_id to nodes."""

    materialized_sets: Dict[
        Union[s_types.Type, s_pointers.PointerLike],
        Tuple[qlast.Statement, Sequence[irast.MaterializeReason]],
    ]
    """A mapping of computed sets that must be computed only once."""

    compiled_stmts: Dict[qlast.Statement, irast.Stmt]
    """A mapping of from input edgeql to compiled IR"""

    alias_result_view_name: Optional[s_name.QualName]
    """The name of a view being defined as an alias."""

    script_params: Dict[str, irast.Param]
    """All parameter definitions from an enclosing multi-statement script.

    Used to make sure the types are consistent."""

    source_map: Dict[s_pointers.PointerLike, irast.ComputableInfo]
    """A mapping of computable pointers to QL source AST and context."""

    type_rewrites: Dict[
        Tuple[s_types.Type, bool], irast.Set | None | Literal[True]]
    """Access policy rewrites for schema-level types.

    None indicates no rewrite, True indicates a compound type
    that had rewrites in its components.
    """

    expr_view_cache: Dict[Tuple[qlast.Base, s_name.Name], irast.Set]
    """Type cache used by expression-level views."""

    shape_type_cache: Dict[
        Tuple[
            s_objtypes.ObjectType,
            s_types.ExprType,
            Tuple[qlast.ShapeElement, ...],
        ],
        s_objtypes.ObjectType,
    ]
    """Type cache for shape expressions."""

    path_scope_map: Dict[irast.Set, ScopeInfo]
    """A dictionary of scope info that are appropriate for a given view."""

    dml_rewrites: Dict[irast.Set, irast.Rewrites]
    """Compiled rewrites that should be attached to InsertStmt or UpdateStmt"""

    warnings: list[errors.EdgeDBError]
    """List of warnings to emit"""

    def __init__(
        self,
        *,
        schema: s_schema.Schema,
        path_scope: Optional[irast.ScopeTreeNode] = None,
        alias_result_view_name: Optional[s_name.QualName] = None,
        options: Optional[GlobalCompilerOptions] = None,
    ) -> None:
        if options is None:
            options = GlobalCompilerOptions()

        if path_scope is None:
            path_scope = irast.new_scope_tree()

        self.options = options
        self.schema = schema
        self.orig_schema = schema
        self.path_scope = path_scope
        self.schema_view_cache = {}
        self.query_parameters = {}
        self.query_globals = {}
        self.set_types = {}
        self.type_origins = {}
        self.inferred_volatility = {}
        self.view_shapes = collections.defaultdict(list)
        self.view_shapes_metadata = collections.defaultdict(
            irast.ViewShapeMetadata)
        self.schema_refs = set()
        self.schema_ref_exprs = {} if options.track_schema_ref_exprs else None
        self.ptr_ref_cache = PointerRefCache()
        self.type_ref_cache = {}
        self.dml_exprs = []
        self.dml_stmts = []
        self.pointer_derivation_map = collections.defaultdict(list)
        self.pointer_specified_info = {}
        self.singletons = []
        self.scope_tree_nodes = weakref.WeakValueDictionary()
        self.materialized_sets = {}
        self.compiled_stmts = {}
        self.alias_result_view_name = alias_result_view_name
        self.script_params = {}
        self.source_map = {}
        self.type_rewrites = {}
        self.shape_type_cache = {}
        self.expr_view_cache = {}
        self.path_scope_map = {}
        self.dml_rewrites = {}
        self.warnings = []

    def add_schema_ref(
        self, sobj: s_obj.Object, expr: Optional[qlast.Base]
    ) -> None:
        self.schema_refs.add(sobj)
        if self.schema_ref_exprs is not None and expr:
            self.schema_ref_exprs.setdefault(sobj, set()).add(expr)

    @overload
    def get_schema_object_and_track(
        self,
        name: s_name.Name,
        expr: Optional[qlast.Base],
        *,
        modaliases: Optional[Mapping[Optional[str], str]] = None,
        type: Optional[Type[s_obj.Object]] = None,
        default: Union[s_obj.Object, s_obj.NoDefaultT] = s_obj.NoDefault,
        label: Optional[str] = None,
        condition: Optional[Callable[[s_obj.Object], bool]] = None,
    ) -> s_obj.Object:
        ...

    @overload
    def get_schema_object_and_track(
        self,
        name: s_name.Name,
        expr: Optional[qlast.Base],
        *,
        modaliases: Optional[Mapping[Optional[str], str]] = None,
        type: Optional[Type[s_obj.Object]] = None,
        default: Union[s_obj.Object, s_obj.NoDefaultT, None] = s_obj.NoDefault,
        label: Optional[str] = None,
        condition: Optional[Callable[[s_obj.Object], bool]] = None,
    ) -> Optional[s_obj.Object]:
        ...

    def get_schema_object_and_track(
        self,
        name: s_name.Name,
        expr: Optional[qlast.Base],
        *,
        modaliases: Optional[Mapping[Optional[str], str]] = None,
        type: Optional[Type[s_obj.Object]] = None,
        default: Union[s_obj.Object, s_obj.NoDefaultT, None] = s_obj.NoDefault,
        label: Optional[str] = None,
        condition: Optional[Callable[[s_obj.Object], bool]] = None,
    ) -> Optional[s_obj.Object]:
        sobj = self.schema.get(
            name, module_aliases=modaliases, type=type,
            condition=condition, label=label,
            default=default)
        if sobj is not None and sobj is not default:
            self.add_schema_ref(sobj, expr)

            if (
                isinstance(sobj, s_types.Type)
                and sobj.get_expr(self.schema) is not None
            ):
                # If the type is derived from an ALIAS declaration,
                # make sure we record the reference to the Alias object
                # as well for correct delta ordering.
                alias_objs = self.schema.get_referrers(
                    sobj,
                    scls_type=s_aliases.Alias,
                    field_name='type',
                )
                for obj in alias_objs:
                    self.add_schema_ref(obj, expr)

        return sobj

    def get_schema_type_and_track(
        self,
        name: s_name.Name,
        expr: Optional[qlast.Base]=None,
        *,
        modaliases: Optional[Mapping[Optional[str], str]] = None,
        default: Union[None, s_obj.Object, s_obj.NoDefaultT] = s_obj.NoDefault,
        label: Optional[str]=None,
        condition: Optional[Callable[[s_obj.Object], bool]]=None,
    ) -> s_types.Type:

        stype = self.get_schema_object_and_track(
            name, expr, modaliases=modaliases, default=default, label=label,
            condition=condition, type=s_types.Type,
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

    view_nodes: Dict[s_name.Name, s_types.Type]
    """A dictionary of newly derived Node classes representing views."""

    view_sets: Dict[s_obj.Object, irast.Set]
    """A dictionary of IR expressions for views declared in the query."""

    suppress_rewrites: FrozenSet[s_types.Type]
    """Types to suppress using rewrites on"""

    aliased_views: ChainMap[s_name.Name, irast.Set]
    """A dictionary of views aliased in a statement body."""

    class_view_overrides: Dict[uuid.UUID, s_types.Type]
    """Object mapping used by implicit view override in SELECT."""

    clause: Optional[str]
    """Statement clause the compiler is currently in."""

    toplevel_stmt: Optional[irast.Stmt]
    """Top-level statement."""

    stmt: Optional[irast.Stmt]
    """Statement node currently being built."""

    qlstmt: Optional[qlast.Statement]
    """Statement source node currently being built."""

    path_id_namespace: FrozenSet[str]
    """A namespace to use for all path ids."""

    pending_stmt_own_path_id_namespace: FrozenSet[str]
    """A path id namespace to add to the fence of the next statement."""

    pending_stmt_full_path_id_namespace: FrozenSet[str]
    """A set of path id namespaces to use in path ids in the next statement."""

    inserting_paths: Dict[irast.PathId, Literal['body'] | Literal['else']]
    """A set of path ids that are currently being inserted."""

    view_map: ChainMap[
        irast.PathId,
        Tuple[Tuple[irast.PathId, irast.Set], ...],
    ]
    """Set translation map.  Used for mapping computable sources..

    When compiling a computable, we need to be able to map references to
    the source back to the correct source set.

    This maps from a namespace-stripped source path_id to the expected
    computable-internal path_id and the actual source set.

    The namespace stripping is necessary to handle the case where
    bindings have added more namespaces to the source set reference.
    (See test_edgeql_scope_computables_13.)
    """

    path_scope: irast.ScopeTreeNode
    """Path scope tree, with per-lexical-scope levels."""

    iterator_ctx: Optional[ContextLevel]
    """The context of the statement where all iterators should be placed."""

    iterator_path_ids: FrozenSet[irast.PathId]
    """The path ids of all in scope iterator variables"""

    scope_id_ctr: compiler.SimpleCounter
    """Path scope id counter."""

    view_rptr: Optional[ViewRPtr]
    """Pointer information for the top-level view of the substatement."""

    view_scls: Optional[s_types.Type]
    """Schema class for the top-level set of the substatement."""

    toplevel_result_view_name: Optional[s_name.QualName]
    """The name to use for the view that is the result of the top statement."""

    partial_path_prefix: Optional[irast.Set]
    """The set used as a prefix for partial paths."""

    implicit_id_in_shapes: bool
    """Whether to include the id property in object shapes implicitly."""

    implicit_tid_in_shapes: bool
    """Whether to include the type id property in object shapes implicitly."""

    implicit_tname_in_shapes: bool
    """Whether to include the type name property in object shapes
       implicitly."""

    implicit_limit: int
    """Implicit LIMIT clause in SELECT statements."""

    special_computables_in_mutation_shape: FrozenSet[str]
    """A set of "special" computable pointers allowed in mutation shape."""

    empty_result_type_hint: Optional[s_types.Type]
    """Type to use if the statement result expression is an empty set ctor."""

    defining_view: Optional[s_objtypes.ObjectType]
    """Whether a view is currently being defined (as opposed to be compiled)"""

    current_schema_views: tuple[s_types.Type, ...]
    """Which schema views are currently being compiled"""

    recompiling_schema_alias: bool
    """Whether we are currently recompiling a schema-level expression alias."""

    compiling_update_shape: bool
    """Whether an UPDATE shape is currently being compiled."""

    active_computeds: ordered.OrderedSet[s_pointers.Pointer]
    """A ordered set of currently compiling computeds"""

    allow_endpoint_linkprops: bool
    """Whether to allow references to endpoint linkpoints (@source, @target)."""

    disallow_dml: Optional[str]
    """Whether we are currently in a place where no dml is allowed,
        if not None, then it is of the form `in a FILTER clause`  """

    active_rewrites: FrozenSet[s_objtypes.ObjectType]
    """For detecting cycles in rewrite rules"""

    active_defaults: FrozenSet[s_objtypes.ObjectType]
    """For detecting cycles in defaults"""

    collection_cast_info: Optional[CollectionCastInfo]
    """For generating errors messages when casting to collections.

    This will be set by the outermost cast and then shared between all
    sub-casts.

    Some casts (eg. arrays) will generate select statements containing other
    type casts. These will also share the outermost cast info.
    """

    no_factoring: bool
    warn_factoring: bool

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

            self.view_nodes = {}
            self.view_sets = {}
            self.suppress_rewrites = frozenset()
            self.aliased_views = collections.ChainMap()
            self.class_view_overrides = {}

            self.toplevel_stmt = None
            self.stmt = None
            self.qlstmt = None
            self.path_id_namespace = frozenset()
            self.pending_stmt_own_path_id_namespace = frozenset()
            self.pending_stmt_full_path_id_namespace = frozenset()
            self.inserting_paths = {}
            self.view_map = collections.ChainMap()
            self.path_scope = env.path_scope
            self.iterator_path_ids = frozenset()
            self.scope_id_ctr = compiler.SimpleCounter()
            self.view_scls = None
            self.expr_exposed = Exposure.UNEXPOSED

            self.partial_path_prefix = None

            self.view_rptr = None
            self.toplevel_result_view_name = None
            self.implicit_id_in_shapes = False
            self.implicit_tid_in_shapes = False
            self.implicit_tname_in_shapes = False
            self.implicit_limit = 0
            self.special_computables_in_mutation_shape = frozenset()
            self.empty_result_type_hint = None
            self.defining_view = None
            self.current_schema_views = ()
            self.compiling_update_shape = False
            self.active_computeds = ordered.OrderedSet()
            self.recompiling_schema_alias = False
            self.active_rewrites = frozenset()
            self.active_defaults = frozenset()

            self.allow_endpoint_linkprops = False
            self.disallow_dml = None
            self.no_factoring = False
            self.warn_factoring = False

            self.collection_cast_info = None

        else:
            self.env = prevlevel.env
            self.derived_target_module = prevlevel.derived_target_module
            self.aliases = prevlevel.aliases

            self.view_nodes = prevlevel.view_nodes
            self.view_sets = prevlevel.view_sets
            self.suppress_rewrites = prevlevel.suppress_rewrites

            self.iterator_path_ids = prevlevel.iterator_path_ids
            self.path_id_namespace = prevlevel.path_id_namespace
            self.pending_stmt_own_path_id_namespace = \
                prevlevel.pending_stmt_own_path_id_namespace
            self.pending_stmt_full_path_id_namespace = \
                prevlevel.pending_stmt_full_path_id_namespace
            self.inserting_paths = prevlevel.inserting_paths
            self.view_map = prevlevel.view_map
            if prevlevel.path_scope is None:
                prevlevel.path_scope = self.env.path_scope
            self.path_scope = prevlevel.path_scope
            self.scope_id_ctr = prevlevel.scope_id_ctr
            self.view_scls = prevlevel.view_scls
            self.expr_exposed = prevlevel.expr_exposed
            self.partial_path_prefix = prevlevel.partial_path_prefix
            self.toplevel_stmt = prevlevel.toplevel_stmt
            self.implicit_id_in_shapes = prevlevel.implicit_id_in_shapes
            self.implicit_tid_in_shapes = prevlevel.implicit_tid_in_shapes
            self.implicit_tname_in_shapes = prevlevel.implicit_tname_in_shapes
            self.implicit_limit = prevlevel.implicit_limit
            self.special_computables_in_mutation_shape = \
                prevlevel.special_computables_in_mutation_shape
            self.empty_result_type_hint = prevlevel.empty_result_type_hint
            self.defining_view = prevlevel.defining_view
            self.current_schema_views = prevlevel.current_schema_views
            self.compiling_update_shape = prevlevel.compiling_update_shape
            self.active_computeds = prevlevel.active_computeds
            self.recompiling_schema_alias = prevlevel.recompiling_schema_alias
            self.active_rewrites = prevlevel.active_rewrites
            self.active_defaults = prevlevel.active_defaults

            self.allow_endpoint_linkprops = prevlevel.allow_endpoint_linkprops
            self.disallow_dml = prevlevel.disallow_dml
            self.no_factoring = prevlevel.no_factoring
            self.warn_factoring = prevlevel.warn_factoring

            self.collection_cast_info = prevlevel.collection_cast_info

            if mode == ContextSwitchMode.SUBQUERY:
                self.anchors = prevlevel.anchors.copy()
                self.modaliases = prevlevel.modaliases.copy()
                self.aliased_views = prevlevel.aliased_views.new_child()
                self.class_view_overrides = \
                    prevlevel.class_view_overrides.copy()

                self.pending_stmt_own_path_id_namespace = frozenset()
                self.pending_stmt_full_path_id_namespace = frozenset()
                self.inserting_paths = prevlevel.inserting_paths.copy()

                self.view_rptr = None
                self.view_scls = None
                self.stmt = None
                self.qlstmt = None

                self.view_rptr = None
                self.toplevel_result_view_name = None

            elif mode == ContextSwitchMode.DETACHED:
                self.anchors = prevlevel.anchors.copy()
                self.modaliases = prevlevel.modaliases.copy()
                self.aliased_views = collections.ChainMap()
                self.view_map = collections.ChainMap()
                self.class_view_overrides = {}
                self.expr_exposed = prevlevel.expr_exposed

                self.view_nodes = {}
                self.view_sets = {}
                self.path_id_namespace = frozenset({self.aliases.get('ns')})
                self.pending_stmt_own_path_id_namespace = frozenset()
                self.pending_stmt_full_path_id_namespace = frozenset()
                self.inserting_paths = {}

                self.view_rptr = None
                self.view_scls = None
                self.stmt = prevlevel.stmt
                self.qlstmt = prevlevel.qlstmt

                self.partial_path_prefix = None

                self.view_rptr = None
                self.toplevel_result_view_name = None
            else:
                self.anchors = prevlevel.anchors
                self.modaliases = prevlevel.modaliases
                self.aliased_views = prevlevel.aliased_views
                self.class_view_overrides = prevlevel.class_view_overrides

                self.stmt = prevlevel.stmt
                self.qlstmt = prevlevel.qlstmt

                self.view_rptr = prevlevel.view_rptr
                self.toplevel_result_view_name = \
                    prevlevel.toplevel_result_view_name

            if mode == ContextSwitchMode.NEWFENCE:
                self.path_scope = self.path_scope.attach_fence()

            if mode == ContextSwitchMode.NEWSCOPE:
                self.path_scope = self.path_scope.attach_branch()

    def subquery(self) -> compiler.CompilerContextManager[ContextLevel]:
        return self.new(ContextSwitchMode.SUBQUERY)

    def newscope(
        self,
        *,
        fenced: bool,
    ) -> compiler.CompilerContextManager[ContextLevel]:
        if fenced:
            mode = ContextSwitchMode.NEWFENCE
        else:
            mode = ContextSwitchMode.NEWSCOPE

        return self.new(mode)

    def detached(self) -> compiler.CompilerContextManager[ContextLevel]:
        return self.new(ContextSwitchMode.DETACHED)

    def create_anchor(
        self, ir: irast.Set, name: str = 'v', *, check_dml: bool = False
    ) -> qlast.Path:
        alias = self.aliases.get(name)
        # TODO: We should probably always check for DML, but I'm
        # concerned about perf, since we don't cache it at all.
        has_dml = check_dml and irutils.contains_dml(ir)
        self.anchors[alias] = ir
        return qlast.Path(
            steps=[qlast.IRAnchor(name=alias, has_dml=has_dml)],
        )

    def maybe_create_anchor(
        self,
        ir: Union[irast.Set, qlast.Expr],
        name: str = 'v',
    ) -> qlast.Expr:
        if isinstance(ir, irast.Set):
            return self.create_anchor(ir, name)
        else:
            return ir

    def get_security_context(self) -> object:
        '''Compute an additional compilation cache key.

        Return an additional key for any compilation caches that may
        vary based on "security contexts" such as whether we are in an
        access policy.
        '''
        # N.B: Whether we are compiling a trigger is not included here
        # since we clear cached rewrites when compiling them in the
        # *pgsql* compiler.
        return bool(self.suppress_rewrites)

    def log_warning(self, warning: errors.EdgeDBError) -> None:
        self.env.warnings.append(warning)

    def allow_factoring(self) -> None:
        self.no_factoring = self.warn_factoring = False

    def schema_factoring(self) -> None:
        self.no_factoring = s_futures.future_enabled(
            self.env.schema, 'simple_scoping'
        )
        self.warn_factoring = s_futures.future_enabled(
            self.env.schema, 'warn_old_scoping'
        )


class CompilerContext(compiler.CompilerContext[ContextLevel]):
    ContextLevelClass = ContextLevel
    default_mode = ContextSwitchMode.NEW


class CollectionCastInfo(NamedTuple):
    """For generating errors messages when casting to collections."""

    from_type: s_types.Type
    to_type: s_types.Type

    path_elements: list[Tuple[str, Optional[str]]]
    """Represents a path to the current collection element being cast.

    A path element is a tuple of the collection type and an optional
    element name. eg. ('tuple', 'a') or ('array', None)

    The list is shared between the outermost context and all its sub contexts.
    When casting a collection, each element's path should be pushed before
    entering the "sub-cast" and popped immediately after.

    In the event of a cast error, the list is preserved at the outermost cast.
    """
