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

import collections
import enum
import typing

from edb.lang.common import compiler
from edb.lang.common import parsing

from edb.lang.edgeql import ast as qlast
from edb.lang.ir import ast as irast

from edb.lang.schema import functions as s_func
from edb.lang.schema import name as s_name
from edb.lang.schema import nodes as s_nodes
from edb.lang.schema import objects as s_obj
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import schema as s_schema
from edb.lang.schema import types as s_types


class ContextSwitchMode(enum.Enum):
    NEW = enum.auto()
    SUBQUERY = enum.auto()
    NEWSCOPE = enum.auto()
    NEWSCOPE_TEMP = enum.auto()
    NEWFENCE = enum.auto()
    NEWFENCE_TEMP = enum.auto()
    DETACHED = enum.auto()


class ViewRPtr:
    def __init__(self, source, *, ptrcls=None, ptrcls_name=None,
                 base_ptrcls=None, ptrcls_is_linkprop=None,
                 derived_ptrcls=None, rptr=None, is_insert=False,
                 is_update=False):
        self.source = source
        self.ptrcls = ptrcls
        self.base_ptrcls = base_ptrcls
        self.ptrcls_name = ptrcls_name
        self.ptrcls_is_linkprop = ptrcls_is_linkprop
        self.derived_ptrcls = derived_ptrcls
        self.rptr = rptr
        self.is_insert = is_insert
        self.is_update = is_update


class StatementMetadata:
    is_unnest_fence: bool
    ignore_offset_limit: bool

    def __init__(self, *, is_unnest_fence=False, ignore_offset_limit=False):
        self.is_unnest_fence = is_unnest_fence
        self.ignore_offset_limit = ignore_offset_limit


class Environment:
    """Compilation environment."""

    schema: s_schema.Schema
    """A Schema instance to use for class resolution."""

    path_scope: irast.ScopeTreeNode
    """Overrall expression path scope tree."""

    schema_view_cache: typing.Dict[s_nodes.Node, s_nodes.Node]
    """Type cache used by schema-level views."""

    query_parameters: typing.Dict[str, s_obj.Object]
    """A mapping of query parameters to their types.  Gets populated during
    the compilation."""

    schema_view_mode: bool
    """Use material types for pointer targets in schema views."""

    def __init__(self, *, schema, path_scope, schema_view_mode: bool=False):
        self.schema = schema
        self.path_scope = path_scope
        self.schema_view_cache = {}
        self.query_parameters = {}
        self.schema_view_mode = schema_view_mode


class ContextLevel(compiler.ContextLevel):

    env: Environment
    """Compilation environment common for all context levels."""

    derived_target_module: typing.Optional[str]
    """The name of the module for classes derived by views."""

    anchors: typing.Dict[str, irast.Set]
    """A mapping of anchor variables (aliases to path expressions passed
    to the compiler programmatically).
    """

    modaliases: typing.Dict[str, str]
    """A combined list of module name aliases declared in the WITH block,
    or passed to the compiler programmatically.
    """

    func: typing.Optional[s_func.Function]
    """Schema function object required when compiling functions bodies."""

    all_sets: typing.List[irast.Set]
    """A list of all Set instances generated."""

    stmt_metadata: typing.Dict[qlast.Statement, StatementMetadata]
    """Extra statement metadata needed by the compiler, but not in AST."""

    source_map: typing.Dict[s_pointers.Pointer,
                            typing.Tuple[qlast.Expr, compiler.ContextLevel,
                                         typing.Optional[irast.WeakNamespace]]]
    """A mapping of computable pointers to QL source AST and context."""

    view_nodes: typing.Dict[s_name.SchemaName, s_nodes.Node]
    """A dictionary of newly derived Node classes representing views."""

    view_sets: typing.Dict[s_nodes.Node, irast.Set]
    """A dictionary of IR expressions for views declared in the query."""

    aliased_views: typing.Dict[str, s_nodes.Node]
    """A dictionary of views aliased in a statement body."""

    expr_view_cache: typing.Dict[typing.Tuple[qlast.Base, str],
                                 irast.Set]
    """Type cache used by expression-level views."""

    shape_type_cache: typing.Dict[typing.Tuple[qlast.ShapeElement, ...],
                                  s_nodes.Node]
    """Type cache for shape expressions."""

    class_view_overrides: typing.Dict[s_name.SchemaName, s_nodes.Node]
    """Object mapping used by implicit view override in SELECT."""

    clause: str
    """Statement clause the compiler is currently in."""

    toplevel_clause: str
    """Top-level statement clause the compiler is currently in."""

    toplevel_stmt: irast.Stmt
    """Top-level statement."""

    stmt: irast.Stmt
    """Statement node currently being built."""

    path_id_namespace: typing.FrozenSet[str]
    """A namespace to use for all path ids."""

    pending_stmt_own_path_id_namespace: typing.FrozenSet[str]
    """A path id namespace to add to the fence of the next statement."""

    pending_stmt_full_path_id_namespace: typing.FrozenSet[str]
    """A set of path id namespaces to use in path ids in the next statement."""

    view_map: typing.Dict[irast.PathId, irast.Set]
    """Set translation map.  Used for views."""

    class_shapes: typing.Dict[s_types.Type,
                              typing.List[s_pointers.Pointer]]
    """Object output or modification shapes."""

    completion_work: typing.List[typing.Callable]
    """A list of callbacks to execute when the whole query has been seen."""

    pending_cardinality: typing.Dict[
        s_pointers.Pointer,
        typing.Tuple[
            typing.Optional[irast.Cardinality],
            parsing.ParserContext,
        ],
    ]
    """A set of derived pointers for which the cardinality is not yet known."""

    pointer_derivation_map: typing.Dict[s_pointers.Pointer, s_pointers.Pointer]
    """A parent: children mapping of derived pointer classes."""

    path_scope: irast.ScopeTreeNode
    """Path scope tree, with per-lexical-scope levels."""

    path_scope_is_temp: bool
    """Whether the current path scope is temporary and is to be discarded."""

    path_scope_map: typing.Dict[irast.Set, irast.ScopeTreeNode]
    """A forest of scope trees used for views."""

    scope_id_ctr: compiler.Counter
    """Path scope id counter."""

    view_rptr: ViewRPtr
    """Pointer information for the top-level view of the substatement."""

    view_scls: s_types.Type
    """Schema class for the top-level set of the substatement."""

    toplevel_result_view_name: s_name.SchemaName
    """The name to use for the view that is the result of the top statement."""

    partial_path_prefix: irast.Set
    """The set used as a prefix for partial paths."""

    implicit_id_in_shapes: bool
    """Whether to include the id property in object shapes implicitly."""

    empty_result_type_hint: s_types.Type
    """Type to use if the statement result expression is an empty set ctor."""

    def __init__(self, prevlevel, mode):
        self.mode = mode

        if prevlevel is None:
            self.env = None
            self.derived_target_module = None
            self.aliases = compiler.AliasGenerator()
            self.anchors = {}
            self.modaliases = {}
            self.func = None
            self.all_sets = []
            self.stmt_metadata = {}
            self.completion_work = []
            self.pending_cardinality = {}
            self.pointer_derivation_map = collections.defaultdict(list)

            self.source_map = {}
            self.view_nodes = {}
            self.view_sets = {}
            self.aliased_views = collections.ChainMap()
            self.expr_view_cache = {}
            self.shape_type_cache = {}
            self.class_view_overrides = {}

            self.clause = None
            self.toplevel_clause = None
            self.toplevel_stmt = None
            self.stmt = None
            self.path_id_namespace = frozenset()
            self.pending_stmt_own_path_id_namespace = frozenset()
            self.pending_stmt_full_path_id_namespace = frozenset()
            self.view_map = collections.ChainMap()
            self.class_shapes = collections.defaultdict(list)
            self.path_scope = None
            self.path_scope_is_temp = False
            self.path_scope_map = {}
            self.scope_id_ctr = compiler.Counter()
            self.view_scls = None
            self.expr_exposed = False

            self.partial_path_prefix = None

            self.view_rptr = None
            self.toplevel_result_view_name = None
            self.implicit_id_in_shapes = False
            self.empty_result_type_hint = None

        else:
            self.env = prevlevel.env
            self.derived_target_module = prevlevel.derived_target_module
            self.aliases = prevlevel.aliases
            self.func = prevlevel.func
            self.all_sets = prevlevel.all_sets
            self.stmt_metadata = prevlevel.stmt_metadata
            self.completion_work = prevlevel.completion_work
            self.pending_cardinality = prevlevel.pending_cardinality
            self.pointer_derivation_map = prevlevel.pointer_derivation_map

            self.source_map = prevlevel.source_map
            self.view_nodes = prevlevel.view_nodes
            self.view_sets = prevlevel.view_sets
            self.expr_view_cache = prevlevel.expr_view_cache
            self.shape_type_cache = prevlevel.shape_type_cache

            self.path_id_namespace = prevlevel.path_id_namespace
            self.pending_stmt_own_path_id_namespace = \
                prevlevel.pending_stmt_own_path_id_namespace
            self.pending_stmt_full_path_id_namespace = \
                prevlevel.pending_stmt_full_path_id_namespace
            self.view_map = prevlevel.view_map
            self.class_shapes = prevlevel.class_shapes
            self.path_scope = prevlevel.path_scope
            self.path_scope_is_temp = prevlevel.path_scope_is_temp
            self.path_scope_map = prevlevel.path_scope_map
            self.scope_id_ctr = prevlevel.scope_id_ctr
            self.view_scls = prevlevel.view_scls
            self.expr_exposed = prevlevel.expr_exposed
            self.toplevel_clause = prevlevel.toplevel_clause
            self.toplevel_stmt = prevlevel.toplevel_stmt
            self.implicit_id_in_shapes = prevlevel.implicit_id_in_shapes
            self.empty_result_type_hint = prevlevel.empty_result_type_hint

            if mode == ContextSwitchMode.SUBQUERY:
                self.anchors = prevlevel.anchors.copy()
                self.modaliases = prevlevel.modaliases.copy()
                self.aliased_views = prevlevel.aliased_views.new_child()
                self.class_view_overrides = \
                    prevlevel.class_view_overrides.copy()

                self.pending_stmt_own_path_id_namespace = None
                self.pending_stmt_full_path_id_namespace = None

                self.view_rptr = None
                self.view_scls = None
                self.clause = None
                self.stmt = None

                self.partial_path_prefix = None

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

                self.view_rptr = None
                self.view_scls = None
                self.clause = None
                self.stmt = None

                self.partial_path_prefix = None

                self.view_rptr = None
                self.toplevel_result_view_name = None
            else:
                self.anchors = prevlevel.anchors
                self.modaliases = prevlevel.modaliases
                self.aliased_views = prevlevel.aliased_views
                self.class_view_overrides = prevlevel.class_view_overrides

                self.clause = prevlevel.clause
                self.stmt = prevlevel.stmt

                self.partial_path_prefix = prevlevel.partial_path_prefix

                self.view_rptr = prevlevel.view_rptr
                self.toplevel_result_view_name = \
                    prevlevel.toplevel_result_view_name

            if mode in {ContextSwitchMode.NEWFENCE_TEMP,
                        ContextSwitchMode.NEWSCOPE_TEMP}:
                if prevlevel.path_scope is None:
                    prevlevel.path_scope = self.env.path_scope

                self.path_scope = prevlevel.path_scope.copy()
                self.path_scope_is_temp = True

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

    def on_pop(self, prevlevel):
        if self.mode in {ContextSwitchMode.NEWFENCE_TEMP,
                         ContextSwitchMode.NEWSCOPE_TEMP}:
            prevlevel.path_scope.remove_subtree(self.path_scope)

    def subquery(self):
        return self.new(ContextSwitchMode.SUBQUERY)

    def newscope(self, *, temporary=False, fenced=False):
        if temporary and fenced:
            mode = ContextSwitchMode.NEWFENCE_TEMP
        elif temporary:
            mode = ContextSwitchMode.NEWSCOPE_TEMP
        elif fenced:
            mode = ContextSwitchMode.NEWFENCE
        else:
            mode = ContextSwitchMode.NEWSCOPE

        return self.new(mode)

    def detached(self):
        return self.new(ContextSwitchMode.DETACHED)


class CompilerContext(compiler.CompilerContext):
    ContextLevelClass = ContextLevel
    default_mode = ContextSwitchMode.NEW
