##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL to IR compiler context."""

import collections
import enum
import typing

from edgedb.lang.common import compiler

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.ir import ast as irast

from edgedb.lang.schema import name as s_name
from edgedb.lang.schema import nodes as s_nodes
from edgedb.lang.schema import objects as so
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import schema as s_schema
from edgedb.lang.schema import types as s_types


class ContextSwitchMode(enum.Enum):
    NEW = enum.auto()
    SUBQUERY = enum.auto()
    NEWSCOPE = enum.auto()
    NEWSCOPE_TEMP = enum.auto()
    NEWFENCE = enum.auto()
    NEWFENCE_TEMP = enum.auto()


class ViewRPtr:
    def __init__(self, source, ptrcls, *,
                 rptr=None, is_insert=False, is_update=False):
        self.source = source
        self.ptrcls = ptrcls
        self.derived_ptrcls = None
        self.rptr = rptr
        self.is_insert = is_insert
        self.is_update = is_update


class ContextLevel(compiler.ContextLevel):

    schema: s_schema.Schema
    """A Schema instance to use for class resolution."""

    derived_target_module: typing.Optional[str]
    """The name of the module for classes derived by views."""

    anchors: typing.Dict[str, irast.Set]
    """A mapping of anchor variables (aliases to path expressions passed
    to the compiler programmatically).
    """

    namespaces: typing.Dict[str, str]
    """A combined list of module name aliases declared in the WITH block,
    or passed to the compiler programmatically.
    """

    arguments: typing.Dict[str, so.Class]
    """A mapping of statement parameter types passed to the compiler
    programmatically."""

    source_map: typing.Dict[s_pointers.Pointer,
                            typing.Tuple[qlast.Expr, compiler.ContextLevel]]
    """A mapping of computable pointers to QL source AST and context."""

    view_nodes: typing.Dict[s_name.SchemaName, s_nodes.Node]
    """A dictionary of newly derived Node classes representing views."""

    view_sets: typing.Dict[s_nodes.Node, irast.Set]
    """A dictionary of IR expressions for views declared in the query."""

    aliased_views: typing.Dict[str, s_nodes.Node]
    """A dictionary of views aliased in a statement body."""

    view_class_map: typing.Dict[s_nodes.Node, s_nodes.Node]  # noqa
    """Class mapping (used by schema-level views)."""

    class_view_overrides: typing.Dict[s_name.SchemaName, s_nodes.Node]  # noqa
    """Class mapping used by implicit view override in SELECT."""

    clause: str
    """Statement clause the compiler is currently in."""

    toplevel_clause: str
    """Top-level statement clause the compiler is currently in."""

    toplevel_stmt: irast.Stmt
    """Top-level statement."""

    stmt: irast.Stmt
    """Statement node currently being built."""

    singletons: typing.Set[irast.Set]
    """A set of Set nodes for which the cardinality is ONE in this context."""

    path_id_namespace: str
    """A namespace to use for all path ids."""

    view_map: typing.Dict[irast.PathId, irast.Set]
    """Set translation map.  Used for views."""

    class_shapes: typing.Dict[s_types.Type,                     # noqa
                              typing.List[s_pointers.Pointer]]  # noqa
    """Class output or modification shapes."""

    path_scope: irast.ScopeBranchNode
    """Path scope tree, with per-lexical-scope levels."""

    pending_path_scope: irast.ScopeBranchNode
    """Path scope tree to be used with the next FSETOF statement."""

    in_aggregate: bool
    """True if the current location is inside an aggregate function call."""

    path_as_type: bool
    """True if path references should be treated as type references."""

    view_rptr: ViewRPtr
    """Pointer information for the top-level view of the substatement."""

    view_scls: s_types.Type
    """Schema class for the top-level set of the substatement."""

    toplevel_result_view_name: s_name.SchemaName
    """The name to use for the view that is the result of the top statement."""

    partial_path_prefix: irast.Set
    """The set used as a prefix for partial paths."""

    def __init__(self, prevlevel, mode):
        self.mode = mode

        if prevlevel is None:
            self.schema = None
            self.derived_target_module = None
            self.aliases = compiler.AliasGenerator()
            self.anchors = {}
            self.namespaces = {}
            self.arguments = {}

            self.source_map = {}
            self.view_nodes = {}
            self.view_sets = {}
            self.aliased_views = collections.ChainMap()
            self.view_class_map = {}
            self.class_view_overrides = {}

            self.clause = None
            self.toplevel_clause = None
            self.toplevel_stmt = None
            self.stmt = None
            self.singletons = set()
            self.path_id_namespace = None
            self.view_map = collections.ChainMap()
            self.class_shapes = collections.defaultdict(list)
            self.path_scope = None
            self.pending_path_scope = None
            self.in_aggregate = False
            self.view_scls = None
            self.expr_exposed = False
            self.path_as_type = False

            self.partial_path_prefix = None

            self.view_rptr = None
            self.toplevel_result_view_name = None

        else:
            self.schema = prevlevel.schema
            self.derived_target_module = prevlevel.derived_target_module
            self.aliases = prevlevel.aliases
            self.arguments = prevlevel.arguments

            self.source_map = prevlevel.source_map
            self.view_nodes = prevlevel.view_nodes
            self.view_sets = prevlevel.view_sets

            self.path_id_namespace = prevlevel.path_id_namespace
            self.view_map = prevlevel.view_map
            self.class_shapes = prevlevel.class_shapes
            self.path_scope = prevlevel.path_scope
            self.view_scls = prevlevel.view_scls
            self.expr_exposed = prevlevel.expr_exposed
            self.toplevel_clause = prevlevel.toplevel_clause
            self.toplevel_stmt = prevlevel.toplevel_stmt

            if mode == ContextSwitchMode.SUBQUERY:
                self.anchors = prevlevel.anchors.copy()
                self.namespaces = prevlevel.namespaces.copy()
                self.aliased_views = prevlevel.aliased_views.new_child()
                self.view_class_map = prevlevel.view_class_map.copy()
                self.class_view_overrides = \
                    prevlevel.class_view_overrides.copy()

                self.view_rptr = None
                self.view_scls = None
                self.clause = None
                self.stmt = None
                self.singletons = prevlevel.singletons.copy()
                self.pending_path_scope = None
                self.in_aggregate = False
                self.path_as_type = False

                self.partial_path_prefix = None

                self.view_rptr = None
                self.toplevel_result_view_name = None

            else:
                self.anchors = prevlevel.anchors
                self.namespaces = prevlevel.namespaces
                self.aliased_views = prevlevel.aliased_views
                self.view_class_map = prevlevel.view_class_map
                self.class_view_overrides = prevlevel.class_view_overrides

                self.clause = prevlevel.clause
                self.stmt = prevlevel.stmt

                self.pending_path_scope = prevlevel.pending_path_scope
                self.in_aggregate = prevlevel.in_aggregate
                self.path_as_type = prevlevel.path_as_type

                self.singletons = prevlevel.singletons

                self.partial_path_prefix = prevlevel.partial_path_prefix

                self.view_rptr = prevlevel.view_rptr
                self.toplevel_result_view_name = \
                    prevlevel.toplevel_result_view_name

            if mode in {ContextSwitchMode.NEWSCOPE,
                        ContextSwitchMode.NEWSCOPE_TEMP}:
                self.path_scope = prevlevel.path_scope.add_branch()
                self.pending_path_scope = self.path_scope

            if mode in {ContextSwitchMode.NEWFENCE,
                        ContextSwitchMode.NEWFENCE_TEMP}:
                self.path_scope = prevlevel.path_scope.add_fence()
                self.pending_path_scope = self.path_scope

            if mode in {ContextSwitchMode.NEWFENCE_TEMP,
                        ContextSwitchMode.NEWSCOPE_TEMP}:
                self.path_scope.protect_parent = True

    def on_pop(self, prevlevel):
        if self.mode in {ContextSwitchMode.NEWFENCE_TEMP,
                         ContextSwitchMode.NEWSCOPE_TEMP}:
            prevlevel.path_scope.remove_child(self.path_scope)

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


class CompilerContext(compiler.CompilerContext):
    ContextLevelClass = ContextLevel
    default_mode = ContextSwitchMode.NEW
