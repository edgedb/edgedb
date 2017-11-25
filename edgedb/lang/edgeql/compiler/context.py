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

from edgedb.lang.schema import objects as so
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import schema as s_schema


class ContextSwitchMode(enum.Enum):
    NEW = enum.auto()
    SUBQUERY = enum.auto()
    NEWSCOPE = enum.auto()
    NEWFENCE = enum.auto()


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

    substmts: typing.Dict[str, irast.Stmt]
    """A dictionary of substatements declared in the WITH block."""

    arguments: typing.Dict[str, so.Class]
    """A mapping of statement parameter types passed to the compiler
    programmatically."""

    source_map: typing.Dict[irast.Set,
                            typing.Tuple[compiler.ContextLevel, qlast.Expr]]
    """A mapping of certain Sets to their QL source AST."""

    clause: str
    """Statement clause the compiler is currently in."""

    stmt: irast.Stmt
    """Statement node currently being built."""

    singletons: typing.Set[irast.Set]
    """A set of Set nodes for which the cardinality is ONE in this context."""

    group_paths: typing.Set[irast.PathId]
    """A set of path ids in the GROUP BY clause of the current statement."""

    path_id_namespace: str
    """A namespace to use for all path ids."""

    view_map: typing.Dict[irast.PathId, irast.Set]
    """Set translation map.  Used for views."""

    path_scope: irast.ScopeBranchNode
    """Full path tree, with per-lexical-scope levels."""

    in_aggregate: bool
    """True if the current location is inside an aggregate function call."""

    path_as_type: bool
    """True if path references should be treated as type references."""

    toplevel_shape_rptr: s_pointers.Pointer
    """Pointer class for the top-level shape of the substatement."""

    result_path_steps: list
    """Root path steps of select's result shape."""

    def __init__(self, prevlevel, mode):
        if prevlevel is None:
            self.schema = None
            self.derived_target_module = None
            self.aliases = compiler.AliasGenerator()
            self.anchors = {}
            self.namespaces = {}
            self.substmts = {}
            self.arguments = {}
            self.source_map = {}

            self.clause = None
            self.stmt = None
            self.singletons = set()
            self.group_paths = set()
            self.path_id_namespace = None
            self.view_map = collections.ChainMap()
            self.path_scope = irast.ScopeFenceNode()
            self.in_aggregate = False
            self.path_as_type = False

            self.result_path_steps = []

            self.toplevel_shape_rptr = None

        else:
            self.schema = prevlevel.schema
            self.derived_target_module = prevlevel.derived_target_module
            self.aliases = prevlevel.aliases
            self.arguments = prevlevel.arguments
            self.source_map = prevlevel.source_map
            self.toplevel_shape_rptr = prevlevel.toplevel_shape_rptr
            self.path_id_namespace = prevlevel.path_id_namespace
            self.view_map = prevlevel.view_map
            self.path_scope = prevlevel.path_scope
            self.group_paths = prevlevel.group_paths
            self.result_path_steps = prevlevel.result_path_steps

            if mode == ContextSwitchMode.SUBQUERY:
                self.anchors = prevlevel.anchors.copy()
                self.namespaces = prevlevel.namespaces.copy()
                self.substmts = prevlevel.substmts.copy()

                self.toplevel_shape_rptr = None
                self.clause = None
                self.stmt = None
                self.singletons = prevlevel.singletons.copy()
                self.in_aggregate = False
                self.path_as_type = False

            else:
                self.anchors = prevlevel.anchors
                self.namespaces = prevlevel.namespaces
                self.substmts = prevlevel.substmts

                self.clause = prevlevel.clause
                self.stmt = prevlevel.stmt

                self.in_aggregate = prevlevel.in_aggregate
                self.path_as_type = prevlevel.path_as_type

                self.result_path_steps = prevlevel.result_path_steps[:]
                self.singletons = prevlevel.singletons

            if mode == ContextSwitchMode.NEWSCOPE:
                self.path_scope = prevlevel.path_scope.add_branch()
                self.group_paths = prevlevel.group_paths.copy()

            if mode == ContextSwitchMode.NEWFENCE:
                self.path_scope = prevlevel.path_scope.add_fence()
                self.group_paths = prevlevel.group_paths.copy()

    def subquery(self):
        return self.new(ContextSwitchMode.SUBQUERY)

    def newscope(self):
        return self.new(ContextSwitchMode.NEWSCOPE)

    def newfence(self):
        return self.new(ContextSwitchMode.NEWFENCE)


class CompilerContext(compiler.CompilerContext):
    ContextLevelClass = ContextLevel
    default_mode = ContextSwitchMode.NEW
