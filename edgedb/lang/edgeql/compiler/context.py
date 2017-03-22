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

from edgedb.lang.ir import ast as irast

from edgedb.lang.common import compiler

from edgedb.lang.schema import objects as so
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import schema as s_schema


class ContextSwitchMode(enum.Enum):
    NEW = enum.auto()
    SUBQUERY = enum.auto()
    NEWSCOPE = enum.auto()


class ContextLevel(compiler.ContextLevel):

    schema: s_schema.Schema
    """A Schema instance to use for class resolution."""

    anchors: typing.Dict[str, irast.Set]
    """A mapping of anchor variables (aliases to path expressions passed
    to the compiler programmatically).
    """

    pathvars: typing.Dict[str, irast.Set]
    """A mapping of path variables (aliases to path expressions declared
    in the WITH block.
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

    clause: str
    """Statement clause the compiler is currently in."""

    stmt: irast.Stmt
    """Statement node currently being built."""

    sets: typing.Dict[irast.PathId, irast.Set]
    """A dictionary of Set nodes representing the paths the compiler
    has seen so far."""

    singletons: typing.Set[irast.Set]
    """A set of Set nodes for which the cardinality is ONE in this context."""

    group_paths: typing.Set[irast.PathId]
    """A set of path ids in the GROUP BY clause of the current statement."""

    path_scope: typing.Dict[irast.PathId, int]
    """A map of path ids together with use counts for this context."""

    stmt_path_scope: typing.Dict[irast.PathId, int]
    """A map of path ids together with use counts for this statement."""

    in_aggregate: bool
    """True if the current location is inside an aggregate function call."""

    path_as_type: bool
    """True if path references should be treated as type references."""

    toplevel_shape_rptrcls: s_pointers.Pointer
    """Pointer class for the top-level shape of the substatement."""

    result_path_steps: list
    """Root path steps of select's result shape."""

    def __init__(self, prevlevel=None, mode=None):
        if prevlevel is None:
            self.schema = None
            self.anchors = {}
            self.pathvars = {}
            self.namespaces = {}
            self.substmts = {}
            self.arguments = {}

            self.clause = None
            self.stmt = None
            self.sets = {}
            self.singletons = set()
            self.group_paths = set()
            self.path_scope = collections.defaultdict(int)
            self.stmt_path_scope = collections.defaultdict(int)
            self.pending_path_scope = set()
            self.aggregated_scope = {}
            self.unaggregated_scope = {}
            self.in_aggregate = False
            self.path_as_type = False

            self.result_path_steps = []

            self.toplevel_shape_rptrcls = None

        else:
            self.schema = prevlevel.schema
            self.arguments = prevlevel.arguments
            self.toplevel_shape_rptrcls = prevlevel.toplevel_shape_rptrcls
            self.path_scope = prevlevel.path_scope
            self.pending_path_scope = prevlevel.pending_path_scope
            self.aggregated_scope = prevlevel.aggregated_scope
            self.unaggregated_scope = prevlevel.unaggregated_scope

            if mode == ContextSwitchMode.SUBQUERY:
                self.anchors = prevlevel.anchors.copy()
                self.pathvars = prevlevel.pathvars.copy()
                self.namespaces = prevlevel.namespaces.copy()
                self.substmts = prevlevel.substmts.copy()

                self.toplevel_shape_rptrcls = None
                self.clause = None
                self.stmt = None
                self.sets = prevlevel.sets
                self.singletons = prevlevel.singletons.copy()
                self.group_paths = set()
                self.stmt_path_scope = collections.defaultdict(int)
                self.in_aggregate = False
                self.path_as_type = False

                self.result_path_steps = []

            else:
                self.anchors = prevlevel.anchors
                self.pathvars = prevlevel.pathvars
                self.namespaces = prevlevel.namespaces
                self.substmts = prevlevel.substmts

                self.clause = prevlevel.clause
                self.stmt = prevlevel.stmt

                self.group_paths = prevlevel.group_paths
                self.stmt_path_scope = prevlevel.stmt_path_scope
                self.in_aggregate = prevlevel.in_aggregate
                self.path_as_type = prevlevel.path_as_type

                self.result_path_steps = prevlevel.result_path_steps[:]
                self.sets = prevlevel.sets
                self.singletons = prevlevel.singletons

            if mode == ContextSwitchMode.NEWSCOPE:
                self.path_scope = prevlevel.path_scope.copy()
                self.stmt_path_scope = prevlevel.stmt_path_scope.copy()
                self.pending_path_scope = set()
                self.aggregated_scope = prevlevel.aggregated_scope.copy()
                self.unaggregated_scope = prevlevel.unaggregated_scope.copy()


class CompilerContext(compiler.CompilerContext):
    ContextLevelClass = ContextLevel
    default_mode = ContextSwitchMode.NEW

    def subquery(self):
        return self.new(ContextSwitchMode.SUBQUERY)

    def newscope(self):
        return self.new(ContextSwitchMode.NEWSCOPE)
