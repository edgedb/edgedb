##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL to IR compiler context."""

import typing

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils
from edgedb.lang.schema import objects as so
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import schema as s_schema


class ContextLevel:

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

    location: str
    """Statement location the compiler is currently in."""

    stmt: irast.Stmt
    """Statement node currently being built."""

    sets: typing.Dict[irutils.LinearPath, irast.Set]
    """A dictionary of Set nodes representing the paths the compiler
    has seen so far."""

    group_paths: typing.Set[irutils.LinearPath]
    """A set of path ids in the GROUP BY clause of the current statement."""

    in_aggregate: bool
    """True if the current location is inside an aggregate function call."""

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

            self.location = None
            self.stmt = None
            self.sets = {}
            self.group_paths = set()
            self.in_aggregate = False

            self.result_path_steps = []

            self.toplevel_shape_rptrcls = None

        else:
            self.schema = prevlevel.schema
            self.arguments = prevlevel.arguments
            self.toplevel_shape_rptrcls = prevlevel.toplevel_shape_rptrcls

            if mode == CompilerContext.SUBQUERY:
                self.anchors = prevlevel.anchors.copy()
                self.pathvars = prevlevel.pathvars.copy()
                self.namespaces = prevlevel.namespaces.copy()
                self.substmts = prevlevel.substmts.copy()

                self.toplevel_shape_rptrcls = None
                self.location = None
                self.stmt = None
                self.sets = {}
                self.group_paths = set()
                self.in_aggregate = False

                self.result_path_steps = []

            else:
                self.anchors = prevlevel.anchors
                self.pathvars = prevlevel.pathvars
                self.namespaces = prevlevel.namespaces
                self.substmts = prevlevel.substmts

                self.location = prevlevel.location
                self.stmt = prevlevel.stmt

                self.group_paths = prevlevel.group_paths
                self.in_aggregate = prevlevel.in_aggregate

                self.result_path_steps = prevlevel.result_path_steps[:]
                self.sets = prevlevel.sets


class CompilerContext:
    NEW, SUBQUERY = range(0, 2)

    def __init__(self):
        self.stack = []
        self.push()

    def push(self, mode=None):
        level = ContextLevel(self.current, mode)
        self.stack.append(level)

        return level

    def pop(self):
        self.stack.pop()

    def new(self, mode=None):
        if not mode:
            mode = CompilerContext.NEW
        return CompilerContextWrapper(self, mode)

    def subquery(self):
        return self.new(CompilerContext.SUBQUERY)

    def _current(self):
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return None

    current = property(_current)


class CompilerContextWrapper:
    def __init__(self, context, mode):
        self.context = context
        self.mode = mode

    def __enter__(self):
        self.context.push(self.mode)
        return self.context

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.pop()
