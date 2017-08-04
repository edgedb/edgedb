##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL compiler path scope helpers."""


import collections
import typing

from edgedb.lang.common import ast
from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import inference as irinference
from edgedb.lang.ir import utils as irutils

from edgedb.lang.edgeql import errors

from . import astutils
from . import context


class PathExtractor(ast.visitor.NodeVisitor):
    def __init__(self, roots_only=False, exclude=set()):
        super().__init__()
        self.paths = collections.OrderedDict()
        self.roots_only = roots_only
        self.exclude = exclude

    def visit_Stmt(self, expr):
        pass

    def visit_Set(self, expr):
        key = expr.path_id
        if key in self.exclude:
            return

        if expr.expr is not None:
            self.visit(expr.expr)

        if expr.rptr is not None:
            self.visit(expr.rptr.source)

        if key and (not self.roots_only or expr.rptr is None):
            if key not in self.paths:
                self.paths[key] = {expr}
            else:
                self.paths[key].add(expr)

    def visit_FunctionCall(self, expr):
        if expr.func.aggregate:
            pass
        else:
            self.generic_visit(expr)


def extract_prefixes(expr, roots_only=False, *, exclude=set()):
    extractor = PathExtractor(roots_only=roots_only, exclude=exclude)
    extractor.visit(expr)
    return extractor.paths


def register_path_scope(
        path_id: irast.PathId, *, stmt_scope: bool=True,
        ctx: context.CompilerContext) -> None:
    if ctx.path_as_type:
        return

    for prefix in path_id.iter_prefixes():
        if not prefix.starts_any_of(ctx.group_paths):
            ctx.path_scope.add(prefix)
            ctx.traced_path_scope.add(prefix)
            if stmt_scope:
                ctx.stmt_local_path_scope.add(prefix)


def get_local_scope_sets(
        *, ctx: context.CompilerContext) -> typing.FrozenSet[irast.Set]:
    return frozenset(
        ctx.sets[path_id] for path_id in ctx.stmt_local_path_scope
        if path_id in ctx.sets
    )


def enforce_singleton(expr: irast.Base, *, ctx: context.ContextLevel) -> None:
    cardinality = irinference.infer_cardinality(
        expr, ctx.singletons, ctx.schema)
    if cardinality != 1:
        raise errors.EdgeQLError(
            'possibly more than one element returned by an expression '
            'where only singletons are allowed',
            context=expr.context)


def update_singletons(expr: irast.Base, *, ctx: context.ContextLevel) -> None:
    prefixes = extract_prefixes(expr, exclude=ctx.group_paths)
    for prefix, ir_sets in prefixes.items():
        for ir_set in ir_sets:
            ir_set = irutils.get_canonical_set(ir_set)
            ctx.singletons.add(ir_set)
            if astutils.is_type_filter(ir_set):
                ctx.singletons.add(ir_set.expr.expr)
