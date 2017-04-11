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
    def __init__(self, roots_only=False):
        super().__init__()
        self.paths = collections.OrderedDict()
        self.roots_only = roots_only

    def visit_Stmt(self, expr):
        pass

    def visit_Set(self, expr):
        key = expr.path_id

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


def extract_prefixes(expr, roots_only=False):
    extractor = PathExtractor(roots_only=roots_only)
    extractor.visit(expr)
    return extractor.paths


def register_path_scope(
        path_id: irast.PathId, *, stmt_scope: bool=True,
        ctx: context.CompilerContext) -> None:
    if not ctx.path_as_type:
        for prefix in path_id.iter_prefixes():
            if (ctx.in_aggregate or not ctx.aggregated_scope or
                    prefix in ctx.unaggregated_scope):
                ctx.path_scope[prefix] += 1
                if stmt_scope:
                    ctx.stmt_path_scope[prefix] += 1


def update_pending_path_scope(
        scope: typing.Dict[irast.PathId, int], *,
        ctx: context.CompilerContext) -> None:
    scope = set(scope)
    promoted_scope = ctx.pending_path_scope & scope
    new_pending_scope = scope - promoted_scope
    ctx.pending_path_scope -= promoted_scope
    ctx.pending_path_scope.update(new_pending_scope)

    for path_id in promoted_scope:
        register_path_scope(path_id, stmt_scope=False, ctx=ctx)


def enforce_singleton(expr: irast.Base, *, ctx: context.ContextLevel) -> None:
    cardinality = irinference.infer_cardinality(
        expr, ctx.singletons, ctx.schema)
    if cardinality != 1:
        raise errors.EdgeQLError(
            'possibly more than one element returned by an expression '
            'where only singletons are allowed',
            context=expr.context)


def update_singletons(expr: irast.Base, *, ctx: context.ContextLevel) -> None:
    for prefix, ir_sets in extract_prefixes(expr).items():
        for ir_set in ir_sets:
            ir_set = irutils.get_canonical_set(ir_set)
            ctx.singletons.add(ir_set)
            if astutils.is_type_filter(ir_set):
                ctx.singletons.add(ir_set.expr.expr)
