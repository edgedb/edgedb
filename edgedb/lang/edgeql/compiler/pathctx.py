##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL compiler path scope helpers."""


import collections

from edgedb.lang.common import ast
from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import inference as irinference

from edgedb.lang.schema import objects as s_obj

from edgedb.lang.edgeql import errors

from . import context


class SingletonPathExtractor(ast.visitor.NodeVisitor):
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

    def visit_ExistPred(self, expr):
        pass

    def visit_DistinctOp(self, expr):
        pass

    def visit_SetOp(self, expr):
        pass

    def visit_BinOp(self, expr):
        if isinstance(expr.op, irast.SetOperator):
            return

        self.generic_visit(expr.left)

        if not isinstance(expr.op, ast.ops.MembershipOperator):
            self.generic_visit(expr.right)


def get_path_id(scls: s_obj.Class, *,
                ctx: context.CompilerContext) -> irast.PathId:
    return irast.PathId(scls, namespace=ctx.path_id_namespace)


def register_path_in_scope(
        path_id: irast.PathId, *,
        ctx: context.CompilerContext) -> None:
    if ctx.path_as_type:
        return
    ctx.path_scope.add_path(path_id)


def enforce_singleton(expr: irast.Base, *, ctx: context.ContextLevel) -> None:
    cardinality = irinference.infer_cardinality(
        expr, ctx.singletons, ctx.schema)
    if cardinality != 1:
        raise errors.EdgeQLError(
            'possibly more than one element returned by an expression '
            'where only singletons are allowed',
            context=expr.context)


def update_singletons(expr: irast.Base, *, ctx: context.ContextLevel) -> None:
    extractor = SingletonPathExtractor()
    extractor.visit(expr)
    prefixes = extractor.paths
    for prefix, ir_sets in prefixes.items():
        for ir_set in ir_sets:
            ctx.singletons.add(ir_set.path_id)
