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


"""EdgeQL compiler functions to process shared clauses."""


import typing

from edb.lang.edgeql import ast as qlast
from edb.lang.ir import ast as irast

from . import context
from . import dispatch
from . import pathctx
from . import setgen


def compile_where_clause(
        where: qlast.Base, *,
        ctx: context.ContextLevel) -> typing.Optional[irast.Base]:

    if where is None:
        return None
    else:
        with ctx.newscope(fenced=True) as subctx:
            subctx.path_scope.unnest_fence = True
            subctx.clause = 'where'
            if subctx.stmt.parent_stmt is None:
                subctx.toplevel_clause = subctx.clause
            ir_expr = dispatch.compile(where, ctx=subctx)
            bool_t = ctx.schema.get('std::bool')
            ir_set = setgen.scoped_set(ir_expr, typehint=bool_t, ctx=subctx)

        return ir_set


def compile_orderby_clause(
        sortexprs: typing.Iterable[qlast.SortExpr], *,
        ctx: context.ContextLevel) -> typing.List[irast.SortExpr]:
    result = []
    if not sortexprs:
        return result

    with ctx.new() as subctx:
        subctx.clause = 'orderby'
        if subctx.stmt.parent_stmt is None:
            subctx.toplevel_clause = subctx.clause

        for sortexpr in sortexprs:
            with subctx.newscope(fenced=True) as exprctx:
                exprctx.path_scope.unnest_fence = True
                ir_sortexpr = dispatch.compile(sortexpr.path, ctx=exprctx)
                ir_sortexpr = setgen.scoped_set(ir_sortexpr, ctx=exprctx)
                ir_sortexpr.context = sortexpr.context
                pathctx.enforce_singleton(ir_sortexpr, ctx=exprctx)

            result.append(
                irast.SortExpr(
                    expr=ir_sortexpr,
                    direction=sortexpr.direction,
                    nones_order=sortexpr.nones_order))

    return result


def compile_limit_offset_clause(
        expr: qlast.Base, *,
        ctx: context.ContextLevel) -> typing.Optional[irast.Set]:
    if expr is not None:
        with ctx.newscope(fenced=True) as subctx:
            subctx.clause = 'offsetlimit'
            ir_expr = dispatch.compile(expr, ctx=subctx)
            int_t = ctx.schema.get('std::int64')
            ir_set = setgen.scoped_set(ir_expr, typehint=int_t, ctx=subctx)
            ir_set.context = expr.context
            pathctx.enforce_singleton(ir_set, ctx=subctx)
    else:
        ir_set = None

    return ir_set
