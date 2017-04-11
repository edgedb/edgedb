##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL compiler functions to process shared clauses."""


import typing

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.ir import ast as irast

from . import context
from . import dispatch
from . import pathctx


def compile_where_clause(
        where: qlast.Base, *,
        ctx: context.ContextLevel) -> typing.Optional[irast.Base]:

    if where is None:
        return None
    else:
        with ctx.new() as subctx:
            subctx.clause = 'where'
            return dispatch.compile(where, ctx=subctx)


def compile_orderby_clause(
        sortexprs: typing.Iterable[qlast.SortExpr], *,
        ctx: context.ContextLevel) -> typing.List[irast.SortExpr]:
    result = []
    if not sortexprs:
        return result

    with ctx.new() as subctx:
        subctx.clause = 'orderby'

        for sortexpr in sortexprs:
            ir_sortexpr = dispatch.compile(sortexpr.path, ctx=subctx)
            ir_sortexpr.context = sortexpr.context
            pathctx.enforce_singleton(ir_sortexpr, ctx=subctx)
            result.append(
                irast.SortExpr(
                    expr=ir_sortexpr,
                    direction=sortexpr.direction,
                    nones_order=sortexpr.nones_order))

    return result
