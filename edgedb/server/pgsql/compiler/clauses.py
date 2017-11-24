##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import typing

from edgedb.lang.ir import ast as irast
from edgedb.server.pgsql import ast as pgast

from . import context
from . import dispatch
from . import pathctx
from . import relctx
from . import relgen
from . import output


def init_stmt(
        stmt: irast.Stmt, ctx: context.CompilerContextLevel,
        parent_ctx: context.CompilerContextLevel) -> None:
    if ctx.toplevel_stmt is None:
        ctx.toplevel_stmt = ctx.stmt
        ctx.path_scope = ctx.path_scope.new_child({
            path_id: ctx.stmt for path_id in stmt.path_scope.paths
        })
        ctx.scope_tree = stmt.path_scope


def fini_stmt(
        stmt: pgast.Query, ctx: context.CompilerContextLevel,
        parent_ctx: context.CompilerContextLevel) -> None:
    if stmt is ctx.toplevel_stmt:
        stmt.argnames = ctx.argmap


def compile_iterator_expr(
        query: pgast.Query, ir_stmt: irast.Stmt, *,
        ctx: context.CompilerContextLevel) \
        -> typing.Optional[pgast.BaseRangeVar]:

    iterator_expr = ir_stmt.iterator_stmt
    if iterator_expr is None:
        return None

    with ctx.new() as subctx:
        subctx.rel = query

        dispatch.compile(iterator_expr, ctx=subctx)
        iterator_rvar = relctx.get_path_rvar(
            query, iterator_expr.path_id, aspect='value', ctx=ctx)
        iterator_query = iterator_rvar.query

        # Regardless of result type, we use transient identity,
        # for path identity of the iterator expression.  This is
        # necessary to maintain correct correlation for the state
        # of iteration in DML statements.
        relctx.ensure_bond_for_expr(
            iterator_expr.expr.result, iterator_query, type='uuid', ctx=subctx)

    return iterator_rvar


def compile_output(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> pgast.OutputVar:
    with ctx.new() as newctx:
        newctx.clause = 'result'
        if newctx.expr_exposed is None:
            newctx.expr_exposed = True

        dispatch.compile(ir_set, ctx=newctx)

        path_id = ir_set.path_id

        if output.in_serialization_ctx(ctx):
            val = pathctx.get_path_serialized_output(
                ctx.rel, path_id, env=ctx.env)
        else:
            if path_id.is_concept_path():
                val = pathctx.get_path_identity_output(
                    ctx.rel, path_id, env=ctx.env)
            else:
                val = pathctx.get_path_value_output(
                    ctx.rel, path_id, env=ctx.env)

    return val


def compile_filter_clause(
        ir_set: typing.Optional[irast.Set], *,
        ctx: context.CompilerContextLevel) -> typing.Optional[pgast.Expr]:
    if ir_set is None:
        return None

    with ctx.new() as ctx1:
        ctx1.clause = 'where'
        ctx1.expr_exposed = False
        ctx1.shape_format = context.ShapeFormat.SERIALIZED

        # In WHERE we compile ir.Set as a boolean disjunction:
        #    EXISTS(SELECT FROM SetRel WHERE SetRel.value)
        with ctx1.subrel() as subctx:
            dispatch.compile(ir_set, ctx=subctx)
            wrapper = subctx.rel
            wrapper.where_clause = pathctx.get_path_value_var(
                wrapper, ir_set.path_id, env=subctx.env)

        where_clause = pgast.SubLink(
            type=pgast.SubLinkType.EXISTS,
            expr=wrapper
        )

    return where_clause


def compile_orderby_clause(
        ir_exprs: typing.List[irast.Base], *,
        ctx: context.CompilerContextLevel) -> pgast.Expr:
    if not ir_exprs:
        return []

    sort_clause = []

    for expr in ir_exprs:
        with ctx.new() as orderctx:
            orderctx.clause = 'orderby'
            orderctx.expr_exposed = False

            # In OPDER BY we compile ir.Set as a subquery:
            #    SELECT SetRel.value FROM SetRel)
            value = relgen.set_as_subquery(
                expr.expr, as_value=True, ctx=orderctx)

            sortexpr = pgast.SortBy(
                node=value,
                dir=expr.direction,
                nulls=expr.nones_order)
            sort_clause.append(sortexpr)

    return sort_clause


def compile_limit_offset_clause(
        ir_set: typing.Optional[irast.Base], *,
        ctx: context.CompilerContextLevel) -> pgast.Expr:
    if ir_set is None:
        return None

    with ctx.new() as ctx1:
        ctx1.clause = 'offsetlimit'
        ctx1.expr_exposed = False

        # In OFFSET/LIMIT we compile ir.Set as a subquery:
        #    SELECT SetRel.value FROM SetRel)
        limit_offset_clause = relgen.set_as_subquery(ir_set, ctx=ctx1)

    return limit_offset_clause
