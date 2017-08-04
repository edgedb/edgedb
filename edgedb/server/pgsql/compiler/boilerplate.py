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


def init_stmt(
        stmt: irast.Stmt, ctx: context.CompilerContextLevel,
        parent_ctx: context.CompilerContextLevel) -> None:
    if ctx.toplevel_stmt is None:
        ctx.toplevel_stmt = ctx.stmt

    ctx.path_scope = stmt.path_scope.copy()

    ctx.local_scope_sets = \
        {s for s in stmt.local_scope_sets
         if s.path_id in ctx.path_scope}

    ctx.stmtmap[stmt] = ctx.stmt
    ctx.stmt_hierarchy[ctx.stmt] = parent_ctx.stmt

    ctx.stmt.view_path_id_map = parent_ctx.view_path_id_map.copy()


def fini_stmt(
        stmt: pgast.Query, ctx: context.CompilerContextLevel,
        parent_ctx: context.CompilerContextLevel) -> None:
    if stmt is ctx.toplevel_stmt:
        stmt.argnames = ctx.argmap


def compile_iterator_expr(
        query: pgast.Query, ir_stmt: irast.Stmt, *,
        ctx: context.CompilerContextLevel) \
        -> typing.Optional[pgast.CommonTableExpr]:

    iterator_expr = ir_stmt.iterator_stmt
    if iterator_expr is None:
        return None

    with ctx.new() as subctx:
        subctx.rel = subctx.query = query

        dispatch.compile(iterator_expr, ctx=subctx)
        iterator_cte = relctx.get_set_cte(iterator_expr, ctx=subctx)
        iterator_query = iterator_cte.query

        # Regardless of result type, we use transient identity,
        # based on row_number() for path identity of the iterator
        # expression.  This is necessary to maintain correct
        # correlation for the state of iteration in DML statements.
        relctx.ensure_bond_for_expr(
            iterator_expr, iterator_query, type='uuid', ctx=subctx)

        iterator_rvar = pathctx.maybe_get_path_rvar(
            ctx.env, query, iterator_expr.path_id)
        relctx.put_parent_range_scope(
            iterator_expr, iterator_rvar, ctx=subctx)

    return iterator_cte
