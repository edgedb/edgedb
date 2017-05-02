##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""Compilation helpers for output formatting and serialization."""


from edgedb.lang.ir import ast as irast

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common

from . import astutils
from . import context
from . import dbobj
from . import dispatch


def rtlist_as_json_object(rtlist):
    keyvals = []

    if hasattr(rtlist.attmap[0], 'is_linkprop'):
        # This is a shape attribute map, use a specialized version.
        for i, pgexpr in enumerate(rtlist.targets):
            key = rtlist.attmap[i]
            if key.is_linkprop:
                key = '@' + key.name
            else:
                key = key.name
            keyvals.append(pgast.Constant(val=key))
            keyvals.append(pgexpr)
    else:
        # Simple rtlist
        for i, pgexpr in enumerate(rtlist.targets):
            keyvals.append(pgast.Constant(val=rtlist.attmap[i]))
            keyvals.append(pgexpr)

    return pgast.FuncCall(
        name=('jsonb_build_object',), args=keyvals)


def in_serialization_ctx(
        ctx: context.CompilerContextLevel) -> bool:
    return (
        (ctx.expr_exposed is None or ctx.expr_exposed) and
        ctx.env.output_format == context.OutputFormat.JSON
    )


def serialize_expr(
        ctx: context.CompilerContextLevel, expr: pgast.Base) -> pgast.Base:
    if in_serialization_ctx(ctx):
        if isinstance(expr, astutils.ResTargetList):
            val = rtlist_as_json_object(expr)
        elif isinstance(expr, pgast.ImplicitRowExpr):
            val = pgast.FuncCall(name=('jsonb_build_array',), args=expr.args)
        else:
            val = pgast.FuncCall(name=('to_jsonb',), args=[expr])
    else:
        val = expr

    return val


def set_to_array(
        env: context.Environment, query: pgast.Query) -> pgast.Query:
    """Convert a set query into an array."""
    rt_name = ensure_query_restarget_name(query, env=env)

    subrvar = pgast.RangeSubselect(
        subquery=query,
        alias=pgast.Alias(
            aliasname=env.aliases.get('aggw')
        )
    )

    result = pgast.SelectStmt(
        target_list=[
            pgast.ResTarget(
                val=pgast.FuncCall(
                    name=('array_agg',),
                    args=[
                        dbobj.get_column(subrvar, rt_name)
                    ],
                )
            )
        ],
        from_clause=[
            subrvar
        ]
    )

    return result


def ensure_query_restarget_name(
        query: pgast.Query, *, hint=None, env: context.Environment) -> str:
    suggested_rt_name = env.aliases.get(hint or 'v')
    rt_name = None

    def _get_restarget(q):
        nonlocal rt_name

        rt = q.target_list[0]
        if rt_name is not None:
            rt.name = rt_name
        elif rt.name is not None:
            rt_name = rt.name
        else:
            if isinstance(rt.val, pgast.ColumnRef):
                rt.name = rt_name = rt.val.name[-1]
            else:
                rt.name = rt_name = suggested_rt_name

    if query.op is not None:
        astutils.for_each_query_in_set(query, _get_restarget)
    else:
        _get_restarget(query)

    return rt_name


def compile_output(
        result_expr: irast.Base, *, ctx: context.CompilerContextLevel) -> None:
    query = ctx.query

    with ctx.new() as newctx:
        newctx.clause = 'result'
        if newctx.expr_exposed is None:
            newctx.expr_exposed = True
        pgexpr = dispatch.compile(result_expr, ctx=newctx)

        if isinstance(pgexpr, astutils.ResTargetList):
            selexprs = []

            for i, rt in enumerate(pgexpr.targets):
                att = pgexpr.attmap[i]

                name = str(att)

                selexprs.append(
                    (rt, common.edgedb_name_to_pg_name(name))
                )
        elif isinstance(pgexpr, pgast.ImplicitRowExpr):
            # Bare tuple

            selexprs = []

            for i, el in enumerate(pgexpr.args):
                selexprs.append(
                    (el, common.edgedb_name_to_pg_name(str(i)))
                )
        else:
            selexprs = [(pgexpr, None)]

    if in_serialization_ctx(ctx):
        val = serialize_expr(ctx, pgexpr)
        target = pgast.ResTarget(name=None, val=val)
        query.target_list.append(target)
    else:
        for pgexpr, alias in selexprs:
            target = pgast.ResTarget(name=alias, val=pgexpr)
            query.target_list.append(target)
