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

"""SQL resolver that compiles public SQL to internal SQL which is executable
in our internal Postgres instance."""

import functools
from typing import *

from edb import errors

from edb.pgsql import ast as pgast
from edb.pgsql.compiler import astutils as pgastutils

from . import dispatch
from . import context

Context = context.ResolverContextLevel


def resolve_BaseRangeVar(
    range_var: pgast.BaseRangeVar, *, ctx: Context
) -> Tuple[pgast.BaseRangeVar, Sequence[context.Table]]:
    # handle join that returns multiple tables and does not use alias
    if isinstance(range_var, pgast.JoinExpr):
        return _resolve_JoinExpr(range_var, ctx=ctx)

    # generate internal alias
    internal_alias = ctx.names.get('relation')
    alias = pgast.Alias(aliasname=internal_alias)

    # general case
    node, table = _resolve_range_var(range_var, alias, ctx=ctx)

    # infer public name and internal alias
    table.alias = range_var.alias.aliasname
    table.reference_as = internal_alias

    return node, (table,)


@functools.singledispatch
def _resolve_range_var(
    ir: pgast.BaseRangeVar,
    alias: pgast.Alias,
    *,
    ctx: context.ResolverContextLevel,
) -> Tuple[pgast.BaseRangeVar, context.Table]:
    raise ValueError(f'no SQL resolve handler for {ir.__class__}')


@_resolve_range_var.register
def _resolve_RelRangeVar(
    range_var: pgast.RelRangeVar,
    alias: pgast.Alias,
    *,
    ctx: Context,
) -> Tuple[pgast.BaseRangeVar, context.Table]:
    ctx.include_inherited = range_var.include_inherited

    relation: Union[pgast.BaseRelation, pgast.CommonTableExpr]
    if isinstance(range_var.relation, pgast.BaseRelation):
        relation, table = dispatch.resolve_relation(range_var.relation, ctx=ctx)
    else:
        relation, cte = resolve_CommonTableExpr(range_var.relation, ctx=ctx)
        table = context.Table()
        table.name = cte.name
        table.columns = cte.columns
        table.reference_as = cte.name

    return (
        pgast.RelRangeVar(
            relation=relation,
            alias=alias,
        ),
        table
    )


@_resolve_range_var.register
def _resolve_RangeSubselect(
    range_var: pgast.RangeSubselect,
    alias: pgast.Alias,
    *,
    ctx: Context,
) -> Tuple[pgast.BaseRangeVar, context.Table]:
    with ctx.empty() as subctx:
        subquery, subtable = dispatch.resolve_relation(range_var.subquery, ctx=subctx)

        result = context.Table()
        result.name = range_var.alias.aliasname
        result.reference_as = alias.aliasname
        result.columns = [
            context.Column(name=col.name, reference_as=col.name)
            for col in subtable.columns
        ]

    return (
        pgast.RangeSubselect(
            subquery=subquery,
            alias=alias,
        ),
        result,
    )


@_resolve_range_var.register
def _resolve_RangeFunction(
    range_var: pgast.RangeFunction,
    alias: pgast.Alias,
    *,
    ctx: Context,
) -> Tuple[pgast.BaseRangeVar, context.Table]:
    raise errors.UnsupportedFeatureError(
        'range functions are not supported', context=range_var.context
    )


def _resolve_JoinExpr(
    range_var: pgast.JoinExpr,
    *,
    ctx: Context,
) -> Tuple[pgast.BaseRangeVar, Sequence[context.Table]]:
    
    def resolve_arg(
        arg: pgast.BaseExpr,
    ) -> Tuple[pgast.BaseExpr, Sequence[context.Table]]:
        with ctx.empty() as subctx:
            if isinstance(arg, pgast.BaseRelation):
                arg, table = dispatch.resolve_relation(arg, ctx=subctx)
                return arg, (table,)
            elif isinstance(arg, pgast.BaseRangeVar):
                return resolve_BaseRangeVar(arg, ctx=subctx)
            else:
                raise ValueError()

    larg, l_tables = resolve_arg(range_var.larg)
    rarg, r_tables = resolve_arg(range_var.rarg)

    tables = list(l_tables) + list(r_tables)
    ctx.scope.tables.extend(tables)

    quals: Optional[pgast.BaseExpr] = None
    if range_var.quals:
        quals = dispatch.resolve(range_var.quals, ctx=ctx)

    if range_var.using_clause:
        for c in range_var.using_clause:
            with ctx.empty() as subctx:
                subctx.scope.tables = list(l_tables)
                l_expr = dispatch.resolve(c, ctx=subctx)
            with ctx.empty() as subctx:
                subctx.scope.tables = list(r_tables)
                r_expr = dispatch.resolve(c, ctx=subctx)
            quals = pgastutils.extend_binop(
                quals,
                pgast.Expr(
                    kind=pgast.ExprKind.OP,
                    name='=',
                    lexpr=l_expr,
                    rexpr=r_expr,
                ),
            )

    node = pgast.JoinExpr(
        type=range_var.type,
        larg=larg,
        rarg=rarg,
        quals=quals,
    )
    return (node, tables)


def resolve_CommonTableExpr(
    cte: pgast.CommonTableExpr, *, ctx: Context
) -> Tuple[pgast.CommonTableExpr, context.CTE]:
    with ctx.isolated() as subctx:

        query, table = dispatch.resolve_relation(cte.query, ctx=subctx)

        result = context.CTE()
        result.name = cte.name
        result.columns = []

        cols_query = table.columns
        cols_names = cte.aliascolnames
        if cols_names and len(cols_query) != len(cols_names):
            raise errors.QueryError(
                f'CTE alias for `{cte.name}` contains {len(cols_names)} '
                f'columns, but the query resolves to `{len(cols_query)}` '
                f'columns',
                context=cte.context,
            )
        for index, col in enumerate(cols_query):
            res_col = context.Column()
            if cols_names:
                res_col.name = cols_names[index]
            else:
                res_col.name = col.name
            res_col.reference_as = col.name
            result.columns.append(res_col)

    node = pgast.CommonTableExpr(
        name=cte.name,
        aliascolnames=None,
        query=query,
        recursive=cte.recursive,
        materialized=cte.materialized,
    )
    return node, result

