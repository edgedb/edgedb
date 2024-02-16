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
from edb.common.parsing import ParserContext
from edb.server.pgcon import errors as pgerror

from edb.pgsql import ast as pgast
from edb.pgsql.compiler import astutils as pgastutils

from . import dispatch
from . import context
from . import range_functions
from . import expr

Context = context.ResolverContextLevel


def resolve_BaseRangeVar(
    range_var: pgast.BaseRangeVar, *, ctx: Context
) -> pgast.BaseRangeVar:
    # handle join that returns multiple tables and does not use alias
    if isinstance(range_var, pgast.JoinExpr):
        return _resolve_JoinExpr(range_var, ctx=ctx)

    # generate internal alias
    internal_alias = ctx.names.get('relation')
    alias = pgast.Alias(
        aliasname=internal_alias, colnames=range_var.alias.colnames
    )

    # general case
    node, table = _resolve_range_var(range_var, alias, ctx=ctx)
    node = node.replace(context=range_var.context)

    # infer public name and internal alias
    table.alias = range_var.alias.aliasname
    table.reference_as = internal_alias

    # pull result relation of inner scope into outer scope
    ctx.scope.tables.append(table)
    return node


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
    with ctx.child() as subctx:
        subctx.include_inherited = range_var.include_inherited

        relation: Union[pgast.BaseRelation, pgast.CommonTableExpr]
        if isinstance(range_var.relation, pgast.BaseRelation):
            relation, table = dispatch.resolve_relation(
                range_var.relation, ctx=subctx
            )
        else:
            relation, cte = resolve_CommonTableExpr(
                range_var.relation, ctx=subctx
            )
            table = context.Table(
                name=cte.name,
                columns=cte.columns,
                reference_as=cte.name,
            )

    table.columns = [
        context.Column(
            name=alias or col.name,
            reference_as=alias or col.reference_as,
            hidden=col.hidden,
            static_val=col.static_val,
        )
        for col, alias in _zip_column_alias(
            table.columns, alias, ctx=range_var.context
        )
    ]

    rel = pgast.RelRangeVar(relation=relation, alias=alias)
    return (rel, table)


@_resolve_range_var.register
def _resolve_RangeSubselect(
    range_var: pgast.RangeSubselect,
    alias: pgast.Alias,
    *,
    ctx: Context,
) -> Tuple[pgast.BaseRangeVar, context.Table]:
    with ctx.lateral() if range_var.lateral else ctx.child() as subctx:
        subquery, subtable = dispatch.resolve_relation(
            range_var.subquery, ctx=subctx
        )

        result = context.Table(
            name=range_var.alias.aliasname,
            reference_as=alias.aliasname,
            columns=[
                context.Column(
                    name=alias or col.name,
                    reference_as=alias or col.name,
                )
                for col, alias in _zip_column_alias(
                    subtable.columns, alias, ctx=range_var.context
                )
            ],
        )
        alias = pgast.Alias(
            aliasname=alias.aliasname,
            colnames=[cast(str, c.reference_as) for c in result.columns],
        )

    node = pgast.RangeSubselect(
        subquery=subquery, alias=alias, lateral=range_var.lateral
    )
    return node, result


def _resolve_JoinExpr(
    range_var: pgast.JoinExpr,
    *,
    ctx: Context,
) -> pgast.BaseRangeVar:

    larg = resolve_BaseRangeVar(range_var.larg, ctx=ctx)
    ltable = ctx.scope.tables[len(ctx.scope.tables) - 1]

    assert len(range_var.joins) == 1, (
        "pg resolver should always produce non-flattened joins")
    join = range_var.joins[0]

    rarg = resolve_BaseRangeVar(join.rarg, ctx=ctx)
    rtable = ctx.scope.tables[len(ctx.scope.tables) - 1]

    quals: Optional[pgast.BaseExpr] = None
    if join.quals:
        quals = dispatch.resolve(join.quals, ctx=ctx)

    if join.using_clause:
        for c in join.using_clause:
            with ctx.child() as subctx:
                subctx.scope.tables = [ltable]
                l_expr = dispatch.resolve(c, ctx=subctx)
            with ctx.child() as subctx:
                subctx.scope.tables = [rtable]
                r_expr = dispatch.resolve(c, ctx=subctx)
            quals = pgastutils.extend_binop(
                quals,
                pgast.Expr(
                    name='=',
                    lexpr=l_expr,
                    rexpr=r_expr,
                ),
            )

    return pgast.JoinExpr(
        larg=larg,
        joins=[pgast.JoinClause(
            type=join.type,
            rarg=rarg,
            quals=quals,
        )],
    )


def resolve_CommonTableExpr(
    cte: pgast.CommonTableExpr, *, ctx: Context
) -> Tuple[pgast.CommonTableExpr, context.CTE]:
    reference_as = None

    with ctx.child() as subctx:
        aliascolnames = cte.aliascolnames

        if isinstance(cte.query, pgast.SelectStmt):
            # When no explicit column names were given, we look into the actual
            # select to see if we can extract the column names from that
            # instead. This is needed for some RECURSIVE CTEs.

            if not aliascolnames:
                if isinstance(cte.query.larg, pgast.SelectStmt):
                    if res := _infer_col_aliases(cte.query.larg):
                        aliascolnames = res

            if not aliascolnames:
                if isinstance(cte.query.rarg, pgast.SelectStmt):
                    if res := _infer_col_aliases(cte.query.rarg):
                        aliascolnames = res

        if cte.recursive and aliascolnames:
            reference_as = [subctx.names.get('col') for _ in aliascolnames]
            columns = [
                context.Column(name=col, reference_as=ref_as)
                for col, ref_as in zip(aliascolnames, reference_as)
            ]
            subctx.scope.ctes.append(
                context.CTE(name=cte.name, columns=columns)
            )

        query, table = dispatch.resolve_relation(cte.query, ctx=subctx)

        result = context.CTE(name=cte.name, columns=[])

        alias = pgast.Alias(aliasname=cte.name, colnames=aliascolnames)

        for col, al in _zip_column_alias(table.columns, alias, cte.context):
            result.columns.append(
                context.Column(
                    name=al or col.name,
                    reference_as=col.name,
                )
            )

        if reference_as:
            for col, ref_as in zip(result.columns, reference_as):
                col.reference_as = ref_as

    node = pgast.CommonTableExpr(
        name=cte.name,
        context=cte.context,
        aliascolnames=reference_as,
        query=query,
        recursive=cte.recursive,
        materialized=cte.materialized,
    )
    return node, result


def _infer_col_aliases(query: pgast.SelectStmt) -> Optional[List[str]]:
    aliases = [expr.infer_alias(t) for t in query.target_list]
    if not all(aliases):
        return None
    return cast(List[str], aliases)


@_resolve_range_var.register
def _resolve_RangeFunction(
    range_var: pgast.RangeFunction,
    alias: pgast.Alias,
    *,
    ctx: Context,
) -> Tuple[pgast.BaseRangeVar, context.Table]:
    with ctx.lateral() if range_var.lateral else ctx.child() as subctx:

        functions = []
        col_names = []
        for function in range_var.functions:

            name = function.name[len(function.name) - 1]
            if name in range_functions.COLUMNS:
                col_names.extend(range_functions.COLUMNS[name])
            elif name == 'unnest':
                col_names.extend('unnest' for _ in function.args)
            else:
                col_names.append(name)

            functions.append(dispatch.resolve(function, ctx=subctx))

        inferred_columns = [context.Column(name=name) for name in col_names]

        if range_var.with_ordinality:
            inferred_columns.append(
                context.Column(name='ordinality', reference_as='ordinality')
            )

        table = context.Table(
            columns=[
                context.Column(
                    name=al or col.name,
                    reference_as=al or ctx.names.get('col'),
                )
                for col, al in _zip_column_alias(
                    inferred_columns, alias, ctx=range_var.context
                )
            ]
        )

        alias = pgast.Alias(
            aliasname=alias.aliasname,
            colnames=[
                cast(str, c.reference_as)
                for c in table.columns
                if not c.hidden
            ],
        )

        node = pgast.RangeFunction(
            lateral=range_var.lateral,
            with_ordinality=range_var.with_ordinality,
            is_rowsfrom=range_var.is_rowsfrom,
            functions=functions,
            alias=alias,
        )
        return node, table


def _zip_column_alias(
    columns: List[context.Column],
    alias: pgast.Alias,
    ctx: Optional[ParserContext],
) -> Iterable[Tuple[context.Column, Optional[str]]]:
    if not alias.colnames:
        return map(lambda c: (c, None), columns)

    columns = [c for c in columns if not c.hidden]

    if len(columns) != len(alias.colnames):
        raise errors.QueryError(
            f'Table alias for `{alias.aliasname}` contains '
            f'{len(alias.colnames)} columns, but the query resolves to '
            f'{len(columns)} columns',
            context=ctx,
            pgext_code=pgerror.ERROR_INVALID_COLUMN_REFERENCE,
        )
    return zip(columns, alias.colnames)
