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

from typing import *

from edb import errors
from edb.edgeql import qltypes

from edb.pgsql import ast as pgast
from edb.pgsql import common as pgcommon
from edb.pgsql.compiler import astutils as pgastutils

from edb.schema import objtypes as s_objtypes
from edb.schema import links as s_links

from . import dispatch
from . import context

Context = context.ResolverContextLevel


@dispatch._resolve.register
def resolve_SelectStmt(
    stmt: pgast.SelectStmt, *, ctx: Context
) -> pgast.SelectStmt:

    # UNION
    if stmt.larg:
        with ctx.isolated() as subctx:
            stmt.larg = dispatch.resolve(stmt.larg, ctx=subctx)
    if stmt.rarg:
        with ctx.isolated() as subctx:
            stmt.rarg = dispatch.resolve(stmt.rarg, ctx=subctx)
    if stmt.larg or stmt.rarg:
        return stmt

    # CTEs
    ctes = None
    if stmt.ctes:
        ctes = [dispatch.resolve(cte, ctx=ctx) for cte in stmt.ctes]

    # FROM
    from_clause: List[pgast.BaseRangeVar] = []
    for clause in stmt.from_clause:

        with ctx.empty() as subctx:
            from_clause.append(dispatch.resolve(clause, ctx=subctx))

            # pull result relation of inner scope into own scope
            if subctx.scope.rel.columns:
                ctx.scope.tables.append(subctx.scope.rel)
            if subctx.scope.join_relations:
                ctx.scope.tables.extend(subctx.scope.join_relations)

    where = dispatch.resolve_opt(stmt.where_clause, ctx=ctx)
    target_list = [
        c for t in stmt.target_list for c in resolve_ResTarget(t, ctx=ctx)
    ]

    distinct_clause = None
    if stmt.distinct_clause:
        distinct_clause = [
            (c if isinstance(c, pgast.Star) else dispatch.resolve(c, ctx=ctx))
            for c in stmt.distinct_clause
        ]

    sort_clause = dispatch.resolve_opt_list(stmt.sort_clause, ctx=ctx)
    limit_offset = dispatch.resolve_opt(stmt.limit_offset, ctx=ctx)
    limit_count = dispatch.resolve_opt(stmt.limit_count, ctx=ctx)

    res = pgast.SelectStmt(
        distinct_clause=distinct_clause,
        from_clause=from_clause,
        target_list=target_list,
        where_clause=where,
        sort_clause=sort_clause,
        limit_offset=limit_offset,
        limit_count=limit_count,
        ctes=ctes,
    )
    return res


@dispatch._resolve.register
def resolve_DMLQuery(cte: pgast.DMLQuery, *, ctx: Context) -> pgast.DMLQuery:
    raise errors.UnsupportedFeatureError(
        'DML queries (INSERT/UPDATE/DELETE) are not supported'
    )


@dispatch._resolve.register
def resolve_CommonTableExpr(
    cte: pgast.CommonTableExpr, *, ctx: Context
) -> pgast.CommonTableExpr:
    with ctx.isolated() as subctx:

        query = dispatch.resolve(cte.query, ctx=subctx)

        result = context.CTE()
        result.name = cte.name
        result.columns = []

        cols_query = subctx.scope.rel.columns
        cols_names = cte.aliascolnames

        if cols_names and len(cols_query) != len(cols_names):
            raise errors.QueryError(
                f'CTE alias for `{cte.name}` contains {len(cols_names)} '
                f'columns, but the query resolves to `{len(cols_query)}` '
                f'columns'
            )
        for index, col in enumerate(cols_query):
            res_col = context.Column()
            if cols_names:
                res_col.name = cols_names[index]
            else:
                res_col.name = col.name
            res_col.reference_as = col.name
            result.columns.append(res_col)

        ctx.scope.ctes.append(result)

    return pgast.CommonTableExpr(
        name=cte.name,
        aliascolnames=None,
        query=query,
        recursive=cte.recursive,
        materialized=cte.materialized,
    )


@dispatch._resolve.register
def resolve_BaseRangeVar(
    range_var: pgast.BaseRangeVar, *, ctx: Context
) -> pgast.BaseRangeVar:
    # store public alias
    if range_var.alias:
        ctx.scope.rel.alias = range_var.alias.aliasname

    # generate internal alias
    aliasname = ctx.names.generate_relation()
    ctx.scope.rel.reference_as = aliasname
    alias = pgast.Alias(aliasname=aliasname)
    return dispatch.resolve_range_var(range_var, alias, ctx=ctx)


@dispatch.resolve_range_var.register
def resolve_RangeSubselect(
    range_var: pgast.RangeSubselect,
    alias: pgast.Alias,
    *,
    ctx: Context,
) -> pgast.RangeSubselect:
    with ctx.empty() as subctx:
        subquery = dispatch.resolve(range_var.subquery, ctx=subctx)

        result = context.Table()
        result.name = range_var.alias.aliasname
        result.reference_as = alias.aliasname
        result.columns = [
            context.Column(name=col.name, reference_as=col.name)
            for col in subctx.scope.rel.columns
        ]

        ctx.scope.rel = result

    return pgast.RangeSubselect(
        subquery=subquery,
        alias=alias,
    )


@dispatch.resolve_range_var.register
def resolve_RelRangeVar(
    range_var: pgast.RelRangeVar,
    alias: pgast.Alias,
    *,
    ctx: Context,
) -> pgast.RelRangeVar:
    ctx.include_inherited = range_var.include_inherited

    return pgast.RelRangeVar(
        relation=dispatch.resolve(range_var.relation, ctx=ctx),
        alias=alias,
    )


@dispatch.resolve_range_var.register
def resolve_RangeFunction(
    range_var: pgast.RangeFunction,
    alias: pgast.Alias,
    *,
    ctx: Context,
) -> pgast.RangeFunction:
    raise errors.UnsupportedFeatureError('range functions are not supported')


@dispatch.resolve_range_var.register
def resolve_JoinExpr(
    range_var: pgast.JoinExpr,
    alias: pgast.Alias,
    *,
    ctx: Context,
) -> pgast.JoinExpr:

    with ctx.empty() as subctx:
        larg = dispatch.resolve(range_var.larg, ctx=subctx)
        l_rel = subctx.scope.rel

    with ctx.empty() as subctx:
        rarg = dispatch.resolve(range_var.rarg, ctx=subctx)
        r_rel = subctx.scope.rel

    ctx.scope.join_relations.extend((l_rel, r_rel))
    ctx.scope.tables.extend((l_rel, r_rel))

    quals: Optional[pgast.BaseExpr] = None
    if range_var.quals:
        quals = dispatch.resolve(range_var.quals, ctx=ctx)

    if range_var.using_clause:
        for c in range_var.using_clause:
            with ctx.empty() as subctx:
                subctx.scope.tables = [l_rel]
                l_expr = dispatch.resolve(c, ctx=subctx)
            with ctx.empty() as subctx:
                subctx.scope.tables = [r_rel]
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

    return pgast.JoinExpr(
        type=range_var.type,
        larg=larg,
        rarg=rarg,
        quals=quals,
        alias=alias,
    )


@dispatch._resolve.register
def resolve_Relation(
    relation: pgast.Relation, *, ctx: Context
) -> pgast.Relation:
    assert relation.name

    # try a CTE
    cte = next((t for t in ctx.scope.ctes if t.name == relation.name), None)
    if cte:
        ctx.scope.rel.name = cte.name
        ctx.scope.rel.columns = cte.columns.copy()
        return pgast.Relation(name=cte.name, schemaname=None)

    # lookup the object in schema
    obj: Optional[s_objtypes.ObjectType] = None
    if (relation.schemaname or 'public') == 'public':
        object_type_name = relation.name[0].upper() + relation.name[1:]

        obj = ctx.schema.get(  # type: ignore
            object_type_name,
            None,
            module_aliases={None: 'default'},
            type=s_objtypes.ObjectType,
        )

        if not obj:
            raise errors.QueryError(f'unknown object `{object_type_name}`')
    else:
        raise errors.QueryError(
            f'unknown relation `{relation.schemaname}.{relation.name}`'
        )

    # extract table name
    ctx.scope.rel.name = obj.get_shortname(ctx.schema).name.lower()

    # extract table columns
    pointers = obj.get_pointers(ctx.schema).objects(ctx.schema)

    columns: List[context.Column] = []
    for p in pointers:
        if p.is_protected_pointer(ctx.schema):
            continue
        if p.get_cardinality(ctx.schema) != qltypes.SchemaCardinality.One:
            continue

        if p.is_property(ctx.schema):
            col = context.Column()
            col.name = p.get_shortname(ctx.schema).name

            if p.is_id_pointer(ctx.schema):
                col.reference_as = 'id'
            else:
                _, dbname = pgcommon.get_backend_name(
                    ctx.schema, p, catenate=False
                )
                col.reference_as = dbname

            columns.append(col)

        if isinstance(p, s_links.Link):
            col = context.Column()
            col.name = p.get_shortname(ctx.schema).name + '_id'

            _, dbname = pgcommon.get_backend_name(
                ctx.schema, p, catenate=False
            )
            col.reference_as = dbname

            columns.append(col)

    # sort by name but put `id` first
    columns.sort(key=lambda c: '!' if c.name == 'id' else c.name or '')

    ctx.scope.rel.columns.extend(columns)

    # compile
    aspect = 'inhview' if ctx.include_inherited else 'table'

    schemaname, dbname = pgcommon.get_backend_name(
        ctx.schema, obj, aspect=aspect, catenate=False
    )

    return pgast.Relation(name=dbname, schemaname=schemaname)


# this function cannot go though dispatch,
# because it may return multiple nodes, due to * notation
def resolve_ResTarget(
    res_target: pgast.ResTarget, *, ctx: Context
) -> Sequence[pgast.ResTarget]:

    alias = res_target.name

    # if just name has been selected, use it as the alias
    if not alias and isinstance(res_target.val, pgast.ColumnRef):
        name = res_target.val.name
        last_name_part = name[len(name) - 1]
        if isinstance(last_name_part, str):
            alias = last_name_part

    # special case for ColumnRef for handing wildcards
    if not alias and isinstance(res_target.val, pgast.ColumnRef):
        col_res = _lookup_column(res_target.val.name, ctx)

        res = []
        for table, column in col_res:
            ctx.scope.rel.columns.append(column)

            assert table.reference_as
            assert column.reference_as
            res.append(
                pgast.ResTarget(
                    val=pgast.ColumnRef(
                        name=(table.reference_as, column.reference_as)
                    ),
                    name=column.name,
                )
            )
        return res

    # base case
    val = dispatch.resolve(res_target.val, ctx=ctx)

    col = context.Column(name=alias, reference_as=alias)
    ctx.scope.rel.columns.append(col)
    return (pgast.ResTarget(val=val, name=alias),)


@dispatch._resolve.register
def resolve_ColumnRef(
    column_ref: pgast.ColumnRef, *, ctx: Context
) -> pgast.ColumnRef:
    res = _lookup_column(column_ref.name, ctx)
    if len(res) != 1:
        raise errors.QueryError(f'bad use of `*` column name')
    table, column = res[0]
    assert table.reference_as
    assert column.reference_as

    return pgast.ColumnRef(name=(table.reference_as, column.reference_as))


def _lookup_column(
    name: Sequence[str | pgast.Star], ctx: Context
) -> Sequence[Tuple[context.Table, context.Column]]:
    matched_columns: List[Tuple[context.Table, context.Column]] = []

    col_name: str | pgast.Star

    if len(name) == 1:
        # look for the column in all tables
        col_name = name[0]

        if isinstance(col_name, pgast.Star):
            return [(t, c) for t in ctx.scope.tables for c in t.columns]
        else:
            for table in ctx.scope.tables:
                matched_columns.extend(_lookup_in_table(col_name, table))

    elif len(name) == 2:
        # look for the column in the specific table
        tab_name = name[0]
        col_name = name[1]

        table = _lookup_table(ctx, cast(str, tab_name))

        if isinstance(col_name, pgast.Star):
            return [(table, c) for c in table.columns]
        else:
            matched_columns.extend(_lookup_in_table(col_name, table))

    if not matched_columns:
        raise errors.QueryError(f'cannot find column `{col_name}`')

    elif len(matched_columns) > 1:
        potential_tables = ', '.join(
            [t.name or '' for t, _ in matched_columns]
        )
        raise errors.QueryError(
            f'ambiguous column `{col_name}` could belong to '
            f'following tables: {potential_tables}'
        )

    return (matched_columns[0],)


def _lookup_in_table(
    col_name: str, table: context.Table
) -> Iterator[Tuple[context.Table, context.Column]]:
    for column in table.columns:
        if column.name == col_name:
            yield (table, column)


def _lookup_table(ctx: Context, tab_name: str) -> context.Table:
    matched_tables = []
    for t in ctx.scope.tables:
        if t.name == tab_name or t.alias == tab_name:
            matched_tables.append(t)

    if not matched_tables:
        raise errors.QueryError(f'cannot find table `{tab_name}`')
    elif len(matched_tables) > 1:
        raise errors.QueryError(f'ambiguous table `{tab_name}`')

    table = matched_tables[0]
    return table


@dispatch._resolve.register
def resolve_SubLink(
    sub_link: pgast.SubLink,
    *,
    ctx: Context,
) -> pgast.SubLink:
    with ctx.empty() as subctx:
        expr = dispatch.resolve(sub_link.expr, ctx=subctx)

    return pgast.SubLink(
        type=sub_link.type,
        expr=expr,
        test_expr=dispatch.resolve_opt(sub_link.test_expr, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_Expr(expr: pgast.Expr, *, ctx: Context) -> pgast.Expr:
    return pgast.Expr(
        kind=expr.kind,
        name=expr.name,
        lexpr=dispatch.resolve(expr.lexpr, ctx=ctx) if expr.lexpr else None,
        rexpr=dispatch.resolve(expr.rexpr, ctx=ctx) if expr.rexpr else None,
    )


@dispatch._resolve.register
def resolve_TypeCast(
    expr: pgast.TypeCast,
    *,
    ctx: Context,
) -> pgast.TypeCast:
    return pgast.TypeCast(
        arg=dispatch.resolve(expr.arg, ctx=ctx),
        type_name=expr.type_name,
    )


@dispatch._resolve.register
def resolve_BaseConstant(
    expr: pgast.BaseConstant,
    *,
    ctx: Context,
) -> pgast.BaseConstant:
    return expr


@dispatch._resolve.register
def resolve_CaseExpr(
    expr: pgast.CaseExpr,
    *,
    ctx: Context,
) -> pgast.CaseExpr:
    return pgast.CaseExpr(
        arg=dispatch.resolve_opt(expr.arg, ctx=ctx),
        args=dispatch.resolve_list(expr.args, ctx=ctx),
        defresult=dispatch.resolve_opt(expr.defresult, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_CaseWhen(
    expr: pgast.CaseWhen,
    *,
    ctx: Context,
) -> pgast.CaseWhen:
    return pgast.CaseWhen(
        expr=dispatch.resolve(expr.expr, ctx=ctx),
        result=dispatch.resolve(expr.result, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_SortBy(
    expr: pgast.SortBy,
    *,
    ctx: Context,
) -> pgast.SortBy:
    return pgast.SortBy(
        node=dispatch.resolve(expr.node, ctx=ctx),
        dir=expr.dir,
        nulls=expr.nulls,
    )


@dispatch._resolve.register
def resolve_FuncCall(
    expr: pgast.FuncCall,
    *,
    ctx: Context,
) -> pgast.FuncCall:
    # TODO: which functions do we want to expose on the outside?
    return pgast.FuncCall(
        name=expr.name,
        args=dispatch.resolve_list(expr.args, ctx=ctx),
        agg_order=dispatch.resolve_opt_list(expr.agg_order, ctx=ctx),
        agg_filter=dispatch.resolve_opt(expr.agg_filter, ctx=ctx),
        agg_star=expr.agg_star,
        agg_distinct=expr.agg_distinct,
        over=dispatch.resolve_opt(expr.over, ctx=ctx),
        with_ordinality=expr.with_ordinality,
    )


@dispatch._resolve.register
def resolve_WindowDef(
    expr: pgast.WindowDef,
    *,
    ctx: Context,
) -> pgast.WindowDef:
    return pgast.WindowDef(
        partition_clause=dispatch.resolve_opt_list(
            expr.partition_clause, ctx=ctx
        ),
        order_clause=dispatch.resolve_opt_list(expr.order_clause, ctx=ctx),
        start_offset=dispatch.resolve_opt(expr.start_offset, ctx=ctx),
        end_offset=dispatch.resolve_opt(expr.end_offset, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_CoalesceExpr(
    expr: pgast.CoalesceExpr,
    *,
    ctx: Context,
) -> pgast.CoalesceExpr:
    return pgast.CoalesceExpr(args=dispatch.resolve_list(expr.args, ctx=ctx))


@dispatch._resolve.register
def resolve_NullTest(
    expr: pgast.NullTest,
    *,
    ctx: Context,
) -> pgast.NullTest:
    return pgast.NullTest(
        arg=dispatch.resolve(expr.arg, ctx=ctx), negated=expr.negated
    )


@dispatch._resolve.register
def resolve_BooleanTest(
    expr: pgast.BooleanTest,
    *,
    ctx: Context,
) -> pgast.BooleanTest:
    return pgast.BooleanTest(
        arg=dispatch.resolve(expr.arg, ctx=ctx),
        negated=expr.negated,
        is_true=expr.is_true,
    )


@dispatch._resolve.register
def resolve_ImplicitRowExpr(
    expr: pgast.ImplicitRowExpr,
    *,
    ctx: Context,
) -> pgast.ImplicitRowExpr:
    return pgast.ImplicitRowExpr(
        args=dispatch.resolve_list(expr.args, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_ParamRef(
    expr: pgast.ParamRef,
    *,
    ctx: Context,
) -> pgast.ParamRef:
    return pgast.ParamRef(number=expr.number)
