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

from edb.schema import objtypes as s_objtypes
from edb.schema import links as s_links

from . import dispatch
from . import context
from . import range_var
from . import expr

Context = context.ResolverContextLevel


@dispatch._resolve_relation.register
def resolve_SelectStmt(
    stmt: pgast.SelectStmt, *, ctx: Context
) -> Tuple[pgast.SelectStmt, context.Table]:

    # UNION
    if stmt.larg or stmt.rarg:
        assert stmt.larg and stmt.rarg

        with ctx.isolated() as subctx:
            larg, ltable = dispatch.resolve_relation(stmt.larg, ctx=subctx)

        with ctx.isolated() as subctx:
            rarg, rtable = dispatch.resolve_relation(stmt.rarg, ctx=subctx)

        # valudate equal columns from both sided
        if len(ltable.columns) != len(rtable.columns):
            raise errors.QueryError(
                f'{stmt.op} requires equal number of columns in both sides',
                context=stmt.context,
            )

        relation = pgast.SelectStmt(
            larg=larg,
            rarg=rarg,
            op=stmt.op,
            all=stmt.all,
        )
        return (relation, ltable)

    # CTEs
    ctes = None
    if stmt.ctes:
        ctes = []
        for cte in stmt.ctes:
            cte, tab = range_var.resolve_CommonTableExpr(cte, ctx=ctx)
            ctes.append(cte)
            ctx.scope.ctes.append(tab)

    # FROM
    from_clause: List[pgast.BaseRangeVar] = []
    for clause in stmt.from_clause:
        from_clause.append(range_var.resolve_BaseRangeVar(clause, ctx=ctx))

    where = dispatch.resolve_opt(stmt.where_clause, ctx=ctx)

    table = context.Table()
    target_list: List[pgast.ResTarget] = []
    for t in stmt.target_list:
        targets, columns = expr.resolve_ResTarget(t, ctx=ctx)
        target_list.extend(targets)
        table.columns.extend(columns)

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
    return (
        res,
        table,
    )


@dispatch._resolve_relation.register
def resolve_DMLQuery(
    query: pgast.DMLQuery, *, ctx: Context
) -> Tuple[pgast.DMLQuery, context.Table]:
    raise errors.UnsupportedFeatureError(
        'DML queries (INSERT/UPDATE/DELETE) are not supported',
        context=query.context,
    )


@dispatch._resolve_relation.register
def resolve_relation(
    relation: pgast.Relation, *, ctx: Context
) -> Tuple[pgast.Relation, context.Table]:
    assert relation.name
    schema_name = relation.schemaname or 'public'

    # try a CTE
    cte = next((t for t in ctx.scope.ctes if t.name == relation.name), None)
    if cte:
        table = context.Table()
        table.name = cte.name
        table.columns = cte.columns.copy()
        return pgast.Relation(name=cte.name, schemaname=None), table

    # lookup the object in schema
    obj: Optional[s_objtypes.ObjectType] = None
    if schema_name == 'public':
        obj = ctx.schema.get(  # type: ignore
            relation.name,
            None,
            module_aliases={None: 'default'},
            type=s_objtypes.ObjectType,
        )

    if not obj:
        raise errors.QueryError(
            f'unknown table `{schema_name}.{relation.name}`',
            context=relation.context,
        )

    # extract table name
    table = context.Table()
    table.name = obj.get_shortname(ctx.schema).name

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
    columns.sort(key=lambda c: () if c.name == 'id' else (c.name or '',))

    table.columns.extend(columns)

    # compile
    aspect = 'inhview' if ctx.include_inherited else 'table'

    schemaname, dbname = pgcommon.get_backend_name(
        ctx.schema, obj, aspect=aspect, catenate=False
    )

    return pgast.Relation(name=dbname, schemaname=schemaname), table
