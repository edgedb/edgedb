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

from edb.pgsql import ast as pgast
from edb.pgsql import common as pgcommon

from edb.schema import objtypes as s_objtypes
from edb.schema import links as s_links
from edb.schema import properties as s_properties
from edb.schema import pointers as s_pointers
from edb.schema import sources as s_sources
from edb.schema import name as sn

from . import dispatch
from . import context
from . import range_var
from . import expr

Context = context.ResolverContextLevel


@dispatch._resolve_relation.register
def resolve_SelectStmt(
    stmt: pgast.SelectStmt, *, ctx: Context
) -> Tuple[pgast.SelectStmt, context.Table]:

    # VALUES
    if stmt.values:
        values = dispatch.resolve_list(stmt.values, ctx=ctx)
        relation = pgast.SelectStmt(values=values)

        first_val = values[0]
        assert isinstance(first_val, pgast.ImplicitRowExpr)
        table = context.Table(
            columns=[
                context.Column(name=ctx.names.get('col'))
                for _ in first_val.args
            ]
        )
        return relation, table

    # UNION
    if stmt.larg or stmt.rarg:
        assert stmt.larg and stmt.rarg

        with ctx.isolated() as subctx:
            larg, ltable = dispatch.resolve_relation(stmt.larg, ctx=subctx)

        with ctx.isolated() as subctx:
            rarg, rtable = dispatch.resolve_relation(stmt.rarg, ctx=subctx)

        # validate equal columns from both sides
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
        table = context.Table(
            name=cte.name,
            columns=cte.columns.copy(),
        )
        return pgast.Relation(name=cte.name, schemaname=None), table

    # lookup the object in schema
    obj: Optional[s_sources.Source] = None
    if schema_name == 'public':
        obj = ctx.schema.get(  # type: ignore
            relation.name,
            None,
            module_aliases={None: 'default'},
            type=s_objtypes.ObjectType,
        )

        # try multi link table
        if not obj:
            obj = _lookup_link_property(relation.name, ctx)

    if not obj:
        raise errors.QueryError(
            f'unknown table `{schema_name}.{relation.name}`',
            context=relation.context,
        )

    # extract table name
    table = context.Table(name=relation.name)

    # extract table columns
    pointers = obj.get_pointers(ctx.schema).objects(ctx.schema)

    columns: List[context.Column] = []
    for p in pointers:
        if p.is_protected_pointer(ctx.schema):
            continue
        card = p.get_cardinality(ctx.schema)
        if card.is_multi():
            continue

        columns.append(_construct_column(p, ctx))

    # sort by name but put `id` first
    columns.sort(key=lambda c: () if c.name == 'id' else (c.name or '',))

    table.columns.extend(columns)

    # compile
    aspect = 'inhview' if ctx.include_inherited else 'table'

    schemaname, dbname = pgcommon.get_backend_name(
        ctx.schema, obj, aspect=aspect, catenate=False
    )

    return pgast.Relation(name=dbname, schemaname=schemaname), table


def _lookup_link_property(name: str, ctx: Context) -> Optional[s_links.Link]:
    if '.' not in name:
        return None
    object_name, link_name = name.split('.')
    parent: s_objtypes.ObjectType = ctx.schema.get(  # type: ignore
        object_name,
        None,
        module_aliases={None: 'default'},
        type=s_objtypes.ObjectType,
    )
    if not parent:
        return None

    link = parent.maybe_get_ptr(
        ctx.schema, sn.UnqualName.from_string(link_name)
    )

    if not isinstance(link, s_links.Link):
        return None

    if link.get_cardinality(ctx.schema).is_single():
        # single links only for tables with at least one property
        # besides source and target
        l_pointers = link.get_pointers(ctx.schema).objects(ctx.schema)
        if len(l_pointers) <= 2:
            return None

    return link


def _construct_column(p: s_pointers.Pointer, ctx: Context) -> context.Column:
    col = context.Column()

    if isinstance(p, s_properties.Property):
        col.name = p.get_shortname(ctx.schema).name

        if p.is_link_source_property(ctx.schema):
            col.reference_as = 'source'
        elif p.is_link_target_property(ctx.schema):
            col.reference_as = 'target'
        elif p.is_id_pointer(ctx.schema):
            col.reference_as = 'id'
        else:
            _, dbname = pgcommon.get_backend_name(
                ctx.schema, p, catenate=False
            )
            col.reference_as = dbname

    if isinstance(p, s_links.Link):
        col.name = p.get_shortname(ctx.schema).name + '_id'

        _, dbname = pgcommon.get_backend_name(ctx.schema, p, catenate=False)
        col.reference_as = dbname

    return col
