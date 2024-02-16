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
from edb.server.pgcon import errors as pgerror

from edb.pgsql import ast as pgast
from edb.pgsql import common as pgcommon
from edb.pgsql import codegen as pgcodegen

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
from . import sql_introspection

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

        with ctx.child() as subctx:
            larg, ltable = dispatch.resolve_relation(stmt.larg, ctx=subctx)

        with ctx.child() as subctx:
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

    # WHERE
    where = dispatch.resolve_opt(stmt.where_clause, ctx=ctx)

    # GROUP BY
    with ctx.child() as subctx:
        register_projections(stmt.target_list, ctx=subctx)

        group_clause = dispatch.resolve_opt_list(stmt.group_clause, ctx=subctx)

    # SELECT projection
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

    # order by can refer to columns in SELECT projection, so we need to add
    # table.columns into scope
    ctx.scope.tables.append(
        context.Table(
            columns=[
                context.Column(name=c.name, reference_as=c.name)
                for c, target in zip(table.columns, stmt.target_list)
                if target.name
                and (
                    not isinstance(target.val, pgast.ColumnRef)
                    or target.val.name[-1] != target.name
                )
            ]
        )
    )

    sort_clause = dispatch.resolve_opt_list(stmt.sort_clause, ctx=ctx)
    limit_offset = dispatch.resolve_opt(stmt.limit_offset, ctx=ctx)
    limit_count = dispatch.resolve_opt(stmt.limit_count, ctx=ctx)

    res = pgast.SelectStmt(
        distinct_clause=distinct_clause,
        from_clause=from_clause,
        target_list=target_list,
        group_clause=group_clause,
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


def register_projections(target_list: List[pgast.ResTarget], *, ctx: Context):
    # add aliases from target_list into scope

    table = context.Table()

    for target in target_list:
        if not target.name:
            continue

        table.columns.append(
            context.Column(name=target.name, reference_as=target.name)
        )
    ctx.scope.tables.append(table)


@dispatch._resolve_relation.register
def resolve_DMLQuery(
    query: pgast.DMLQuery, *, ctx: Context
) -> Tuple[pgast.DMLQuery, context.Table]:
    raise errors.UnsupportedFeatureError(
        'DML queries (INSERT/UPDATE/DELETE) are not supported',
        context=query.context,
    )


PG_TOAST_TABLE: List[
    Tuple[sql_introspection.ColumnName, sql_introspection.ColumnType]
] = [
    ('chunk_id', None),
    ('chunk_seq', None),
    ('chunk_data', None),
]


@dispatch._resolve_relation.register
def resolve_relation(
    relation: pgast.Relation, *, ctx: Context
) -> Tuple[pgast.Relation, context.Table]:
    assert relation.name

    if relation.catalogname and relation.catalogname != 'postgres':
        raise errors.QueryError(
            f'queries cross databases are not supported',
            context=relation.context,
        )

    # try information_schema, pg_catalog and pg_toast
    preset_tables = None
    if relation.schemaname == 'information_schema':
        preset_tables = (sql_introspection.INFORMATION_SCHEMA, 'edgedbsql')
    elif not relation.schemaname or relation.schemaname == 'pg_catalog':
        preset_tables = (sql_introspection.PG_CATALOG, 'edgedbsql')
    elif relation.schemaname == 'pg_toast':
        preset_tables = ({relation.name: PG_TOAST_TABLE}, 'pg_toast')

    if preset_tables and relation.name in preset_tables[0]:
        cols = [
            context.Column(name=n, reference_as=n)
            for n, _type in preset_tables[0][relation.name]
        ]
        cols.extend(_construct_system_columns())
        table = context.Table(name=relation.name, columns=cols)
        rel = pgast.Relation(name=relation.name, schemaname=preset_tables[1])

        return rel, table

    schema_name = relation.schemaname

    # try a CTE
    if not schema_name or schema_name == 'public':
        cte = next((t for t in ctx.scope.ctes if t.name == relation.name), None)
        if cte:
            table = context.Table(name=cte.name, columns=cte.columns.copy())
            return pgast.Relation(name=cte.name, schemaname=None), table

    def public_to_default(s: str) -> str:
        # make sure to match `public`, `public::blah`, but not `public_blah`
        if s == 'public':
            return 'default'
        if s.startswith('public::'):
            return 'default' + s[6:]
        return s

    # lookup the object in schema
    schemas = [schema_name] if schema_name else ctx.options.search_path
    modules = [public_to_default(s) for s in schemas]

    obj: Optional[s_sources.Source | s_properties.Property] = None
    for module in modules:
        if obj:
            break

        object_name = sn.QualName(module, relation.name)
        obj = ctx.schema.get(  # type: ignore
            object_name,
            None,
            module_aliases={None: 'default'},
            type=s_objtypes.ObjectType,
        )

    # try pointer table
    for module in modules:
        if obj:
            break
        obj = _lookup_pointer_table(module, relation.name, ctx)

    if not obj:
        rel_name = pgcodegen.generate_source(relation)
        raise errors.QueryError(
            f'unknown table `{rel_name}`',
            context=relation.context,
            pgext_code=pgerror.ERROR_UNDEFINED_TABLE,
        )

    # extract table name
    table = context.Table(name=relation.name)

    # extract table columns
    # when changing this, make sure to update sql information_schema
    columns: List[context.Column] = []

    if isinstance(obj, s_sources.Source):
        pointers = obj.get_pointers(ctx.schema).objects(ctx.schema)

        for p in pointers:
            card = p.get_cardinality(ctx.schema)
            if card.is_multi():
                continue
            if p.get_computable(ctx.schema):
                continue

            columns.append(_construct_column(p, ctx, ctx.include_inherited))
    else:
        for c in ['source', 'target']:
            columns.append(context.Column(name=c, reference_as=c))

    def column_order_key(c: context.Column) -> Tuple[int, str]:
        spec = {
            'id': 0,
            'source': 0,
            'target': 1
        }
        return (spec.get(c.reference_as or '', 2), c.name or '')

    # sort by name but put `id` first
    columns.sort(key=column_order_key)
    table.columns.extend(columns)

    aspect = 'inhview' if ctx.include_inherited else 'table'

    table.columns.extend(_construct_system_columns())

    schemaname, dbname = pgcommon.get_backend_name(
        ctx.schema, obj, aspect=aspect, catenate=False
    )

    return pgast.Relation(name=dbname, schemaname=schemaname), table


def _lookup_pointer_table(
    module: str, name: str, ctx: Context
) -> Optional[s_links.Link | s_properties.Property]:
    # Pointer tables are either:
    # - multi link tables
    # - single link tables with at least one property besides source and target
    # - multi property tables

    if '.' not in name:
        return None
    object_name, link_name = name.split('.')
    object_name_qual = sn.QualName(module, object_name)

    parent: s_objtypes.ObjectType = ctx.schema.get(  # type: ignore
        object_name_qual,
        None,
        module_aliases={None: 'default'},
        type=s_objtypes.ObjectType,
    )
    if not parent:
        return None

    pointer = parent.maybe_get_ptr(
        ctx.schema, sn.UnqualName.from_string(link_name)
    )

    if not pointer:
        return None
    if pointer.get_computable(ctx.schema) or pointer.get_internal(ctx.schema):
        return None

    match pointer:
        case s_links.Link():
            if pointer.get_cardinality(ctx.schema).is_single():
                # single links only for tables with at least one property
                # besides source and target
                l_pointers = pointer.get_pointers(ctx.schema).objects(
                    ctx.schema
                )
                if len(l_pointers) <= 2:
                    return None

            return pointer

        case s_properties.Property():
            if pointer.get_cardinality(ctx.schema).is_single():
                return None
            return pointer

    raise NotImplementedError()


def _construct_column(
    p: s_pointers.Pointer, ctx: Context, include_inherited: bool
) -> context.Column:
    col = context.Column()
    short_name = p.get_shortname(ctx.schema)

    if isinstance(p, s_properties.Property):
        col.name = short_name.name

        if p.is_link_source_property(ctx.schema):
            col.reference_as = 'source'
        elif p.is_link_target_property(ctx.schema):
            col.reference_as = 'target'
        elif p.is_id_pointer(ctx.schema):
            col.reference_as = 'id'
        else:
            _, dbname = pgcommon.get_backend_name(ctx.schema, p, catenate=False)
            col.reference_as = dbname

    elif isinstance(p, s_links.Link):
        if short_name.name == '__type__':
            col.name = '__type__'
            col.reference_as = '__type__'

            if not include_inherited:
                # When using FROM ONLY, we will be referencing actual tables
                # and not inheritance views. Actual tables don't contain
                # __type__ column, which means that we have to provide value
                # in some other way. Fortunately, it is a constant value, so we
                # can compute it statically.
                source_id = p.get_source_type(ctx.schema).get_id(ctx.schema)
                col.static_val = source_id
        else:
            col.name = short_name.name + '_id'
            _, dbname = pgcommon.get_backend_name(ctx.schema, p, catenate=False)
            col.reference_as = dbname

    return col


def _construct_system_columns() -> List[context.Column]:
    return [
        context.Column(name=c, reference_as=c, hidden=True)
        for c in ['tableoid', 'xmin', 'cmin', 'xmax', 'cmax', 'ctid']
    ]
