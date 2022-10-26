from calendar import c
from typing import *

from edb.edgeql import qltypes

from edb.pgsql import ast as pgast
from edb.pgsql import common as pgcommon

from edb.schema import name as s_name
from edb.schema import schema as s_schema
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import properties as s_prop
from edb.schema import links as s_links

from . import dispatch
from . import context


@dispatch._resolve.register(pgast.SelectStmt)
def resolve_SelectStmt(
    stmt: pgast.SelectStmt, *, ctx: context.ResolverContextLevel
) -> pgast.SelectStmt:

    if stmt.larg:
        with ctx.isolated() as subctx:
            stmt.larg = dispatch.resolve(stmt.larg, ctx=subctx)
    if stmt.rarg:
        with ctx.isolated() as subctx:
            stmt.rarg = dispatch.resolve(stmt.rarg, ctx=subctx)
    if stmt.larg or stmt.rarg:
        return stmt

    from_clause: List[pgast.BaseRangeVar] = []
    for clause in stmt.from_clause:
        with ctx.empty() as subctx:
            from_clause.append(dispatch.resolve(clause, ctx=subctx))

            if subctx.rel.columns:
                ctx.scope.tables.append(subctx.rel)
            if subctx.scope.tables:
                ctx.scope.tables.extend(subctx.scope.tables)

    return pgast.SelectStmt(
        from_clause=from_clause,
        target_list=[
            c for t in stmt.target_list for c in resolve_ResTarget(t, ctx=ctx)
        ],
    )


@dispatch._resolve.register(pgast.BaseRangeVar)
def resolve_BaseRangeVar(
    range_var: pgast.BaseRangeVar, *, ctx: context.ResolverContextLevel
) -> pgast.BaseRangeVar:
    # store public alias
    if range_var.alias:
        ctx.rel.alias = range_var.alias.aliasname

    # generate internal alias
    aliasname = ctx.names.generate_relation()
    ctx.rel.reference_as = aliasname
    alias = pgast.Alias(aliasname=aliasname)
    return dispatch.resolve_range_var(range_var, alias, ctx=ctx)


@dispatch.resolve_range_var.register(pgast.RangeSubselect)
def resolve_RangeSubselect(
    range_var: pgast.RangeSubselect,
    alias: pgast.Alias,
    *,
    ctx: context.ResolverContextLevel,
) -> pgast.RangeSubselect:
    subquery = dispatch.resolve(range_var.subquery, ctx=ctx)
    return pgast.RangeSubselect(
        subquery=subquery,
        alias=alias,
    )


@dispatch.resolve_range_var.register(pgast.RelRangeVar)
def resolve_RelRangeVar(
    range_var: pgast.RelRangeVar,
    alias: pgast.Alias,
    *,
    ctx: context.ResolverContextLevel,
) -> pgast.RelRangeVar:
    ctx.include_inherited = range_var.include_inherited

    return pgast.RelRangeVar(
        relation=dispatch.resolve(range_var.relation, ctx=ctx),
        alias=alias,
    )


@dispatch.resolve_range_var.register(pgast.JoinExpr)
def resolve_JoinExpr(
    range_var: pgast.JoinExpr,
    alias: pgast.Alias,
    *,
    ctx: context.ResolverContextLevel,
) -> pgast.JoinExpr:

    with ctx.empty() as subctx:
        larg = dispatch.resolve(range_var.larg, ctx=subctx)
        ctx.scope.tables.append(subctx.rel)

    with ctx.empty() as subctx:
        rarg = dispatch.resolve(range_var.rarg, ctx=subctx)
        ctx.scope.tables.append(subctx.rel)

    using_clause = None
    if range_var.using_clause:
        using_clause = [
            dispatch.resolve(c, ctx=ctx) for c in range_var.using_clause
        ]

    quals = None
    if range_var.quals:
        quals = dispatch.resolve(range_var.quals, ctx=ctx)

    return pgast.JoinExpr(
        type=range_var.type,
        larg=larg,
        rarg=rarg,
        using_clause=using_clause,
        quals=quals,
        alias=alias,
    )


@dispatch._resolve.register(pgast.Relation)
def resolve_Query(
    relation: pgast.Relation, *, ctx: context.ResolverContextLevel
) -> pgast.Relation:
    assert relation.name

    # lookup the object in schema
    object_type_name = relation.name[0].upper() + relation.name[1:]

    obj: s_objtypes.ObjectType = ctx.schema.get(  # type: ignore
        object_type_name,
        None,
        module_aliases={None: 'default'},
        type=s_objtypes.ObjectType,
    )

    if not obj:
        raise BaseException(f'unknown object `{object_type_name}`')

    # extract table name
    ctx.rel.name = obj.get_shortname(ctx.schema).name.lower()

    # extract table columns
    pointers = obj.get_pointers(ctx.schema).objects(ctx.schema)

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

            ctx.rel.columns.append(col)

        if isinstance(p, s_links.Link):
            col = context.Column()
            col.name = p.get_shortname(ctx.schema).name + '_id'

            _, dbname = pgcommon.get_backend_name(
                ctx.schema, p, catenate=False
            )
            col.reference_as = dbname

            ctx.rel.columns.append(col)

    # compile
    aspect = 'inhview' if ctx.include_inherited else 'table'

    schemaname, dbname = pgcommon.get_backend_name(
        ctx.schema, obj, aspect=aspect, catenate=False
    )

    return pgast.Relation(name=dbname, schemaname=schemaname)


@dispatch._resolve.register(pgast.ColumnRef)
def resolve_ColumnRef(
    column_ref: pgast.ColumnRef, *, ctx: context.ResolverContextLevel
) -> pgast.ColumnRef:
    res = _lookup_column(column_ref.name, ctx)
    if len(res) != 1:
        raise BaseException(f'bad use of `*` column name')
    table, column = res[0]
    assert table.reference_as
    assert column.reference_as

    return pgast.ColumnRef(name=(table.reference_as, column.reference_as))


def _lookup_column(
    name: Sequence[str | pgast.Star], ctx: context.ResolverContextLevel
) -> Sequence[Tuple[context.Table, context.Column]]:
    matched_columns: List[Tuple[context.Table, context.Column]] = []

    col_name: str | pgast.Star

    if len(name) == 1:
        # look for the column in all tables
        col_name = name[0]

        if isinstance(col_name, pgast.Star):
            return [(t, c) for c in table.columns for t in ctx.tables]
        else:
            for table in ctx.scope.tables:
                matched_columns.extend(_lookup_in_table(col_name, table))

    elif len(name) == 2:
        # look for the column in the specific table
        tab_name = name[0]
        col_name = name[1]

        table = _lookup_table(ctx, tab_name)

        if isinstance(col_name, pgast.Star):
            return [(table, c) for c in table.columns]
        else:
            matched_columns.extend(_lookup_in_table(col_name, table))

    if not matched_columns:
        for table in ctx.scope.tables:
            print(table.name, [col.name for col in table.columns])

        raise BaseException(f'cannot find column `{col_name}`')
    elif len(matched_columns) > 1:
        potential_tables = ', '.join(
            [t.name or '' for t, _ in matched_columns]
        )
        raise BaseException(
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


def _lookup_table(
    ctx: context.ResolverContextLevel, tab_name: str
) -> context.Table:
    matched_tables = []
    for t in ctx.scope.tables:
        if t.name == tab_name or t.alias == tab_name:
            matched_tables.append(t)

    if not matched_tables:
        raise BaseException(f'cannot find table `{tab_name}`')
    elif len(matched_tables) > 1:
        raise BaseException(f'ambiguous table `{tab_name}`')

    table = matched_tables[0]
    return table


# this function cannot go though dispatch,
# because it may return multiple nodes, due to * notation
def resolve_ResTarget(
    res_target: pgast.ResTarget, *, ctx: context.ResolverContextLevel
) -> Sequence[pgast.ResTarget]:

    alias = res_target.name

    # if just name has been selected, use it as the alias
    if not alias and isinstance(res_target.val, pgast.ColumnRef):
        name = res_target.val.name
        last_name_part = name[len(name) - 1]
        if isinstance(last_name_part, str):
            alias = last_name_part

    # special case ColumnRef for handing *
    if isinstance(res_target.val, pgast.ColumnRef):
        val = _lookup_column(res_target.val.name, ctx)

        res = []
        for table, column in val:
            ctx.rel.columns.append(column)
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

    col = context.Column()
    col.name = alias
    col.reference_as = alias

    ctx.rel.columns.append(col)
    return (pgast.ResTarget(val=val, name=alias),)


@dispatch._resolve.register(pgast.Expr)
def resolve_Expr(
    expr: pgast.Expr, *, ctx: context.ResolverContextLevel
) -> pgast.Expr:
    return pgast.Expr(
        kind=expr.kind,
        name=expr.name,
        lexpr=dispatch.resolve(expr.lexpr, ctx=ctx) if expr.lexpr else None,
        rexpr=dispatch.resolve(expr.rexpr, ctx=ctx) if expr.rexpr else None,
    )
