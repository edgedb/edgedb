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

from typing import Optional, Tuple, List, cast, Set
import uuid

from edb import errors
from edb.server.pgcon import errors as pgerror

from edb.edgeql import qltypes

from edb.pgsql import ast as pgast
from edb.pgsql import common as pgcommon
from edb.pgsql import codegen as pgcodegen
from edb.pgsql import inheritance as pginheritance
from edb.pgsql import types as pgtypes

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
from . import command

Context = context.ResolverContextLevel


@dispatch._resolve_relation.register
def resolve_SelectStmt(
    stmt: pgast.SelectStmt, *, include_inherited: bool, ctx: Context
) -> Tuple[pgast.SelectStmt, context.Table]:
    # CTEs
    ctes: List[pgast.CommonTableExpr] = []
    if stmt.ctes:
        for cte in stmt.ctes:
            cte, tab = range_var.resolve_CommonTableExpr(cte, ctx=ctx)
            ctes.extend(extract_ctes_from_ctx(ctx))
            ctes.append(cte)
            ctx.scope.ctes.append(tab)

    # VALUES
    if stmt.values:
        values = dispatch.resolve_list(stmt.values, ctx=ctx)
        relation = pgast.SelectStmt(
            values=values, ctes=ctes + extract_ctes_from_ctx(ctx)
        )

        first_val = values[0]
        assert isinstance(first_val, pgast.ImplicitRowExpr)
        table = context.Table(
            columns=[
                context.Column(
                    name=f'column{index + 1}',
                    kind=context.ColumnByName(
                        reference_as=f'column{index + 1}'
                    ),
                )
                for index, _ in enumerate(first_val.args)
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
                span=stmt.span,
            )

        relation = stmt.replace(
            larg=cast(pgast.Query, larg),
            rarg=cast(pgast.Query, rarg),
            ctes=ctes + extract_ctes_from_ctx(ctx),
        )
        return (relation, ltable)

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

    # HAVING
    having = dispatch.resolve_opt(stmt.having_clause, ctx=ctx)

    # SELECT projection
    table = context.Table()
    target_list: List[pgast.ResTarget] = []
    names: Set[str] = set()
    for t in stmt.target_list:
        targets, columns = expr.resolve_ResTarget(
            t, existing_names=names, ctx=ctx
        )

        target_list.extend(targets)
        table.columns.extend(columns)
        names.update(c.name for c in columns)

    distinct_clause = None
    if stmt.distinct_clause:
        distinct_clause = [
            (c if isinstance(c, pgast.Star) else dispatch.resolve(c, ctx=ctx))
            for c in stmt.distinct_clause
        ]

    # order by can refer to columns in SELECT projection, so we need to add
    # table.columns into scope
    projected_table = context.Table(
        columns=[
            context.Column(
                name=c.name,
                kind=context.ColumnByName(reference_as=c.name),
            )
            for c, target in zip(table.columns, stmt.target_list)
            if target.name
            and (
                not isinstance(target.val, pgast.ColumnRef)
                or target.val.name[-1] != target.name
            )
        ]
    )
    if len(projected_table.columns) > 0:
        ctx.scope.tables.append(projected_table)

    sort_clause = dispatch.resolve_opt_list(stmt.sort_clause, ctx=ctx)
    limit_offset = dispatch.resolve_opt(stmt.limit_offset, ctx=ctx)
    limit_count = dispatch.resolve_opt(stmt.limit_count, ctx=ctx)
    locking_clause = dispatch.resolve_opt_list(stmt.locking_clause, ctx=ctx)

    ctes.extend(extract_ctes_from_ctx(ctx))

    res = pgast.SelectStmt(
        distinct_clause=distinct_clause,
        from_clause=from_clause,
        target_list=target_list,
        group_clause=group_clause,
        having_clause=having,
        where_clause=where,
        sort_clause=sort_clause,
        limit_offset=limit_offset,
        limit_count=limit_count,
        locking_clause=locking_clause,
        ctes=ctes if len(ctes) > 0 else None,
    )
    return (
        res,
        table,
    )


# If current context is top-level, return additional CTEs that need to be
# injected. They were probably generated by DML.
def extract_ctes_from_ctx(
    ctx: context.ResolverContextLevel,
) -> List[pgast.CommonTableExpr]:
    if ctx.subquery_depth != 0:
        return []

    res = list(ctx.ctes_buffer)
    ctx.ctes_buffer.clear()
    return res


def register_projections(target_list: List[pgast.ResTarget], *, ctx: Context):
    # add aliases from target_list into scope

    table = context.Table()

    for target in target_list:
        if not target.name:
            continue

        table.columns.append(
            context.Column(
                name=target.name,
                kind=context.ColumnByName(reference_as=target.name),
            )
        )
    ctx.scope.tables.append(table)


PG_TOAST_TABLE: List[
    Tuple[sql_introspection.ColumnName, sql_introspection.ColumnType, int]
] = [
    ('chunk_id', None, 13),
    ('chunk_seq', None, 13),
    ('chunk_data', None, 13),
]


@dispatch._resolve_relation.register
def resolve_relation(
    relation: pgast.Relation, *, include_inherited: bool, ctx: Context
) -> Tuple[pgast.Relation, context.Table]:
    assert relation.name

    if relation.catalogname and relation.catalogname != 'postgres':
        raise errors.QueryError(
            f'queries cross databases are not supported',
            span=relation.span,
        )

    # try information_schema, pg_catalog and pg_toast
    preset_tables = None
    if relation.schemaname == 'information_schema':
        preset_tables = (
            sql_introspection.INFORMATION_SCHEMA,
            pgcommon.versioned_schema('edgedbsql'),
        )
    elif not relation.schemaname or relation.schemaname == 'pg_catalog':
        preset_tables = (
            sql_introspection.PG_CATALOG,
            pgcommon.versioned_schema('edgedbsql'),
        )
    elif relation.schemaname == 'pg_toast':
        preset_tables = ({relation.name: PG_TOAST_TABLE}, 'pg_toast')

    if preset_tables and relation.name in preset_tables[0]:
        cols = [
            context.Column(name=n, kind=context.ColumnByName(reference_as=n))
            for n, _type, _ver_since in preset_tables[0][relation.name]
        ]
        cols.extend(_construct_system_columns())
        table = context.Table(
            name=relation.name, columns=cols, is_direct_relation=True
        )
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
            span=relation.span,
            pgext_code=pgerror.ERROR_UNDEFINED_TABLE,
        )

    # extract table name
    table = context.Table(schema_id=obj.id, name=relation.name)

    # extract table columns
    # when changing this, make sure to update sql information_schema
    columns: List[context.Column] = []

    if isinstance(obj, s_sources.Source):
        pointers = obj.get_pointers(ctx.schema).objects(ctx.schema)

        for p in pointers:
            card = p.get_cardinality(ctx.schema)
            if card.is_multi():
                continue

            columns.append(_construct_column(p, ctx))
    else:
        for c in ['source', 'target']:
            columns.append(
                context.Column(
                    name=c, kind=context.ColumnByName(reference_as=c)
                )
            )

    def column_order_key(c: context.Column) -> Tuple[int, str]:
        spec = {'id': 0, 'source': 0, 'target': 1}
        order: int
        if isinstance(c.kind, context.ColumnByName):
            order = spec.get(c.kind.reference_as, 2)
        else:
            order = 2
        return (order, c.name or '')

    # sort by name but put `id` first
    columns.sort(key=column_order_key)
    table.columns.extend(columns)

    table.columns.extend(_construct_system_columns())

    if ctx.options.apply_access_policies and _has_access_policies(obj, ctx):
        if isinstance(obj, s_objtypes.ObjectType):
            rel = _compile_read_of_obj_table(
                obj, include_inherited, table, ctx
            )
        else:
            # TODO: implement access policy filtering for link and
            # multi-property tables
            rel = _relation_of_table(obj, table, ctx)
    else:
        if include_inherited and _has_sub_types(obj, ctx):
            rel = _relation_of_inheritance_cte(obj, ctx)
        else:
            rel = _relation_of_table(obj, table, ctx)

    return rel, table


def _has_access_policies(
    obj: s_sources.Source | s_properties.Property, ctx: Context
):
    if isinstance(obj, s_pointers.Pointer):
        return False
    assert isinstance(obj, s_objtypes.ObjectType)

    policies = obj.get_access_policies(ctx.schema)
    return len(policies) > 0


def _has_sub_types(obj: s_sources.Source | s_properties.Property, ctx: Context):
    return len(obj.children(ctx.schema)) > 0


def _relation_of_table(
    obj: s_sources.Source | s_properties.Property,
    table: context.Table,
    ctx: Context,
) -> pgast.Relation:
    schemaname, dbname = pgcommon.get_backend_name(
        ctx.schema, obj, aspect='table', catenate=False
    )
    relation = pgast.Relation(name=dbname, schemaname=schemaname)

    table.is_direct_relation = True
    # When referencing actual tables, we need to statically provide __type__,
    # since this column does not exist in the database.
    for col in table.columns:
        if col.name == '__type__':
            col.kind = context.ColumnStaticVal(val=obj.id)
            break

    return relation


def _relation_of_inheritance_cte(
    obj: s_sources.Source | s_properties.Property, ctx: Context
) -> pgast.Relation:
    if obj not in ctx.inheritance_ctes:
        cte = pgast.CommonTableExpr(
            name=ctx.alias_generator.get('inh'),
            query=pginheritance.get_inheritance_view(ctx.schema, obj),
        )
        ctx.ctes_buffer.append(cte)
        ctx.inheritance_ctes[obj] = cte.name
    return pgast.Relation(name=ctx.inheritance_ctes[obj])


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


def _construct_column(p: s_pointers.Pointer, ctx: Context) -> context.Column:
    short_name = p.get_shortname(ctx.schema)

    col_name: str
    kind: context.ColumnKind

    if isinstance(p, s_properties.Property):
        col_name = short_name.name

        if p.get_computable(ctx.schema):
            kind = context.ColumnComputable(pointer=p)
        elif p.is_link_source_property(ctx.schema):
            kind = context.ColumnByName(reference_as='source')
        elif p.is_link_target_property(ctx.schema):
            kind = context.ColumnByName(reference_as='target')
        elif p.is_id_pointer(ctx.schema):
            kind = context.ColumnByName(reference_as='id')
        else:
            _, dbname = pgcommon.get_backend_name(ctx.schema, p, catenate=False)
            kind = context.ColumnByName(reference_as=dbname)

    elif isinstance(p, s_links.Link):

        if p.get_computable(ctx.schema):
            col_name = short_name.name + '_id'
            kind = context.ColumnComputable(pointer=p)
        elif short_name.name == '__type__':
            col_name = '__type__'
            kind = context.ColumnByName(reference_as='__type__')
        else:
            col_name = short_name.name + '_id'
            _, dbname = pgcommon.get_backend_name(ctx.schema, p, catenate=False)
            kind = context.ColumnByName(reference_as=dbname)

    return context.Column(name=col_name, kind=kind)


def _construct_system_columns() -> List[context.Column]:
    return [
        context.Column(
            name=c, kind=context.ColumnByName(reference_as=c), hidden=True
        )
        for c in ['tableoid', 'xmin', 'cmin', 'xmax', 'cmax', 'ctid']
    ]


def _compile_read_of_obj_table(
    obj: s_objtypes.ObjectType,
    include_inherited: bool,
    table: context.Table,
    ctx: Context,
) -> pgast.Relation:
    from edb.edgeql import ast as qlast
    from edb.edgeql import compiler as qlcompiler
    from edb.ir import ast as irast
    from edb.pgsql import compiler as pgcompiler
    from edb.pgsql.compiler import enums as pgce

    obj_name: sn.QualName = obj.get_name(ctx.schema)
    assert obj_name
    ql_stmt = qlast.SelectQuery(
        result=qlast.Path(
            steps=[qlast.ObjectRef(module=obj_name.module, name=obj_name.name)]
        )
    )

    if not include_inherited:
        ql_stmt.where = qlast.BinOp(
            left=qlast.Path(
                partial=True,
                steps=[qlast.Ptr(name='__type__'), qlast.Ptr(name='id')],
            ),
            op='=',
            right=qlast.TypeCast(
                expr=qlast.Constant.string(str(obj.id)),
                type=qlast.TypeName(maintype=qlast.ObjectRef(name='uuid')),
            ),
        )

    ir_stmt = qlcompiler.compile_ast_to_ir(
        ql_stmt,
        ctx.schema,
        options=qlcompiler.CompilerOptions(apply_user_access_policies=True),
    )

    sql_tree = pgcompiler.compile_ir_to_sql_tree(
        ir_stmt,
        output_format=pgcompiler.OutputFormat.NATIVE_INTERNAL,
        alias_generator=ctx.alias_generator,
    )
    command.merge_params(sql_tree, ir_stmt, ctx)

    # add CTEs to resolver's CTE buffer
    assert isinstance(sql_tree.ast, pgast.Query)
    if sql_tree.ast.ctes:
        ctx.ctes_buffer.extend(sql_tree.ast.ctes)
        sql_tree.ast.ctes.clear()

    SYSTEM_COLS = {'tableoid', 'xmin', 'cmin', 'xmax', 'cmax', 'ctid'}

    # pull all expected columns out of the result rvar
    obj_id = irast.PathId.from_type(ctx.schema, obj, env=None)
    for column in table.columns:
        if not isinstance(column.kind, context.ColumnByName):
            continue

        if column.kind.reference_as in {'id', 'source', 'target', '__type__'}:
            ptr = obj.getptr(
                ctx.schema, sn.UnqualName.from_string(column.kind.reference_as)
            )
            ptr_id = irast.PathId.from_pointer(ctx.schema, ptr, env=None)
        elif column.kind.reference_as in SYSTEM_COLS:
            el_name = sn.QualName('__object__', column.kind.reference_as)
            ptr_ref = irast.SpecialPointerRef(
                name=el_name,
                shortname=el_name,
                out_source=obj_id.target,
                out_target=pgtypes.pg_oid_typeref,
                out_cardinality=qltypes.Cardinality.AT_MOST_ONE,
            )
            ptr_id = obj_id.extend(ptrref=ptr_ref)
        else:
            ptr = ctx.schema.get_by_id(
                uuid.UUID(column.kind.reference_as), type=s_pointers.Pointer
            )
            ptr_id = irast.PathId.from_pointer(ctx.schema, ptr, env=None)

        output = pgcompiler.pathctx.get_path_output(
            sql_tree.ast,
            ptr_id,
            aspect=pgce.PathAspect.VALUE,
            env=sql_tree.env,
        )
        assert isinstance(output, pgast.ColumnRef)
        assert isinstance(output.name[-1], str)

        # override how this column will be referenced as
        column.kind.reference_as = output.name[-1]

    cte_name = ctx.alias_generator.get('tbl')
    ctx.ctes_buffer.append(
        pgast.CommonTableExpr(
            name=cte_name,
            query=sql_tree.ast,
        )
    )
    return pgast.Relation(name=cte_name)
