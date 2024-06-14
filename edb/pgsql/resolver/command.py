#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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

from typing import List, Optional, Dict, Tuple, Iterable

from edb import errors
from edb.pgsql import ast as pgast
from edb.pgsql import compiler as pgcompiler

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler

from edb.ir import ast as irast
from edb.ir import typeutils as irtypeutils

from edb.schema import objtypes as s_objtypes
from edb.schema import name as sn

from edb.server.pgcon import errors as pgerror

from . import dispatch
from . import context
from . import expr as pg_res_expr
from . import relation as pg_res_rel

Context = context.ResolverContextLevel


@dispatch._resolve.register
def resolve_CopyStmt(stmt: pgast.CopyStmt, *, ctx: Context) -> pgast.CopyStmt:

    query: Optional[pgast.Query]

    if stmt.query:
        query = dispatch.resolve(stmt.query, ctx=ctx)

    elif stmt.relation:
        relation, table = dispatch.resolve_relation(stmt.relation, ctx=ctx)
        table.reference_as = ctx.names.get('rel')

        selected_columns = _pull_columns_from_table(
            table,
            ((c, stmt.span) for c in stmt.colnames) if stmt.colnames else None,
        )

        # The relation being copied is potentially a view and views cannot be
        # copied if referenced by name, so we just always wrap it into a SELECT.

        # This is probably a view based on edgedb schema, so wrap it into
        # a select query.
        query = pgast.SelectStmt(
            from_clause=[
                pgast.RelRangeVar(
                    alias=pgast.Alias(aliasname=table.reference_as),
                    relation=relation,
                )
            ],
            target_list=[
                pgast.ResTarget(
                    val=pg_res_expr.resolve_column_kind(table, c.kind, ctx=ctx)
                )
                for c in selected_columns
            ],
        )
    else:
        raise AssertionError('CopyStmt must either have relation or query set')

    # WHERE
    where = dispatch.resolve_opt(stmt.where_clause, ctx=ctx)

    # COPY will always be top-level, so we must extract CTEs
    query.ctes = list(ctx.ctes_buffer)
    ctx.ctes_buffer = []

    return pgast.CopyStmt(
        relation=None,
        colnames=None,
        query=query,
        is_from=stmt.is_from,
        is_program=stmt.is_program,
        filename=stmt.filename,
        # TODO: forbid some options?
        options=stmt.options,
        where_clause=where,
    )


def _pull_columns_from_table(
    table: context.Table,
    col_names: Optional[Iterable[Tuple[str, pgast.Span | None]]],
) -> List[context.Column]:
    if not col_names:
        return [c for c in table.columns if not c.hidden]

    col_map: Dict[str, context.Column] = {
        col.name: col for col in table.columns
    }

    res = []
    for name, span in col_names:
        col = col_map.get(name, None)
        if not col:
            raise errors.QueryError(
                f'column {name} does not exist',
                span=span,
            )
        res.append(col)
    return res


@dispatch._resolve_relation.register
def resolve_InsertStmt(
    stmt: pgast.InsertStmt, *, ctx: Context
) -> Tuple[pgast.Query, context.Table]:

    # determine the subject object we are inserting into
    assert isinstance(stmt.relation, pgast.RelRangeVar)
    assert isinstance(stmt.relation.relation, pgast.Relation)
    _sub_rel, sub_table = pg_res_rel.resolve_relation(
        stmt.relation.relation, ctx=ctx
    )
    assert sub_table.schema_id
    sub = ctx.schema.get_by_id(sub_table.schema_id)
    sub_name = sub.get_name(ctx.schema)
    assert isinstance(sub_name, sn.QualName)

    if not isinstance(sub, s_objtypes.ObjectType):
        raise NotImplementedError('DML supported for object type tables only')

    expected_columns = _pull_columns_from_table(
        sub_table,
        ((c.name, c.span) for c in stmt.cols) if stmt.cols else None,
    )

    # compile value that is to be inserted normally
    val_rel: pgast.BaseRelation
    if stmt.select_stmt:
        val_rel = stmt.select_stmt
    else:
        # INSERT INTO x DEFAULT VALUES
        val_rel = pgast.SelectStmt(values=[])

        # edgeql compiler will provide default values
        # (and complain about missing ones)
        expected_columns = []
    val_rel, val_table = dispatch.resolve_relation(val_rel, ctx=ctx)

    if len(expected_columns) != len(val_table.columns):
        raise errors.QueryError(
            f'INSERT expected {len(expected_columns)} columns, '
            f'but got {len(val_table.columns)}',
            span=val_rel.span,
        )

    # construct the CTE that produces the value to be inserted
    assert isinstance(val_rel, pgast.Query)  # TODO: ensure query
    source_cte = pgast.CommonTableExpr(
        name=ctx.names.get('cte'),
        query=val_rel,  # TODO: ensure query
    )
    source_cte_table = context.Table(
        reference_as=source_cte.name,
        columns=list(val_table.columns),
    )

    # if we are sure that we are inserting a single row,
    # we can skip for loops and the iterator, so we generate better SQL
    is_single_row = isinstance(val_rel, pgast.SelectStmt) and (
        (val_rel.values and len(val_rel.values) == 1)
        or (
            isinstance(val_rel.limit_count, pgast.NumericConstant)
            and val_rel.limit_count.val == '1'
        )
    )

    # prepare anchors for inserted value columns
    source_name = '__sql_source__'
    iterator_name = '__sql_iterator__'
    source_id = irast.PathId.from_type(
        ctx.schema,
        sub,
        typename=sn.QualName('__derived__', source_name),
        env=None,
    )

    # source needs an identity column, so we need to invent one
    if isinstance(val_rel, pgast.SelectStmt) and not val_rel.values:
        source_identity = ctx.names.get('identity')
        val_rel.target_list.append(
            pgast.ResTarget(
                name=source_identity,
                val=pgast.FuncCall(
                    name=('row_number',), args=(), over=pgast.WindowDef()
                ),
            )
        )

        val_rel.path_outputs[(source_id, 'value')] = pgast.ColumnRef(
            name=(source_identity,)
        )

    insert_shape = []
    for expected_col, val_col in zip(expected_columns, val_table.columns):
        is_link = False
        if expected_col.name.endswith('_id'):
            # this if prevents *properties* that and with _id
            # I'm not sure if this is a problem
            ptr_name = expected_col.name[0:-3]
            is_link = True
        else:
            ptr_name = expected_col.name

        # TODO: handle link_ids
        ptr = sub.getptr(ctx.schema, sn.UnqualName(ptr_name))

        ptrref = irtypeutils.ptrref_from_ptrcls(
            schema=ctx.schema, ptrcls=ptr, cache=None, typeref_cache=None
        )
        ptr_id = source_id.extend(ptrref=ptrref)

        ptr_ql: qlast.Expr = qlast.Path(
            steps=[
                (
                    qlast.IRAnchor(name=source_name)
                    if is_single_row
                    else qlast.ObjectRef(name=iterator_name)
                ),
                qlast.Ptr(name=ptr_name),
            ]
        )
        if is_link:
            ptr_target = ptr.get_target(ctx.schema)
            assert ptr_target
            ptr_target_name: sn.Name = ptr_target.get_name(ctx.schema)
            assert isinstance(ptr_target_name, sn.QualName)
            ptr_ql = qlast.TypeCast(
                type=qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        module=ptr_target_name.module,
                        name=ptr_target_name.name,
                    )
                ),
                expr=ptr_ql,
            )
        insert_shape.append(
            qlast.ShapeElement(
                expr=qlast.Path(steps=[qlast.Ptr(name=ptr_name)]),
                compexpr=ptr_ql,
            )
        )

        # prepare pg node that provides value for this pointer
        val_col_pg = pg_res_expr.resolve_column_kind(
            source_cte_table, val_col.kind, ctx=ctx
        )
        
        # TODO: an exhaustive consideration if this assertion is actually true
        assert isinstance(val_col_pg, pgast.ColumnRef)

        if is_link:
            # val_col_pg = pgast.TypeCast(
            #     arg=val_col_pg, type_name=pgast.TypeName(name=('uuid',))
            # )
            #   pgast.ExprOutputVar(expr=val_col_pg)
            val_rel.path_outputs[(ptr_id, 'identity')] = val_col_pg
            val_rel.path_outputs[(ptr_id, 'value')] = val_col_pg
        else:
            val_rel.path_outputs[(ptr_id, 'value')] = val_col_pg

    ql_stmt: qlast.Expr = qlast.InsertQuery(
        subject=qlast.ObjectRef(
            name=sub_name.name,
            module=sub_name.module,
        ),
        shape=insert_shape,
    )
    if not is_single_row:
        ql_stmt = qlast.ForQuery(
            iterator=qlast.Path(steps=[qlast.IRAnchor(name=source_name)]),
            iterator_alias=iterator_name,
            result=ql_stmt,
        )

    # compile synthetic ql statement into SQL
    options = qlcompiler.CompilerOptions(
        modaliases={None: 'default'},
        make_globals_empty=True,  # TODO: globals in SQL
        singletons={source_id},
        anchors={'__sql_source__': source_id},
        allow_user_specified_id=True,  # TODO: should this be enabled?
    )
    ir_stmt = qlcompiler.compile_ast_to_ir(
        ql_stmt,
        schema=ctx.schema,
        options=options,
    )
    sql_result = pgcompiler.compile_ir_to_sql_tree(
        ir_stmt,
        external_rels={source_id: (source_cte, ('source', 'identity'))},
        output_format=pgcompiler.OutputFormat.NATIVE_INTERNAL,
    )
    assert isinstance(sql_result.ast, pgast.Query)

    # inject the value CTE
    assert sql_result.ast.ctes
    sql_result.ast.ctes.insert(0, source_cte)

    result_table = context.Table(columns=[])  # TODO

    return sql_result.ast, result_table


@dispatch._resolve_relation.register
def resolve_DMLQuery(
    query: pgast.DMLQuery, *, include_inherited: bool, ctx: Context
) -> Tuple[pgast.DMLQuery, context.Table]:
    raise errors.QueryError(
        'DML queries (UPDATE/DELETE) are not supported',
        span=query.span,
        pgext_code=pgerror.ERROR_FEATURE_NOT_SUPPORTED,
    )
