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

from typing import List, Optional, Dict, Tuple

from edb import errors
from edb.pgsql import ast as pgast
from edb.pgsql import compiler as pgcompiler

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler

from edb.ir import ast as irast

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

        if stmt.colnames:
            col_map: Dict[str, context.Column] = {
                col.name: col for col in table.columns
            }
            # TODO: handle error from unknown column name
            selected_columns = (col_map[name] for name in stmt.colnames)
        else:
            selected_columns = (c for c in table.columns if not c.hidden)

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

    expected_columns: List[context.Column]
    if stmt.cols:
        col_map: Dict[str, context.Column] = {
            col.name: col for col in sub_table.columns
        }
        # TODO: handle error from unknown column name
        expected_columns = [
            col_map[ins_target.name] for ins_target in stmt.cols
        ]
    else:
        expected_columns = [c for c in sub_table.columns if not c.hidden]

    # compile value that is to be inserted normally
    assert stmt.select_stmt  # TODO: INSERT DEFAULT VALUES
    val_rel, val_table = dispatch.resolve_relation(stmt.select_stmt, ctx=ctx)

    if len(expected_columns) != len(val_table.columns):
        raise errors.QueryError(
            f'INSERT expected {len(expected_columns)} columns, '
            'but got {len(val_table.columns)}',
            span=stmt.select_stmt.span,
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

    # prepare anchors for inserted value columns
    anchor_name = '__sql_source__'
    sub_id = irast.PathId.from_type(
        ctx.schema,
        sub,
        typename=sn.QualName('__derived__', anchor_name),
        env=None,
    )

    insert_shape = []
    for expected_col, val_col in zip(expected_columns, val_table.columns):
        # TODO: handle pointer not existing
        # TODO: handle link_ids
        ptr = sub.getptr(ctx.schema, sn.UnqualName(expected_col.name))
        ptr_id = irast.PathId.from_pointer(ctx.schema, ptr, env=None)

        insert_shape.append(
            qlast.ShapeElement(
                expr=qlast.Path(steps=[qlast.Ptr(name=expected_col.name)]),
                compexpr=qlast.Path(
                    steps=[
                        qlast.IRAnchor(name=anchor_name),
                        qlast.Ptr(name=expected_col.name),
                    ]
                ),
            )
        )

        # prepare pg node that provides value for this pointer
        val_col_pg = pg_res_expr.resolve_column_kind(
            source_cte_table, val_col.kind, ctx=ctx
        )
        source_cte.query.path_outputs[(ptr_id, 'value')] = pgast.ExprOutputVar(
            expr=val_col_pg
        )

    ql_stmt = qlast.InsertQuery(
        subject=qlast.ObjectRef(
            name=sub_name.name,
            module=sub_name.module,
        ),
        shape=insert_shape,
    )

    # compile synthetic ql statement into SQL
    options = qlcompiler.CompilerOptions(
        modaliases={None: 'default'},
        make_globals_empty=True,  # TODO: globals in SQL
        singletons={sub_id},
        anchors={'__sql_source__': sub_id},
    )
    ir_stmt = qlcompiler.compile_ast_to_ir(
        ql_stmt,
        schema=ctx.schema,
        options=options,
    )
    sql_result = pgcompiler.compile_ir_to_sql_tree(
        ir_stmt,
        external_rels={sub_id: (source_cte, ('source', 'identity'))},
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
