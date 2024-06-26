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

from edb.server.pgcon import errors as pgerror

from edb import errors
from edb.pgsql import ast as pgast
from edb.pgsql import compiler as pgcompiler

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler

from edb.ir import ast as irast
from edb.ir import typeutils as irtypeutils

from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import name as sn


from . import dispatch
from . import context
from . import expr as pg_res_expr
from . import relation as pg_res_rel
from . import range_var as pg_res_range_var

Context = context.ResolverContextLevel


@dispatch._resolve.register
def resolve_CopyStmt(stmt: pgast.CopyStmt, *, ctx: Context) -> pgast.CopyStmt:

    query: Optional[pgast.Query]

    if stmt.query:
        query = dispatch.resolve(stmt.query, ctx=ctx)

    elif stmt.relation:
        relation, table = dispatch.resolve_relation(stmt.relation, ctx=ctx)
        table.reference_as = ctx.alias_generator.get('rel')

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

    if ctx.subquery_depth >= 2:
        print(stmt.span)
        raise errors.QueryError(
            'WITH clause containing a data-modifying statement must be at '
            'the top level',
            span=stmt.span,
            pgext_code=pgerror.ERROR_FEATURE_NOT_SUPPORTED,
        )

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
        raise errors.QueryError(
            'DML supported for object type tables only',
            span=stmt.span,
            pgext_code=pgerror.ERROR_FEATURE_NOT_SUPPORTED,
        )

    expected_columns = _pull_columns_from_table(
        sub_table,
        ((c.name, c.span) for c in stmt.cols) if stmt.cols else None,
    )

    val_rel, val_table = compile_insert_value(
        stmt.select_stmt, stmt.ctes, expected_columns, ctx
    )
    value_ctes = val_rel.ctes if val_rel.ctes else []
    val_rel.ctes = None

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

    source_ql: qlast.PathElement = (
        qlast.IRAnchor(name=source_name)
        if is_single_row
        else qlast.ObjectRef(name=iterator_name)
    )

    pre_projection: List[pgast.ResTarget] = []
    source_outputs: Dict[Tuple[irast.PathId, str], pgast.OutputVar] = {}
    insert_shape = []
    for expected_col, val_col in zip(expected_columns, val_table.columns):
        ptr, ptr_name, is_link = get_pointer_for_column(expected_col, sub, ctx)

        # prepare pre-projection of this pointer value
        val_col_pg = pg_res_expr.resolve_column_kind(
            val_table, val_col.kind, ctx=ctx
        )
        if is_link:
            val_col_pg = pgast.TypeCast(
                arg=val_col_pg, type_name=pgast.TypeName(name=('uuid',))
            )
        pre_projection.append(pgast.ResTarget(name=ptr_name, val=val_col_pg))

        # prepare the outputs of the source CTE
        ptr_id = get_ptr_id(source_id, ptr, ctx)
        output_var: pgast.OutputVar = pgast.ColumnRef(
            name=(ptr_name,), nullable=True
        )
        if is_link:
            source_outputs[(ptr_id, 'identity')] = output_var
            source_outputs[(ptr_id, 'value')] = output_var
        else:
            source_outputs[(ptr_id, 'value')] = output_var

        # prepare insert shape that will use the paths from source_outputs
        insert_shape.append(
            construct_insert_element_for_ptr(
                source_ql,
                ptr_name,
                ptr,
                is_link,
                ctx,
            )
        )

    # construct the CTE that produces the value to be inserted
    # The original value query must be wrapped so we can add type casts
    # for link ids.
    assert isinstance(val_rel, pgast.Query)
    source_cte = pgast.CommonTableExpr(
        name=ctx.alias_generator.get('ins_source'),
        query=pgast.SelectStmt(
            from_clause=[pgast.RangeSubselect(subquery=val_rel)],
            target_list=pre_projection,
            path_outputs=source_outputs,
        ),
    )

    # source needs an identity column, so we need to invent one
    source_identity = ctx.alias_generator.get('identity')
    source_cte.query.target_list.append(
        pgast.ResTarget(
            name=source_identity,
            val=pgast.FuncCall(
                name=(
                    'edgedb',
                    'uuid_generate_v4',
                ),
                args=(),
            ),
        )
    )
    output_var = pgast.ColumnRef(name=(source_identity,))
    source_cte.query.path_outputs[(source_id, 'identity')] = output_var
    source_cte.query.path_outputs[(source_id, 'iterator')] = output_var
    source_cte.query.path_outputs[(source_id, 'value')] = output_var

    # the core thing
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

    subject_pointers: List[Tuple[str, str]] = []
    if stmt.returning_list:
        # wrap into a select shape that selects all pointers
        # (because they might be be used by RETURNING clause)
        select_shape: List[qlast.ShapeElement] = []
        for column in sub_table.columns:
            if column.hidden:
                continue

            ptr, ptr_name, is_link = get_pointer_for_column(column, sub, ctx)
            select_shape.append(
                qlast.ShapeElement(
                    expr=qlast.Path(steps=[qlast.Ptr(name=ptr_name)]),
                )
            )
            subject_pointers.append((column.name, ptr_name))

        ql_stmt = qlast.SelectQuery(
            result=qlast.Shape(expr=ql_stmt, elements=select_shape)
        )

    ir_stmt: irast.Statement
    try:
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
        assert isinstance(ir_stmt.expr, irast.SetE)
        sql_result = pgcompiler.compile_ir_to_sql_tree(
            ir_stmt,
            external_rels={source_id: (source_cte, ('source', 'identity'))},
            output_format=pgcompiler.OutputFormat.NATIVE_INTERNAL,
            alias_generator=ctx.alias_generator,
        )
    except errors.QueryError as e:
        raise errors.QueryError(
            msg=e.args[0],
            span=stmt.span,
            # not sure if this is ok, but it is better than InternalServerError,
            # which is the default
            pgext_code=pgerror.ERROR_DATA_EXCEPTION,
        )

    assert isinstance(sql_result.ast, pgast.Query)
    assert sql_result.ast.ctes
    ctes = value_ctes + [source_cte] + sql_result.ast.ctes
    sql_result.ast.ctes.clear()

    if ctx.subquery_depth == 0:
        # this is top-level, this SELECT must contain all CTEs
        sql_result.ast.ctes = ctes
    elif ctx.subquery_depth == 1:
        # parent is top-level, add CTEs to it
        ctx.ctes_buffer.extend(ctes)
    else:
        # this case is caught earlier
        raise AssertionError()

    result_table = context.Table(
        alias=stmt.relation.alias.aliasname, columns=[]
    )
    result_query = sql_result.ast

    if stmt.returning_list:
        return returning_rows(
            stmt.returning_list,
            subject_pointers,
            result_query,
            result_table,
            ctx,
        )
    else:
        result_query.target_list.clear()
        return result_query, result_table


def compile_insert_value(
    value_query: Optional[pgast.Query],
    value_ctes: Optional[List[pgast.CommonTableExpr]],
    expected_columns: List[context.Column],
    ctx: context.ResolverContextLevel,
) -> Tuple[pgast.BaseRelation, context.Table]:
    # VALUES (DEFAULT)
    if isinstance(value_query, pgast.SelectStmt) and value_query.values:
        # find DEFAULT keywords in VALUES

        def is_default(e: pgast.BaseExpr) -> bool:
            return isinstance(e, pgast.Keyword) and e.name == 'DEFAULT'

        default_columns = set()
        for row in value_query.values:
            assert isinstance(row, pgast.ImplicitRowExpr)

            for to_remove, col in enumerate(row.args):
                if is_default(col):
                    default_columns.add(to_remove)

        # remove DEFAULT keywords and expected columns,
        # so EdgeQL insert will not get those columns, which will use the
        # property defaults.
        for to_remove in sorted(default_columns, reverse=True):
            del expected_columns[to_remove]

            for r_index, row in enumerate(value_query.values):
                assert isinstance(row, pgast.ImplicitRowExpr)

                if not is_default(row.args[to_remove]):
                    raise errors.QueryError(
                        'DEFAULT keyword is supported only when '
                        'used for a column in all rows',
                        span=value_query.span,
                        pgext_code=pgerror.ERROR_FEATURE_NOT_SUPPORTED,
                    )
                cols = list(row.args)
                del cols[to_remove]
                value_query.values[r_index] = row.replace(args=cols)

    # INSERT INTO x DEFAULT VALUES
    value_query: pgast.BaseRelation
    if not value_query:
        value_query = pgast.SelectStmt(values=[])
        # edgeql compiler will provide default values
        # (and complain about missing ones)
        expected_columns = []

    # compile these CTEs as they were defined on value relation
    value_query.ctes = value_ctes

    # compile value that is to be inserted
    val_rel, val_table = dispatch.resolve_relation(value_query, ctx=ctx)

    if len(expected_columns) != len(val_table.columns):
        col_names = ', '.join(c.name for c in expected_columns)
        raise errors.QueryError(
            f'INSERT expected {len(expected_columns)} columns, '
            f'but got {len(val_table.columns)} (expecting {col_names})',
            span=value_query.span,
        )

    return val_rel, val_table


def returning_rows(
    returning_list: List[pgast.ResTarget],
    subject_pointers: List[Tuple[str, str]],
    inserted_query: pgast.Query,
    inserted_table: context.Table,
    ctx: context.ResolverContextLevel,
) -> Tuple[pgast.Query, context.Table]:
    # extract pointers to be used in returning columns

    # compiler output of an insert produces a SELECT whose target list is
    # [ROW(...)] that contains all pointers.
    # This is really inconvenient to use, so I'm discarding it here.
    # I'm not sure it will always have this form (or why it has it).
    assert len(inserted_query.target_list) == 1
    assert isinstance(inserted_query.target_list[0].val, pgast.ImplicitRowExpr)
    inserted_query.target_list.clear()

    # prepare a map from pointer name into pgast
    ptr_map: Dict[Tuple[str, str], pgast.BaseExpr] = {}
    for (ptr_id, aspect), output_var in inserted_query.path_namespace.items():
        qual_name = ptr_id.rptr_name()
        if not qual_name:
            continue
        ptr_map[qual_name.name, aspect] = output_var

    for col_name, ptr_name in subject_pointers:
        val = ptr_map.get((ptr_name, 'serialized'), None)
        if not val:
            val = ptr_map.get((ptr_name, 'value'), None)
        assert val, 'ptr was in the shape, but is not in path_namespace'

        inserted_query.target_list.append(
            pgast.ResTarget(
                name=col_name,
                val=val,
            )
        )
        inserted_table.columns.append(
            context.Column(
                name=col_name,
                kind=context.ColumnByName(reference_as=col_name),
            )
        )

    with ctx.empty() as sctx:
        sctx.scope.tables.append(inserted_table)

        tmp_ctes = inserted_query.ctes
        inserted_query.ctes = None
        inserted_query = pgast.SelectStmt(
            from_clause=[
                pgast.RangeSubselect(
                    subquery=inserted_query,
                )
            ],
            target_list=[],
        )
        inserted_query.ctes = tmp_ctes
        inserted_table = context.Table()

        for t in returning_list:
            targets, columns = pg_res_expr.resolve_ResTarget(t, ctx=sctx)
            inserted_query.target_list.extend(targets)
            inserted_table.columns.extend(columns)
    return inserted_query, inserted_table


def construct_insert_element_for_ptr(
    source_ql: qlast.PathElement,
    ptr_name: str,
    ptr: s_pointers.Pointer,
    is_link: bool,
    ctx: context.ResolverContextLevel,
):
    ptr_ql: qlast.Expr = qlast.Path(
        steps=[
            source_ql,
            qlast.Ptr(name=ptr_name),
        ]
    )
    if is_link:
        # add .id for links, which will figure out that it has uuid type.
        # This will make type cast to the object type into "find_by_id".
        assert isinstance(ptr_ql, qlast.Path)
        ptr_ql.steps.append(qlast.Ptr(name='id'))

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
    return qlast.ShapeElement(
        expr=qlast.Path(steps=[qlast.Ptr(name=ptr_name)]),
        operation=qlast.ShapeOperation(op=qlast.ShapeOp.ASSIGN),
        compexpr=ptr_ql,
    )


def get_pointer_for_column(
    col: context.Column,
    subject_stype: s_objtypes.ObjectType,
    ctx: context.ResolverContextLevel,
) -> Tuple[s_pointers.Pointer, str, bool]:
    is_link = False
    if col.name.endswith('_id'):
        # this if prevents *properties* that and with _id
        # I'm not sure if this is a problem
        ptr_name = col.name[0:-3]
        is_link = True
    else:
        ptr_name = col.name

    ptr = subject_stype.getptr(ctx.schema, sn.UnqualName(ptr_name))

    return ptr, ptr_name, is_link


def get_ptr_id(
    source_id: irast.PathId,
    ptr: s_pointers.Pointer,
    ctx: context.ResolverContextLevel,
) -> irast.PathId:
    ptrref = irtypeutils.ptrref_from_ptrcls(
        schema=ctx.schema, ptrcls=ptr, cache=None, typeref_cache=None
    )
    return source_id.extend(ptrref=ptrref)


@dispatch._resolve_relation.register
def resolve_DMLQuery(
    query: pgast.DMLQuery, *, include_inherited: bool, ctx: Context
) -> Tuple[pgast.DMLQuery, context.Table]:
    raise errors.QueryError(
        'DML queries (UPDATE/DELETE) are not supported',
        span=query.span,
        pgext_code=pgerror.ERROR_FEATURE_NOT_SUPPORTED,
    )
