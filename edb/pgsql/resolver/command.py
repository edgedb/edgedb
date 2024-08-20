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

from typing import List, Optional, Dict, Tuple, Iterable, Mapping, Set
import dataclasses
import functools

from edb.server.pgcon import errors as pgerror

from edb import errors
from edb.pgsql import ast as pgast
from edb.pgsql import compiler as pgcompiler
from edb.pgsql.compiler import enums as pgce

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import compiler as qlcompiler

from edb.ir import ast as irast
from edb.ir import typeutils as irtypeutils

from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import links as s_links
from edb.schema import properties as s_properties
from edb.schema import name as sn
from edb.schema import utils as s_utils


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
    ctx.ctes_buffer.clear()

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


def compile_dml(
    stmt: pgast.Base, *, ctx: Context
) -> List[pgast.CommonTableExpr]:
    # extract all dml stmts
    dml_stmts_sql = _collect_dml_stmts(stmt)
    if len(dml_stmts_sql) == 0:
        return []

    # un-compile each SQL dml stmt into EdgeQL
    stmts = [_uncompile_dml_stmt(s, ctx=ctx) for s in dml_stmts_sql]

    # merge EdgeQL stmts & compile to SQL
    ctx.compiled_dml, ctes = _compile_uncompiled_dml(stmts, ctx=ctx)

    return ctes


def _collect_dml_stmts(stmt: pgast.Base) -> List[pgast.DMLQuery]:
    if not isinstance(stmt, pgast.Query):
        return []

    # DML can only be in the top-level statement or its CTEs.
    # If it is in any of the nested CTEs, throw errors later on
    res: List[pgast.DMLQuery] = []
    if stmt.ctes:
        for cte in stmt.ctes:
            if isinstance(cte.query, pgast.DMLQuery):
                res.append(cte.query)

    if isinstance(stmt, pgast.DMLQuery):
        res.append(stmt)
    return res


@dataclasses.dataclass(kw_only=True, eq=False, repr=False)
class UncompiledDML:
    # the input DML node
    input: pgast.Query

    # schema object associated with the table that is the subject of the DML
    subject: s_objtypes.ObjectType | s_links.Link | s_properties.Property

    # EdgeQL equivalent to the input node
    ql_stmt: qlast.Expr

    # additional params needed during compilation of the edgeql node
    ql_returning_shape: List[qlast.ShapeElement]
    ql_singletons: Set[irast.PathId]
    ql_anchors: Mapping[str, irast.PathId]
    external_rels: Mapping[
        irast.PathId, Tuple[pgast.BaseRelation, Tuple[str, ...]]
    ]

    # list of column names of the subject type, along with pointer name
    # these columns will be available within RETURNING clause
    subject_columns: List[Tuple[str, str]]

    # data needed for stitching the compiled ast into the resolver output
    early_result: context.CompiledDML


@functools.singledispatch
def _uncompile_dml_stmt(stmt: pgast.DMLQuery, *, ctx: Context):
    """
    Takes an SQL DML query and produces an equivalent EdgeQL query plus a bunch
    of metadata needed to extract associated CTEs from result of the EdgeQL
    compiler.

    In this context:
    - subject is the object type/pointer being updated,
    - source is the source of the subject (when subject is a pointer),
    - value is the relation that provides new value to be inserted/updated,
    - ptr-s are (usually) pointers on the subject.
    """

    raise errors.QueryError(
        f'{stmt.__class__.__name__} are not supported',
        span=stmt.span,
        pgext_code=pgerror.ERROR_FEATURE_NOT_SUPPORTED,
    )


def _uncompile_dml_subject(
    rvar: pgast.RelRangeVar, *, ctx: Context
) -> Tuple[
    context.Table, s_objtypes.ObjectType | s_links.Link | s_properties.Property
]:
    """
    Determines the subject object of a DML operation.
    This can either be an ObjectType or a Pointer for link tables.
    """

    assert isinstance(rvar.relation, pgast.Relation)
    _sub_rel, sub_table = pg_res_rel.resolve_relation(
        rvar.relation, include_inherited=False, ctx=ctx
    )
    if not sub_table.schema_id:
        # this happens doing DML on an introspection table or (somehow) a CTE
        raise errors.QueryError(
            msg=f'cannot write into table "{sub_table.name}"',
            pgext_code=pgerror.ERROR_UNDEFINED_TABLE,
        )

    sub = ctx.schema.get_by_id(sub_table.schema_id)
    assert isinstance(
        sub, (s_objtypes.ObjectType, s_links.Link, s_properties.Property)
    )
    return sub_table, sub


def _uncompile_subject_columns(
    sub: s_objtypes.ObjectType | s_links.Link | s_properties.Property,
    sub_table: context.Table,
    res: UncompiledDML,
    *,
    ctx: Context,
):
    '''
    Instruct UncompiledDML to wrap the EdgeQL DML into a select shape that
    selects all pointers.
    This is applied when a RETURNING clause is present and these columns might
    be used in the clause.
    '''
    for column in sub_table.columns:
        if column.hidden:
            continue
        _, ptr_name, _ = _get_pointer_for_column(column, sub, ctx)
        res.subject_columns.append((column.name, ptr_name))


@_uncompile_dml_stmt.register
def _uncompile_insert_stmt(
    stmt: pgast.InsertStmt, *, ctx: Context
) -> UncompiledDML:
    # determine the subject object
    sub_table, sub = _uncompile_dml_subject(stmt.relation, ctx=ctx)

    expected_columns = _pull_columns_from_table(
        sub_table,
        ((c.name, c.span) for c in stmt.cols) if stmt.cols else None,
    )

    res: UncompiledDML
    if isinstance(sub, s_objtypes.ObjectType):
        res = _uncompile_insert_object_stmt(
            stmt, sub, sub_table, expected_columns, ctx=ctx
        )
    elif isinstance(sub, (s_links.Link, s_properties.Property)):
        res = _uncompile_insert_pointer_stmt(
            stmt, sub, sub_table, expected_columns, ctx=ctx
        )
    else:
        raise NotImplementedError()

    if stmt.returning_list:
        _uncompile_subject_columns(sub, sub_table, res, ctx=ctx)
    return res


def _uncompile_insert_object_stmt(
    stmt: pgast.InsertStmt,
    sub: s_objtypes.ObjectType,
    sub_table: context.Table,
    expected_columns: List[context.Column],
    *,
    ctx: Context,
) -> UncompiledDML:
    """
    Translates a 'SQL INSERT into an object type table' to an EdgeQL insert.
    """

    # handle DEFAULT and prepare the value relation
    value_relation, expected_columns = _uncompile_default_value(
        stmt.select_stmt, stmt.ctes, expected_columns
    )

    # if we are sure that we are inserting a single row
    # we can skip for-loops and iterators, which produces better SQL
    is_value_single = _has_at_most_one_row(stmt.select_stmt)

    # prepare anchors for inserted value columns
    value_name = ctx.alias_generator.get('ins_val')
    iterator_name = ctx.alias_generator.get('ins_iter')
    value_id = irast.PathId.from_type(
        ctx.schema,
        sub,
        typename=sn.QualName('__derived__', value_name),
        env=None,
    )

    value_ql: qlast.PathElement = (
        qlast.IRAnchor(name=value_name)
        if is_value_single
        else qlast.ObjectRef(name=iterator_name)
    )

    # a phantom relation that is supposed to hold the inserted value
    # (in the resolver, this will be replaced by the real value relation)
    value_cte_name = ctx.alias_generator.get('ins_value')
    value_rel = pgast.Relation(name=value_cte_name)
    value_columns = []
    insert_shape = []
    for index, expected_col in enumerate(expected_columns):
        ptr, ptr_name, is_link = _get_pointer_for_column(expected_col, sub, ctx)
        value_columns.append((ptr_name, is_link))

        # inject type annotation into value relation
        if is_link:
            _try_inject_type_cast(
                value_relation, index, pgast.TypeName(name=('uuid',))
            )

        # prepare the outputs of the source CTE
        ptr_id = _get_ptr_id(value_id, ptr, ctx)
        output_var = pgast.ColumnRef(name=(ptr_name,), nullable=True)
        if is_link:
            value_rel.path_outputs[(ptr_id, pgce.PathAspect.IDENTITY)] = (
                output_var
            )
            value_rel.path_outputs[(ptr_id, pgce.PathAspect.VALUE)] = output_var
        else:
            value_rel.path_outputs[(ptr_id, pgce.PathAspect.VALUE)] = output_var

        # prepare insert shape that will use the paths from source_outputs
        insert_shape.append(
            _construct_assign_element_for_ptr(
                value_ql,
                ptr_name,
                ptr,
                is_link,
                ctx,
            )
        )

    # source needs an iterator column, so we need to invent one
    # Here we only decide on the name of that iterator column, the actual column
    # is generated later, when resolving the DML stmt.
    value_iterator = ctx.alias_generator.get('iter')
    output_var = pgast.ColumnRef(name=(value_iterator,))
    value_rel.path_outputs[(value_id, pgce.PathAspect.ITERATOR)] = output_var
    value_rel.path_outputs[(value_id, pgce.PathAspect.VALUE)] = output_var

    # construct the EdgeQL DML AST
    sub_name = sub.get_name(ctx.schema)
    ql_stmt: qlast.Expr = qlast.InsertQuery(
        subject=s_utils.name_to_ast_ref(sub_name),
        shape=insert_shape,
    )
    if not is_value_single:
        # value relation might contain multiple rows
        # to express this in EdgeQL, we must wrap `insert` into a `for` query
        ql_stmt = qlast.ForQuery(
            iterator=qlast.Path(steps=[qlast.IRAnchor(name=value_name)]),
            iterator_alias=iterator_name,
            result=ql_stmt,
        )

    ql_returning_shape: List[qlast.ShapeElement] = []
    if stmt.returning_list:
        # construct the shape that will extract all needed column of the subject
        # table (because they might be be used by RETURNING clause)
        for column in sub_table.columns:
            if column.hidden:
                continue
            _, ptr_name, _ = _get_pointer_for_column(column, sub, ctx)
            ql_returning_shape.append(
                qlast.ShapeElement(
                    expr=qlast.Path(steps=[qlast.Ptr(name=ptr_name)]),
                )
            )

    return UncompiledDML(
        input=stmt,
        subject=sub,
        ql_stmt=ql_stmt,
        ql_returning_shape=ql_returning_shape,
        ql_singletons={value_id},
        ql_anchors={value_name: value_id},
        external_rels={value_id: (value_rel, ('source', 'identity'))},
        early_result=context.CompiledDML(
            value_cte_name=value_cte_name,
            value_relation_input=value_relation,
            value_columns=value_columns,
            value_iterator_name=value_iterator,
            # these will be populated after compilation
            output_ctes=[],
            output_relation_name='',
            output_namespace={},
        ),
        # these will be populated by _uncompile_dml_stmt
        subject_columns=[],
    )


def _construct_assign_element_for_ptr(
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
        ptr_ql = qlast.TypeCast(
            type=qlast.TypeName(
                maintype=s_utils.name_to_ast_ref(ptr_target_name)
            ),
            expr=ptr_ql,
        )
    return qlast.ShapeElement(
        expr=qlast.Path(steps=[qlast.Ptr(name=ptr_name)]),
        operation=qlast.ShapeOperation(op=qlast.ShapeOp.ASSIGN),
        compexpr=ptr_ql,
    )


def _uncompile_insert_pointer_stmt(
    stmt: pgast.InsertStmt,
    sub: s_links.Link | s_properties.Property,
    sub_table: context.Table,
    expected_columns: List[context.Column],
    *,
    ctx: Context,
) -> UncompiledDML:
    """
    Translates a SQL 'INSERT INTO a link / multi-property table' into
    an `EdgeQL update SourceObject { subject: ... }`.
    """

    if not any(c.name == 'source' for c in expected_columns):
        raise errors.QueryError(
            'column source is required when inserting into link tables',
            span=stmt.span,
        )
    if not any(c.name == 'target' for c in expected_columns):
        raise errors.QueryError(
            'column target is required when inserting into link tables',
            span=stmt.span,
        )

    sub_source = sub.get_source(ctx.schema)
    assert isinstance(sub_source, s_objtypes.ObjectType)
    sub_target = sub.get_target(ctx.schema)
    assert sub_target

    # handle DEFAULT and prepare the value relation
    value_relation, expected_columns = _uncompile_default_value(
        stmt.select_stmt, stmt.ctes, expected_columns
    )

    # if we are sure that we are inserting a single row
    # we can skip for-loops and iterators, which produces better SQL
    is_value_single = _has_at_most_one_row(stmt.select_stmt)

    # prepare anchors for inserted value columns
    value_name = ctx.alias_generator.get('ins_val')
    iterator_name = ctx.alias_generator.get('ins_iter')
    source_id = irast.PathId.from_type(
        ctx.schema,
        sub_source,
        typename=sn.QualName('__derived__', value_name),
        env=None,
    )
    link_ref = irtypeutils.ptrref_from_ptrcls(
        schema=ctx.schema, ptrcls=sub, cache=None, typeref_cache=None
    )
    value_id: irast.PathId = source_id.extend(ptrref=link_ref)

    value_ql: qlast.PathElement = (
        qlast.IRAnchor(name=value_name)
        if is_value_single
        else qlast.ObjectRef(name=iterator_name)
    )

    # a phantom relation that is supposed to hold the inserted value
    # (in the resolver, this will be replaced by the real value relation)
    value_cte_name = ctx.alias_generator.get('ins_value')
    value_rel = pgast.Relation(name=value_cte_name)
    value_columns: List[Tuple[str, bool]] = []
    for index, expected_col in enumerate(expected_columns):
        if expected_col.name == 'source':
            ptr_name = 'source'
            is_link = True

            var = pgast.ColumnRef(name=(ptr_name,), nullable=True)
            value_rel.path_outputs[(source_id, pgce.PathAspect.VALUE)] = var
            value_rel.path_outputs[(source_id, pgce.PathAspect.IDENTITY)] = var
        elif expected_col.name == 'target':
            ptr_name = 'target'
            is_link = isinstance(sub, s_links.Link)

            ptr_id = value_id.tgt_path()
            var = pgast.ColumnRef(name=(ptr_name,), nullable=True)
            value_rel.path_outputs[(ptr_id, pgce.PathAspect.VALUE)] = var
            value_rel.path_outputs[(ptr_id, pgce.PathAspect.IDENTITY)] = var
        else:
            # link pointer

            assert isinstance(sub, s_links.Link)
            ptr_name = expected_col.name
            ptr = sub.maybe_get_ptr(ctx.schema, sn.UnqualName(ptr_name))
            assert ptr
            is_link = False

            ptr_id = _get_ptr_id(value_id.ptr_path(), ptr, ctx)
            var = pgast.ColumnRef(name=(ptr_name,), nullable=True)
            value_rel.path_outputs[(ptr_id, pgce.PathAspect.VALUE)] = var

        # inject type annotation into value relation
        if is_link:
            _try_inject_type_cast(
                value_relation, index, pgast.TypeName(name=('uuid',))
            )

        value_columns.append((ptr_name, is_link))

    # source needs an iterator column, so we need to invent one
    # Here we only decide on the name of that iterator column, the actual column
    # is generated later, when resolving the DML stmt.
    value_iterator = ctx.alias_generator.get('iter')
    var = pgast.ColumnRef(name=(value_iterator,))
    value_rel.path_outputs[(source_id, pgce.PathAspect.ITERATOR)] = var
    value_rel.path_outputs[(value_id, pgce.PathAspect.ITERATOR)] = var

    # construct the EdgeQL DML AST
    sub_name = sub.get_name(ctx.schema)
    sub_source_name = sub_source.get_name(ctx.schema)
    sub_target_name = sub_target.get_name(ctx.schema)

    sub_name = sub.get_shortname(ctx.schema)

    ql_sub_source_ref = s_utils.name_to_ast_ref(sub_source_name)
    ql_sub_target_ref = s_utils.name_to_ast_ref(sub_target_name)

    ql_ptr_val: qlast.Expr
    if isinstance(sub, s_links.Link):
        ql_ptr_val = qlast.Shape(
            expr=qlast.TypeCast(
                expr=qlast.Path(  # target
                    steps=[value_ql, qlast.Ptr(name=sub_name.name)]
                ),
                type=qlast.TypeName(maintype=ql_sub_target_ref),
            ),
            elements=[
                qlast.ShapeElement(
                    expr=qlast.Path(
                        steps=[qlast.Ptr(name=ptr_name, type='property')],
                    ),
                    compexpr=qlast.Path(
                        steps=[
                            value_ql,
                            qlast.Ptr(name=sub_name.name),
                            qlast.Ptr(name=ptr_name, type='property'),
                        ],
                    ),
                )
                for ptr_name, _ in value_columns
                if ptr_name not in ('source', 'target')
            ],
        )
    else:
        # multi pointer
        ql_ptr_val = qlast.Path(  # target
            steps=[value_ql, qlast.Ptr(name=sub_name.name)]
        )

    ql_stmt: qlast.Expr = qlast.UpdateQuery(
        subject=qlast.Path(steps=[ql_sub_source_ref]),
        where=qlast.BinOp(  # ObjectType == value.source
            left=qlast.Path(steps=[ql_sub_source_ref]),
            op='=',
            right=qlast.Path(steps=[value_ql]),
        ),
        shape=[
            qlast.ShapeElement(
                expr=qlast.Path(steps=[qlast.Ptr(name=sub_name.name)]),
                operation=(
                    qlast.ShapeOperation(op=qlast.ShapeOp.APPEND)
                    if (
                        sub.get_cardinality(ctx.schema)
                        == qltypes.SchemaCardinality.Many
                    )
                    else qlast.ShapeOperation(op=qlast.ShapeOp.ASSIGN)
                ),
                compexpr=ql_ptr_val,
            )
        ],
    )
    if not is_value_single:
        # value relation might contain multiple rows
        # to express this in EdgeQL, we must wrap `insert` into a `for` query
        ql_stmt = qlast.ForQuery(
            iterator=qlast.Path(steps=[qlast.IRAnchor(name=value_name)]),
            iterator_alias=iterator_name,
            result=ql_stmt,
        )
    # ql_stmt = qlast.Path(steps=[ql_stmt, qlast.Ptr(name=sub_name.name)])

    ql_returning_shape: List[qlast.ShapeElement] = []
    if stmt.returning_list:
        # construct the shape that will extract all needed column of the subject
        # table (because they might be be used by RETURNING clause)
        for column in sub_table.columns:
            if column.hidden:
                continue
            if column.name in ('source', 'target'):
                # no need to include in shape, they will be present anyway
                continue

            ql_returning_shape.append(
                qlast.ShapeElement(
                    expr=qlast.Path(steps=[qlast.Ptr(name=column.name)]),
                    compexpr=qlast.Path(
                        partial=True,
                        steps=[
                            qlast.Ptr(name=sub_name.name),
                            qlast.Ptr(name=column.name, type='property'),
                        ],
                    ),
                )
            )

    return UncompiledDML(
        input=stmt,
        subject=sub,
        ql_stmt=ql_stmt,
        ql_returning_shape=ql_returning_shape,
        ql_singletons={source_id},
        ql_anchors={value_name: source_id},
        external_rels={source_id: (value_rel, ('source', 'identity'))},
        early_result=context.CompiledDML(
            value_cte_name=value_cte_name,
            value_relation_input=value_relation,
            value_columns=value_columns,
            value_iterator_name=value_iterator,
            # these will be populated after compilation
            output_ctes=[],
            output_relation_name='',
            output_namespace={},
        ),
        # these will be populated by _uncompile_dml_stmt
        subject_columns=[],
    )


def _has_at_most_one_row(query: pgast.Query | None) -> bool:
    if not query:
        return True
    return isinstance(query, pgast.SelectStmt) and (
        (query.values and len(query.values) <= 1)
        or (
            isinstance(query.limit_count, pgast.NumericConstant)
            and query.limit_count.val == '1'
        )
    )


def _uncompile_default_value(
    value_query: Optional[pgast.Query],
    value_ctes: Optional[List[pgast.CommonTableExpr]],
    expected_columns: List[context.Column],
) -> Tuple[pgast.BaseRelation, List[context.Column]]:
    # INSERT INTO x DEFAULT VALUES
    if not value_query:
        value_query = pgast.SelectStmt(values=[])
        # edgeql compiler will provide default values
        # (and complain about missing ones)
        expected_columns = []
        return value_query, expected_columns

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

        if (
            len(value_query.values) > 0
            and isinstance(value_query.values[0], pgast.ImplicitRowExpr)
            and len(value_query.values[0].args) == 0
        ):
            # special case: `VALUES (), (), ..., ()`
            # This is syntactically incorrect, so we transform it into:
            # `SELECT FROM (VALUES (NULL), (NULL), ..., (NULL)) _`
            value_query = pgast.SelectStmt(
                target_list=[],
                from_clause=[
                    pgast.RangeSubselect(
                        subquery=pgast.SelectStmt(
                            values=[
                                pgast.ImplicitRowExpr(
                                    args=[pgast.NullConstant()]
                                )
                                for _ in value_query.values
                            ]
                        ),
                        alias=pgast.Alias(aliasname='_'),
                    )
                ],
            )

    # compile these CTEs as they were defined on value relation
    assert not value_query.ctes
    value_query.ctes = value_ctes

    return value_query, expected_columns


@_uncompile_dml_stmt.register
def _uncompile_delete_stmt(
    stmt: pgast.DeleteStmt, *, ctx: Context
) -> UncompiledDML:
    # determine the subject object
    sub_table, sub = _uncompile_dml_subject(stmt.relation, ctx=ctx)

    res: UncompiledDML
    if isinstance(sub, s_objtypes.ObjectType):
        res = _uncompile_delete_object_stmt(stmt, sub, sub_table, ctx=ctx)
    elif isinstance(sub, (s_links.Link, s_properties.Property)):
        res = _uncompile_delete_pointer_stmt(stmt, sub, sub_table, ctx=ctx)
    else:
        raise NotImplementedError()

    if stmt.returning_list:
        _uncompile_subject_columns(sub, sub_table, res, ctx=ctx)
    return res


def _uncompile_delete_object_stmt(
    stmt: pgast.DeleteStmt,
    sub: s_objtypes.ObjectType,
    sub_table: context.Table,
    *,
    ctx: Context,
) -> UncompiledDML:
    """
    Translates a 'SQL DELETE of object type table' to an EdgeQL delete.
    """

    # prepare value relation
    # For deletes, value relation contains a single column of ids of all the
    # objects that need to be deleted. We construct this relation from WHERE
    # and USING clauses of DELETE.

    assert isinstance(stmt.relation, pgast.RelRangeVar)
    val_sub_rvar = stmt.relation.alias.aliasname or stmt.relation.relation.name
    assert val_sub_rvar
    value_relation = pgast.SelectStmt(
        ctes=stmt.ctes,
        target_list=[
            pgast.ResTarget(
                val=pgast.ColumnRef(
                    name=(val_sub_rvar, 'id'),
                )
            )
        ],
        from_clause=[
            pgast.RelRangeVar(
                relation=stmt.relation.relation,
                alias=pgast.Alias(aliasname=val_sub_rvar),
            )
        ]
        + stmt.using_clause,
        where_clause=stmt.where_clause,
    )
    stmt.ctes = []

    # prepare anchors for inserted value columns
    value_name = ctx.alias_generator.get('del_val')
    value_id = irast.PathId.from_type(
        ctx.schema,
        sub,
        typename=sn.QualName('__derived__', value_name),
        env=None,
    )

    value_ql = qlast.IRAnchor(name=value_name)

    # a phantom relation that contains a single column, which is the id of all
    # the objects that should be deleted.
    value_cte_name = ctx.alias_generator.get('del_value')
    value_rel = pgast.Relation(name=value_cte_name)
    value_columns = [('id', False)]

    output_var = pgast.ColumnRef(name=('id',), nullable=False)
    value_rel.path_outputs[(value_id, pgce.PathAspect.IDENTITY)] = output_var
    value_rel.path_outputs[(value_id, pgce.PathAspect.VALUE)] = output_var
    value_rel.path_outputs[(value_id, pgce.PathAspect.ITERATOR)] = output_var

    # construct the EdgeQL DML AST
    sub_name = sub.get_name(ctx.schema)
    ql_stmt: qlast.Expr = qlast.DeleteQuery(
        subject=qlast.Path(steps=[s_utils.name_to_ast_ref(sub_name)]),
        where=qlast.BinOp(
            left=qlast.Path(partial=True, steps=[qlast.Ptr(name='id')]),
            op='IN',
            right=qlast.Path(steps=[value_ql, qlast.Ptr(name='id')]),
        ),
    )

    ql_returning_shape: List[qlast.ShapeElement] = []
    if stmt.returning_list:
        # construct the shape that will extract all needed column of the subject
        # table (because they might be be used by RETURNING clause)
        for column in sub_table.columns:
            if column.hidden:
                continue
            _, ptr_name, _ = _get_pointer_for_column(column, sub, ctx)
            ql_returning_shape.append(
                qlast.ShapeElement(
                    expr=qlast.Path(steps=[qlast.Ptr(name=ptr_name)]),
                )
            )

    return UncompiledDML(
        input=stmt,
        subject=sub,
        ql_stmt=ql_stmt,
        ql_returning_shape=ql_returning_shape,
        ql_singletons={value_id},
        ql_anchors={value_name: value_id},
        external_rels={value_id: (value_rel, ('source', 'identity'))},
        early_result=context.CompiledDML(
            value_cte_name=value_cte_name,
            value_relation_input=value_relation,
            value_columns=value_columns,
            value_iterator_name=None,
            # these will be populated after compilation
            output_ctes=[],
            output_relation_name='',
            output_namespace={},
        ),
        # these will be populated by _uncompile_dml_stmt
        subject_columns=[],
    )


def _uncompile_delete_pointer_stmt(
    stmt: pgast.DeleteStmt,
    sub: s_links.Link | s_properties.Property,
    sub_table: context.Table,
    *,
    ctx: Context,
) -> UncompiledDML:
    """
    Translates a SQL 'DELETE FROM a link / multi-property table' into
    an `EdgeQL update SourceObject { subject: ... }`.
    """

    sub_source = sub.get_source(ctx.schema)
    assert isinstance(sub_source, s_objtypes.ObjectType)
    sub_target = sub.get_target(ctx.schema)
    assert sub_target

    # prepare value relation
    # For link deletes, value relation contains two columns: source and target
    # of all links that need to be deleted. We construct this relation from
    # WHERE and USING clauses of DELETE.

    assert isinstance(stmt.relation, pgast.RelRangeVar)
    val_sub_rvar = stmt.relation.alias.aliasname or stmt.relation.relation.name
    assert val_sub_rvar
    value_relation = pgast.SelectStmt(
        ctes=stmt.ctes,
        target_list=[
            pgast.ResTarget(
                val=pgast.ColumnRef(
                    name=(val_sub_rvar, 'source'),
                )
            ),
            pgast.ResTarget(
                val=pgast.ColumnRef(
                    name=(val_sub_rvar, 'target'),
                )
            ),
        ],
        from_clause=[
            pgast.RelRangeVar(
                relation=stmt.relation.relation,
                alias=pgast.Alias(aliasname=val_sub_rvar),
            )
        ]
        + stmt.using_clause,
        where_clause=stmt.where_clause,
    )
    stmt.ctes = []

    # if we are sure that we are updating a single source object,
    # we can skip for-loops and iterators, which produces better SQL
    is_value_single = False

    # prepare anchors for inserted value columns
    value_name = ctx.alias_generator.get('ins_val')
    iterator_name = ctx.alias_generator.get('ins_iter')
    source_id = irast.PathId.from_type(
        ctx.schema,
        sub_source,
        typename=sn.QualName('__derived__', value_name),
        env=None,
    )
    link_ref = irtypeutils.ptrref_from_ptrcls(
        schema=ctx.schema, ptrcls=sub, cache=None, typeref_cache=None
    )
    value_id: irast.PathId = source_id.extend(ptrref=link_ref)

    value_ql: qlast.PathElement = (
        qlast.IRAnchor(name=value_name)
        if is_value_single
        else qlast.ObjectRef(name=iterator_name)
    )

    # a phantom relation that is supposed to hold the two source and target
    # columns of rows that need to be deleted.
    value_cte_name = ctx.alias_generator.get('del_value')
    value_rel = pgast.Relation(name=value_cte_name)
    value_columns = [('source', False), ('target', False)]

    var = pgast.ColumnRef(name=('source',), nullable=True)
    value_rel.path_outputs[(source_id, pgce.PathAspect.VALUE)] = var
    value_rel.path_outputs[(source_id, pgce.PathAspect.IDENTITY)] = var

    tgt_id = value_id.tgt_path()
    var = pgast.ColumnRef(name=('target',), nullable=True)
    value_rel.path_outputs[(tgt_id, pgce.PathAspect.VALUE)] = var
    value_rel.path_outputs[(tgt_id, pgce.PathAspect.IDENTITY)] = var

    # source needs an iterator column, so we need to invent one
    # Here we only decide on the name of that iterator column, the actual column
    # is generated later, when resolving the DML stmt.
    value_iterator = ctx.alias_generator.get('iter')
    var = pgast.ColumnRef(name=(value_iterator,))
    value_rel.path_outputs[(source_id, pgce.PathAspect.ITERATOR)] = var
    value_rel.path_outputs[(value_id, pgce.PathAspect.ITERATOR)] = var

    # construct the EdgeQL DML AST
    sub_name = sub.get_name(ctx.schema)
    sub_source_name = sub_source.get_name(ctx.schema)
    sub_target_name = sub_target.get_name(ctx.schema)

    sub_name = sub.get_shortname(ctx.schema)

    ql_sub_source_ref = s_utils.name_to_ast_ref(sub_source_name)
    ql_sub_target_ref = s_utils.name_to_ast_ref(sub_target_name)

    ql_ptr_val: qlast.Expr = qlast.Path(
        steps=[value_ql, qlast.Ptr(name=sub_name.name)]
    )
    if isinstance(sub, s_links.Link):
        ql_ptr_val = qlast.TypeCast(
            expr=ql_ptr_val,
            type=qlast.TypeName(maintype=ql_sub_target_ref),
        )

    ql_stmt: qlast.Expr = qlast.UpdateQuery(
        subject=qlast.Path(steps=[ql_sub_source_ref]),
        where=qlast.BinOp(  # ObjectType == value.source
            left=qlast.Path(steps=[ql_sub_source_ref]),
            op='=',
            right=qlast.Path(steps=[value_ql]),
        ),
        shape=[
            qlast.ShapeElement(
                expr=qlast.Path(steps=[qlast.Ptr(name=sub_name.name)]),
                operation=qlast.ShapeOperation(op=qlast.ShapeOp.SUBTRACT),
                compexpr=ql_ptr_val,
            )
        ],
    )
    if not is_value_single:
        # value relation might contain multiple rows
        # to express this in EdgeQL, we must wrap `delete` into a `for` query
        ql_stmt = qlast.ForQuery(
            iterator=qlast.Path(steps=[qlast.IRAnchor(name=value_name)]),
            iterator_alias=iterator_name,
            result=ql_stmt,
        )

    ql_returning_shape: List[qlast.ShapeElement] = []
    if stmt.returning_list:
        # construct the shape that will extract all needed column of the subject
        # table (because they might be be used by RETURNING clause)
        for column in sub_table.columns:
            if column.hidden:
                continue
            if column.name in ('source', 'target'):
                # no need to include in shape, they will be present anyway
                continue

            ql_returning_shape.append(
                qlast.ShapeElement(
                    expr=qlast.Path(steps=[qlast.Ptr(name=column.name)]),
                    compexpr=qlast.Path(
                        partial=True,
                        steps=[
                            qlast.Ptr(name=sub_name.name),
                            qlast.Ptr(name=column.name, type='property'),
                        ],
                    ),
                )
            )

    return UncompiledDML(
        input=stmt,
        subject=sub,
        ql_stmt=ql_stmt,
        ql_returning_shape=ql_returning_shape,
        ql_singletons={source_id},
        ql_anchors={value_name: source_id},
        external_rels={source_id: (value_rel, ('source', 'identity'))},
        early_result=context.CompiledDML(
            value_cte_name=value_cte_name,
            value_relation_input=value_relation,
            value_columns=value_columns,
            value_iterator_name=value_iterator,
            # these will be populated after compilation
            output_ctes=[],
            output_relation_name='',
            output_namespace={},
        ),
        # these will be populated by _uncompile_dml_stmt
        subject_columns=[],
    )


@_uncompile_dml_stmt.register
def _uncompile_update_stmt(
    stmt: pgast.UpdateStmt, *, ctx: Context
) -> UncompiledDML:
    # determine the subject object
    sub_table, sub = _uncompile_dml_subject(stmt.relation, ctx=ctx)

    # convert the general repr of SET clause into a list of columns
    update_targets: List[pgast.UpdateTarget] = []
    for target in stmt.targets:
        if isinstance(target, pgast.UpdateTarget):
            if target.indirection:
                raise errors.QueryError(
                    'indirections in UPDATE SET not supported',
                    pgext_code=pgerror.ERROR_FEATURE_NOT_SUPPORTED,
                    span=stmt.span,
                )

            update_targets.append(target)
        elif isinstance(target, pgast.MultiAssignRef):

            if not isinstance(
                target.source, (pgast.ImplicitRowExpr, pgast.RowExpr)
            ):
                raise errors.QueryError(
                    'multi-assigns UPDATE SET are supported only for plain row '
                    'literals (`ROW(...)`)',
                    pgext_code=pgerror.ERROR_FEATURE_NOT_SUPPORTED,
                    span=stmt.span,
                )

            update_targets.extend(
                pgast.UpdateTarget(name=c, val=v, span=v.span)
                for (c, v) in zip(target.columns, target.source.args)
            )
        else:
            raise NotImplementedError()

    set_columns = _pull_columns_from_table(
        sub_table,
        ((c.name, c.span) for c in update_targets),
    )
    column_updates = list(zip(set_columns, (c.val for c in update_targets)))

    res: UncompiledDML
    if isinstance(sub, s_objtypes.ObjectType):
        res = _uncompile_update_object_stmt(
            stmt, sub, sub_table, column_updates, ctx=ctx
        )
    elif isinstance(sub, (s_links.Link, s_properties.Property)):
        raise errors.QueryError(
            f'UPDATE of link tables is not supported',
            pgext_code=pgerror.ERROR_FEATURE_NOT_SUPPORTED,
            span=stmt.span,
        )
    else:
        raise NotImplementedError()

    if stmt.returning_list:
        _uncompile_subject_columns(sub, sub_table, res, ctx=ctx)
    return res


def _uncompile_update_object_stmt(
    stmt: pgast.UpdateStmt,
    sub: s_objtypes.ObjectType,
    sub_table: context.Table,
    column_updates: List[Tuple[context.Column, pgast.BaseExpr]],
    *,
    ctx: Context,
) -> UncompiledDML:
    """
    Translates a 'SQL UPDATE into an object type table' to an EdgeQL update.
    """

    def is_default(e: pgast.BaseExpr) -> bool:
        return isinstance(e, pgast.Keyword) and e.name == 'DEFAULT'

    # prepare value relation

    # For updates, value relation contains:
    # - `id` column, that contains the id of the subject,
    # - one column for each of the pointers on the subject to be updated,
    # We construct this relation from WHERE and FROM clauses of UPDATE.

    assert isinstance(stmt.relation, pgast.RelRangeVar)
    val_sub_rvar = stmt.relation.alias.aliasname or stmt.relation.relation.name
    assert val_sub_rvar
    value_relation = pgast.SelectStmt(
        ctes=stmt.ctes,
        target_list=[
            pgast.ResTarget(
                val=pgast.ColumnRef(
                    name=(val_sub_rvar, 'id'),
                )
            )
        ]
        + [
            pgast.ResTarget(val=val, name=c.name)
            for c, val in column_updates
            if not is_default(val)  # skip DEFAULT column updates
        ],
        from_clause=[
            pgast.RelRangeVar(
                relation=stmt.relation.relation,
                alias=pgast.Alias(aliasname=val_sub_rvar),
            )
        ]
        + stmt.from_clause,
        where_clause=stmt.where_clause,
    )
    stmt.ctes = []

    # prepare anchors for inserted value columns
    value_name = ctx.alias_generator.get('upd_val')
    iterator_name = ctx.alias_generator.get('upd_iter')
    value_id = irast.PathId.from_type(
        ctx.schema,
        sub,
        typename=sn.QualName('__derived__', value_name),
        env=None,
    )

    value_ql: qlast.PathElement = qlast.ObjectRef(name=iterator_name)

    # a phantom relation that is supposed to hold the inserted value
    # (in the resolver, this will be replaced by the real value relation)
    value_cte_name = ctx.alias_generator.get('upd_value')
    value_rel = pgast.Relation(name=value_cte_name)

    output_var = pgast.ColumnRef(name=('id',))
    value_rel.path_outputs[(value_id, pgce.PathAspect.ITERATOR)] = output_var
    value_rel.path_outputs[(value_id, pgce.PathAspect.VALUE)] = output_var

    value_columns = [('id', False)]
    update_shape = []
    for index, (col, val) in enumerate(column_updates):
        ptr, ptr_name, is_link = _get_pointer_for_column(col, sub, ctx)
        if not is_default(val):
            value_columns.append((ptr_name, is_link))

        # inject type annotation into value relation
        if is_link:
            _try_inject_type_cast(
                value_relation, index + 1, pgast.TypeName(name=('uuid',))
            )

        # prepare the outputs of the source CTE
        ptr_id = _get_ptr_id(value_id, ptr, ctx)
        output_var = pgast.ColumnRef(name=(ptr_name,), nullable=True)
        if is_link:
            value_rel.path_outputs[(ptr_id, pgce.PathAspect.IDENTITY)] = (
                output_var
            )
            value_rel.path_outputs[(ptr_id, pgce.PathAspect.VALUE)] = output_var
        else:
            value_rel.path_outputs[(ptr_id, pgce.PathAspect.VALUE)] = output_var

        # prepare insert shape that will use the paths from source_outputs
        if is_default(val):
            # special case: DEFAULT
            default_ql: qlast.Expr
            if ptr.get_default(ctx.schema) is None:
                default_ql = qlast.Set(elements=[])  # NULL
            else:
                default_ql = qlast.Path(
                    steps=[qlast.SpecialAnchor(name='__default__')]
                )
            update_shape.append(
                qlast.ShapeElement(
                    expr=qlast.Path(steps=[qlast.Ptr(name=ptr_name)]),
                    operation=qlast.ShapeOperation(op=qlast.ShapeOp.ASSIGN),
                    compexpr=default_ql,
                )
            )
        else:
            # base case
            update_shape.append(
                _construct_assign_element_for_ptr(
                    value_ql,
                    ptr_name,
                    ptr,
                    is_link,
                    ctx,
                )
            )

    # construct the EdgeQL DML AST
    sub_name = sub.get_name(ctx.schema)
    ql_sub_ref = s_utils.name_to_ast_ref(sub_name)

    ql_stmt: qlast.Expr = qlast.UpdateQuery(
        subject=qlast.Path(steps=[ql_sub_ref]),
        where=qlast.BinOp(  # ObjectType == value.source
            left=qlast.Path(steps=[ql_sub_ref]),
            op='=',
            right=qlast.Path(steps=[value_ql]),
        ),
        shape=update_shape,
    )

    # value relation might contain multiple rows
    # to express this in EdgeQL, we must wrap `update` into a `for` query
    ql_stmt = qlast.ForQuery(
        iterator=qlast.Path(steps=[qlast.IRAnchor(name=value_name)]),
        iterator_alias=iterator_name,
        result=ql_stmt,
    )

    ql_returning_shape: List[qlast.ShapeElement] = []
    if stmt.returning_list:
        # construct the shape that will extract all needed column of the subject
        # table (because they might be be used by RETURNING clause)
        for column in sub_table.columns:
            if column.hidden:
                continue
            _, ptr_name, _ = _get_pointer_for_column(column, sub, ctx)
            ql_returning_shape.append(
                qlast.ShapeElement(
                    expr=qlast.Path(steps=[qlast.Ptr(name=ptr_name)]),
                )
            )

    return UncompiledDML(
        input=stmt,
        subject=sub,
        ql_stmt=ql_stmt,
        ql_returning_shape=ql_returning_shape,
        ql_singletons={value_id},
        ql_anchors={value_name: value_id},
        external_rels={value_id: (value_rel, ('source', 'identity'))},
        early_result=context.CompiledDML(
            value_cte_name=value_cte_name,
            value_relation_input=value_relation,
            value_columns=value_columns,
            value_iterator_name=None,
            # these will be populated after compilation
            output_ctes=[],
            output_relation_name='',
            output_namespace={},
        ),
        # these will be populated by _uncompile_dml_stmt
        subject_columns=[],
    )


def _compile_uncompiled_dml(
    stmts: List[UncompiledDML], ctx: context.ResolverContextLevel
) -> Tuple[
    Mapping[pgast.Query, context.CompiledDML],
    List[pgast.CommonTableExpr],
]:
    """
    Compiles *all* DML statements in the query.

    Statements must already be uncompiled into equivalent EdgeQL statements.
    Will merge the statements into one large shape of all DML queries and
    compile that with a single invocation of EdgeQL compiler.

    Returns:
    - mapping from the original SQL statement into CompiledDML and
    - a list of "global" CTEs that should be injected at the end of top-level
      CTE list.
    """

    # merge params
    singletons = set()
    anchors: Dict[str, irast.PathId] = {}
    for stmt in stmts:
        singletons.update(stmt.ql_singletons)
        anchors.update(stmt.ql_anchors)

    # construct the main query
    ql_aliases: List[qlast.AliasedExpr | qlast.ModuleAliasDecl] = []
    ql_stmt_shape: List[qlast.ShapeElement] = []
    ql_stmt_shape_names = []
    for index, stmt in enumerate(stmts):
        name = f'dml_{index}'
        ql_stmt_shape_names.append(name)
        ql_aliases.append(
            qlast.AliasedExpr(
                alias=name,
                expr=qlast.DetachedExpr(expr=stmt.ql_stmt),
            )
        )
        ql_stmt_shape.append(
            qlast.ShapeElement(
                expr=qlast.Path(steps=[qlast.Ptr(name=name)]),
                compexpr=qlast.Shape(
                    expr=qlast.Path(steps=[qlast.ObjectRef(name=name)]),
                    elements=stmt.ql_returning_shape,
                ),
            )
        )
    ql_stmt = qlast.SelectQuery(
        aliases=ql_aliases,
        result=qlast.Shape(expr=None, elements=ql_stmt_shape),
    )

    ir_stmt: irast.Statement
    try:
        # compile synthetic ql statement into SQL
        options = qlcompiler.CompilerOptions(
            modaliases={None: 'default'},
            make_globals_empty=True,  # TODO: globals in SQL
            singletons=singletons,
            anchors=anchors,
            allow_user_specified_id=True,  # TODO: should this be enabled?
        )
        ir_stmt = qlcompiler.compile_ast_to_ir(
            ql_stmt,
            schema=ctx.schema,
            options=options,
        )
        external_rels, ir_stmts = _merge_and_prepare_external_rels(
            ir_stmt, stmts, ql_stmt_shape_names
        )
        sql_result = pgcompiler.compile_ir_to_sql_tree(
            ir_stmt,
            external_rels=external_rels,
            output_format=pgcompiler.OutputFormat.NATIVE_INTERNAL,
            alias_generator=ctx.alias_generator,
        )
    except errors.QueryError as e:
        raise errors.QueryError(
            msg=e.args[0],
            details=e.details,
            hint=e.hint,
            # not sure if this is ok, but it is better than InternalServerError,
            # which is the default
            pgext_code=pgerror.ERROR_DATA_EXCEPTION,
        )

    assert isinstance(sql_result.ast, pgast.Query)
    assert sql_result.ast.ctes
    ctes = sql_result.ast.ctes

    result = {}
    for stmt, ir_mutating_stmt in zip(stmts, ir_stmts):
        stmt_ctes: List[pgast.CommonTableExpr] = []
        found_it = False
        while len(ctes) > 0:
            matches = ctes[0].for_dml_stmt == ir_mutating_stmt
            if not matches and found_it:
                # use all matching CTEs plus all preceding
                break
            if matches:
                found_it = True

            stmt_ctes.append(ctes.pop(0))

        # Find the output CTE by filtering by the name of relation that is being
        # manipulated. This will filter out stmts for link tables and triggers.
        # XXX: this is the least reliable part of SQL DML
        subject_id = str(stmt.subject.id)
        output_cte = next(
            c
            for c in reversed(stmt_ctes)
            if isinstance(c.query, pgast.DMLQuery)
            and isinstance(c.query.relation, pgast.RelRangeVar)
            and c.query.relation.relation.name == subject_id
        )
        output_rel = output_cte.query

        # prepare a map from pointer name into pgast
        ptr_map: Dict[Tuple[str, str], pgast.BaseExpr] = {}
        for (ptr_id, aspect), output_var in output_rel.path_outputs.items():
            qual_name = ptr_id.rptr_name()
            if not qual_name:
                ptr_map['id', aspect] = output_var
            else:
                ptr_map[qual_name.name, aspect] = output_var
        output_namespace: Dict[str, pgast.BaseExpr] = {}
        for col_name, ptr_name in stmt.subject_columns:
            val = ptr_map.get((ptr_name, 'serialized'), None)
            if not val:
                val = ptr_map.get((ptr_name, 'value'), None)
            if not val:
                val = ptr_map.get((ptr_name, 'identity'), None)
            if ptr_name in ('source', 'target'):
                val = pgast.ColumnRef(name=(ptr_name,))
            assert val, f'{ptr_name} was in shape, but not in path_namespace'
            output_namespace[col_name] = val

        result[stmt.input] = context.CompiledDML(
            value_cte_name=stmt.early_result.value_cte_name,
            value_relation_input=stmt.early_result.value_relation_input,
            value_columns=stmt.early_result.value_columns,
            value_iterator_name=stmt.early_result.value_iterator_name,
            output_ctes=stmt_ctes,
            output_relation_name=output_cte.name,
            output_namespace=output_namespace,
        )

    # The remaining CTEs do not belong to any one specific DML statement and
    # should be included to at the end of the top-level query.
    # They were probably generated by "after all" triggers.

    return result, ctes


def _merge_and_prepare_external_rels(
    ir_stmt: irast.Statement,
    stmts: List[UncompiledDML],
    stmt_names: List[str],
) -> Tuple[
    Dict[irast.PathId, Tuple[pgast.BaseRelation, Tuple[str, ...]]],
    List[irast.MutatingStmt],
]:
    """Construct external rels used for compiling all DML statements at once."""

    # This should be straight-forward, but because we put DML into shape
    # elements, ql compiler will put each binding into a separate namespace.
    # So we need to find the correct path_id for each DML stmt in the IR by
    # looking at the paths in the shape elements.

    assert isinstance(ir_stmt.expr, irast.SetE)
    assert isinstance(ir_stmt.expr.expr, irast.SelectStmt)

    ir_shape = ir_stmt.expr.expr.result.shape
    assert ir_shape

    # extract stmt name from the shape elements
    shape_elements_by_name = {}
    for b, _ in ir_shape:
        rptr_name = b.path_id.rptr_name()
        if not rptr_name:
            continue
        shape_elements_by_name[rptr_name.name] = b.expr.expr

    external_rels: Dict[
        irast.PathId, Tuple[pgast.BaseRelation, Tuple[str, ...]]
    ] = {}
    ir_stmts = []
    for stmt, name in zip(stmts, stmt_names):
        # find the associated binding (this is real funky)
        element = shape_elements_by_name[name]

        while not isinstance(element, irast.MutatingStmt):
            if isinstance(element, irast.SelectStmt):
                element = element.result.expr
            elif isinstance(element, irast.Pointer):
                element = element.source.expr
            else:
                raise NotImplementedError('cannot find mutating stmt')
        ir_stmts.append(element)

        subject_path_id = element.result.path_id
        subject_namespace = subject_path_id.namespace

        # add all external rels, but add the namespace to their output's ids
        for rel_id, (rel, aspects) in stmt.external_rels.items():
            for (out_id, out_asp), out in list(rel.path_outputs.items()):
                # HACK: this is a hacky hack to get the path_id used by the
                # pointers within the DML statement's namespace
                out_id = out_id.replace_namespace(subject_namespace)
                out_id._prefix = out_id._get_prefix(1).replace_namespace(set())
                rel.path_outputs[out_id, out_asp] = out
            external_rels[rel_id] = (rel, aspects)
    return external_rels, ir_stmts


@dispatch._resolve_relation.register
def resolve_DMLQuery(
    stmt: pgast.DMLQuery, *, include_inherited: bool, ctx: Context
) -> Tuple[pgast.Query, context.Table]:
    assert stmt.relation

    if ctx.subquery_depth >= 2:
        raise errors.QueryError(
            'WITH clause containing a data-modifying statement must be at '
            'the top level',
            span=stmt.span,
            pgext_code=pgerror.ERROR_FEATURE_NOT_SUPPORTED,
        )

    compiled_dml = ctx.compiled_dml[stmt]

    _resolve_dml_value_rel(compiled_dml, ctx=ctx)

    ctx.ctes_buffer.extend(compiled_dml.output_ctes)

    return _fini_resolve_dml(stmt, compiled_dml, ctx=ctx)


def _resolve_dml_value_rel(compiled_dml: context.CompiledDML, *, ctx: Context):
    # resolve the value relation
    with ctx.child() as sctx:
        # this subctx is needed so it is not deemed as top-level which would
        # extract and attach CTEs, but not make the available to all
        # following CTEs

        # but it is not a "real" subquery context
        sctx.subquery_depth -= 1

        val_rel, val_table = dispatch.resolve_relation(
            compiled_dml.value_relation_input, ctx=sctx
        )
        assert isinstance(val_rel, pgast.Query)

    if len(compiled_dml.value_columns) != len(val_table.columns):
        col_names = ', '.join(c for c, _ in compiled_dml.value_columns)
        raise errors.QueryError(
            f'Expected {len(compiled_dml.value_columns)} columns '
            f'({col_names}), but got {len(val_table.columns)}',
            span=compiled_dml.value_relation_input.span,
        )

    if val_rel.ctes:
        ctx.ctes_buffer.extend(val_rel.ctes)
        val_rel.ctes = None

    val_table.alias = ctx.alias_generator.get('rel')

    # wrap the value relation into a "pre-projection",
    # so we can add type casts for link ids and an iterator column
    pre_projection_needed = compiled_dml.value_iterator_name or (
        any(cast_to_uuid for _, cast_to_uuid in compiled_dml.value_columns)
    )
    if pre_projection_needed:
        value_target_list: List[pgast.ResTarget] = []
        for val_col, (ptr_name, cast_to_uuid) in zip(
            val_table.columns, compiled_dml.value_columns
        ):
            # prepare pre-projection of this pointer value
            val_col_pg = pg_res_expr.resolve_column_kind(
                val_table, val_col.kind, ctx=ctx
            )
            if cast_to_uuid:
                val_col_pg = pgast.TypeCast(
                    arg=val_col_pg, type_name=pgast.TypeName(name=('uuid',))
                )
            value_target_list.append(
                pgast.ResTarget(name=ptr_name, val=val_col_pg)
            )

        if compiled_dml.value_iterator_name:
            # source needs an iterator column, so we need to invent one
            # The name of the column was invented before (in pre-processing) so
            # it could be used in DML CTEs.
            value_target_list.append(
                pgast.ResTarget(
                    name=compiled_dml.value_iterator_name,
                    val=pgast.FuncCall(
                        name=('edgedb', 'uuid_generate_v4'),
                        args=(),
                    ),
                )
            )

        val_rel = pgast.SelectStmt(
            from_clause=[
                pgast.RangeSubselect(
                    subquery=val_rel,
                    alias=pgast.Alias(aliasname=val_table.alias)
                )
            ],
            target_list=value_target_list,
        )

    value_cte = pgast.CommonTableExpr(
        name=compiled_dml.value_cte_name,
        query=val_rel,
    )
    ctx.ctes_buffer.append(value_cte)


def _fini_resolve_dml(
    stmt: pgast.DMLQuery, compiled_dml: context.CompiledDML, *, ctx: Context
) -> Tuple[pgast.Query, context.Table]:
    if stmt.returning_list:
        res_query, res_table = _resolve_returning_rows(
            stmt.returning_list,
            compiled_dml.output_relation_name,
            compiled_dml.output_namespace,
            stmt.relation.alias.aliasname,
            ctx,
        )
    else:
        if ctx.subquery_depth == 0:
            # when a top-level DML query have a RETURNING clause,
            # we inject a COUNT(*) clause so we can efficiently count
            # modified rows which will be converted into CommandComplete tag.
            res_query = pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(
                        val=pgast.FuncCall(
                            name=('count',), agg_star=True, args=[]
                        ),
                    )
                ],
                from_clause=[
                    pgast.RelRangeVar(
                        relation=pgast.Relation(
                            name=compiled_dml.output_relation_name
                        )
                    )
                ],
            )
        else:
            # nested DML queries without RETURNING does not need any result
            res_query = pgast.SelectStmt()
        res_table = context.Table()

    if not res_query.ctes:
        res_query.ctes = []
    res_query.ctes.extend(pg_res_rel.extract_ctes_from_ctx(ctx))

    return res_query, res_table


def _resolve_returning_rows(
    returning_list: List[pgast.ResTarget],
    output_relation_name: str,
    output_namespace: Mapping[str, pgast.BaseExpr],
    subject_alias: Optional[str],
    ctx: context.ResolverContextLevel,
) -> Tuple[pgast.Query, context.Table]:
    # relation that provides the values of inserted pointers
    inserted_rvar_name = ctx.alias_generator.get('ins')
    inserted_query = pgast.SelectStmt(
        from_clause=[
            pgast.RelRangeVar(
                relation=pgast.Relation(name=output_relation_name),
            )
        ]
    )
    inserted_table = context.Table(
        alias=subject_alias,
        reference_as=inserted_rvar_name,
    )

    for col_name, val in output_namespace.items():
        inserted_query.target_list.append(
            pgast.ResTarget(name=col_name, val=val)
        )
        inserted_table.columns.append(
            context.Column(
                name=col_name,
                kind=context.ColumnByName(reference_as=col_name),
            )
        )

    with ctx.empty() as sctx:
        sctx.scope.tables.append(inserted_table)

        returning_query = pgast.SelectStmt(
            from_clause=[
                pgast.RangeSubselect(
                    alias=pgast.Alias(aliasname=inserted_rvar_name),
                    subquery=inserted_query,
                )
            ],
            target_list=[],
        )
        returning_table = context.Table()

        for t in returning_list:
            targets, columns = pg_res_expr.resolve_ResTarget(t, ctx=sctx)
            returning_query.target_list.extend(targets)
            returning_table.columns.extend(columns)
    return returning_query, returning_table


def _get_pointer_for_column(
    col: context.Column,
    subject: s_objtypes.ObjectType | s_links.Link | s_properties.Property,
    ctx: context.ResolverContextLevel,
) -> Tuple[s_pointers.Pointer, str, bool]:
    if isinstance(
        subject, (s_links.Link, s_properties.Property)
    ) and col.name in ('source', 'target'):
        return subject, col.name, False
    assert not isinstance(subject, s_properties.Property)

    is_link = False
    if col.name.endswith('_id'):
        # this condition will break *properties* that end with _id
        # I'm not sure if this is a problem
        ptr_name = col.name[0:-3]
        is_link = True
    else:
        ptr_name = col.name

    ptr = subject.maybe_get_ptr(ctx.schema, sn.UnqualName(ptr_name))
    assert ptr
    return ptr, ptr_name, is_link


def _get_ptr_id(
    source_id: irast.PathId,
    ptr: s_pointers.Pointer,
    ctx: context.ResolverContextLevel,
) -> irast.PathId:
    ptrref = irtypeutils.ptrref_from_ptrcls(
        schema=ctx.schema, ptrcls=ptr, cache=None, typeref_cache=None
    )
    return source_id.extend(ptrref=ptrref)


def _try_inject_type_cast(
    rel: pgast.BaseRelation,
    pos: int,
    ty: pgast.TypeName,
):
    """
    If a relation is simple, injects type annotation for a column.
    This is needed for Postgres to correctly infer the type so it will be able
    to bind to correct paramater types.
    """

    if not isinstance(rel, pgast.SelectStmt):
        return

    if rel.values:
        for row_i, row in enumerate(rel.values):
            if isinstance(row, pgast.ImplicitRowExpr) and pos < len(row.args):
                args = list(row.args)
                args[pos] = pgast.TypeCast(arg=args[pos], type_name=ty)
                rel.values[row_i] = row.replace(args=args)

    elif rel.target_list and pos < len(rel.target_list):
        target = rel.target_list[pos]
        rel.target_list[pos] = target.replace(
            val=pgast.TypeCast(arg=target.val, type_name=ty)
        )
