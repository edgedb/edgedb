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

from edb.server.pgcon import errors as pgerror

from edb import errors
from edb.pgsql import ast as pgast
from edb.pgsql import compiler as pgcompiler
from edb.pgsql.compiler import enums as pgce

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler

from edb.ir import ast as irast
from edb.ir import typeutils as irtypeutils

from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import links as s_links
from edb.schema import name as sn


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

    # preprocess each SQL dml stmt into EdgeQL
    stmts = [_preprocess_insert_stmt(s, ctx=ctx) for s in dml_stmts_sql]

    # merge EdgeQL stmts & compile to SQL
    ctx.compiled_dml, ctes = _compile_preprocessed_dml(stmts, ctx=ctx)

    return ctes


def _collect_dml_stmts(stmt: pgast.Base) -> List[pgast.InsertStmt]:
    if not isinstance(stmt, pgast.Query):
        return []

    # DML can only be in the top-level statement or its CTEs.
    # If it is in any of the nested CTEs, throw errors later on
    res: List[pgast.InsertStmt] = []
    if stmt.ctes:
        for cte in stmt.ctes:
            if isinstance(cte.query, pgast.InsertStmt):
                res.append(cte.query)

    if isinstance(stmt, pgast.InsertStmt):
        res.append(stmt)
    return res


@dataclasses.dataclass(kw_only=True, eq=False, repr=False)
class PreprocessedDML:
    # the input DML node
    input: pgast.Query

    # EdgeQL equivalent to the input node
    ql_stmt: qlast.Expr

    # additional params needed during compilation of the edgeql node
    ql_singletons: Set[irast.PathId]
    ql_anchors: Mapping[str, irast.PathId]
    external_rels: Mapping[
        irast.PathId, Tuple[pgast.BaseRelation, Tuple[str, ...]]
    ]

    # list of column names of the subject type, along with pointer name
    subject_columns: List[Tuple[str, str]]

    # data needed for stitching the compiled ast into the resolver output
    early_result: context.CompiledDML


def _preprocess_insert_stmt(
    stmt: pgast.InsertStmt, *, ctx: Context
) -> PreprocessedDML:
    """
    Takes SQL INSERT query and produces an equivalent EdgeQL insert query
    and a bunch of metadata needed to extract associated CTEs from result of the
    EdgeQL compiler.
    """

    # determine the subject object we are inserting into
    assert isinstance(stmt.relation, pgast.RelRangeVar)
    assert isinstance(stmt.relation.relation, pgast.Relation)
    _sub_rel, sub_table = pg_res_rel.resolve_relation(
        stmt.relation.relation, include_inherited=False, ctx=ctx
    )
    assert sub_table.schema_id  # TODO: raise a proper error here
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

    # handle DEFAULT and prepare the value relation
    value_relation, expected_columns = _preprocess_insert_value(
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
    for expected_col in expected_columns:
        ptr, ptr_name, is_link = _get_pointer_for_column(expected_col, sub, ctx)
        value_columns.append((expected_col, ptr))

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
            _construct_insert_element_for_ptr(
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

    # the core thing
    ql_stmt: qlast.Expr = qlast.InsertQuery(
        subject=qlast.ObjectRef(
            name=sub_name.name,
            module=sub_name.module,
        ),
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

    subject_columns: List[Tuple[str, str]] = []
    if stmt.returning_list:
        # wrap into a select shape that selects all pointers
        # (because they might be be used by RETURNING clause)
        select_shape: List[qlast.ShapeElement] = []
        for column in sub_table.columns:
            if column.hidden:
                continue

            ptr, ptr_name, is_link = _get_pointer_for_column(column, sub, ctx)
            select_shape.append(
                qlast.ShapeElement(
                    expr=qlast.Path(steps=[qlast.Ptr(name=ptr_name)]),
                )
            )
            subject_columns.append((column.name, ptr_name))

        ql_stmt = qlast.SelectQuery(
            result=qlast.Shape(expr=ql_stmt, elements=select_shape)
        )

    return PreprocessedDML(
        input=stmt,
        ql_stmt=ql_stmt,
        ql_singletons={value_id},
        ql_anchors={value_name: value_id},
        external_rels={value_id: (value_rel, ('source', 'identity'))},
        subject_columns=subject_columns,
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
    )


def _has_at_most_one_row(query: pgast.Query | None) -> bool:
    return isinstance(query, pgast.SelectStmt) and (
        (query.values and len(query.values) == 1)
        or (
            isinstance(query.limit_count, pgast.NumericConstant)
            and query.limit_count.val == '1'
        )
    )


def _preprocess_insert_value(
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

    # compile these CTEs as they were defined on value relation
    assert not value_query.ctes
    value_query.ctes = value_ctes

    return value_query, expected_columns


def _compile_preprocessed_dml(
    stmts: List[PreprocessedDML], ctx: context.ResolverContextLevel
) -> Tuple[
    Mapping[pgast.Query, context.CompiledDML],
    List[pgast.CommonTableExpr],
]:
    """
    Compiles *all* DML statements in the query.

    Statements must already be preprocessed into equivalent EdgeQL statements.
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
    ql_stmt_shape: List[qlast.ShapeElement] = []
    ql_stmt_shape_names = []
    for index, stmt in enumerate(stmts):
        name = f'dml_{index}'
        ql_stmt_shape_names.append(name)
        ql_stmt_shape.append(
            qlast.ShapeElement(
                expr=qlast.Path(steps=[qlast.Ptr(name=name)]),
                compexpr=stmt.ql_stmt,
            )
        )
    ql_stmt = qlast.SelectQuery(
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
            # not sure if this is ok, but it is better than InternalServerError,
            # which is the default
            pgext_code=pgerror.ERROR_DATA_EXCEPTION,
        )

    assert isinstance(sql_result.ast, pgast.Query)
    assert sql_result.ast.ctes
    ctes = sql_result.ast.ctes

    result = {}
    for stmt, ir_mutating_stmt in zip(stmts, ir_stmts):
        stmt_ctes = []
        found_it = False
        while len(ctes) > 0:
            cte = ctes.pop(0)
            stmt_ctes.append(cte)

            matches = cte.for_dml_stmt == ir_mutating_stmt
            if not matches and found_it:
                # use all matching CTEs + 1
                break
            if matches:
                found_it = True

        last_query = stmt_ctes[-1].query

        # prepare a map from pointer name into pgast
        ptr_map: Dict[Tuple[str, str], pgast.BaseExpr] = {}
        for (ptr_id, aspect), output_var in last_query.path_outputs.items():
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
            assert val, f'{ptr_name} was in shape, but not in path_namespace'
            output_namespace[col_name] = val

        result[stmt.input] = context.CompiledDML(
            value_cte_name=stmt.early_result.value_cte_name,
            value_relation_input=stmt.early_result.value_relation_input,
            value_columns=stmt.early_result.value_columns,
            value_iterator_name=stmt.early_result.value_iterator_name,
            output_ctes=stmt_ctes,
            output_relation_name=stmt_ctes[-1].name,
            output_namespace=output_namespace,
        )

    # The remaining CTEs do not belong to any one specific DML statement and
    # should be included to at the end of the top-level query.
    # They were probably generated by "after all" triggers.

    return result, ctes


def _merge_and_prepare_external_rels(
    ir_stmt: irast.Statement,
    stmts: List[PreprocessedDML],
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
            assert isinstance(element, irast.SelectStmt)
            element = element.result.expr
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
def resolve_InsertStmt(
    stmt: pgast.InsertStmt, *, include_inherited: bool, ctx: Context
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
    if len(compiled_dml.value_columns) != len(val_table.columns):
        col_names = ', '.join(c.name for c, _ in compiled_dml.value_columns)
        raise errors.QueryError(
            f'INSERT expected {len(compiled_dml.value_columns)} columns, '
            f'but got {len(val_table.columns)} (expecting {col_names})',
            span=compiled_dml.value_relation_input.span,
        )

    # wrap the value relation, to we can add type casts for link ids
    value_target_list: List[pgast.ResTarget] = []
    for val_col, (_, ptr) in zip(val_table.columns, compiled_dml.value_columns):
        is_link = isinstance(ptr, s_links.Link)
        ptr_name = ptr.get_shortname(ctx.schema).name

        # prepare pre-projection of this pointer value
        val_col_pg = pg_res_expr.resolve_column_kind(
            val_table, val_col.kind, ctx=ctx
        )
        if is_link:
            val_col_pg = pgast.TypeCast(
                arg=val_col_pg, type_name=pgast.TypeName(name=('uuid',))
            )
        value_target_list.append(pgast.ResTarget(name=ptr_name, val=val_col_pg))

    # source needs an iterator column, so we need to invent one
    # The name of the column was invented before (in pre-processing) so it could
    # be used in DML CTEs.
    value_target_list.append(
        pgast.ResTarget(
            name=compiled_dml.value_iterator_name,
            val=pgast.FuncCall(
                name=('edgedb', 'uuid_generate_v4'),
                args=(),
            ),
        )
    )

    assert isinstance(val_rel, pgast.Query)
    if val_rel.ctes:
        ctx.ctes_buffer.extend(val_rel.ctes)
        val_rel.ctes = None
    value_cte = pgast.CommonTableExpr(
        name=compiled_dml.value_cte_name,
        query=pgast.SelectStmt(
            from_clause=[pgast.RangeSubselect(subquery=val_rel)],
            target_list=value_target_list,
        ),
    )
    ctx.ctes_buffer.append(value_cte)
    ctx.ctes_buffer.extend(compiled_dml.output_ctes)

    if stmt.returning_list:
        res_query, res_table = _resolve_returning_rows(
            stmt.returning_list,
            compiled_dml.output_relation_name,
            compiled_dml.output_namespace,
            stmt.relation.alias.aliasname,
            ctx,
        )
    else:
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


def _construct_insert_element_for_ptr(
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


def _get_pointer_for_column(
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


def _get_ptr_id(
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
