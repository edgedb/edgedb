#
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

from typing import Optional, Tuple, Iterator, Sequence, Dict, List, cast, Set
import uuid

from edb import errors

from edb.pgsql import ast as pgast
from edb.pgsql import common
from edb.pgsql import compiler as pgcompiler
from edb.pgsql.compiler import enums as pgce
from edb.server.compiler import dbstate

from edb.schema import types as s_types

from edb.ir import ast as irast


from edb.edgeql import compiler as qlcompiler

from . import dispatch
from . import context
from . import static
from . import command

Context = context.ResolverContextLevel


def infer_alias(res_target: pgast.ResTarget) -> Optional[str]:
    if res_target.name:
        return res_target.name

    val = res_target.val

    if isinstance(val, pgast.TypeCast):
        val = val.arg

    if isinstance(val, pgast.FuncCall):
        return val.name[-1]

    if isinstance(val, pgast.ImplicitRowExpr):
        return 'row'

    # if just name has been selected, use it as the alias
    if isinstance(val, pgast.ColumnRef):
        name = val.name
        if isinstance(name[-1], str):
            return name[-1]

    return None


# this function cannot go though dispatch,
# because it may return multiple nodes, due to * notation
def resolve_ResTarget(
    res_target: pgast.ResTarget,
    *,
    existing_names: Optional[Set[str]] = None,
    ctx: Context,
) -> Tuple[Sequence[pgast.ResTarget], Sequence[context.Column]]:
    alias = infer_alias(res_target)

    # special case for ColumnRef for handing wildcards
    if not alias and isinstance(res_target.val, pgast.ColumnRef):
        col_res = _lookup_column(res_target.val, ctx)

        res = []
        columns = []
        for table, column in col_res:
            columns.append(column)
            res.append(
                pgast.ResTarget(
                    val=resolve_column_kind(table, column.kind, ctx=ctx),
                    name=column.name,
                )
            )
        return (res, columns)

    # base case
    val = dispatch.resolve(res_target.val, ctx=ctx)

    # special case for statically-evaluated FuncCall
    if (
        not alias
        and isinstance(val, pgast.StringConstant)
        and isinstance(res_target.val, pgast.FuncCall)
    ):
        alias = static.name_in_pg_catalog(res_target.val.name)

    if not res_target.name and existing_names and alias in existing_names:
        # when a name already exists, don't infer the same name
        # this behavior is technically different than Postgres, but it is also
        # not documented and users should not be relying on it.
        # It does help us in some cases (passing `SELECT a.id, b.id` into DML).
        alias = None

    name: str = alias or ctx.alias_generator.get('col')
    col = context.Column(
        name=name, kind=context.ColumnByName(reference_as=name)
    )
    new_target = pgast.ResTarget(val=val, name=name, span=res_target.span)
    return (new_target,), (col,)


def resolve_column_kind(
    table: context.Table, column: context.ColumnKind, *, ctx: Context
) -> pgast.BaseExpr:
    match column:
        case context.ColumnByName(reference_as=reference_as):
            if table.reference_as:
                return pgast.ColumnRef(name=(table.reference_as, reference_as))
            else:
                # In some cases tables might not have an assigned alias
                # because that is not syntactically possible (COPY), or because
                # the table being referenced is currently being assembled
                # (e.g. ORDER BY refers to a newly defined column).

                # So we make an assumption that in such cases, this will not
                # be ambiguous. I think this is not strictly correct.
                return pgast.ColumnRef(name=(reference_as,))

        case context.ColumnStaticVal(val=val):
            # special case: __type__ static value
            return _uuid_const(val)
        case context.ColumnComputable(pointer=pointer):

            expr = pointer.get_expr(ctx.schema)
            assert expr

            source = pointer.get_source(ctx.schema)
            assert isinstance(source, s_types.Type)
            source_id = irast.PathId.from_type(ctx.schema, source, env=None)

            singletons = [source]
            options = qlcompiler.CompilerOptions(
                modaliases={None: 'default'},
                anchors={'__source__': source},
                path_prefix_anchor='__source__',
                singletons=singletons,
                make_globals_empty=False,
            )
            compiled = expr.compiled(ctx.schema, options=options, context=None)

            sql_tree = pgcompiler.compile_ir_to_sql_tree(
                compiled.irast,
                external_rvars={
                    (source_id, pgce.PathAspect.SOURCE): pgast.RelRangeVar(
                        alias=pgast.Alias(
                            aliasname=table.reference_as,
                        ),
                        relation=pgast.Relation(name=table.reference_as),
                    ),
                },
                output_format=pgcompiler.OutputFormat.NATIVE_INTERNAL,
            )
            command.merge_params(sql_tree, compiled.irast, ctx)

            assert isinstance(sql_tree.ast, pgast.BaseExpr)
            return sql_tree.ast
        case _:
            raise NotImplementedError(column)


@dispatch._resolve.register
def resolve_ColumnRef(
    column_ref: pgast.ColumnRef, *, ctx: Context
) -> pgast.BaseExpr:
    res = _lookup_column(column_ref, ctx)
    table, column = res[0]

    if len(res) != 1:
        # Lookup can have multiple results only when using *.
        assert table.reference_as
        return pgast.ColumnRef(name=(table.reference_as, pgast.Star()))

    return resolve_column_kind(table, column.kind, ctx=ctx)


def _uuid_const(val: uuid.UUID):
    return pgast.TypeCast(
        arg=pgast.StringConstant(val=str(val)),
        type_name=pgast.TypeName(name=('uuid',)),
    )


def _lookup_column(
    column_ref: pgast.ColumnRef,
    ctx: Context,
) -> Sequence[Tuple[context.Table, context.Column]]:
    matched_columns: List[Tuple[context.Table, context.Column]] = []

    name = column_ref.name
    col_name: str | pgast.Star

    if len(name) == 1:
        # look for the column in all tables
        col_name = name[0]

        if isinstance(col_name, pgast.Star):
            return [
                (t, c)
                for t in ctx.scope.tables
                for c in t.columns
                if not c.hidden
            ]

        for table in ctx.scope.tables:
            matched_columns.extend(_lookup_in_table(col_name, table))

        if not matched_columns:
            # is it a reference to a rel var?
            try:
                tab = _lookup_table(col_name, ctx)
                assert tab.reference_as
                col = context.Column(
                    name=tab.reference_as,
                    kind=context.ColumnByName(reference_as=tab.reference_as),
                )
                return [(context.Table(), col)]
            except errors.QueryError:
                pass

    elif len(name) >= 2:
        # look for the column in the specific table
        tab_name, col_name = name[-2:]

        try:
            table = _lookup_table(cast(str, tab_name), ctx)
        except errors.QueryError as e:
            e.set_span(column_ref.span)
            raise

        if isinstance(col_name, pgast.Star):
            return [(table, c) for c in table.columns if not c.hidden]
        else:
            matched_columns.extend(_lookup_in_table(col_name, table))

    if not matched_columns:
        raise errors.QueryError(
            f'cannot find column `{col_name}`', span=column_ref.span
        )

    # apply precedence
    if len(matched_columns) > 1:
        max_precedence = max(t.precedence for t, _ in matched_columns)
        matched_columns = [
            (t, c) for t, c in matched_columns if t.precedence == max_precedence
        ]

    if len(matched_columns) > 1:
        potential_tables = ', '.join([t.name or '' for t, _ in matched_columns])
        raise errors.QueryError(
            f'ambiguous column `{col_name}` could belong to '
            f'following tables: {potential_tables}',
            span=column_ref.span,
        )

    return (matched_columns[0],)


def _lookup_in_table(
    col_name: str, table: context.Table
) -> Iterator[Tuple[context.Table, context.Column]]:
    for column in table.columns:
        if column.name == col_name:
            yield (table, column)


def _lookup_table(tab_name: str, ctx: Context) -> context.Table:
    matched_tables: List[context.Table] = []
    for t in ctx.scope.tables:
        t_name = t.alias or t.name
        if t_name == tab_name:
            matched_tables.append(t)

    if not matched_tables:
        raise errors.QueryError(f'cannot find table `{tab_name}`')

    # apply precedence
    if len(matched_tables) > 1:
        max_precedence = max(t.precedence for t in matched_tables)
        matched_tables = [
            t for t in matched_tables if t.precedence == max_precedence
        ]

    if len(matched_tables) > 1:
        raise errors.QueryError(f'ambiguous table `{tab_name}`')

    table = matched_tables[0]
    return table


@dispatch._resolve.register
def resolve_SubLink(
    sub_link: pgast.SubLink,
    *,
    ctx: Context,
) -> pgast.SubLink:
    with ctx.child() as subctx:
        expr = dispatch.resolve(sub_link.expr, ctx=subctx)

    return pgast.SubLink(
        operator=sub_link.operator,
        expr=expr,
        test_expr=dispatch.resolve_opt(sub_link.test_expr, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_Expr(expr: pgast.Expr, *, ctx: Context) -> pgast.Expr:
    return pgast.Expr(
        name=expr.name,
        lexpr=dispatch.resolve(expr.lexpr, ctx=ctx) if expr.lexpr else None,
        rexpr=dispatch.resolve(expr.rexpr, ctx=ctx) if expr.rexpr else None,
    )


@dispatch._resolve.register
def resolve_TypeCast(
    expr: pgast.TypeCast,
    *,
    ctx: Context,
) -> pgast.BaseExpr:
    if res := static.eval_TypeCast(expr, ctx=ctx):
        return res
    return pgast.TypeCast(
        arg=dispatch.resolve(expr.arg, ctx=ctx),
        type_name=expr.type_name,
    )


@dispatch._resolve.register
def resolve_BaseConstant(
    expr: pgast.BaseConstant,
    *,
    ctx: Context,
) -> pgast.BaseConstant:
    return expr


@dispatch._resolve.register
def resolve_CaseExpr(
    expr: pgast.CaseExpr,
    *,
    ctx: Context,
) -> pgast.CaseExpr:
    return pgast.CaseExpr(
        arg=dispatch.resolve_opt(expr.arg, ctx=ctx),
        args=dispatch.resolve_list(expr.args, ctx=ctx),
        defresult=dispatch.resolve_opt(expr.defresult, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_CaseWhen(
    expr: pgast.CaseWhen,
    *,
    ctx: Context,
) -> pgast.CaseWhen:
    return pgast.CaseWhen(
        expr=dispatch.resolve(expr.expr, ctx=ctx),
        result=dispatch.resolve(expr.result, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_SortBy(
    expr: pgast.SortBy,
    *,
    ctx: Context,
) -> pgast.SortBy:
    return pgast.SortBy(
        node=dispatch.resolve(expr.node, ctx=ctx),
        dir=expr.dir,
        nulls=expr.nulls,
    )


func_calls_remapping: Dict[Tuple[str, ...], Tuple[str, ...]] = {
    ('information_schema', '_pg_truetypid'): (
        common.versioned_schema('edgedbsql'),
        '_pg_truetypid',
    ),
    ('information_schema', '_pg_truetypmod'): (
        common.versioned_schema('edgedbsql'),
        '_pg_truetypmod',
    ),
    ('pg_catalog', 'format_type'): ('edgedb', '_format_type'),
}


@dispatch._resolve.register
def resolve_FuncCall(
    call: pgast.FuncCall,
    *,
    ctx: Context,
) -> pgast.BaseExpr:
    # Special case: some function calls (mostly from pg_catalog) are
    # intercepted and statically evaluated.
    if res := static.eval_FuncCall(call, ctx=ctx):
        return res

    # Remap function name and default to the original name.
    # Effectively, this exposes all non-remapped functions.
    name = func_calls_remapping.get(call.name, call.name)

    return pgast.FuncCall(
        name=name,
        args=dispatch.resolve_list(call.args, ctx=ctx),
        agg_order=dispatch.resolve_opt_list(call.agg_order, ctx=ctx),
        agg_filter=dispatch.resolve_opt(call.agg_filter, ctx=ctx),
        agg_star=call.agg_star,
        agg_distinct=call.agg_distinct,
        over=dispatch.resolve_opt(call.over, ctx=ctx),
        with_ordinality=call.with_ordinality,
    )


@dispatch._resolve.register
def resolve_WindowDef(
    expr: pgast.WindowDef,
    *,
    ctx: Context,
) -> pgast.WindowDef:
    return pgast.WindowDef(
        partition_clause=dispatch.resolve_opt_list(
            expr.partition_clause, ctx=ctx
        ),
        order_clause=dispatch.resolve_opt_list(expr.order_clause, ctx=ctx),
        start_offset=dispatch.resolve_opt(expr.start_offset, ctx=ctx),
        end_offset=dispatch.resolve_opt(expr.end_offset, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_CoalesceExpr(
    expr: pgast.CoalesceExpr,
    *,
    ctx: Context,
) -> pgast.CoalesceExpr:
    return pgast.CoalesceExpr(args=dispatch.resolve_list(expr.args, ctx=ctx))


@dispatch._resolve.register
def resolve_NullTest(
    expr: pgast.NullTest,
    *,
    ctx: Context,
) -> pgast.NullTest:
    return pgast.NullTest(
        arg=dispatch.resolve(expr.arg, ctx=ctx), negated=expr.negated
    )


@dispatch._resolve.register
def resolve_BooleanTest(
    expr: pgast.BooleanTest,
    *,
    ctx: Context,
) -> pgast.BooleanTest:
    return pgast.BooleanTest(
        arg=dispatch.resolve(expr.arg, ctx=ctx),
        negated=expr.negated,
        is_true=expr.is_true,
    )


@dispatch._resolve.register
def resolve_ImplicitRowExpr(
    expr: pgast.ImplicitRowExpr,
    *,
    ctx: Context,
) -> pgast.ImplicitRowExpr:
    return pgast.ImplicitRowExpr(
        args=dispatch.resolve_list(expr.args, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_RowExpr(
    expr: pgast.RowExpr,
    *,
    ctx: Context,
) -> pgast.RowExpr:
    return pgast.RowExpr(
        args=dispatch.resolve_list(expr.args, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_ParamRef(
    expr: pgast.ParamRef,
    *,
    ctx: Context,
) -> pgast.ParamRef:
    internal_index: Optional[int] = None
    param: Optional[dbstate.SQLParam] = None
    for i, p in enumerate(ctx.query_params):
        if isinstance(p, dbstate.SQLParamExternal) and p.index == expr.number:
            param = p
            internal_index = i + 1
            break
    if not param:
        param = dbstate.SQLParamExternal(index=expr.number)
        internal_index = len(ctx.query_params) + 1
        ctx.query_params.append(param)
    assert internal_index

    return pgast.ParamRef(number=internal_index)


@dispatch._resolve.register
def resolve_ArrayExpr(
    expr: pgast.ArrayExpr,
    *,
    ctx: Context,
) -> pgast.ArrayExpr:
    return pgast.ArrayExpr(
        elements=dispatch.resolve_list(expr.elements, ctx=ctx)
    )


@dispatch._resolve.register
def resolve_Indirection(
    expr: pgast.Indirection,
    *,
    ctx: Context,
) -> pgast.Indirection:
    return pgast.Indirection(
        arg=dispatch.resolve(expr.arg, ctx=ctx),
        indirection=dispatch.resolve_list(expr.indirection, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_RecordIndirectionOp(
    expr: pgast.RecordIndirectionOp,
    *,
    ctx: Context,
) -> pgast.RecordIndirectionOp:
    return expr


@dispatch._resolve.register
def resolve_Slice(
    expr: pgast.Slice,
    *,
    ctx: Context,
) -> pgast.Slice:
    return pgast.Slice(
        lidx=dispatch.resolve_opt(expr.lidx, ctx=ctx),
        ridx=dispatch.resolve_opt(expr.ridx, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_Index(
    expr: pgast.Index,
    *,
    ctx: Context,
) -> pgast.Index:
    return pgast.Index(
        idx=dispatch.resolve(expr.idx, ctx=ctx),
    )


@dispatch._resolve.register
def resolve_SQLValueFunction(
    expr: pgast.SQLValueFunction,
    *,
    ctx: Context,
) -> pgast.BaseExpr:
    return static.eval_SQLValueFunction(expr, ctx=ctx)


@dispatch._resolve.register
def resolve_CollateClause(
    expr: pgast.CollateClause,
    *,
    ctx: Context,
) -> pgast.BaseExpr:
    return pgast.CollateClause(
        arg=dispatch.resolve(expr.arg, ctx=ctx), collname=expr.collname
    )


@dispatch._resolve.register
def resolve_MinMaxExpr(
    expr: pgast.MinMaxExpr,
    *,
    ctx: Context,
) -> pgast.BaseExpr:
    return pgast.MinMaxExpr(
        op=expr.op,
        args=dispatch.resolve_list(expr.args, ctx=ctx),
    )
