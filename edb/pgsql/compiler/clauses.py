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


from __future__ import annotations

from typing import *

from edb.edgeql import qltypes
from edb.ir import ast as irast
from edb.pgsql import ast as pgast
from edb.pgsql import types as pg_types

from . import context
from . import dispatch
from . import output
from . import pathctx
from . import relctx
from . import relgen


def init_stmt(
        stmt: irast.Stmt, ctx: context.CompilerContextLevel,
        parent_ctx: context.CompilerContextLevel) -> None:
    if ctx.toplevel_stmt is context.NO_STMT:
        parent_ctx.toplevel_stmt = ctx.toplevel_stmt = ctx.stmt


def fini_stmt(
        stmt: pgast.Query, ctx: context.CompilerContextLevel,
        parent_ctx: context.CompilerContextLevel) -> None:

    if stmt is ctx.toplevel_stmt:
        # Type rewrites go first.
        if stmt.ctes is None:
            stmt.ctes = []
        stmt.ctes[:0] = list(ctx.type_ctes.values())

        stmt.argnames = argmap = ctx.argmap

        if not ctx.env.use_named_params:
            # Adding unused parameters into a CTE
            targets = []
            for param in ctx.env.query_params:
                if param.name in argmap:
                    continue
                if param.name.isdecimal():
                    idx = int(param.name) + 1
                else:
                    idx = len(argmap) + 1
                argmap[param.name] = pgast.Param(
                    index=idx,
                    required=param.required,
                )
                targets.append(pgast.ResTarget(val=pgast.TypeCast(
                    arg=pgast.ParamRef(number=idx),
                    type_name=pgast.TypeName(
                        name=pg_types.pg_type_from_ir_typeref(param.ir_type)
                    )
                )))
            if targets:
                ctx.toplevel_stmt.append_cte(
                    pgast.CommonTableExpr(
                        name="__unused_vars",
                        query=pgast.SelectStmt(target_list=targets)
                    )
                )


def get_volatility_ref(
        path_id: irast.PathId, *,
        ctx: context.CompilerContextLevel) -> Optional[pgast.BaseExpr]:
    """Produce an appropriate volatility_ref from a path_id."""

    ref: Optional[pgast.BaseExpr] = relctx.maybe_get_path_var(
        ctx.rel, path_id, aspect='identity', ctx=ctx)
    if not ref:
        rvar = relctx.maybe_get_path_rvar(
            ctx.rel, path_id, aspect='value', ctx=ctx)
        if rvar and isinstance(rvar.query, pgast.ReturningQuery):
            # If we are selecting from a nontrivial subquery, manually
            # add a volatility ref based on row_number. We do it
            # manually because the row number isn't /really/ the
            # identity of the set.
            name = ctx.env.aliases.get('key')
            rvar.query.target_list.append(
                pgast.ResTarget(
                    name=name,
                    val=pgast.FuncCall(name=('row_number',), args=[],
                                       over=pgast.WindowDef())
                )
            )
            ref = pgast.ColumnRef(name=[rvar.alias.aliasname, name])
        else:
            ref = relctx.maybe_get_path_var(
                ctx.rel, path_id, aspect='value', ctx=ctx)

    return ref


def setup_iterator_volatility(
        iterator: Optional[Union[irast.Set, pgast.IteratorCTE]], *,
        is_cte: bool=False,
        ctx: context.CompilerContextLevel) -> None:
    if iterator is None:
        return

    old = () if is_cte else ctx.volatility_ref

    path_id = iterator.path_id
    ref: Optional[pgast.BaseExpr] = None

    # We use a callback scheme here to avoid inserting volatility ref
    # columns unless there is actually a volatile operation that
    # requires it.
    def get_ref(
            xctx: context.CompilerContextLevel) -> Optional[pgast.BaseExpr]:
        nonlocal ref
        if ref is None:
            ref = get_volatility_ref(path_id, ctx=xctx)
        return ref

    ctx.volatility_ref = old + (get_ref,)


def compile_materialized_exprs(
        query: pgast.SelectStmt, stmt: irast.Stmt, *,
        ctx: context.CompilerContextLevel) -> None:
    if not stmt.materialized_sets:
        return

    if stmt in ctx.materializing:
        return

    with context.output_format(ctx, context.OutputFormat.NATIVE), (
            ctx.new()) as matctx:
        matctx.materializing |= {stmt}
        matctx.expr_exposed = True

        for mat_set in stmt.materialized_sets.values():
            if len(mat_set.uses) <= 1:
                continue
            assert mat_set.materialized
            if relctx.find_rvar(
                    query, flavor='packed',
                    path_id=mat_set.materialized.path_id, ctx=matctx):
                continue

            mat_ids = set(mat_set.uses)

            # We pack optional things into arrays also, since it works.
            # TODO: use NULL?
            card = mat_set.cardinality
            is_singleton = card.is_single() and not card.can_be_zero()

            matctx.path_scope = matctx.path_scope.new_child()
            for mat_id in mat_ids:
                matctx.path_scope[mat_id] = None
            mat_qry = relgen.set_as_subquery(
                mat_set.materialized, as_value=True, ctx=matctx
            )

            if not is_singleton:
                mat_qry = relgen.set_to_array(
                    ir_set=mat_set.materialized,
                    query=mat_qry,
                    materializing=True,
                    ctx=matctx)

            if not mat_qry.target_list[0].name:
                mat_qry.target_list[0].name = ctx.env.aliases.get('v')

            ref = pgast.ColumnRef(name=[mat_qry.target_list[0].name])
            for mat_id in mat_ids:
                pathctx.put_path_packed_output(
                    mat_qry, mat_id, ref, multi=not is_singleton)

            mat_rvar = relctx.rvar_for_rel(mat_qry, lateral=True, ctx=matctx)
            for mat_id in mat_ids:
                relctx.include_rvar(
                    query, mat_rvar, path_id=mat_id,
                    flavor='packed', pull_namespace=False, ctx=matctx,
                )


def compile_iterator_expr(
        query: pgast.SelectStmt, iterator_expr: irast.Set, *,
        ctx: context.CompilerContextLevel) \
        -> pgast.PathRangeVar:

    assert isinstance(iterator_expr.expr, irast.SelectStmt)

    with ctx.new() as subctx:
        subctx.rel = query

        already_existed = bool(relctx.maybe_get_path_rvar(
            query, iterator_expr.path_id, aspect='value', ctx=ctx))
        dispatch.visit(iterator_expr, ctx=subctx)
        iterator_rvar = relctx.get_path_rvar(
            query, iterator_expr.path_id, aspect='value', ctx=ctx)
        iterator_query = iterator_rvar.query

        # Regardless of result type, we use transient identity,
        # for path identity of the iterator expression.  This is
        # necessary to maintain correct correlation for the state
        # of iteration in DML statements.
        # The already_existed check is to avoid adding in bogus volatility refs
        # when we reprocess an iterator that was hoisted.
        if not already_existed:
            relctx.ensure_bond_for_expr(
                iterator_expr.expr.result, iterator_query, type='uuid',
                ctx=subctx)

    return iterator_rvar


def compile_output(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> pgast.OutputVar:
    with ctx.new() as newctx:
        dispatch.visit(ir_set, ctx=newctx)

        path_id = ir_set.path_id

        if (output.in_serialization_ctx(ctx) and
                newctx.stmt is newctx.toplevel_stmt):
            val = pathctx.get_path_serialized_output(
                ctx.rel, path_id, env=ctx.env)
        else:
            val = pathctx.get_path_value_output(
                ctx.rel, path_id, env=ctx.env)

    return val


def compile_filter_clause(
        ir_set: irast.Set,
        cardinality: qltypes.Cardinality, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    where_clause: pgast.BaseExpr

    with ctx.new() as ctx1:
        ctx1.expr_exposed = False

        if (
            cardinality is qltypes.Cardinality.ONE
            or cardinality is qltypes.Cardinality.AT_MOST_ONE
        ):
            where_clause = dispatch.compile(ir_set, ctx=ctx)
        else:
            # In WHERE we compile ir.Set as a boolean disjunction:
            #    EXISTS(SELECT FROM SetRel WHERE SetRel.value)
            with ctx1.subrel() as subctx:
                dispatch.visit(ir_set, ctx=subctx)
                wrapper = subctx.rel
                wrapper.where_clause = pathctx.get_path_value_var(
                    wrapper, ir_set.path_id, env=subctx.env)

            where_clause = pgast.SubLink(
                type=pgast.SubLinkType.EXISTS,
                expr=wrapper
            )

    return where_clause


def compile_orderby_clause(
        ir_exprs: Sequence[irast.SortExpr], *,
        ctx: context.CompilerContextLevel) -> List[pgast.SortBy]:

    sort_clause = []

    for expr in ir_exprs:
        with ctx.new() as orderctx:
            orderctx.expr_exposed = False

            # In OPDER BY we compile ir.Set as a subquery:
            #    SELECT SetRel.value FROM SetRel)
            value = relgen.set_as_subquery(
                expr.expr, as_value=True, ctx=orderctx)

            sortexpr = pgast.SortBy(
                node=value,
                dir=expr.direction,
                nulls=expr.nones_order)
            sort_clause.append(sortexpr)

    return sort_clause


def compile_limit_offset_clause(
        ir_set: Optional[irast.Set], *,
        ctx: context.CompilerContextLevel) -> Optional[pgast.BaseExpr]:
    if ir_set is None:
        return None

    with ctx.new() as ctx1:
        ctx1.expr_exposed = False

        # In OFFSET/LIMIT we compile ir.Set as a subquery:
        #    SELECT SetRel.value FROM SetRel)
        limit_offset_clause = relgen.set_as_subquery(ir_set, ctx=ctx1)

    return limit_offset_clause
