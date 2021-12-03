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

import random

from edb.edgeql import qltypes
from edb.ir import ast as irast
from edb.pgsql import ast as pgast
from edb.pgsql import types as pg_types

from . import astutils
from . import context
from . import dispatch
from . import output
from . import pathctx
from . import relctx
from . import relgen


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
            assert mat_set.finalized
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
                mat_qry = relctx.set_to_array(
                    path_id=mat_set.materialized.path_id,
                    query=mat_qry,
                    materializing=True,
                    ctx=matctx)

            if not mat_qry.target_list[0].name:
                mat_qry.target_list[0].name = ctx.env.aliases.get('v')

            ref = pgast.ColumnRef(
                name=[mat_qry.target_list[0].name],
                is_packed_multi=not is_singleton,
            )
            for mat_id in mat_ids:
                pathctx.put_path_packed_output(mat_qry, mat_id, ref)

            mat_rvar = relctx.rvar_for_rel(mat_qry, lateral=True, ctx=matctx)
            for mat_id in mat_ids:
                relctx.include_rvar(
                    query, mat_rvar, path_id=mat_id,
                    flavor='packed', update_mask=False, pull_namespace=False,
                    ctx=matctx,
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

        # If the iterator value is nullable, add a null test. This
        # makes sure that we don't spuriously produce output when
        # iterating over options pointers.
        assert isinstance(iterator_query, pgast.SelectStmt)
        iterator_var = pathctx.get_path_value_var(
            iterator_query, path_id=iterator_expr.path_id, env=ctx.env)
        if iterator_var.nullable:
            iterator_query.where_clause = astutils.extend_binop(
                iterator_query.where_clause,
                pgast.NullTest(arg=iterator_var, negated=True))

        # Regardless of result type, we use transient identity,
        # for path identity of the iterator expression.  This is
        # necessary to maintain correct correlation for the state
        # of iteration in DML statements.
        # The already_existed check is to avoid adding in bogus volatility refs
        # when we reprocess an iterator that was hoisted.
        if not already_existed:
            relctx.ensure_bond_for_expr(
                iterator_expr.expr.result, iterator_query, ctx=subctx)

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

        if cardinality.is_single():
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


def _get_target_from_range(
    target: pgast.BaseExpr, rvar: pgast.BaseRangeVar
) -> Optional[pgast.BaseExpr]:
    """Try to read a target out of a very simple rvar.

    The goal here is to allow collapsing trivial pass-through subqueries.
    In particular, given a target `foo.bar` and an rvar
    `(SELECT <expr> as "bar") AS "foo"`, we produce <expr>.

    We can also recursively handle the nested case.
    """
    if (
        not isinstance(rvar, pgast.RangeSubselect)

        # Check that the relation name matches the rvar
        or not isinstance(target, pgast.ColumnRef)
        or not target.name
        or target.name[0] != rvar.alias.aliasname

        # And that the rvar is a simple subquery with one target
        # and at most one from clause
        or not (subq := rvar.subquery)
        or len(subq.target_list) != 1
        or not isinstance(subq, pgast.SelectStmt)
        or not astutils.select_is_simple(subq)
        or len(subq.from_clause) > 1

        # And that the one target matches
        or not (inner_tgt := rvar.subquery.target_list[0])
        or inner_tgt.name != target.name[1]
    ):
        return None

    if subq.from_clause:
        return _get_target_from_range(inner_tgt.val, subq.from_clause[0])
    else:
        return inner_tgt.val


def collapse_query(query: pgast.Query) -> pgast.BaseExpr:
    """Try to collapse trivial queries into simple expressions.

    In particular, we want to transform
    `(SELECT foo.bar FROM LATERAL (SELECT <expr> as "bar") AS "foo")`
    into simply `<expr>`.
    """
    if not isinstance(query, pgast.SelectStmt):
        return query

    if (
        not isinstance(query, pgast.SelectStmt)
        or len(query.target_list) != 1
        or len(query.from_clause) != 1
    ):
        return query

    val = _get_target_from_range(
        query.target_list[0].val, query.from_clause[0])
    if val:
        return val
    else:
        return query


def compile_orderby_clause(
        ir_exprs: Sequence[irast.SortExpr], *,
        ctx: context.CompilerContextLevel) -> List[pgast.SortBy]:

    sort_clause = []

    for expr in ir_exprs:
        with ctx.new() as orderctx:
            orderctx.expr_exposed = False

            # In ORDER BY we compile ir.Set as a subquery:
            #    SELECT SetRel.value FROM SetRel)
            subq = relgen.set_as_subquery(
                expr.expr, as_value=True, ctx=orderctx)
            # pg apparently can't use indexes for ordering if the body
            # of an ORDER BY is a subquery, so try to collapse the query
            # into a simple expression.
            value = collapse_query(subq)

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


def scan_check_ctes(
    stmt: pgast.Query,
    check_ctes: List[pgast.CommonTableExpr],
    *,
    ctx: context.CompilerContextLevel,
) -> None:
    if not check_ctes:
        return

    # Scan all of the check CTEs to enforce constraints that are
    # checked as explicit queries and not Postgres constraints or
    # triggers.

    # To make sure that Postgres can't optimize the checks away, we
    # reference them in the where clause of an UPDATE to a dummy
    # table.

    # Add a big random number, so that different queries should try to
    # access different "rows" of the table, in case that matters.
    base_int = random.randint(0, (1 << 60) - 1)
    val: pgast.BaseExpr = pgast.NumericConstant(val=str(base_int))

    for check_cte in check_ctes:
        # We want the CTE to be MATERIALIZED, because otherwise
        # Postgres might not fully evaluate all its columns when
        # scanning it.
        check_cte.materialized = True
        check = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=pgast.FuncCall(name=('count',), args=[pgast.Star()]),
                )
            ],
            from_clause=[
                relctx.rvar_for_rel(check_cte, ctx=ctx),
            ],
        )
        val = pgast.Expr(
            kind=pgast.ExprKind.OP, name='+', lexpr=val, rexpr=check)

    update_query = pgast.UpdateStmt(
        targets=[pgast.UpdateTarget(
            name='flag', val=pgast.BooleanConstant(val='true')
        )],
        relation=pgast.RelRangeVar(relation=pgast.Relation(
            schemaname='edgedb', name='_dml_dummy')),
        where_clause=pgast.Expr(
            kind=pgast.ExprKind.OP, name='=',
            lexpr=pgast.ColumnRef(name=['id']),
            rexpr=val,
        )
    )
    stmt.append_cte(pgast.CommonTableExpr(
        query=update_query,
        name=ctx.env.aliases.get(hint='check_scan')
    ))


def fini_toplevel(
        stmt: pgast.Query, ctx: context.CompilerContextLevel) -> None:

    scan_check_ctes(stmt, ctx.env.check_ctes, ctx=ctx)

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
            stmt.append_cte(
                pgast.CommonTableExpr(
                    name="__unused_vars",
                    query=pgast.SelectStmt(target_list=targets)
                )
            )
