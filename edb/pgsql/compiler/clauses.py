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

from typing import Optional, Union, Sequence, List

import random

from edb.common import ast as ast_visitor

from edb.edgeql import qltypes
from edb.ir import ast as irast
from edb.ir import utils as irutils
from edb.pgsql import ast as pgast
from edb.pgsql import types as pg_types

from . import astutils
from . import context
from . import dispatch
from . import dml
from . import enums as pgce
from . import output
from . import pathctx
from . import relctx
from . import relgen


def get_volatility_ref(
        path_id: irast.PathId,
        stmt: pgast.SelectStmt,
        *,
        ctx: context.CompilerContextLevel) -> Optional[pgast.BaseExpr]:
    """Produce an appropriate volatility_ref from a path_id."""

    ref: Optional[pgast.BaseExpr] = relctx.maybe_get_path_var(
        stmt, path_id, aspect=pgce.PathAspect.ITERATOR, ctx=ctx)
    if not ref:
        ref = relctx.maybe_get_path_var(
            stmt, path_id, aspect=pgce.PathAspect.IDENTITY, ctx=ctx)
    if not ref:
        rvar = relctx.maybe_get_path_rvar(
            stmt, path_id, aspect=pgce.PathAspect.VALUE, ctx=ctx)
        if (
            rvar
            and isinstance(rvar.query, pgast.ReturningQuery)
            # Expanded inhviews might be unions, which can't naively have
            # a row_number stuck on; they should be safe to just grab
            # the path_id value from, though
            and rvar.tag != 'expanded-inhview'
        ):
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
                stmt, path_id, aspect=pgce.PathAspect.VALUE, ctx=ctx)

    return ref


def setup_iterator_volatility(
        iterator: Optional[Union[irast.Set, pgast.IteratorCTE]], *,
        ctx: context.CompilerContextLevel) -> None:
    if iterator is None:
        return

    path_id = iterator.path_id

    # We use a callback scheme here to avoid inserting volatility ref
    # columns unless there is actually a volatile operation that
    # requires it.
    ctx.volatility_ref += (
        lambda stmt, xctx: get_volatility_ref(path_id, stmt, ctx=xctx),)


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

        # HACK: Sort longer paths before shorter ones
        # We want foo->bar to appear before foo
        mat_sets = sorted(
            (stmt.materialized_sets.values()),
            key=lambda m: -len(m.materialized.path_id),
        )

        for mat_set in mat_sets:
            if len(mat_set.uses) <= 1:
                continue
            assert mat_set.finalized, "materialized set was not finalized!"
            if relctx.find_rvar(
                    query, flavor='packed',
                    path_id=mat_set.materialized.path_id, ctx=matctx):
                continue

            _compile_materialized_expr(query, mat_set, ctx=matctx)


def _compile_materialized_expr(
    query: pgast.SelectStmt,
    mat_set: irast.MaterializedSet,
    *,
    ctx: context.CompilerContextLevel,
) -> None:
    mat_ids = set(mat_set.uses)

    # We pack optional things into arrays also, since it works.
    # TODO: use NULL?
    card = mat_set.cardinality
    assert card != qltypes.Cardinality.UNKNOWN
    is_singleton = card.is_single() and not card.can_be_zero()

    old_scope = ctx.path_scope
    ctx.path_scope = old_scope.new_child()
    for mat_id in mat_ids:
        for k in old_scope:
            if k.startswith(mat_id):
                ctx.path_scope[k] = None
    mat_qry = relgen.set_as_subquery(
        mat_set.materialized, as_value=True, ctx=ctx
    )

    if not is_singleton:
        mat_qry = relctx.set_to_array(
            path_id=mat_set.materialized.path_id,
            query=mat_qry,
            ctx=ctx)

    if not mat_qry.target_list[0].name:
        mat_qry.target_list[0].name = ctx.env.aliases.get('v')

    ref = pgast.ColumnRef(
        name=[mat_qry.target_list[0].name],
        is_packed_multi=not is_singleton,
    )
    for mat_id in mat_ids:
        pathctx.put_path_packed_output(mat_qry, mat_id, ref)

    mat_rvar = relctx.rvar_for_rel(mat_qry, lateral=True, ctx=ctx)
    for mat_id in mat_ids:
        relctx.include_rvar(
            query, mat_rvar, path_id=mat_id,
            flavor='packed', update_mask=False, pull_namespace=False,
            ctx=ctx,
        )


def compile_iterator_expr(
        query: pgast.SelectStmt, iterator_expr: irast.Set, *,
        is_dml: bool,
        ctx: context.CompilerContextLevel) \
        -> pgast.PathRangeVar:

    assert isinstance(iterator_expr.expr, (irast.GroupStmt, irast.SelectStmt))

    with ctx.new() as subctx:
        subctx.expr_exposed = False
        subctx.rel = query

        dispatch.visit(iterator_expr, ctx=subctx)
        iterator_rvar = relctx.get_path_rvar(
            query,
            iterator_expr.path_id,
            aspect=pgce.PathAspect.VALUE,
            ctx=ctx,
        )
        iterator_query = iterator_rvar.query

        # If the iterator value is nullable, add a null test. This
        # makes sure that we don't spuriously produce output when
        # iterating over optional pointers.
        is_optional = ctx.scope_tree.is_optional(iterator_expr.path_id)
        if isinstance(iterator_query, pgast.SelectStmt):
            iterator_var = pathctx.get_path_value_var(
                iterator_query, path_id=iterator_expr.path_id, env=ctx.env)
        if not is_optional:
            if isinstance(iterator_query, pgast.SelectStmt):
                iterator_var = pathctx.get_path_value_var(
                    iterator_query, path_id=iterator_expr.path_id, env=ctx.env)
                if iterator_var.nullable:
                    iterator_query.where_clause = astutils.extend_binop(
                        iterator_query.where_clause,
                        pgast.NullTest(arg=iterator_var, negated=True))
            elif isinstance(iterator_query, pgast.Relation):
                # will never be null
                pass
            else:
                raise NotImplementedError()

        # For DML-containing FOR, regardless of result type, iterators need
        # their own transient identity for path identity of the
        # iterator expression in order maintain correct correlation
        # for the state of iteration in DML statements, even when
        # there are duplicates in the iterator.  This gets tracked as
        # a special ITERATOR aspect in order to distinguish it from
        # actual object identity.
        #
        # We also do this for optional iterators, since object
        # identity isn't safe to use as a volatility ref if the object
        # might be NULL.
        if is_dml or is_optional:
            relctx.create_iterator_identity_for_path(
                iterator_expr.path_id, iterator_query,
                apply_volatility=is_dml,
                ctx=subctx)

            pathctx.put_path_rvar(
                query,
                iterator_expr.path_id,
                iterator_rvar,
                aspect=pgce.PathAspect.ITERATOR,
            )

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


def compile_volatile_bindings(
    stmt: irast.Stmt,
    *,
    ctx: context.CompilerContextLevel
) -> None:
    for binding, volatility in (stmt.bindings or ()):
        # If something we are WITH binding contains DML, we want to
        # compile it *now*, in the context of its initial appearance
        # and not where the variable is used.
        #
        # Similarly, if something we are WITH binding is volatile and the stmt
        # contains dml, we similarly want to compile it *now*.

        # If the binding is a with binding for a DML stmt, manually construct
        # the CTEs.
        #
        # Note: This condition is checked first, because if the binding
        # *references* DML then contains_dml is true. If the binding is compiled
        # normally, since the referenced DML was already compiled, the rvar will
        # be retrieved, and no CTEs will be set up.
        if volatility.is_volatile() and irutils.contains_dml(stmt):
            _compile_volatile_binding_for_dml(stmt, binding, ctx=ctx)

        # For typical DML, just compile it. This will populate dml_stmts with
        # the CTEs, which will be picked up when the variable is referenced.
        elif irutils.contains_dml(binding):
            with ctx.substmt() as bctx:
                dispatch.compile(binding, ctx=bctx)


def _compile_volatile_binding_for_dml(
    stmt: irast.Stmt,
    binding: irast.Set,
    *,
    ctx: context.CompilerContextLevel
) -> None:
    materialized_set = None
    if (
        stmt.materialized_sets
        and binding.typeref.id in stmt.materialized_sets
    ):
        materialized_set = stmt.materialized_sets[binding.typeref.id]
    assert materialized_set is not None

    last_iterator = ctx.enclosing_cte_iterator

    with (
        context.output_format(ctx, context.OutputFormat.NATIVE),
        ctx.newrel() as matctx
    ):
        matctx.materializing |= {stmt}
        matctx.expr_exposed = True

        dml.merge_iterator(last_iterator, matctx.rel, ctx=matctx)
        setup_iterator_volatility(last_iterator, ctx=matctx)

        _compile_materialized_expr(
            matctx.rel, materialized_set, ctx=matctx
        )

        # Add iterator identity
        bind_pathid = (
            irast.PathId.new_dummy(ctx.env.aliases.get('bind_path'))
        )
        with matctx.subrel() as bind_pathid_ctx:
            relctx.create_iterator_identity_for_path(
                bind_pathid, bind_pathid_ctx.rel, ctx=bind_pathid_ctx
            )
        bind_id_rvar = relctx.rvar_for_rel(
            bind_pathid_ctx.rel, lateral=True, ctx=matctx
        )
        relctx.include_rvar(
            matctx.rel, bind_id_rvar, path_id=bind_pathid, ctx=matctx
        )

    bind_cte = pgast.CommonTableExpr(
        name=ctx.env.aliases.get('bind'),
        query=matctx.rel,
        materialized=False,
    )

    bind_iterator = pgast.IteratorCTE(
        path_id=bind_pathid,
        cte=bind_cte,
        parent=last_iterator,
        iterator_bond=True,
    )
    ctx.toplevel_stmt.append_cte(bind_cte)

    # Merge the new iterator
    ctx.path_scope = ctx.path_scope.new_child()
    dml.merge_iterator(bind_iterator, ctx.rel, ctx=ctx)
    setup_iterator_volatility(bind_iterator, ctx=ctx)

    ctx.enclosing_cte_iterator = bind_iterator


def compile_filter_clause(
        ir_set: irast.Set,
        cardinality: qltypes.Cardinality, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    where_clause: pgast.BaseExpr

    with ctx.new() as ctx1:
        ctx1.expr_exposed = False

        assert cardinality != qltypes.Cardinality.UNKNOWN
        if cardinality.is_single():
            where_clause = dispatch.compile(ir_set, ctx=ctx1)
        else:
            # In WHERE we compile ir.Set as a boolean disjunction:
            #    EXISTS(SELECT FROM SetRel WHERE SetRel.value)
            with ctx1.subrel() as subctx:
                dispatch.visit(ir_set, ctx=subctx)
                wrapper = subctx.rel
                wrapper.where_clause = pathctx.get_path_value_var(
                    wrapper, ir_set.path_id, env=subctx.env)

            where_clause = pgast.SubLink(operator="EXISTS", expr=wrapper)

    return where_clause


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
            value = astutils.collapse_query(subq)

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


def make_check_scan(
    check_cte: pgast.CommonTableExpr,
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.BaseExpr:
    return pgast.SelectStmt(
        target_list=[
            pgast.ResTarget(
                val=pgast.FuncCall(name=('count',), args=[pgast.Star()]),
            )
        ],
        from_clause=[
            relctx.rvar_for_rel(check_cte, ctx=ctx),
        ],
    )


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
        check = make_check_scan(check_cte, ctx=ctx)
        val = pgast.Expr(name="+", lexpr=val, rexpr=check)

    update_query = pgast.UpdateStmt(
        targets=[pgast.UpdateTarget(
            name='flag', val=pgast.BooleanConstant(val=True)
        )],
        relation=pgast.RelRangeVar(relation=pgast.Relation(
            schemaname='edgedb', name='_dml_dummy')),
        where_clause=pgast.Expr(
            name="=",
            lexpr=pgast.ColumnRef(name=["id"]),
            rexpr=val,
        )
    )
    stmt.append_cte(pgast.CommonTableExpr(
        query=update_query,
        name=ctx.env.aliases.get(hint='check_scan')
    ))


def insert_ctes(
    stmt: pgast.Query, ctx: context.CompilerContextLevel
) -> None:
    if stmt.ctes is None:
        stmt.ctes = []
    stmt.ctes[:0] = [
        *ctx.param_ctes.values(),
        *ctx.ptr_inheritance_ctes.values(),
        *ctx.ordered_type_ctes,
    ]


def fini_toplevel(
        stmt: pgast.Query, ctx: context.CompilerContextLevel) -> None:

    scan_check_ctes(stmt, ctx.env.check_ctes, ctx=ctx)

    # Type rewrites and inheritance CTEs go first.
    insert_ctes(stmt, ctx)

    if ctx.env.named_param_prefix is None:
        # Adding unused parameters into a CTE

        # Find the used parameters by searching the query, so we don't
        # get confused if something has been compiled but then omitted
        # from the output for some reason.
        param_refs = ast_visitor.find_children(stmt, pgast.ParamRef)

        used = {param_ref.number for param_ref in param_refs}

        targets = []
        for param in ctx.env.query_params:
            pgparam = ctx.argmap[param.name]
            if pgparam.index in used or param.sub_params:
                continue
            targets.append(pgast.ResTarget(val=pgast.TypeCast(
                arg=pgast.ParamRef(number=pgparam.index),
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


def populate_argmap(
    params: List[irast.Param],
    globals: List[irast.Global],
    *,
    ctx: context.CompilerContextLevel,
) -> None:
    physical_index = 1
    logical_index = 1
    for map_extra in (False, True):
        for param in params:
            if (
                ctx.env.named_param_prefix is not None
                and not param.name.isdecimal()
            ):
                continue
            if param.name.startswith('__edb_arg_') != map_extra:
                continue

            ctx.argmap[param.name] = pgast.Param(
                index=physical_index,
                logical_index=logical_index,
                required=param.required,
            )
            if not param.sub_params:
                physical_index += 1
            if not param.is_sub_param:
                logical_index += 1
    for param in globals:
        ctx.argmap[param.name] = pgast.Param(
            index=physical_index,
            required=param.required,
            logical_index=-1,
        )
        physical_index += 1
        if param.has_present_arg:
            ctx.argmap[param.name + "present__"] = pgast.Param(
                index=physical_index,
                required=True,
                logical_index=-1,
            )
            physical_index += 1
