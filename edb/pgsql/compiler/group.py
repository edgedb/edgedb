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

from typing import Optional, AbstractSet, List

from edb.edgeql import ast as qlast
from edb.edgeql import desugar_group
from edb.ir import ast as irast
from edb.ir import utils as irutils
from edb.pgsql import ast as pgast

from . import astutils
from . import clauses
from . import context
from . import dispatch
from . import enums as pgce
from . import output
from . import pathctx
from . import relctx
from . import relgen


def compile_grouping_atom(
    el: qlast.GroupingAtom,
    stmt: irast.GroupStmt, *, ctx: context.CompilerContextLevel
) -> pgast.Base:
    '''Compile a GroupingAtom into sql grouping sets'''
    if isinstance(el, qlast.GroupingIdentList):
        return pgast.GroupingOperation(
            args=[
                compile_grouping_atom(at, stmt, ctx=ctx) for at in el.elements
            ],
        )

    assert isinstance(el, qlast.ObjectRef)
    alias_set, _ = stmt.using[el.name]
    return pathctx.get_path_value_var(
        ctx.rel, alias_set.path_id, env=ctx.env)


def compile_grouping_el(
    el: qlast.GroupingElement,
    stmt: irast.GroupStmt, *, ctx: context.CompilerContextLevel
) -> pgast.Base:
    '''Compile a GroupingElement into sql grouping sets'''
    if isinstance(el, qlast.GroupingSets):
        return pgast.GroupingOperation(
            operation='GROUPING SETS',
            args=[compile_grouping_el(sub, stmt, ctx=ctx) for sub in el.sets],
        )
    elif isinstance(el, qlast.GroupingOperation):
        return pgast.GroupingOperation(
            operation=el.oper,
            args=[
                compile_grouping_atom(at, stmt, ctx=ctx) for at in el.elements
            ],
        )
    elif isinstance(el, qlast.GroupingSimple):
        return compile_grouping_atom(el.element, stmt, ctx=ctx)
    raise AssertionError('Unknown GroupingElement')


def _compile_grouping_value(
        stmt: irast.GroupStmt, used_args: AbstractSet[str], *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    '''Produce the value for the grouping binding saying what is grouped on'''
    assert stmt.grouping_binding
    grouprel = ctx.rel

    # If there is only one grouping set, hardcode the output
    if all(isinstance(b, qlast.GroupingSimple) for b in stmt.by):
        return pgast.ArrayExpr(
            elements=[
                pgast.StringConstant(val=desugar_group.key_name(arg))
                for arg in used_args
            ],
        )

    using = {k: stmt.using[k] for k in used_args}

    args = [
        pathctx.get_path_var(
            grouprel,
            alias_set.path_id,
            aspect=pgce.PathAspect.VALUE,
            env=ctx.env,
        )
        for alias_set, _ in using.values()
    ]

    # Call grouping on each element we group on to produce a bitmask
    grouping_alias = ctx.env.aliases.get('g')
    grouping_call = pgast.FuncCall(name=('grouping',), args=args)
    subq = pgast.SelectStmt(
        target_list=[
            pgast.ResTarget(name=grouping_alias, val=grouping_call),
        ]
    )
    q = pgast.SelectStmt(
        from_clause=[pgast.RangeSubselect(
            subquery=subq,
            alias=pgast.Alias(aliasname=ctx.env.aliases.get())
        )]
    )

    grouping_ref = pgast.ColumnRef(name=(grouping_alias,))

    # Generate a call to ARRAY[...] with a case for each grouping
    # element, then array_remove out the NULLs.
    els: List[pgast.BaseExpr] = []
    for i, name in enumerate(using):
        name = desugar_group.key_name(name)
        mask = 1 << (len(using) - i - 1)
        # (CASE (e & <mask>) WHEN 0 THEN '<name>' ELSE NULL END)

        els.append(pgast.CaseExpr(
            arg=pgast.Expr(
                name='&',
                lexpr=grouping_ref,
                rexpr=pgast.LiteralExpr(expr=str(mask))
            ),
            args=[
                pgast.CaseWhen(
                    expr=pgast.LiteralExpr(expr='0'),
                    result=pgast.StringConstant(val=name)
                )
            ],
            defresult=pgast.NullConstant()
        ))

    val = pgast.FuncCall(
        name=('array_remove',),
        args=[pgast.ArrayExpr(elements=els), pgast.NullConstant()]
    )

    q.target_list.append(pgast.ResTarget(val=val))

    return q


def _compile_grouping_binding(
        stmt: irast.GroupStmt, *, used_args: AbstractSet[str],
        ctx: context.CompilerContextLevel) -> None:
    assert stmt.grouping_binding
    pathctx.put_path_var(
        ctx.rel,
        stmt.grouping_binding.path_id,
        _compile_grouping_value(stmt, used_args=used_args, ctx=ctx),
        aspect=pgce.PathAspect.VALUE,
    )


def _compile_group(
        stmt: irast.GroupStmt, *,
        ctx: context.CompilerContextLevel,
        parent_ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    clauses.compile_volatile_bindings(stmt, ctx=ctx)

    query = ctx.stmt

    # Compile a GROUP BY into a subquery, along with all the aggregations
    with ctx.subrel() as groupctx:
        grouprel = groupctx.rel

        # First compile the actual subject
        # subrel *solely* for path id map reasons
        with groupctx.subrel() as subjctx:
            subjctx.expr_exposed = False

            dispatch.visit(stmt.subject, ctx=subjctx)
            if stmt.subject.path_id.is_objtype_path():
                # This shouldn't technically be needed but we generate
                # better code with it.
                relgen.ensure_source_rvar(
                    stmt.subject, subjctx.rel, ctx=subjctx)

        subj_rvar = relctx.rvar_for_rel(
            subjctx.rel, ctx=groupctx, lateral=True)
        aspects = pathctx.list_path_aspects(subjctx.rel, stmt.subject.path_id)

        pathctx.put_path_id_map(
            subjctx.rel,
            stmt.group_binding.path_id, stmt.subject.path_id)

        # update_mask=False because we are doing this solely to remap
        # elements individually and don't want to affect the mask.
        relctx.include_rvar(
            grouprel, subj_rvar, stmt.group_binding.path_id,
            aspects=aspects,
            update_mask=False, ctx=groupctx)
        relctx.include_rvar(
            grouprel, subj_rvar, stmt.subject.path_id,
            aspects=aspects,
            update_mask=False, ctx=groupctx)

        # Now we compile the bindings
        groupctx.path_scope = subjctx.path_scope.new_child()
        groupctx.path_scope[stmt.group_binding.path_id] = None
        if stmt.grouping_binding:
            groupctx.path_scope[stmt.grouping_binding.path_id] = None

        # Compile all the 'using' items
        for _alias, (value, using_card) in stmt.using.items():
            # If the using bit is nullable, we need to compile it
            # as optional, or we'll get in trouble.
            # TODO: Can we do better here and not do this
            # in obvious cases like directly referencing an optional
            # property.
            if using_card.can_be_zero():
                groupctx.force_optional = ctx.force_optional | {value.path_id}
            groupctx.path_scope[value.path_id] = None

            dispatch.visit(value, ctx=groupctx)
            groupctx.force_optional = ctx.force_optional

        # Compile all of the aggregate calls that we found. This lets us
        # compute things like sum and count without needing to materialize
        # the result.
        for group_use, skippable in stmt.group_aggregate_sets.items():
            if not group_use:
                continue
            with groupctx.subrel() as hoistctx:
                hoistctx.skippable_sources |= skippable

                assert irutils.is_set_instance(group_use, irast.FunctionCall)
                relgen.process_set_as_agg_expr_inner(
                    group_use,
                    aspect=pgce.PathAspect.VALUE,
                    wrapper=None,
                    for_group_by=True,
                    ctx=hoistctx,
                )
                pathctx.get_path_value_output(
                    rel=hoistctx.rel, path_id=group_use.path_id, env=ctx.env)
                pathctx.put_path_value_var(
                    grouprel, group_use.path_id, hoistctx.rel
                )

        packed = False
        # Materializing the actual grouping sets
        if None in stmt.group_aggregate_sets:
            packed = True
            # TODO: Be able to directly output the final serialized version
            # if it is consumed directly.
            with context.output_format(ctx, context.OutputFormat.NATIVE), (
                    groupctx.new()) as matctx:
                matctx.materializing |= {stmt}
                matctx.expr_exposed = True

                mat_qry = relgen.set_as_subquery(
                    stmt.group_binding, as_value=True, ctx=matctx)
                mat_qry = relctx.set_to_array(
                    path_id=stmt.group_binding.path_id,
                    for_group_by=True,
                    query=mat_qry,
                    ctx=matctx)
                if not mat_qry.target_list[0].name:
                    mat_qry.target_list[0].name = ctx.env.aliases.get('v')

                ref = pgast.ColumnRef(
                    name=[mat_qry.target_list[0].name],
                    is_packed_multi=True,
                )
                pathctx.put_path_packed_output(
                    mat_qry, stmt.group_binding.path_id, ref)

                pathctx.put_path_var(
                    grouprel,
                    stmt.group_binding.path_id,
                    mat_qry,
                    aspect=pgce.PathAspect.VALUE,
                    flavor='packed',
                )

        used_args = desugar_group.collect_grouping_atoms(stmt.by)

        if stmt.grouping_binding:
            _compile_grouping_binding(stmt, used_args=used_args, ctx=groupctx)

        # We want to make sure that every grouping key is associated
        # with exactly one output from the query. The means that
        # tuples must be packed up and keys must not have an extra
        # serialized output.
        #
        # We do this by manually packing up any TupleVarBases and
        # copying value aspects to serialized.
        # of the grouping keys get an extra serialized output from
        # grouprel, so we just copy all their value aspects to their
        # serialized aspects.
        using = {k: stmt.using[k] for k in used_args}
        for using_val, _ in using.values():
            uvar = pathctx.get_path_var(
                grouprel,
                using_val.path_id,
                aspect=pgce.PathAspect.VALUE,
                env=ctx.env,
            )
            if isinstance(uvar, pgast.TupleVarBase):
                uvar = output.output_as_value(uvar, env=ctx.env)
                pathctx.put_path_var(
                    grouprel,
                    using_val.path_id,
                    uvar,
                    aspect=pgce.PathAspect.VALUE,
                    force=True,
                )

            uout = pathctx.get_path_output(
                grouprel,
                using_val.path_id,
                aspect=pgce.PathAspect.VALUE,
                env=ctx.env,
            )
            pathctx._put_path_output_var(
                grouprel,
                using_val.path_id,
                pgce.PathAspect.SERIALIZED,
                uout,
            )

        grouprel.group_clause = [
            compile_grouping_el(el, stmt, ctx=groupctx) for el in stmt.by
        ]

    group_rvar = relctx.rvar_for_rel(grouprel, ctx=ctx, lateral=True)
    if packed:
        relctx.include_rvar(
            query, group_rvar, path_id=stmt.group_binding.path_id,
            flavor='packed', update_mask=False, pull_namespace=False,
            aspects=(pgce.PathAspect.VALUE,),
            ctx=ctx)
    else:
        # Not include_rvar because we don't actually provide the path id!
        relctx.rel_join(query, group_rvar, ctx=ctx)

    # Set up the hoisted aggregates and bindings to be found
    # in the group subquery.
    for group_use in [
        *stmt.group_aggregate_sets,
        *[x for x, _ in stmt.using.values()],
        stmt.grouping_binding,
    ]:
        if group_use:
            pathctx.put_path_rvar(
                query,
                group_use.path_id,
                group_rvar,
                aspect=pgce.PathAspect.VALUE,
            )

    vol_ref = None

    def _get_volatility_ref() -> Optional[pgast.BaseExpr]:
        nonlocal vol_ref
        if vol_ref:
            return vol_ref

        name = ctx.env.aliases.get('key')
        grouprel.target_list.append(
            pgast.ResTarget(
                name=name,
                val=pgast.FuncCall(name=('row_number',), args=[],
                                   over=pgast.WindowDef())
            )
        )
        vol_ref = pgast.ColumnRef(name=[group_rvar.alias.aliasname, name])
        return vol_ref

    with ctx.new() as outctx:
        # Inherit the path_scope we made earlier (with the GROUP bindings
        # removed), so that we'll always look for those in the right place.
        outctx.path_scope = groupctx.path_scope

        outctx.volatility_ref += (lambda stmt, xctx: _get_volatility_ref(),)

        # Process materialized sets
        clauses.compile_materialized_exprs(query, stmt, ctx=outctx)

        clauses.compile_output(stmt.result, ctx=outctx)

    with ctx.new() as ictx:
        ictx.path_scope = groupctx.path_scope

        # FILTER and ORDER BY need to have the base result as a
        # volatility ref.
        clauses.setup_iterator_volatility(stmt.result, ctx=ictx)

        # The FILTER clause.
        if stmt.where is not None:
            query.where_clause = astutils.extend_binop(
                query.where_clause,
                clauses.compile_filter_clause(
                    stmt.where, stmt.where_card, ctx=ictx))

        # The ORDER BY clause
        if stmt.orderby is not None:
            with ictx.new() as octx:
                query.sort_clause = clauses.compile_orderby_clause(
                    stmt.orderby, ctx=octx)

    return query


def compile_group(
        stmt: irast.GroupStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    with ctx.substmt() as sctx:
        return _compile_group(stmt, ctx=sctx, parent_ctx=ctx)
