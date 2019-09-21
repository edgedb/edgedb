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

from typing import *  # NoQA

from edb.ir import ast as irast
from edb.ir import utils as irutils

from edb.pgsql import ast as pgast

from . import astutils
from . import clauses
from . import context
from . import dispatch
from . import dml
from . import pathctx
from . import relctx


@dispatch.compile.register(irast.SelectStmt)
def compile_SelectStmt(
        stmt: irast.SelectStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    if ctx.singleton_mode:
        return dispatch.compile(stmt.result, ctx=ctx)

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common setup.
        clauses.init_stmt(stmt, ctx=ctx, parent_ctx=parent_ctx)

        query = ctx.stmt

        if not isinstance(stmt.result.expr, irast.MutatingStmt):
            iterators = irutils.get_iterator_sets(stmt)
            for iterator_set in iterators:
                # Process FOR clause.
                clauses.compile_iterator_expr(query, iterator_set, ctx=ctx)

        # Process the result expression;
        outvar = clauses.compile_output(stmt.result, ctx=ctx)

        # The FILTER clause.
        if stmt.where is not None:
            query.where_clause = astutils.extend_binop(
                query.where_clause,
                clauses.compile_filter_clause(
                    stmt.where, stmt.where_card, ctx=ctx))

        if outvar.nullable and query is ctx.toplevel_stmt:
            # A nullable var has bubbled up to the top,
            # filter out NULLs.
            valvar: pgast.BaseExpr = pathctx.get_path_value_var(
                query, stmt.result.path_id, env=ctx.env)
            if isinstance(valvar, pgast.TupleVar):
                valvar = pgast.ImplicitRowExpr(
                    args=[e.val for e in valvar.elements])

            query.where_clause = astutils.extend_binop(
                query.where_clause,
                pgast.NullTest(arg=valvar, negated=True)
            )

        # The ORDER BY clause
        query.sort_clause = clauses.compile_orderby_clause(
            stmt.orderby, ctx=ctx)

        # The OFFSET clause
        query.limit_offset = clauses.compile_limit_offset_clause(
            stmt.offset, ctx=ctx)

        # The LIMIT clause
        query.limit_count = clauses.compile_limit_offset_clause(
            stmt.limit, ctx=ctx)

        clauses.fini_stmt(query, ctx, parent_ctx)

    return query


@dispatch.compile.register(irast.GroupStmt)
def compile_GroupStmt(
        stmt: irast.GroupStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        clauses.init_stmt(stmt, ctx=ctx, parent_ctx=parent_ctx)

        group_path_id = stmt.group_path_id

        # Process the GROUP .. BY part into a subquery.
        with ctx.subrel() as gctx:
            gctx.expr_exposed = False
            gquery = gctx.rel
            pathctx.put_path_bond(gquery, group_path_id)
            relctx.update_scope(stmt.subject, gquery, ctx=gctx)
            stmt.subject.path_scope = None
            clauses.compile_output(stmt.subject, ctx=gctx)
            subj_rvar = pathctx.get_path_rvar(
                gquery, stmt.subject.path_id, aspect='value', env=gctx.env)
            relctx.ensure_bond_for_expr(
                stmt.subject, subj_rvar.query, ctx=gctx)

            group_paths = set()

            part_clause: List[pgast.BaseExpr] = []

            for ir_gexpr in stmt.groupby:
                with gctx.new() as subctx:
                    partexpr = dispatch.compile(ir_gexpr, ctx=subctx)

                part_clause.append(partexpr)
                group_paths.add(ir_gexpr.path_id)

            # Since we will be computing arbitrary expressions
            # based on the grouped sets, it is more efficient
            # to compute the "group bond" as a small unique
            # value than it is to use GROUP BY and aggregate
            # actual id values into an array.
            #
            # To achieve this we use the first_value() window
            # function while using the GROUP BY clause as
            # a partition clause.  We use the id of the first
            # object in each partition if GROUP BY input is
            # a ObjectType, otherwise we generate the id using
            # row_number().
            if stmt.subject.path_id.is_objtype_path():
                first_val = pathctx.get_path_identity_var(
                    gquery, stmt.subject.path_id, env=ctx.env)
            else:
                with ctx.subrel() as subctx:
                    wrapper = subctx.rel

                    gquery_rvar = relctx.rvar_for_rel(gquery, ctx=subctx)
                    wrapper.from_clause = [gquery_rvar]
                    relctx.pull_path_namespace(
                        target=wrapper, source=gquery_rvar, ctx=subctx)

                    new_part_clause: List[pgast.BaseExpr] = []

                    for i, expr in enumerate(part_clause):
                        path_id = stmt.groupby[i].path_id
                        pathctx.put_path_value_var(
                            gquery, path_id, expr, force=True, env=ctx.env)
                        output_ref = pathctx.get_path_value_output(
                            gquery, path_id, env=ctx.env)
                        assert isinstance(output_ref, pgast.ColumnRef)
                        new_part_clause.append(
                            astutils.get_column(gquery_rvar, output_ref)
                        )

                    part_clause = new_part_clause

                    first_val = pathctx.get_rvar_path_identity_var(
                        gquery_rvar, stmt.subject.path_id, env=ctx.env)

                    gquery = wrapper
                    pathctx.put_path_bond(gquery, group_path_id)

            group_id = pgast.FuncCall(
                name=('first_value',),
                args=[first_val],
                over=pgast.WindowDef(
                    partition_clause=part_clause
                )
            )

            pathctx.put_path_identity_var(
                gquery, group_path_id, group_id, env=ctx.env)

            pathctx.put_path_value_var(
                gquery, group_path_id, group_id, env=ctx.env)

        group_cte = pgast.CommonTableExpr(
            query=gquery,
            name=ctx.env.aliases.get('g')
        )

        group_cte_rvar = relctx.rvar_for_rel(group_cte, ctx=ctx)

        # Generate another subquery contaning distinct values of
        # path expressions in BY.
        with ctx.subrel() as gvctx:
            gvquery = gvctx.rel
            relctx.include_rvar(
                gvquery, group_cte_rvar, path_id=group_path_id, ctx=gvctx)

            pathctx.put_path_bond(gvquery, group_path_id)

            for group_set in stmt.groupby:
                dispatch.visit(group_set, ctx=gvctx)
                path_id = group_set.path_id
                if path_id.is_objtype_path():
                    pathctx.put_path_bond(gvquery, path_id)

            gvquery.distinct_clause = [
                pathctx.get_path_identity_var(
                    gvquery, group_path_id, env=ctx.env)
            ]

            for path_id, aspect in list(gvquery.path_rvar_map):
                if path_id not in group_paths and path_id != group_path_id:
                    gvquery.path_rvar_map.pop((path_id, aspect))

            for path_id, aspect in list(gquery.path_rvar_map):
                if path_id in group_paths:
                    gquery.path_rvar_map.pop((path_id, aspect))
                    gquery.path_namespace.pop((path_id, aspect), None)
                    gquery.path_outputs.pop((path_id, aspect), None)

        groupval_cte = pgast.CommonTableExpr(
            query=gvquery,
            name=ctx.env.aliases.get('gv')
        )

        groupval_cte_rvar = relctx.rvar_for_rel(groupval_cte, ctx=ctx)

        o_stmt = stmt.result.expr
        assert isinstance(o_stmt, irast.SelectStmt)

        # process the result expression;
        with ctx.subrel() as selctx:
            selquery = selctx.rel
            outer_id = stmt.result.path_id
            inner_id = o_stmt.result.path_id

            relctx.include_specific_rvar(
                selquery, groupval_cte_rvar, group_path_id,
                aspects=['identity'], ctx=ctx)

            for path_id in group_paths:
                selctx.path_scope[path_id] = selquery
                pathctx.put_path_rvar(selquery, path_id, groupval_cte_rvar,
                                      aspect='value', env=ctx.env)

            selctx.group_by_rels = selctx.group_by_rels.copy()
            selctx.group_by_rels[group_path_id, stmt.subject.path_id] = \
                group_cte

            selquery.view_path_id_map = {
                outer_id: inner_id
            }

            selquery.ctes.append(group_cte)

            sortoutputs = []

            selquery.ctes.append(groupval_cte)

            clauses.compile_output(o_stmt.result, ctx=selctx)

            # The WHERE clause
            if o_stmt.where is not None:
                selquery.where_clause = astutils.extend_binop(
                    selquery.where_clause,
                    clauses.compile_filter_clause(
                        o_stmt.where, o_stmt.where_card, ctx=selctx))

            for ir_sortexpr in o_stmt.orderby:
                alias = ctx.env.aliases.get('s')
                sexpr = dispatch.compile(ir_sortexpr.expr, ctx=selctx)
                selquery.target_list.append(
                    pgast.ResTarget(
                        val=sexpr,
                        name=alias
                    )
                )
                sortoutputs.append(alias)

        if not gvquery.target_list:
            # No values were pulled from the group-values rel,
            # we must remove the DISTINCT clause to prevent
            # a syntax error.
            gvquery.distinct_clause[:] = []

        query = ctx.rel
        result_rvar = relctx.rvar_for_rel(selquery, lateral=True, ctx=ctx)
        relctx.include_rvar(query, result_rvar, path_id=outer_id, ctx=ctx)

        for rt in selquery.target_list:
            if rt.name is None:
                rt.name = ctx.env.aliases.get('v')
            if rt.name not in sortoutputs:
                query.target_list.append(
                    pgast.ResTarget(
                        val=astutils.get_column(result_rvar, rt.name),
                        name=rt.name
                    )
                )

        for i, ir_oexpr in enumerate(o_stmt.orderby):
            sort_ref = astutils.get_column(result_rvar, sortoutputs[i])
            sortexpr = pgast.SortBy(
                node=sort_ref,
                dir=ir_oexpr.direction,
                nulls=ir_oexpr.nones_order)
            query.sort_clause.append(sortexpr)

        # The OFFSET clause
        if o_stmt.offset:
            with ctx.new() as ctx1:
                ctx1.expr_exposed = False
                query.limit_offset = dispatch.compile(o_stmt.offset, ctx=ctx1)

        # The LIMIT clause
        if o_stmt.limit:
            with ctx.new() as ctx1:
                ctx1.expr_exposed = False
                query.limit_count = dispatch.compile(o_stmt.limit, ctx=ctx1)

        clauses.fini_stmt(query, ctx, parent_ctx)

    return query


@dispatch.compile.register(irast.InsertStmt)
def compile_InsertStmt(
        stmt: irast.InsertStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common DML bootstrap.
        wrapper, insert_cte, insert_rvar, _ = dml.init_dml_stmt(
            stmt, pgast.InsertStmt(), parent_ctx=parent_ctx, ctx=ctx)

        # Process INSERT body.
        dml.process_insert_body(
            stmt, wrapper, insert_cte, insert_rvar, ctx=ctx)

        # Wrap up.
        return dml.fini_dml_stmt(stmt, wrapper, insert_cte, insert_rvar,
                                 parent_ctx=parent_ctx, ctx=ctx)


@dispatch.compile.register(irast.UpdateStmt)
def compile_UpdateStmt(
        stmt: irast.UpdateStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common DML bootstrap.
        wrapper, update_cte, update_rvar, range_cte = dml.init_dml_stmt(
            stmt, pgast.UpdateStmt(), parent_ctx=parent_ctx, ctx=ctx)

        # Process UPDATE body.
        assert range_cte is not None
        dml.process_update_body(stmt, wrapper, update_cte, range_cte, ctx=ctx)

        return dml.fini_dml_stmt(stmt, wrapper, update_cte, update_rvar,
                                 parent_ctx=parent_ctx, ctx=ctx)


@dispatch.compile.register(irast.DeleteStmt)
def compile_DeleteStmt(
        stmt: irast.DeleteStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common DML bootstrap
        wrapper, delete_cte, delete_rvar, range_cte = dml.init_dml_stmt(
            stmt, pgast.DeleteStmt(), parent_ctx=parent_ctx, ctx=ctx)
        assert range_cte is not None
        ctx.toplevel_stmt.ctes.append(range_cte)
        ctx.toplevel_stmt.ctes.append(delete_cte)

        # Wrap up.
        return dml.fini_dml_stmt(stmt, wrapper, delete_cte, delete_rvar,
                                 parent_ctx=parent_ctx, ctx=ctx)
