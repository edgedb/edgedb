##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import typing

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import views as s_views
from edgedb.server.pgsql import ast as pgast

from . import boilerplate
from . import context
from . import dbobj
from . import dispatch
from . import dml
from . import output
from . import pathctx
from . import relctx
from . import relgen


@dispatch.compile.register(irast.SelectStmt)
def compile_SelectStmt(
        stmt: irast.SelectStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    if ctx.singleton_mode:
        return dispatch.compile(stmt.result, ctx=ctx)

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common setup.
        boilerplate.init_stmt(stmt, ctx=ctx, parent_ctx=parent_ctx)

        # Process FOR clause.
        boilerplate.compile_iterator_expr(ctx.query, stmt, ctx=ctx)

        query = ctx.query

        # Process the result expression;
        compile_output(stmt.result, ctx=ctx)

        # The FILTER clause.
        query.where_clause = compile_filter_clause(stmt.where, ctx=ctx)

        simple_wrapper = irutils.is_simple_wrapper(stmt)

        if not simple_wrapper:
            relctx.enforce_path_scope(
                query, ctx.parent_path_scope_refs, ctx=ctx)

        parent_range = relctx.get_parent_range_scope(stmt.result, ctx=ctx)
        if parent_range is not None:
            parent_rvar, _ = parent_range
            parent_scope = {}
            for path_id in parent_rvar.path_scope:
                tr_path_id = pathctx.reverse_map_path_id(
                    path_id, parent_ctx.view_path_id_map)

                parent_scope[tr_path_id] = pathctx.LazyPathVarRef(
                    pathctx.get_rvar_path_identity_var,
                    ctx.env, parent_rvar, path_id)

            relctx.enforce_path_scope(
                query, parent_scope, ctx=ctx)

        # The ORDER BY clause
        query.sort_clause = compile_orderby_clause(stmt.orderby, ctx=ctx)

        # The OFFSET clause
        query.limit_offset = compile_limit_offset_clause(stmt.offset, ctx=ctx)

        # The LIMIT clause
        query.limit_count = compile_limit_offset_clause(stmt.limit, ctx=ctx)

        if not parent_ctx.correct_set_assumed and not simple_wrapper:
            enforce_uniqueness = (
                (query is ctx.toplevel_stmt or ctx.expr_exposed) and
                not parent_ctx.unique_set_assumed and
                isinstance(stmt.result.scls, s_concepts.Concept)
            )
            query = relgen.ensure_correct_set(
                stmt, query, enforce_uniqueness=enforce_uniqueness, ctx=ctx)

        boilerplate.fini_stmt(query, ctx, parent_ctx)

    return query


@dispatch.compile.register(irast.GroupStmt)
def compile_GroupStmt(
        stmt: irast.GroupStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        boilerplate.init_stmt(stmt, ctx=ctx, parent_ctx=parent_ctx)

        group_path_id = stmt.group_path_id

        # Process the GROUP .. BY part into a subquery.
        with ctx.subquery() as gctx:
            gctx.expr_exposed = False
            gquery = gctx.query
            compile_output(stmt.subject, ctx=gctx)
            subj_rvar = gquery.from_clause[0]
            relctx.ensure_bond_for_expr(
                stmt.subject, subj_rvar.query, ctx=gctx)

            group_paths = set()

            part_clause = []

            for expr in stmt.groupby:
                with gctx.new() as subctx:
                    subctx.path_scope_refs = gctx.parent_path_scope_refs.copy()
                    partexpr = dispatch.compile(expr, ctx=subctx)

                part_clause.append(partexpr)
                group_paths.add(expr)

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
            # a Concept, otherwise we generate the id using
            # row_number().
            if isinstance(stmt.subject.scls, s_concepts.Concept):
                first_val = pathctx.get_path_identity_var(
                    gquery, stmt.subject.path_id, env=ctx.env)
            else:
                with ctx.subquery() as subctx:
                    wrapper = subctx.query

                    gquery_rvar = dbobj.rvar_for_rel(ctx.env, gquery)
                    wrapper.from_clause = [gquery_rvar]
                    relctx.pull_path_namespace(
                        target=wrapper, source=gquery_rvar, ctx=subctx)

                    new_part_clause = []

                    for i, expr in enumerate(part_clause):
                        path_id = stmt.groupby[i].path_id
                        pathctx.put_path_value_var(
                            gquery, path_id, expr, force=True, env=ctx.env)
                        alias = pathctx.get_path_value_output(
                            gquery, path_id, env=ctx.env)
                        new_part_clause.append(
                            dbobj.get_column(gquery_rvar, alias)
                        )

                    part_clause = new_part_clause

                    first_val = pathctx.get_rvar_path_identity_var(
                        gquery_rvar, stmt.subject.path_id, env=ctx.env)

                    gquery = wrapper

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

            pathctx.put_path_bond(gquery, group_path_id)

        group_cte = pgast.CommonTableExpr(
            query=gquery,
            name=ctx.genalias('g')
        )

        # Generate another subquery contaning distinct values of
        # path expressions in BY.
        with ctx.subquery() as gvctx:
            gvctx.path_scope = frozenset(
                {group_path_id} | {s.path_id for s in stmt.groupby})

            relctx.replace_set_cte_subtree(
                stmt.subject, group_cte, ctx=gvctx)

            for group_set in stmt.groupby:
                relctx.replace_set_cte_subtree(
                    group_set, group_cte, ctx=gvctx)

                group_expr = dispatch.compile(group_set, ctx=gvctx)
                path_id = group_set.path_id

                pathctx.put_path_identity_var(
                    gvctx.query, path_id, group_expr, env=gvctx.env)

                pathctx.put_path_value_var(
                    gvctx.query, path_id, group_expr, env=gvctx.env)

                if isinstance(path_id[-1], (s_concepts.Concept, s_views.View)):
                    pathctx.put_path_bond(gvctx.query, path_id)

            relctx.include_range(gvctx.query, group_cte.query, ctx=gvctx)

            for path_id in list(gvctx.query.path_rvar_map):
                if path_id not in gvctx.path_scope:
                    gvctx.query.path_rvar_map.pop(path_id)

            for path_id in list(gvctx.query.path_namespace):
                if path_id not in gvctx.path_scope:
                    gvctx.query.path_namespace.pop(path_id)

            gvctx.query.distinct_clause = [
                pathctx.get_path_identity_var(
                    gvctx.query, group_path_id, env=ctx.env)
            ]

        groupval_cte = pgast.CommonTableExpr(
            query=gvctx.query,
            name=ctx.genalias('gv')
        )

        o_stmt = stmt.result.expr

        # process the result expression;
        with ctx.subquery() as selctx:
            outer_id = stmt.result.path_id
            inner_id = o_stmt.result.path_id

            selctx.query.view_path_id_map = {
                outer_id: inner_id
            }

            selctx.path_scope = o_stmt.path_scope
            selctx.local_scope_sets = o_stmt.local_scope_sets

            selctx.query.ctes.append(group_cte)
            # relctx.pop_prefix_ctes(stmt.subject.path_id, ctx=selctx)
            relctx.replace_set_cte_subtree(
                stmt.subject, group_cte, lax=False, ctx=selctx)
            # When GROUP subject appears in aggregates, which by
            # default use lax paths, we still want to use the group
            # CTE as the source.
            relctx.replace_set_cte_subtree(
                stmt.subject, group_cte, lax=True, ctx=selctx)

            sortoutputs = []

            selctx.query.ctes.append(groupval_cte)
            for grouped_set in group_paths:
                relctx.replace_set_cte_subtree(
                    grouped_set, groupval_cte, recursive=False, ctx=selctx)

            compile_output(o_stmt.result, ctx=selctx)

            relctx.enforce_path_scope(
                selctx.query, selctx.parent_path_scope_refs, ctx=selctx)

            # The WHERE clause
            selctx.query.where_clause = compile_filter_clause(
                o_stmt.where, ctx=selctx)

            for ir_sortexpr in o_stmt.orderby:
                alias = ctx.genalias('s')
                sexpr = dispatch.compile(ir_sortexpr.expr, ctx=selctx)
                selctx.query.target_list.append(
                    pgast.ResTarget(
                        val=sexpr,
                        name=alias
                    )
                )
                sortoutputs.append(alias)

        if not gvctx.query.target_list:
            # group expressions were not used in output, discard the
            # GV CTE.
            selctx.query.ctes.remove(groupval_cte)

        query = ctx.query
        result_rvar = relctx.include_range(
            query, selctx.query, lateral=True, ctx=ctx)

        for rt in selctx.query.target_list:
            if rt.name is None:
                rt.name = ctx.genalias('v')
            if rt.name not in sortoutputs:
                query.target_list.append(
                    pgast.ResTarget(
                        val=dbobj.get_column(result_rvar, rt.name),
                        name=rt.name
                    )
                )

        for i, expr in enumerate(o_stmt.orderby):
            sort_ref = dbobj.get_column(result_rvar, sortoutputs[i])
            sortexpr = pgast.SortBy(
                node=sort_ref,
                dir=expr.direction,
                nulls=expr.nones_order)
            query.sort_clause.append(sortexpr)

        # The OFFSET clause
        if o_stmt.offset:
            with ctx.new() as ctx1:
                ctx1.clause = 'offsetlimit'
                ctx1.expr_exposed = False
                query.limit_offset = dispatch.compile(o_stmt.offset, ctx=ctx1)

        # The LIMIT clause
        if o_stmt.limit:
            with ctx.new() as ctx1:
                ctx1.clause = 'offsetlimit'
                ctx1.expr_exposed = False
                query.limit_count = dispatch.compile(o_stmt.limit, ctx=ctx1)

        if not parent_ctx.correct_set_assumed:
            enforce_uniqueness = (
                (query is ctx.toplevel_stmt or ctx.expr_exposed) and
                not parent_ctx.unique_set_assumed and
                isinstance(stmt.result.scls, s_concepts.Concept)
            )
            query = relgen.ensure_correct_set(
                stmt, query, enforce_uniqueness=enforce_uniqueness, ctx=ctx)

        boilerplate.fini_stmt(query, ctx, parent_ctx)

    return query


@dispatch.compile.register(irast.InsertStmt)
def compile_InsertStmt(
        stmt: irast.InsertStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common DML bootstrap
        wrapper, insert_cte, _ = dml.init_dml_stmt(
            stmt, pgast.InsertStmt(), parent_ctx=parent_ctx, ctx=ctx)

        # Process INSERT body
        dml.process_insert_body(stmt, wrapper, insert_cte, ctx=ctx)
        relctx.enforce_path_scope(wrapper, ctx.parent_path_scope_refs, ctx=ctx)

        return dml.fini_dml_stmt(stmt, wrapper, insert_cte,
                                 parent_ctx=parent_ctx, ctx=ctx)


@dispatch.compile.register(irast.UpdateStmt)
def compile_UpdateStmt(
        stmt: irast.UpdateStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common DML bootstrap
        wrapper, update_cte, range_cte = dml.init_dml_stmt(
            stmt, pgast.UpdateStmt(), parent_ctx=parent_ctx, ctx=ctx)

        # Process UPDATE body
        dml.process_update_body(stmt, wrapper, update_cte, range_cte,
                                ctx=ctx)
        relctx.enforce_path_scope(wrapper, ctx.parent_path_scope_refs, ctx=ctx)

        return dml.fini_dml_stmt(stmt, wrapper, update_cte,
                                 parent_ctx=parent_ctx, ctx=ctx)


@dispatch.compile.register(irast.DeleteStmt)
def compile_DeleteStmt(
        stmt: irast.DeleteStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.subquery() as ctx:
        # Common DML bootstrap
        wrapper, delete_cte, range_cte = dml.init_dml_stmt(
            stmt, pgast.DeleteStmt(), parent_ctx=parent_ctx, ctx=ctx)

        ctx.toplevel_stmt.ctes.append(range_cte)
        ctx.toplevel_stmt.ctes.append(delete_cte)

        relctx.enforce_path_scope(wrapper, ctx.parent_path_scope_refs, ctx=ctx)

        return dml.fini_dml_stmt(stmt, wrapper, delete_cte,
                                 parent_ctx=parent_ctx, ctx=ctx)


def compile_output(
        ir_set: irast.Base, *, ctx: context.CompilerContextLevel) -> None:
    with ctx.new() as newctx:
        newctx.clause = 'result'
        if newctx.expr_exposed is None:
            newctx.expr_exposed = True
        dispatch.compile(ir_set, ctx=newctx)
        set_cte = relctx.get_set_cte(
            irutils.get_canonical_set(ir_set), ctx=newctx)

        rvar = ctx.subquery_map[ctx.rel][set_cte]

        path_id = ir_set.path_id
        if ctx.rel.view_path_id_map:
            path_id = pathctx.reverse_map_path_id(
                path_id, ctx.rel.view_path_id_map)

        pathctx.put_path_rvar(ctx.env, ctx.rel, path_id, rvar)

        if output.in_serialization_ctx(ctx):
            pathctx.get_path_serialized_output(ctx.rel, path_id, env=ctx.env)
        else:
            pathctx.get_path_value_output(ctx.rel, path_id, env=ctx.env)


def compile_filter_clause(
        ir_set: typing.Optional[irast.Base], *,
        ctx: context.CompilerContextLevel) -> pgast.Expr:
    if ir_set is None:
        return None

    with ctx.new() as ctx1:
        ctx1.clause = 'where'
        ctx1.expr_exposed = False
        ctx1.shape_format = context.ShapeFormat.SERIALIZED
        relgen.init_scoped_set_ctx(ir_set, ctx=ctx1)
        where_clause = dispatch.compile(ir_set, ctx=ctx1)

    return where_clause


def compile_orderby_clause(
        ir_exprs: typing.List[irast.Base], *,
        ctx: context.CompilerContextLevel) -> pgast.Expr:
    if not ir_exprs:
        return []

    sort_clause = []

    for expr in ir_exprs:
        with ctx.new() as orderctx:
            orderctx.clause = 'orderby'
            relgen.init_scoped_set_ctx(expr.expr, ctx=orderctx)
            sortexpr = pgast.SortBy(
                node=dispatch.compile(expr.expr, ctx=orderctx),
                dir=expr.direction,
                nulls=expr.nones_order)
            sort_clause.append(sortexpr)

    return sort_clause


def compile_limit_offset_clause(
        ir_set: typing.Optional[irast.Base], *,
        ctx: context.CompilerContextLevel) -> pgast.Expr:
    if ir_set is None:
        return None

    with ctx.new() as ctx1:
        relgen.init_scoped_set_ctx(ir_set, ctx=ctx1)
        ctx1.clause = 'offsetlimit'
        ctx1.expr_as_isolated_set = True
        ctx1.expr_exposed = False
        limit_offset_clause = dispatch.compile(ir_set, ctx=ctx1)

    return limit_offset_clause
