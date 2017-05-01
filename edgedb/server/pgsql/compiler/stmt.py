##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import objects as s_obj
from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common

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
        ctx: context.CompilerContext) -> pgast.Query:

    if ctx.singleton_mode:
        return dispatch.compile(stmt.result, ctx=ctx)

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        boilerplate.init_stmt(stmt, ctx=ctx, parent_ctx=parent_ctx)

        query = ctx.query

        # Process the result expression;
        output.compile_output(stmt.result, ctx=ctx)

        if len(query.target_list) == 1:
            resalias = output.ensure_query_restarget_name(
                query, env=ctx.env)
            pathctx.put_path_output(
                ctx.env, query, stmt.result.path_id, resalias)

        if query is not ctx.toplevel_stmt:
            specific_scope = {s for s in ctx.stmt_specific_path_scope
                              if s.path_id in ctx.parent_path_bonds}

            for ir_set in specific_scope:
                if (isinstance(ir_set.scls, s_concepts.Concept) and
                        ir_set.path_id not in query.path_bonds):
                    # The selector does not include this path explicitly,
                    # so we must do so here.
                    cte = relgen.set_to_cte(ir_set, ctx=ctx)
                    relctx.include_range(
                        ctx.rel, cte, join_type='left',
                        replace_bonds=False, ctx=ctx)

        # The WHERE clause
        if stmt.where:
            with ctx.new() as ctx1:
                ctx1.clause = 'where'
                ctx1.expr_exposed = False
                ctx1.shape_format = context.ShapeFormat.SERIALIZED
                query.where_clause = dispatch.compile(stmt.where, ctx=ctx1)

        simple_wrapper = irutils.is_simple_wrapper(stmt)

        if not simple_wrapper:
            relctx.enforce_path_scope(
                query, ctx.parent_path_bonds, ctx=ctx)

        if simple_wrapper and ctx.shape_format == context.ShapeFormat.FLAT:
            # This is a simple wrapper around a flat shape.
            # Make sure we pull out all target refs as-is
            subquery_rvar = query.from_clause[0]
            subquery = subquery_rvar.query
            query.path_outputs = subquery.path_outputs.copy()
            query.target_list = []
            for rt in subquery.target_list:
                query.target_list.append(
                    pgast.ResTarget(
                        val=dbobj.get_column(subquery_rvar, rt.name),
                        name=rt.name
                    )
                )

        # The ORDER BY clause
        with ctx.new() as orderctx:
            orderctx.clause = 'orderby'
            query = orderctx.query

            for expr in stmt.orderby:
                sortexpr = pgast.SortBy(
                    node=dispatch.compile(expr.expr, ctx=orderctx),
                    dir=expr.direction,
                    nulls=expr.nones_order)
                query.sort_clause.append(sortexpr)

        # The OFFSET clause
        if stmt.offset:
            with ctx.new() as ctx1:
                ctx1.clause = 'offsetlimit'
                ctx1.expr_as_isolated_set = True
                ctx1.expr_exposed = False
                query.limit_offset = dispatch.compile(stmt.offset, ctx=ctx1)

        # The LIMIT clause
        if stmt.limit:
            with ctx.new() as ctx1:
                ctx1.clause = 'offsetlimit'
                ctx1.expr_as_isolated_set = True
                ctx1.expr_exposed = False
                query.limit_count = dispatch.compile(stmt.limit, ctx=ctx1)

        if not parent_ctx.correct_set_assumed and not simple_wrapper:
            enforce_uniqueness = (
                (query is ctx.toplevel_stmt or ctx.expr_exposed) and
                not parent_ctx.unique_set_assumed
            )
            # enforce_uniqueness = query is ctx.toplevel_stmt
            query = relgen.ensure_correct_set(
                stmt, query, enforce_uniqueness=enforce_uniqueness, ctx=ctx)

        boilerplate.fini_stmt(query, ctx, parent_ctx)

    return query


@dispatch.compile.register(irast.GroupStmt)
def compile_GroupStmt(
        stmt: irast.GroupStmt, *,
        ctx: context.CompilerContext) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        boilerplate.init_stmt(stmt, ctx=ctx, parent_ctx=parent_ctx)

        group_path_id = stmt.group_path_id
        ctx.stmt_path_scope = ctx.stmt_path_scope.copy()
        ctx.stmt_path_scope[group_path_id] = 1
        ctx.parent_stmt_path_scope[group_path_id] = 1

        # Process the GROUP .. BY part into a subquery.
        with ctx.subquery() as gctx:
            gctx.expr_exposed = False
            gquery = gctx.query
            output.compile_output(stmt.subject, ctx=gctx)
            subj_rvar = gquery.from_clause[0]
            relctx.ensure_bond_for_expr(
                stmt.subject, subj_rvar.query, ctx=gctx)

            group_paths = set()

            part_clause = []

            for expr in stmt.groupby:
                with gctx.new() as subctx:
                    subctx.path_bonds = gctx.parent_path_bonds.copy()
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
                first_val = pathctx.get_path_var(
                    ctx.env, gquery, stmt.subject.path_id)
            else:
                with ctx.subquery() as subctx:
                    wrapper = subctx.query

                    gquery_rvar = dbobj.rvar_for_rel(ctx.env, gquery)
                    wrapper.from_clause = [gquery_rvar]
                    relctx.pull_path_namespace(
                        target=wrapper, source=gquery_rvar, ctx=subctx)

                    new_part_clause = []

                    for expr in part_clause:
                        alias = ctx.env.aliases.get('p')
                        gquery.target_list.append(
                            pgast.ResTarget(
                                val=expr,
                                name=alias
                            )
                        )
                        new_part_clause.append(
                            dbobj.get_column(gquery_rvar, alias)
                        )

                    part_clause = new_part_clause

                    first_val = pathctx.get_rvar_path_var(
                        ctx.env, gquery_rvar, stmt.subject.path_id)

                    restype = irutils.infer_type(stmt.subject, ctx.env.schema)
                    if isinstance(restype, s_obj.Tuple):
                        for n in restype.element_types:
                            cn = common.edgedb_name_to_pg_name(n)
                            wrapper.target_list.append(
                                pgast.ResTarget(
                                    val=dbobj.get_column(gquery_rvar, cn),
                                    name=cn
                                )
                            )

                    gquery = wrapper

            group_id = pgast.FuncCall(
                name=('first_value',),
                args=[first_val],
                over=pgast.WindowDef(
                    partition_clause=part_clause
                )
            )

            gid_alias = ctx.genalias('gid')
            gquery.target_list.append(
                pgast.ResTarget(
                    val=group_id,
                    name=gid_alias
                )
            )

            pathctx.put_path_output(
                ctx.env, gquery, group_path_id, gid_alias, raw=True)
            pathctx.put_path_output(
                ctx.env, gquery, group_path_id, gid_alias, raw=False)
            pathctx.put_path_bond(gquery, group_path_id)

        group_cte = pgast.CommonTableExpr(
            query=gquery,
            name=ctx.genalias('g')
        )

        # Generate another subquery contaning distinct values of
        # path expressions in BY.
        with ctx.subquery() as gvctx:
            gvctx.stmt_path_scope = collections.defaultdict(int)
            gvctx.stmt_path_scope[group_path_id] = 1

            relctx.replace_set_cte_subtree(
                stmt.subject, group_cte, ctx=gvctx)

            for group_set in stmt.groupby:
                relctx.replace_set_cte_subtree(
                    group_set, group_cte, ctx=gvctx)
                group_expr = dispatch.compile(group_set, ctx=gvctx)
                path_id = group_set.path_id
                if isinstance(group_set.expr, irast.TupleIndirection):
                    alias = group_set.expr.name
                else:
                    alias = pathctx.get_path_output_alias(
                        ctx.env, path_id)
                gvctx.query.target_list.append(
                    pgast.ResTarget(
                        val=group_expr,
                        name=alias
                    )
                )
                pathctx.put_path_output(
                    ctx.env, gvctx.query, path_id, alias, raw=True)
                pathctx.put_path_output(
                    ctx.env, gvctx.query, path_id, alias, raw=False)
                pathctx.put_path_bond(gvctx.query, path_id)
                gvctx.stmt_path_scope[path_id] = 1

            relctx.include_range(gvctx.query, group_cte.query, ctx=gvctx)

            for path_id in list(gvctx.query.path_rvar_map):
                c_path_id = pathctx.get_canonical_path_id(
                    ctx.schema, path_id)
                if c_path_id not in gvctx.stmt_path_scope:
                    gvctx.query.path_rvar_map.pop(path_id)

            gvctx.query.distinct_clause = [
                pathctx.get_path_var(ctx.env, gvctx.query, group_path_id)
            ]

        groupval_cte = pgast.CommonTableExpr(
            query=gvctx.query,
            name=ctx.genalias('gv')
        )

        o_stmt = stmt.result.expr

        # process the result expression;
        with ctx.subquery() as selctx:

            selctx.stmt_path_scope = o_stmt.path_scope.copy()
            selctx.stmt_path_scope[group_path_id] = 1

            selctx.stmt_specific_path_scope = \
                {s for s in o_stmt.specific_path_scope
                 if s.path_id in selctx.stmt_path_scope}

            selctx.parent_stmt_path_scope = ctx.parent_stmt_path_scope

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

            output.compile_output(o_stmt.result, ctx=selctx)

            relctx.enforce_path_scope(
                selctx.query, selctx.parent_path_bonds, ctx=selctx)

            # The WHERE clause
            if o_stmt.where:
                with selctx.new() as ctx1:
                    selctx.clause = 'where'
                    selctx.query.where_clause = dispatch.compile(
                        o_stmt.where, ctx=ctx1)

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

        if len(query.target_list) == 1:
            resalias = output.ensure_query_restarget_name(
                query, env=ctx.env)
            pathctx.put_path_output(
                ctx.env, query, o_stmt.result.path_id, resalias)

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
            query = relgen.ensure_correct_set(
                stmt, query, query is ctx.toplevel_stmt, ctx=ctx)

        boilerplate.fini_stmt(query, ctx, parent_ctx)

    return query


@dispatch.compile.register(irast.InsertStmt)
def compile_InsertStmt(
        stmt: irast.InsertStmt, *,
        ctx: context.CompilerContext) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common DML bootstrap
        wrapper, insert_cte, _ = dml.init_dml_stmt(
            stmt, pgast.InsertStmt(), parent_ctx=parent_ctx, ctx=ctx)

        # Process INSERT body
        dml.process_insert_body(stmt, wrapper, insert_cte, ctx=ctx)
        relctx.enforce_path_scope(wrapper, ctx.parent_path_bonds, ctx=ctx)

        return dml.fini_dml_stmt(stmt, wrapper, insert_cte,
                                 parent_ctx=parent_ctx, ctx=ctx)


@dispatch.compile.register(irast.UpdateStmt)
def compile_UpdateStmt(
        stmt: irast.UpdateStmt, *,
        ctx: context.CompilerContext) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common DML bootstrap
        wrapper, update_cte, range_cte = dml.init_dml_stmt(
            stmt, pgast.UpdateStmt(), parent_ctx=parent_ctx, ctx=ctx)

        # Process UPDATE body
        dml.process_update_body(stmt, wrapper, update_cte, range_cte,
                                ctx=ctx)
        relctx.enforce_path_scope(wrapper, ctx.parent_path_bonds, ctx=ctx)

        return dml.fini_dml_stmt(stmt, wrapper, update_cte,
                                 parent_ctx=parent_ctx, ctx=ctx)


@dispatch.compile.register(irast.DeleteStmt)
def compile_DeleteStmt(
        stmt: irast.DeleteStmt, *,
        ctx: context.CompilerContext) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.subquery() as ctx:
        # Common DML bootstrap
        wrapper, delete_cte, _ = dml.init_dml_stmt(
            stmt, pgast.DeleteStmt(), parent_ctx=parent_ctx, ctx=ctx)
        relctx.enforce_path_scope(wrapper, ctx.parent_path_bonds, ctx=ctx)

        return dml.fini_dml_stmt(stmt, wrapper, delete_cte,
                                 parent_ctx=parent_ctx, ctx=ctx)
