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

from edb.ir import ast as irast
from edb.ir import utils as irutils

from edb.pgsql import ast as pgast

from . import astutils
from . import clauses
from . import context
from . import dispatch
from . import dml
from . import pathctx


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

        for binding in (stmt.bindings or ()):
            # If something we are WITH binding contains DML, we want to
            # compile it *now*, in the context of its initial appearance
            # and not where the variable is used. This will populate
            # dml_stmts with the CTEs, which will be picked up when the
            # variable is referenced.
            if irutils.contains_dml(binding):
                with ctx.substmt() as bctx:
                    dispatch.compile(binding, ctx=bctx)

        query = ctx.stmt

        iterator_set = stmt.iterator_stmt
        last_iterator: Optional[irast.Set] = None
        if iterator_set and irutils.contains_dml(stmt):
            # If we have iterators and we contain nested DML
            # statements, we need to hoist the iterators into CTEs and
            # then explicitly join them back into the query.
            iterator = dml.compile_iterator_cte(iterator_set, ctx=ctx)
            ctx.path_scope = ctx.path_scope.new_child()
            dml.merge_iterator(iterator, ctx.rel, ctx=ctx)

            ctx.enclosing_cte_iterator = iterator
            last_iterator = stmt.iterator_stmt

        else:
            if iterator_set:
                # Process FOR clause.
                with ctx.new() as ictx:
                    clauses.setup_iterator_volatility(last_iterator, ctx=ictx)
                    iterator_rvar = clauses.compile_iterator_expr(
                        query, iterator_set, ctx=ictx)
                for aspect in {'identity', 'value'}:
                    pathctx.put_path_rvar(
                        query,
                        path_id=iterator_set.path_id,
                        rvar=iterator_rvar,
                        aspect=aspect,
                        env=ctx.env,
                    )
                last_iterator = iterator_set

        # Process materialized sets
        clauses.compile_materialized_exprs(query, stmt, ctx=ctx)

        # Process the result expression.
        with ctx.new() as ictx:
            clauses.setup_iterator_volatility(last_iterator, ctx=ictx)
            outvar = clauses.compile_output(stmt.result, ctx=ictx)

        with ctx.new() as ictx:
            # FILTER and ORDER BY need to have the base result as a
            # volatility ref.
            clauses.setup_iterator_volatility(stmt.result, ctx=ictx)

            # The FILTER clause.
            if stmt.where is not None:
                query.where_clause = astutils.extend_binop(
                    query.where_clause,
                    clauses.compile_filter_clause(
                        stmt.where, stmt.where_card, ctx=ctx))

            # The ORDER BY clause
            if stmt.orderby is not None:
                with ctx.new() as ictx:
                    query.sort_clause = clauses.compile_orderby_clause(
                        stmt.orderby, ctx=ictx)

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

        # The OFFSET clause
        query.limit_offset = clauses.compile_limit_offset_clause(
            stmt.offset, ctx=ctx)

        # The LIMIT clause
        query.limit_count = clauses.compile_limit_offset_clause(
            stmt.limit, ctx=ctx)

        clauses.fini_stmt(query, ctx, parent_ctx)

    return query


@dispatch.compile.register(irast.InsertStmt)
def compile_InsertStmt(
        stmt: irast.InsertStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common DML bootstrap.
        parts = dml.init_dml_stmt(stmt, parent_ctx=parent_ctx, ctx=ctx)

        top_typeref = stmt.subject.typeref
        if top_typeref.material_type is not None:
            top_typeref = top_typeref.material_type
        insert_cte, _ = parts.dml_ctes[top_typeref]

        # Process INSERT body.
        dml.process_insert_body(
            ir_stmt=stmt,
            insert_cte=insert_cte,
            dml_parts=parts,
            ctx=ctx,
        )

        # Wrap up.
        return dml.fini_dml_stmt(
            stmt, ctx.rel, parts, parent_ctx=parent_ctx, ctx=ctx)


@dispatch.compile.register(irast.UpdateStmt)
def compile_UpdateStmt(
        stmt: irast.UpdateStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common DML bootstrap.
        parts = dml.init_dml_stmt(stmt, parent_ctx=parent_ctx, ctx=ctx)
        range_cte = parts.range_cte
        assert range_cte is not None

        toplevel = ctx.toplevel_stmt
        toplevel.ctes.append(range_cte)

        for typeref, (update_cte, _) in parts.dml_ctes.items():
            # Process UPDATE body.
            dml.process_update_body(
                ir_stmt=stmt,
                update_cte=update_cte,
                dml_parts=parts,
                typeref=typeref,
                ctx=ctx,
            )

        return dml.fini_dml_stmt(
            stmt, ctx.rel, parts, parent_ctx=parent_ctx, ctx=ctx)


@dispatch.compile.register(irast.DeleteStmt)
def compile_DeleteStmt(
        stmt: irast.DeleteStmt, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common DML bootstrap
        parts = dml.init_dml_stmt(stmt, parent_ctx=parent_ctx, ctx=ctx)

        range_cte = parts.range_cte
        assert range_cte is not None
        ctx.toplevel_stmt.ctes.append(range_cte)

        for delete_cte, _ in parts.dml_ctes.values():
            ctx.toplevel_stmt.ctes.append(delete_cte)

        # Wrap up.
        return dml.fini_dml_stmt(
            stmt, ctx.rel, parts, parent_ctx=parent_ctx, ctx=ctx)
