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

from typing import Optional

from edb import errors

from edb.ir import ast as irast
from edb.ir import utils as irutils

from edb.pgsql import ast as pgast

from . import astutils
from . import clauses
from . import context
from . import dispatch
from . import enums as pgce
from . import group
from . import dml
from . import output
from . import pathctx


@dispatch.compile.register(irast.SelectStmt)
def compile_SelectStmt(
    stmt: irast.SelectStmt, *, ctx: context.CompilerContextLevel
) -> pgast.BaseExpr:

    if ctx.singleton_mode:
        if not irutils.is_trivial_select(stmt):
            raise errors.UnsupportedFeatureError(
                'Clause on SELECT statement in simple expression')

        return dispatch.compile(stmt.result, ctx=ctx)

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common setup.
        clauses.compile_volatile_bindings(stmt, ctx=ctx)

        query = ctx.stmt

        # Process materialized sets
        clauses.compile_materialized_exprs(query, stmt, ctx=ctx)

        iterator_set = stmt.iterator_stmt
        last_iterator: Optional[irast.Set] = None
        if iterator_set:
            if irutils.contains_dml(stmt):
                # If we have iterators and we contain nested DML
                # statements, we need to hoist the iterators into CTEs and
                # then explicitly join them back into the query.
                iterator = dml.compile_iterator_cte(iterator_set, ctx=ctx)
                ctx.path_scope = ctx.path_scope.new_child()
                dml.merge_iterator(iterator, ctx.rel, ctx=ctx)

                ctx.enclosing_cte_iterator = iterator
                last_iterator = stmt.iterator_stmt

            else:
                # Process FOR clause.
                with ctx.new() as ictx:
                    clauses.setup_iterator_volatility(last_iterator, ctx=ictx)
                    iterator_rvar = clauses.compile_iterator_expr(
                        query, iterator_set, is_dml=False, ctx=ictx)
                for aspect in {pgce.PathAspect.IDENTITY, pgce.PathAspect.VALUE}:
                    pathctx.put_path_rvar(
                        query,
                        path_id=iterator_set.path_id,
                        rvar=iterator_rvar,
                        aspect=aspect,
                    )
                last_iterator = iterator_set

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
                        stmt.where, stmt.where_card, ctx=ictx))

            # The ORDER BY clause
            if stmt.orderby is not None:
                with ictx.new() as octx:
                    query.sort_clause = clauses.compile_orderby_clause(
                        stmt.orderby, ctx=octx)

        # Need to filter out NULLs in certain cases:
        if outvar.nullable and (
            # A nullable var has bubbled up to the top
            query is ctx.toplevel_stmt
            # The cardinality is being overridden, so we need to make
            # sure there aren't extra NULLs in single set
            or stmt.card_inference_override
            # There is a LIMIT or OFFSET clause and NULLs would interfere
            or stmt.limit
            or stmt.offset
        ):
            valvar = pathctx.get_path_value_var(
                query, stmt.result.path_id, env=ctx.env)
            output.add_null_test(valvar, query)

        # The OFFSET clause
        query.limit_offset = clauses.compile_limit_offset_clause(
            stmt.offset, ctx=ctx)

        # The LIMIT clause
        query.limit_count = clauses.compile_limit_offset_clause(
            stmt.limit, ctx=ctx)

    return query


@dispatch.compile.register(irast.GroupStmt)
def compile_GroupStmt(
    stmt: irast.GroupStmt, *, ctx: context.CompilerContextLevel
) -> pgast.BaseExpr:
    return group.compile_group(stmt, ctx=ctx)


@dispatch.compile.register(irast.InsertStmt)
def compile_InsertStmt(
    stmt: irast.InsertStmt, *, ctx: context.CompilerContextLevel
) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common DML bootstrap.
        parts = dml.init_dml_stmt(stmt, ctx=ctx)

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
        return dml.fini_dml_stmt(stmt, ctx.rel, parts, ctx=ctx)


@dispatch.compile.register(irast.UpdateStmt)
def compile_UpdateStmt(
    stmt: irast.UpdateStmt, *, ctx: context.CompilerContextLevel
) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common DML bootstrap.
        parts = dml.init_dml_stmt(stmt, ctx=ctx)
        range_cte = parts.range_cte
        assert range_cte is not None

        toplevel = ctx.toplevel_stmt
        toplevel.append_cte(range_cte)

        for typeref, (update_cte, _) in parts.dml_ctes.items():
            # Process UPDATE body.
            dml.process_update_body(
                ir_stmt=stmt,
                update_cte=update_cte,
                dml_parts=parts,
                typeref=typeref,
                ctx=ctx,
            )

        return dml.fini_dml_stmt(stmt, ctx.rel, parts, ctx=ctx)


@dispatch.compile.register(irast.DeleteStmt)
def compile_DeleteStmt(
    stmt: irast.DeleteStmt, *, ctx: context.CompilerContextLevel
) -> pgast.Query:

    parent_ctx = ctx
    with parent_ctx.substmt() as ctx:
        # Common DML bootstrap
        parts = dml.init_dml_stmt(stmt, ctx=ctx)

        range_cte = parts.range_cte
        assert range_cte is not None
        ctx.toplevel_stmt.append_cte(range_cte)

        for typeref, (delete_cte, _) in parts.dml_ctes.items():
            dml.process_delete_body(
                ir_stmt=stmt,
                delete_cte=delete_cte,
                typeref=typeref,
                ctx=ctx,
            )

        # Wrap up.
        return dml.fini_dml_stmt(stmt, ctx.rel, parts, ctx=ctx)
