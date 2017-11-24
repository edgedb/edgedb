##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL statement compilation routines."""


import typing

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import name as s_name

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors

from . import astutils
from . import clauses
from . import context
from . import dispatch
from . import pathctx
from . import setgen
from . import shapegen
from . import stmtctx


@dispatch.compile.register(qlast.SelectQuery)
def compile_SelectQuery(
        expr: qlast.SelectQuery, *, ctx: context.ContextLevel) -> irast.Base:
    if astutils.is_degenerate_select(expr):
        # Compile "SELECT Path" as "Path"
        with ctx.new() as sctx:
            process_with_block(expr, ctx=sctx, parent_ctx=ctx)
            result = compile_result_clause(
                expr.result, ctx.toplevel_shape_rptr, None, ctx=sctx)

        return result

    with ctx.subquery() as sctx:
        stmt = irast.SelectStmt()
        init_stmt(stmt, expr, ctx=sctx, parent_ctx=ctx)

        output = expr.result

        if (isinstance(output, qlast.Shape) and
                isinstance(output.expr, qlast.Path) and
                output.expr.steps):
            sctx.result_path_steps = output.expr.steps

        stmt.result = compile_result_clause(
            expr.result, ctx.toplevel_shape_rptr, expr.result_alias, ctx=sctx)

        stmt.where = clauses.compile_where_clause(
            expr.where, ctx=sctx)

        stmt.orderby = clauses.compile_orderby_clause(
            expr.orderby, ctx=sctx)

        stmt.offset = clauses.compile_limit_offset_clause(
            expr.offset, ctx=sctx)

        stmt.limit = clauses.compile_limit_offset_clause(
            expr.limit, ctx=sctx)

        result = fini_stmt(stmt, expr, ctx=sctx, parent_ctx=ctx)

    # Query cardinality inference must be ran in parent context.
    if expr.single:
        stmt.result.context = expr.result.context
        # XXX: correct cardinality inference depends on
        # query selectivity estimator, which is not done yet.
        # pathctx.enforce_singleton(stmt.result, ctx=ctx)
        stmt.singleton = True

    return result


@dispatch.compile.register(qlast.GroupQuery)
def compile_GroupQuery(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    with ctx.subquery() as ictx:
        stmt = irast.GroupStmt()
        init_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

        c = s_concepts.Concept(
            name=s_name.Name(
                module='__group__', name=ctx.aliases.get('Group')),
            bases=[ctx.schema.get('std::Object')]
        )
        c.acquire_ancestor_inheritance(ctx.schema)

        stmt.group_path_id = pathctx.get_path_id(c, ctx=ictx)
        pathctx.register_path_in_scope(stmt.group_path_id, ctx=ictx)

        with ictx.newfence() as subjctx:
            subjctx.clause = 'input'

            subject_set = setgen.scoped_set(
                dispatch.compile(expr.subject, ctx=subjctx), ctx=subjctx)

            stmt.subject = stmtctx.declare_aliased_set(
                subject_set, expr.subject_alias, ctx=ictx)

            with subjctx.new() as grpctx:
                # Prevent singleton inference of GROUP input from
                # leaking beyond the BY clause.
                grpctx.singletons = ictx.singletons.copy()
                pathctx.update_singletons(subject_set, ctx=grpctx)

                stmt.groupby = compile_groupby_clause(
                    expr.groupby, singletons=grpctx.singletons, ctx=grpctx)

            pathctx.update_singletons(stmt.subject, ctx=ictx)

        output = expr.result

        if (isinstance(output, qlast.Shape) and
                isinstance(output.expr, qlast.Path) and
                output.expr.steps):
            ictx.result_path_steps = output.expr.steps

        with ictx.subquery() as isctx, isctx.newfence() as sctx:
            o_stmt = sctx.stmt = irast.SelectStmt()

            o_stmt.result = compile_result_clause(
                expr.result, ctx.toplevel_shape_rptr,
                expr.result_alias, ctx=sctx)

            o_stmt.where = clauses.compile_where_clause(
                expr.where, ctx=sctx)

            o_stmt.orderby = clauses.compile_orderby_clause(
                expr.orderby, ctx=sctx)

            o_stmt.offset = clauses.compile_limit_offset_clause(
                expr.offset, ctx=sctx)

            o_stmt.limit = clauses.compile_limit_offset_clause(
                expr.limit, ctx=sctx)

            stmt.result = setgen.scoped_set(o_stmt, ctx=sctx)

        result = fini_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.InsertQuery)
def compile_InsertQuery(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    with ctx.subquery() as ictx:
        stmt = irast.InsertStmt()
        init_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

        subject = dispatch.compile(expr.subject, ctx=ictx)

        stmt.subject = shapegen.compile_shape(
            subject, expr.shape,
            require_expressions=True,
            include_implicit=False,
            ctx=ictx)

        stmt.result = subject

        explicit_ptrs = {
            el.rptr.ptrcls.shortname for el in stmt.subject.shape
        }

        for pn, ptrcls in subject.scls.pointers.items():
            if (not ptrcls.default or
                    pn in explicit_ptrs or
                    ptrcls.is_special_pointer() or
                    ptrcls.is_pure_computable()):
                continue

            targetstep = setgen.extend_path(subject, ptrcls, ctx=ictx)
            el = setgen.computable_ptr_set(targetstep.rptr, ctx=ictx)
            stmt.subject.shape.append(el)

        result = fini_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.UpdateQuery)
def compile_UpdateQuery(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    with ctx.subquery() as ictx:
        stmt = irast.UpdateStmt()
        init_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

        subject = stmtctx.declare_aliased_set(
            dispatch.compile(expr.subject, ctx=ictx),
            expr.subject_alias, ctx=ictx)

        subj_type = irutils.infer_type(subject, ictx.schema)
        if not isinstance(subj_type, s_concepts.Concept):
            raise errors.EdgeQLError(
                f'cannot update non-Concept objects',
                context=expr.subject.context
            )

        stmt.where = clauses.compile_where_clause(
            expr.where, ctx=ictx)

        stmt.result = subject.__copy__()

        stmt.subject = shapegen.compile_shape(
            subject, expr.shape,
            require_expressions=True,
            include_implicit=False,
            ctx=ictx)

        result = fini_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.DeleteQuery)
def compile_DeleteQuery(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    with ctx.subquery() as ictx:
        stmt = irast.DeleteStmt()
        init_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

        with ictx.newfence() as scopectx:
            # Pretend the DELETE statement is a view.
            subjset = setgen.scoped_set(
                dispatch.compile(expr.subject, ctx=scopectx),
                ctx=scopectx)

        subject = stmtctx.declare_aliased_set(
            subjset, expr.subject_alias, ctx=ictx)

        subj_type = irutils.infer_type(subject, ictx.schema)
        if not isinstance(subj_type, s_concepts.Concept):
            raise errors.EdgeQLError(
                f'cannot delete non-Concept objects',
                context=expr.subject.context
            )

        stmt.subject = stmt.result = subject

        result = fini_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.Shape)
def compile_Shape(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    subj = dispatch.compile(expr.expr, ctx=ctx)
    ir_set = setgen.ensure_set(subj, ctx=ctx)
    return shapegen.compile_shape(
        ir_set, expr.elements, rptr=ir_set.rptr, ctx=ctx)


def init_stmt(
        irstmt: irast.Stmt, qlstmt: qlast.Statement, *,
        ctx: context.ContextLevel, parent_ctx: context.ContextLevel) -> None:

    ctx.stmt = irstmt
    irstmt.parent_stmt = parent_ctx.stmt

    if irstmt.parent_stmt is None:
        irstmt.path_scope = parent_ctx.path_scope

    process_with_block(qlstmt, ctx=ctx, parent_ctx=parent_ctx)

    if qlstmt.iterator is not None:
        with ctx.newfence() as scopectx:
            # Paths in the statement must not be coalesced with
            # paths in view definitions, so the view scope must
            # "ignore the parent" when paths are added to the parent
            # scope.
            scopectx.path_scope.ignore_parent = True
            iterator_stmt = stmtctx.declare_view(
                qlstmt.iterator, qlstmt.iterator_alias, ctx=scopectx)
            irstmt.iterator_stmt = setgen.scoped_set(
                iterator_stmt, ctx=scopectx)

        ctx.singletons.add(irstmt.iterator_stmt.path_id)
        pathctx.register_path_in_scope(irstmt.iterator_stmt.path_id, ctx=ctx)


def fini_stmt(
        irstmt: irast.Stmt, qlstmt: qlast.Statement, *,
        ctx: context.ContextLevel,
        parent_ctx: context.ContextLevel) \
        -> typing.Union[irast.Set, irast.Stmt]:
    if irstmt.parent_stmt is None:
        irstmt.argument_types = ctx.arguments
        result = irstmt
        irutils.infer_type(irstmt, schema=ctx.schema)
    else:
        result = setgen.generated_set(irstmt, ctx=ctx)
        pathctx.register_path_in_scope(result.path_id, ctx=ctx)

    return result


def process_with_block(
        edgeql_tree: qlast.Base, *,
        ctx: context.ContextLevel, parent_ctx: context.ContextLevel) -> None:
    for with_entry in edgeql_tree.aliases:
        if isinstance(with_entry, qlast.NamespaceAliasDecl):
            ctx.namespaces[with_entry.alias] = with_entry.namespace

        elif isinstance(with_entry, qlast.AliasedExpr):
            with ctx.new() as scopectx:
                scopectx.path_scope = parent_ctx.path_scope.add_fence()
                # Paths in the statement must not be coalesced with
                # paths in view definitions, so the view scope must
                # "ignore the parent" when paths are added to the parent
                # scope.
                scopectx.path_scope.ignore_parent = True

                stmtctx.declare_view(
                    with_entry.expr, with_entry.alias, ctx=scopectx)

        else:
            raise RuntimeError(
                f'unexpected expression in WITH block: {with_entry}')


def compile_result_clause(
        result: qlast.Base,
        toplevel_rptr: typing.Optional[irast.Pointer],
        result_alias: typing.Optional[str]=None, *,
        ctx: context.ContextLevel) -> irast.Set:
    with ctx.new() as sctx:
        sctx.clause = 'result'

        if isinstance(result, qlast.Shape):
            expr = setgen.ensure_set(
                dispatch.compile(result.expr, ctx=sctx), ctx=sctx)
            shape = result.elements
        else:
            expr = dispatch.compile(result, ctx=sctx)
            shape = None

        pathctx.update_singletons(expr, ctx=sctx)

        if shape is not None:
            if expr.rptr is not None:
                rptr = expr.rptr
            else:
                rptr = toplevel_rptr

            expr = shapegen.compile_shape(expr, shape, rptr=rptr, ctx=sctx)

    expr = stmtctx.declare_aliased_set(expr, result_alias, ctx=ctx)
    pathctx.update_singletons(expr, ctx=ctx)

    return expr


def compile_groupby_clause(
        groupexprs: typing.Iterable[qlast.Base], *,
        singletons: typing.Set[irast.Set],
        ctx: context.ContextLevel) -> typing.List[irast.Set]:
    result = []
    if not groupexprs:
        return result

    with ctx.new() as sctx:
        sctx.clause = 'groupby'
        sctx.singletons = sctx.singletons.copy()
        sctx.singletons.update(singletons)

        ir_groupexprs = []
        for groupexpr in groupexprs:
            with sctx.newfence() as scopectx:
                ir_groupexpr = setgen.scoped_set(
                    dispatch.compile(groupexpr, ctx=scopectx), ctx=scopectx)
                ir_groupexpr.context = groupexpr.context
                ir_groupexprs.append(ir_groupexpr)

                ctx.singletons.add(ir_groupexpr.path_id)
                ctx.group_paths.add(ir_groupexpr.path_id)

    return ir_groupexprs
