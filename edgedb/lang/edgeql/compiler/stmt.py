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
from edgedb.lang.schema import types as s_types

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors

from . import astutils
from . import clauses
from . import context
from . import dispatch
from . import pathctx
from . import setgen
from . import viewgen
from . import schemactx
from . import stmtctx


@dispatch.compile.register(qlast.SelectQuery)
def compile_SelectQuery(
        expr: qlast.SelectQuery, *, ctx: context.ContextLevel) -> irast.Base:
    if astutils.is_degenerate_select(expr) and ctx.toplevel_stmt is not None:
        # Compile implicit "SELECT Path" as "Path"
        with ctx.new() as sctx:
            process_with_block(expr, ctx=sctx, parent_ctx=ctx)
            sctx.aliased_views = ctx.aliased_views.new_child()
            sctx.namespaces = ctx.namespaces.copy()
            sctx.anchors = ctx.anchors.copy()
            result = compile_result_clause(
                expr.result,
                view_scls=ctx.view_scls,
                view_rptr=ctx.view_rptr,
                view_name=ctx.toplevel_result_view_name,
                ctx=sctx)
            result = fini_stmt(result, expr, ctx=sctx, parent_ctx=ctx)

        return result

    with ctx.subquery() as sctx:
        stmt = irast.SelectStmt()
        init_stmt(stmt, expr, ctx=sctx, parent_ctx=ctx)

        if stmt is not ctx.toplevel_stmt:
            # Top-level statements have a scope branch created in init_stmt.
            if ctx.pending_path_scope is not None:
                sctx.path_scope = ctx.pending_path_scope
            else:
                sctx.path_scope = ctx.path_scope.add_fence()

        if expr.offset is not None or expr.limit is not None:
            # LIMIT and OFFSET are infix operators with both
            # operands being SET OF, so we need to compile
            # the body of the statement behind a fence.
            sctx.path_scope = sctx.path_scope.add_fence()

        stmt.result = compile_result_clause(
            expr.result,
            view_scls=ctx.view_scls,
            view_rptr=ctx.view_rptr,
            result_alias=expr.result_alias,
            view_name=ctx.toplevel_result_view_name,
            ctx=sctx)

        stmt.where = clauses.compile_where_clause(
            expr.where, ctx=sctx)

        stmt.orderby = clauses.compile_orderby_clause(
            expr.orderby, ctx=sctx)

        if expr.offset is not None or expr.limit is not None:
            with sctx.new() as olctx:
                olctx.path_scope = sctx.path_scope.parent

                stmt.offset = clauses.compile_limit_offset_clause(
                    expr.offset, ctx=olctx)

                stmt.limit = clauses.compile_limit_offset_clause(
                    expr.limit, ctx=olctx)

        result = fini_stmt(stmt, expr, ctx=sctx, parent_ctx=ctx)

        if sctx.path_scope is not ctx.pending_path_scope:
            result = setgen.scoped_set(result, ctx=sctx)

    return result


@dispatch.compile.register(qlast.ForQuery)
def compile_ForQuery(
        qlstmt: qlast.ForQuery, *, ctx: context.ContextLevel) -> irast.Base:
    with ctx.subquery() as sctx:
        stmt = irast.SelectStmt()
        init_stmt(stmt, qlstmt, ctx=sctx, parent_ctx=ctx)

        if stmt is not ctx.toplevel_stmt:
            # Top-level statements have a scope branch created in init_stmt.
            if ctx.pending_path_scope is not None:
                sctx.path_scope = ctx.pending_path_scope
            else:
                sctx.path_scope = ctx.path_scope.add_fence()

        if qlstmt.offset is not None or qlstmt.limit is not None:
            # LIMIT and OFFSET are infix operators with both
            # operands being SET OF, so we need to compile
            # the body of the statement behind a fence.
            sctx.path_scope = sctx.path_scope.add_fence()

        with sctx.newscope(fenced=True) as scopectx:
            iterator = qlstmt.iterator
            if isinstance(iterator, qlast.Set) and len(iterator.elements) == 1:
                iterator = iterator.elements[0]

            stmt.iterator_stmt = setgen.scoped_set(
                stmtctx.declare_view(iterator, qlstmt.iterator_alias,
                                     ctx=scopectx),
                ctx=scopectx)

        sctx.singletons.add(stmt.iterator_stmt.path_id)
        pathctx.register_set_in_scope(stmt.iterator_stmt, ctx=sctx)

        stmt.result = compile_result_clause(
            qlstmt.result,
            view_scls=ctx.view_scls,
            view_rptr=ctx.view_rptr,
            result_alias=qlstmt.result_alias,
            view_name=ctx.toplevel_result_view_name,
            ctx=sctx)

        stmt.where = clauses.compile_where_clause(
            qlstmt.where, ctx=sctx)

        stmt.orderby = clauses.compile_orderby_clause(
            qlstmt.orderby, ctx=sctx)

        if qlstmt.offset is not None or qlstmt.limit is not None:
            sctx.path_scope = sctx.path_scope.parent

            stmt.offset = clauses.compile_limit_offset_clause(
                qlstmt.offset, ctx=sctx)

            stmt.limit = clauses.compile_limit_offset_clause(
                qlstmt.limit, ctx=sctx)

        result = fini_stmt(stmt, qlstmt, ctx=sctx, parent_ctx=ctx)

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
        pathctx.register_set_in_scope(stmt.group_path_id, ctx=ictx)

        with ictx.newscope(fenced=True) as subjctx:
            subjctx.clause = 'input'

            subject_set = setgen.scoped_set(
                dispatch.compile(expr.subject, ctx=subjctx), ctx=subjctx)

            alias = expr.subject_alias or subject_set.path_id[0].name
            stmt.subject = stmtctx.declare_inline_view(
                subject_set, alias, ctx=ictx)

            with subjctx.new() as grpctx:
                stmt.groupby = compile_groupby_clause(
                    expr.groupby, singletons=grpctx.singletons, ctx=grpctx)

        with ictx.subquery() as isctx, isctx.newscope(fenced=True) as sctx:
            o_stmt = sctx.stmt = irast.SelectStmt()

            o_stmt.result = compile_result_clause(
                expr.result,
                view_scls=ctx.view_scls,
                view_rptr=ctx.view_rptr,
                result_alias=expr.result_alias,
                view_name=ctx.toplevel_result_view_name,
                ctx=sctx)

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

        stmt.subject = compile_query_subject(
            subject,
            shape=expr.shape,
            view_rptr=ctx.view_rptr,
            compile_views=True,
            result_alias=expr.subject_alias,
            is_insert=True,
            ctx=ictx)

        stmt.result = setgen.class_set(
            stmt.subject.scls.material_type(), ctx=ctx)

        result = fini_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.UpdateQuery)
def compile_UpdateQuery(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    with ctx.subquery() as ictx:
        stmt = irast.UpdateStmt()
        init_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

        subject = dispatch.compile(expr.subject, ctx=ictx)
        subj_type = irutils.infer_type(subject, ictx.schema)
        if not isinstance(subj_type, s_concepts.Concept):
            raise errors.EdgeQLError(
                f'cannot update non-Concept objects',
                context=expr.subject.context
            )

        stmt.subject = compile_query_subject(
            subject,
            shape=expr.shape,
            view_rptr=ctx.view_rptr,
            compile_views=True,
            result_alias=expr.subject_alias,
            is_update=True,
            ctx=ictx)

        stmt.result = setgen.class_set(
            stmt.subject.scls.material_type(), ctx=ctx)

        stmt.where = clauses.compile_where_clause(
            expr.where, ctx=ictx)

        result = fini_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.DeleteQuery)
def compile_DeleteQuery(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    with ctx.subquery() as ictx:
        stmt = irast.DeleteStmt()
        init_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

        # DELETE Expr is a delete(SET OF X), so we need a scope fence.
        with ictx.newscope(fenced=True) as scopectx:
            subject = setgen.scoped_set(
                dispatch.compile(expr.subject, ctx=scopectx), ctx=scopectx)

        subj_type = irutils.infer_type(subject, ictx.schema)
        if not isinstance(subj_type, s_concepts.Concept):
            raise errors.EdgeQLError(
                f'cannot delete non-Concept objects',
                context=expr.subject.context
            )

        stmt.subject = compile_query_subject(
            subject, shape=None, result_alias=expr.subject_alias, ctx=ictx)

        stmt.result = setgen.class_set(
            stmt.subject.scls.material_type(), ctx=ctx)
        stmt.result.path_id = stmt.subject.path_id

        result = fini_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.Shape)
def compile_Shape(
        shape: qlast.Shape, *, ctx: context.ContextLevel) -> irast.Base:
    expr = setgen.ensure_set(dispatch.compile(shape.expr, ctx=ctx), ctx=ctx)
    expr.scls = viewgen.process_view(
        scls=expr.scls, path_id=expr.path_id, elements=shape.elements, ctx=ctx)

    return expr


def init_stmt(
        irstmt: irast.Stmt, qlstmt: qlast.Statement, *,
        ctx: context.ContextLevel, parent_ctx: context.ContextLevel) -> None:

    ctx.stmt = irstmt
    if ctx.toplevel_stmt is None:
        parent_ctx.toplevel_stmt = ctx.toplevel_stmt = irstmt
        parent_ctx.path_scope = ctx.path_scope = irast.ScopeFenceNode()

    irstmt.parent_stmt = parent_ctx.stmt

    process_with_block(qlstmt, ctx=ctx, parent_ctx=parent_ctx)


def fini_stmt(
        irstmt: irast.Base, qlstmt: qlast.Statement, *,
        ctx: context.ContextLevel,
        parent_ctx: context.ContextLevel) -> irast.Set:
    irstmt.cardinality = qlstmt.cardinality

    view_name = parent_ctx.toplevel_result_view_name
    t = irutils.infer_type(irstmt, ctx.schema)

    if t.name == view_name:
        # The view statement did contain a view declaration and
        # generated a view class with the requested name.
        view = t
        path_id = pathctx.get_path_id(view, ctx=parent_ctx)
    elif view_name is not None:
        # The view statement did _not_ contain a view declaration,
        # but we still want the correct path_id.
        view = schemactx.derive_view(t, derived_name=view_name, ctx=parent_ctx)
        path_id = pathctx.get_path_id(view, ctx=parent_ctx)
    else:
        view = None
        path_id = None

    if ctx.stmt is ctx.toplevel_stmt:
        result = setgen.scoped_set(irstmt, path_id=path_id, ctx=ctx)
    else:
        result = setgen.ensure_set(irstmt, path_id=path_id, ctx=ctx)

    if view is not None:
        parent_ctx.view_sets[view] = result
        result.scls = view

    return result


def process_with_block(
        edgeql_tree: qlast.Base, *,
        ctx: context.ContextLevel, parent_ctx: context.ContextLevel) -> None:
    for with_entry in edgeql_tree.aliases:
        if isinstance(with_entry, qlast.NamespaceAliasDecl):
            ctx.namespaces[with_entry.alias] = with_entry.namespace

        elif isinstance(with_entry, qlast.AliasedExpr):
            with ctx.new() as scopectx:
                scopectx.expr_exposed = False
                stmtctx.declare_view(
                    with_entry.expr, with_entry.alias, ctx=scopectx)

        else:
            raise RuntimeError(
                f'unexpected expression in WITH block: {with_entry}')


def compile_result_clause(
        result: qlast.Base, *,
        view_scls: typing.Optional[s_types.Type]=None,
        view_rptr: typing.Optional[context.ViewRPtr]=None,
        view_name: typing.Optional[s_name.SchemaName]=None,
        result_alias: typing.Optional[str]=None,
        ctx: context.ContextLevel) -> irast.Set:
    with ctx.new() as sctx:
        sctx.clause = 'result'
        if sctx.stmt is ctx.toplevel_stmt:
            sctx.toplevel_clause = sctx.clause
            sctx.expr_exposed = True

        if isinstance(result, qlast.Shape):
            result_expr = result.expr
            shape = result.elements
        else:
            result_expr = result
            shape = None

        if result_alias:
            expr = stmtctx.declare_view(
                result_expr, alias=result_alias, temp_scope=False, ctx=sctx)

            pathctx.register_set_in_scope(expr, ctx=sctx)
        else:
            expr = setgen.ensure_set(
                dispatch.compile(result_expr, ctx=sctx), ctx=sctx)

        result = compile_query_subject(
            expr, shape=shape, view_rptr=view_rptr, view_name=view_name,
            result_alias=result_alias,
            view_scls=view_scls,
            compile_views=ctx.stmt is ctx.toplevel_stmt,
            ctx=sctx)

        ctx.partial_path_prefix = result

    return result


def compile_query_subject(
        expr: irast.Set, *,
        shape: typing.Optional[typing.List[qlast.ShapeElement]]=None,
        view_rptr: typing.Optional[context.ViewRPtr]=None,
        view_name: typing.Optional[s_name.SchemaName]=None,
        result_alias: typing.Optional[str]=None,
        view_scls: typing.Optional[s_types.Type]=None,
        compile_views: bool=True,
        is_insert: bool=False,
        is_update: bool=False,
        ctx: context.ContextLevel) -> irast.Set:

    if shape is not None and view_scls is None:
        if (view_name is None and
                isinstance(result_alias, s_name.SchemaName)):
            view_name = result_alias
        inner_path_id = expr.path_id

        view_scls = viewgen.process_view(
            scls=expr.scls, path_id=expr.path_id,
            elements=shape, view_rptr=view_rptr,
            view_name=view_name, is_insert=is_insert,
            is_update=is_update, ctx=ctx)
    else:
        inner_path_id = None

    if view_scls is not None:
        expr.scls = view_scls

    if compile_views and expr.scls is not None:
        rptr = view_rptr.rptr if view_rptr is not None else None
        viewgen.compile_view_shapes(expr, rptr=rptr, ctx=ctx)

    if inner_path_id is not None and len(inner_path_id) == 1:
        ctx.class_view_overrides[inner_path_id[0].name] = expr.scls

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
        if sctx.stmt.parent_stmt is None:
            sctx.toplevel_clause = sctx.clause

        sctx.singletons = sctx.singletons.copy()
        sctx.singletons.update(singletons)

        ir_groupexprs = []
        for groupexpr in groupexprs:
            with sctx.newscope(fenced=True) as scopectx:
                ir_groupexpr = setgen.scoped_set(
                    dispatch.compile(groupexpr, ctx=scopectx), ctx=scopectx)
                ir_groupexpr.context = groupexpr.context
                ir_groupexprs.append(ir_groupexpr)

                ctx.singletons.add(ir_groupexpr.path_id)

    return ir_groupexprs
