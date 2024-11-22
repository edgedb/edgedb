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


"""EdgeQL statement compilation routines."""


from __future__ import annotations
from typing import (
    Any,
    Optional,
    Tuple,
    Type,
    Union,
    Sequence,
    DefaultDict,
    List,
    cast
)

from collections import defaultdict
import textwrap

from edb import errors
from edb.common import ast
from edb.common.typeutils import not_none

from edb.ir import ast as irast
from edb.ir import typeutils
from edb.ir import utils as irutils

from edb.schema import ddl as s_ddl
from edb.schema import functions as s_func
from edb.schema import links as s_links
from edb.schema import properties as s_props
from edb.schema import modules as s_mod
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import pseudo as s_pseudo
from edb.schema import schema as s_schema
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from edb.edgeql import ast as qlast
from edb.edgeql import utils as qlutils
from edb.edgeql import qltypes
from edb.edgeql import desugar_group

from . import astutils
from . import clauses
from . import context
from . import config_desc
from . import dispatch
from . import inference
from . import pathctx
from . import policies
from . import setgen
from . import viewgen
from . import schemactx
from . import stmtctx
from . import typegen
from . import conflicts


def try_desugar(
    expr: qlast.Query, *, ctx: context.ContextLevel
) -> Optional[irast.Set]:
    new_syntax = desugar_group.try_group_rewrite(expr, aliases=ctx.aliases)
    if new_syntax:
        return dispatch.compile(new_syntax, ctx=ctx)
    return None


def _protect_expr(
    expr: Optional[qlast.Expr], *, ctx: context.ContextLevel
) -> None:
    if ctx.no_factoring or ctx.warn_factoring:
        while isinstance(expr, qlast.Shape):
            expr.allow_factoring = True
            expr = expr.expr
        if isinstance(expr, qlast.Path):
            expr.allow_factoring = True


@dispatch.compile.register(qlast.SelectQuery)
def compile_SelectQuery(
    expr: qlast.SelectQuery, *, ctx: context.ContextLevel
) -> irast.Set:
    if rewritten := try_desugar(expr, ctx=ctx):
        return rewritten

    _protect_expr(expr.result, ctx=ctx)

    with ctx.subquery() as sctx:
        stmt = irast.SelectStmt()
        init_stmt(stmt, expr, ctx=sctx, parent_ctx=ctx)
        if expr.implicit:
            # Make sure path prefix does not get blown away by
            # implicit subqueries.
            sctx.partial_path_prefix = ctx.partial_path_prefix
            stmt.implicit_wrapper = True

        # If there is an offset or a limit, this query was a wrapper
        # around something else, and we need to forward_rptr

        forward_rptr = (
            bool(expr.offset)
            or bool(expr.limit)
            or expr.rptr_passthrough
            # We need to preserve view_rptr if this SELECT is just
            # an implicit wrapping of a single DISTINCT, because otherwise
            # using a DISTINCT to satisfy link multiplicity requirement
            # will kill the link properties.
            #
            # This includes problems with initializing the schema itself.
            or (
                isinstance(expr.result, qlast.UnaryOp)
                and expr.result.op == 'DISTINCT'
            )
            or (
                isinstance(expr.result, qlast.FunctionCall)
                and expr.result.func in (
                    'assert_distinct', 'assert_single', 'assert_exists')
            )
        )

        stmt.result = compile_result_clause(
            expr.result,
            view_scls=ctx.view_scls,
            view_rptr=ctx.view_rptr,
            result_alias=expr.result_alias,
            view_name=ctx.toplevel_result_view_name,
            forward_rptr=forward_rptr,
            ctx=sctx)

        stmt.where = clauses.compile_where_clause(expr.where, ctx=sctx)

        stmt.orderby = clauses.compile_orderby_clause(expr.orderby, ctx=sctx)

        stmt.offset = clauses.compile_limit_offset_clause(
            expr.offset, ctx=sctx)

        stmt.limit = clauses.compile_limit_offset_clause(
            expr.limit, ctx=sctx)

        result = fini_stmt(stmt, ctx=sctx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.ForQuery)
def compile_ForQuery(
    qlstmt: qlast.ForQuery, *, ctx: context.ContextLevel
) -> irast.Set:
    if rewritten := try_desugar(qlstmt, ctx=ctx):
        return rewritten

    with ctx.subquery() as sctx:
        stmt = irast.SelectStmt(span=qlstmt.span)
        init_stmt(stmt, qlstmt, ctx=sctx, parent_ctx=ctx)

        # As an optimization, if the iterator is a singleton set, use
        # the element directly.
        iterator = qlstmt.iterator
        if isinstance(iterator, qlast.Set) and len(iterator.elements) == 1:
            iterator = iterator.elements[0]

        contains_dml = astutils.contains_dml(qlstmt.result, ctx=ctx)

        with sctx.new() as ectx:
            if ectx.expr_exposed:
                ectx.expr_exposed = context.Exposure.BINDING
            iterator_view = stmtctx.declare_view(
                astutils.ensure_ql_select(iterator),
                s_name.UnqualName(qlstmt.iterator_alias),
                factoring_fence=contains_dml,
                path_id_namespace=sctx.path_id_namespace,
                binding_kind=irast.BindingKind.For,
                ctx=ectx,
            )

        iterator_stmt = setgen.new_set_from_set(iterator_view, ctx=sctx)
        iterator_view.is_visible_binding_ref = True
        stmt.iterator_stmt = iterator_stmt

        iterator_type = setgen.get_set_type(iterator_stmt, ctx=ctx)
        if iterator_type.is_any(ctx.env.schema):
            raise errors.QueryError(
                'FOR statement has iterator of indeterminate type',
                span=ctx.env.type_origins.get(iterator_type),
            )

        view_scope_info = sctx.env.path_scope_map[iterator_view]

        if (
            qlstmt.optional
            and not qlstmt.from_desugaring
            and not ctx.env.options.testmode
        ):
            raise errors.UnsupportedFeatureError(
                "'FOR OPTIONAL' is an internal testing feature",
                span=qlstmt.span,
            )

        pathctx.register_set_in_scope(
            iterator_stmt,
            path_scope=sctx.path_scope,
            optional=qlstmt.optional,
            ctx=sctx,
        )

        sctx.iterator_path_ids |= {stmt.iterator_stmt.path_id}
        node = sctx.path_scope.find_descendant(iterator_stmt.path_id)
        if node is not None:
            # If the body contains DML, then we need to prohibit
            # correlation between the iterator and the enclosing
            # query, since the correlation imposes compilation issues
            # we aren't willing to tackle.
            #
            # Do this by sticking the iterator subtree onto a branch
            # with a factoring fence.
            if contains_dml:
                node = node.attach_branch()
                node.factoring_fence = True
                node.factoring_allowlist.update(ctx.iterator_path_ids)
                node = node.attach_branch()

            node.attach_subtree(
                view_scope_info.path_scope,
                span=iterator.span,
                ctx=ctx,
            )

        # Compile the body
        with sctx.newscope(fenced=True) as bctx:
            stmt.result = setgen.scoped_set(
                compile_result_clause(
                    # Make sure it is a stmt, so that shapes inside the body
                    # get resolved there.
                    astutils.ensure_ql_query(qlstmt.result),
                    view_scls=ctx.view_scls,
                    view_rptr=ctx.view_rptr,
                    view_name=ctx.toplevel_result_view_name,
                    forward_rptr=True,
                    ctx=bctx,
                ),
                ctx=bctx,
            )

        # Inject an implicit limit if appropriate
        if ((ctx.expr_exposed or sctx.stmt is ctx.toplevel_stmt)
                and ctx.implicit_limit):
            stmt.limit = dispatch.compile(
                qlast.Constant.integer(ctx.implicit_limit),
                ctx=sctx,
            )

        result = fini_stmt(stmt, ctx=sctx, parent_ctx=ctx)

    return result


def _make_group_binding(
    stype: s_types.Type,
    alias: str,
    *,
    ctx: context.ContextLevel,
) -> irast.Set:
    """Make a binding for one of the "dummy" bindings used in group"""
    binding_type = schemactx.derive_view(
        stype,
        derived_name=s_name.QualName('__derived__', alias),
        preserve_shape=True, ctx=ctx)

    binding_set = setgen.class_set(binding_type, ctx=ctx)
    binding_set.is_visible_binding_ref = True

    name = s_name.UnqualName(alias)
    ctx.aliased_views[name] = binding_set
    ctx.view_sets[binding_type] = binding_set
    ctx.env.path_scope_map[binding_set] = context.ScopeInfo(
        path_scope=ctx.path_scope,
        binding_kind=irast.BindingKind.For,
        pinned_path_id_ns=ctx.path_id_namespace,
    )

    return binding_set


@dispatch.compile.register(qlast.InternalGroupQuery)
def compile_InternalGroupQuery(
    expr: qlast.InternalGroupQuery, *, ctx: context.ContextLevel
) -> irast.Set:
    # We disallow use of FOR GROUP except for when running in test mode.
    if not expr.from_desugaring and not ctx.env.options.testmode:
        raise errors.UnsupportedFeatureError(
            "'FOR GROUP' is an internal testing feature",
            span=expr.span,
        )

    _protect_expr(expr.subject, ctx=ctx)
    _protect_expr(expr.result, ctx=ctx)

    with ctx.subquery() as sctx:
        stmt = irast.GroupStmt(by=expr.by)
        init_stmt(stmt, expr, ctx=sctx, parent_ctx=ctx)

        with sctx.newscope(fenced=True) as topctx:
            # N.B: Subject is exposed because we want any shape on the
            # subject to be exposed on bare references to the group
            # alias.  This is frankly pretty dodgy behavior for
            # FOR GROUP to have but the real GROUP needs to
            # maintain shapes, and this is the easiest way to handle
            # that.
            stmt.subject = compile_result_clause(
                expr.subject,
                result_alias=expr.subject_alias,
                exprtype=s_types.ExprType.Group,
                ctx=topctx)

            if topctx.partial_path_prefix:
                pathctx.register_set_in_scope(
                    topctx.partial_path_prefix, ctx=topctx)

            # compile the USING
            assert expr.using is not None

            for using_entry in expr.using:
                # Fail on keys named 'id', since we can't put them
                # in the output free object.
                if using_entry.alias == 'id':
                    raise errors.UnsupportedFeatureError(
                        "may not name a grouping alias 'id'",
                        span=using_entry.span,
                    )
                elif desugar_group.key_name(using_entry.alias) == 'id':
                    raise errors.UnsupportedFeatureError(
                        "may not group by a field named id",
                        span=using_entry.expr.span,
                        hint="try 'using id_ := .id'",
                    )

                with topctx.newscope(fenced=True) as scopectx:
                    if scopectx.expr_exposed:
                        scopectx.expr_exposed = context.Exposure.BINDING
                    binding = stmtctx.declare_view(
                        using_entry.expr,
                        s_name.UnqualName(using_entry.alias),
                        binding_kind=irast.BindingKind.With,
                        path_id_namespace=scopectx.path_id_namespace,
                        ctx=scopectx,
                    )
                    binding.span = using_entry.expr.span
                    stmt.using[using_entry.alias] = (
                        setgen.new_set_from_set(binding, ctx=sctx),
                        qltypes.Cardinality.UNKNOWN)
                    binding.is_visible_binding_ref = True

            subject_stype = setgen.get_set_type(stmt.subject, ctx=topctx)
            stmt.group_binding = _make_group_binding(
                subject_stype, expr.group_alias, ctx=topctx)

            # # Compile the shape on the group binding, in case we need it
            # viewgen.late_compile_view_shapes(stmt.group_binding, ctx=topctx)

            if expr.grouping_alias:
                ctx.env.schema, grouping_stype = s_types.Array.create(
                    ctx.env.schema,
                    element_type=(
                        ctx.env.schema.get('std::str', type=s_types.Type)
                    )
                )
                stmt.grouping_binding = _make_group_binding(
                    grouping_stype, expr.grouping_alias, ctx=topctx)

        # Check that the by clause is legit
        by_refs = ast.find_children(stmt.by, qlast.ObjectRef)
        for by_ref in by_refs:
            if by_ref.name not in stmt.using:
                raise errors.InvalidReferenceError(
                    f"variable '{by_ref.name}' referenced in BY but not "
                    f"declared in USING",
                    span=by_ref.span,
                )

        # compile the output
        # newscope because we don't want the result to get assigned the
        # same statement scope as the subject and elements, which we
        # need to stick in the real GROUP BY
        with sctx.newscope(fenced=True) as bctx:
            pathctx.register_set_in_scope(
                stmt.group_binding, path_scope=bctx.path_scope, ctx=bctx
            )

            # Compile the shape on the group binding, in case we need it
            viewgen.late_compile_view_shapes(stmt.group_binding, ctx=bctx)

            node = bctx.path_scope.find_descendant(stmt.group_binding.path_id)
            not_none(node).is_group = True
            for using_value, _ in stmt.using.values():
                pathctx.register_set_in_scope(
                    using_value, path_scope=bctx.path_scope, ctx=bctx
                )

            if stmt.grouping_binding:
                pathctx.register_set_in_scope(
                    stmt.grouping_binding, path_scope=bctx.path_scope, ctx=bctx
                )

            stmt.result = compile_result_clause(
                astutils.ensure_ql_query(expr.result),
                result_alias=expr.result_alias,
                ctx=bctx)

            stmt.where = clauses.compile_where_clause(expr.where, ctx=bctx)

            stmt.orderby = clauses.compile_orderby_clause(
                expr.orderby, ctx=bctx)

        result = fini_stmt(stmt, ctx=sctx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.GroupQuery)
def compile_GroupQuery(
    expr: qlast.GroupQuery, *, ctx: context.ContextLevel
) -> irast.Set:
    return dispatch.compile(
        desugar_group.desugar_group(expr, ctx.aliases),
        ctx=ctx,
    )


@dispatch.compile.register(qlast.InsertQuery)
def compile_InsertQuery(
    expr: qlast.InsertQuery, *, ctx: context.ContextLevel
) -> irast.Set:

    if ctx.disallow_dml:
        raise errors.QueryError(
            f'INSERT statements cannot be used {ctx.disallow_dml}',
            hint=(
                f'To resolve this try to factor out the mutation '
                f'expression into the top-level WITH block.'
            ),
            span=expr.span,
        )

    # Record this node in the list of potential DML expressions.
    ctx.env.dml_exprs.append(expr)

    with ctx.subquery() as ictx:
        stmt = irast.InsertStmt(span=expr.span)
        init_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

        with ictx.new() as ectx:
            ectx.expr_exposed = context.Exposure.UNEXPOSED
            subject = dispatch.compile(
                qlast.Path(steps=[expr.subject], allow_factoring=True), ctx=ectx
            )
        assert isinstance(subject, irast.Set)

        subject_stype = setgen.get_set_type(subject, ctx=ictx)

        # If we are INSERTing a type that we are in the ELSE block of,
        # we need to error out.
        if ictx.inserting_paths.get(subject.path_id) == 'else':
            setgen.raise_self_insert_error(
                subject_stype, expr.subject.span, ctx=ctx)

        if subject_stype.get_abstract(ctx.env.schema):
            raise errors.QueryError(
                f'cannot insert into abstract '
                f'{subject_stype.get_verbosename(ctx.env.schema)}',
                span=expr.subject.span)

        if subject_stype.is_free_object_type(ctx.env.schema):
            raise errors.QueryError(
                f'free objects cannot be inserted',
                span=expr.subject.span)

        if subject_stype.is_view(ctx.env.schema):
            raise errors.QueryError(
                f'cannot insert into expression alias '
                f'{str(subject_stype.get_shortname(ctx.env.schema))!r}',
                span=expr.subject.span)

        if _is_forbidden_stdlib_type_for_mod(subject_stype, ctx):
            raise errors.QueryError(
                f'cannot insert standard library type '
                f'{subject_stype.get_displayname(ctx.env.schema)}',
                span=expr.subject.span)

        with ictx.new() as bodyctx:
            # Self-references in INSERT are prohibited.
            pathctx.ban_inserting_path(
                subject.path_id, location='body', ctx=bodyctx)

            bodyctx.class_view_overrides = ictx.class_view_overrides.copy()
            bodyctx.implicit_id_in_shapes = False
            bodyctx.implicit_tid_in_shapes = False
            bodyctx.implicit_tname_in_shapes = False
            bodyctx.implicit_limit = 0

            stmt.subject = compile_query_subject(
                subject,
                shape=expr.shape,
                view_rptr=ctx.view_rptr,
                compile_views=True,
                exprtype=s_types.ExprType.Insert,
                ctx=bodyctx,
                span=expr.span,
            )

        stmt_subject_stype = setgen.get_set_type(subject, ctx=ictx)
        assert isinstance(stmt_subject_stype, s_objtypes.ObjectType)

        stmt.conflict_checks = conflicts.compile_inheritance_conflict_checks(
            stmt, stmt_subject_stype, ctx=ictx)

        if expr.unless_conflict is not None:
            constraint_spec, else_branch = expr.unless_conflict

            if constraint_spec:
                stmt.on_conflict = conflicts.compile_insert_unless_conflict_on(
                    stmt, stmt_subject_stype, constraint_spec, else_branch,
                    ctx=ictx)
            else:
                stmt.on_conflict = conflicts.compile_insert_unless_conflict(
                    stmt, stmt_subject_stype, ctx=ictx)

        mat_stype = schemactx.get_material_type(stmt_subject_stype, ctx=ctx)
        result = setgen.class_set(
            mat_stype, path_id=stmt.subject.path_id, ctx=ctx
        )

        with ictx.new() as resultctx:
            stmt.result = compile_query_subject(
                result,
                view_scls=ctx.view_scls,
                view_name=ctx.toplevel_result_view_name,
                compile_views=ictx.stmt is ictx.toplevel_stmt,
                ctx=resultctx,
                span=expr.span,
            )

        if pol_condition := policies.compile_dml_write_policies(
            mat_stype, result, mode=qltypes.AccessKind.Insert, ctx=ictx
        ):
            stmt.write_policies[mat_stype.id] = pol_condition

        # Compute the unioned output type if needed
        if stmt.on_conflict and stmt.on_conflict.else_ir:
            final_typ = typegen.infer_common_type(
                [stmt.result, stmt.on_conflict.else_ir], ctx.env)
            if final_typ is None:
                raise errors.QueryError('could not determine INSERT type',
                                        span=stmt.span)
            stmt.final_typeref = typegen.type_to_typeref(final_typ, env=ctx.env)

        # Wrap the statement.
        result = fini_stmt(stmt, ctx=ictx, parent_ctx=ctx)

        # If we have an ELSE clause, and this is a toplevel statement,
        # we need to compile_query_subject *again* on the outer query,
        # in order to produce a view for the joined output, which we
        # need to have to generate the proper type descriptor.  This
        # feels like somewhat of a hack; I think it might be possible
        # to do something more general elsewhere.
        if (
            expr.unless_conflict
            and expr.unless_conflict[1]
            and ictx.stmt is ctx.toplevel_stmt
        ):
            with ictx.new() as resultctx:
                resultctx.expr_exposed = context.Exposure.EXPOSED
                result = compile_query_subject(
                    result,
                    view_name=ctx.toplevel_result_view_name,
                    compile_views=ictx.stmt is ctx.toplevel_stmt,
                    ctx=resultctx,
                    span=result.span,
                )

    return result


@dispatch.compile.register(qlast.UpdateQuery)
def compile_UpdateQuery(
    expr: qlast.UpdateQuery, *, ctx: context.ContextLevel
) -> irast.Set:

    if ctx.disallow_dml:
        raise errors.QueryError(
            f'UPDATE statements cannot be used {ctx.disallow_dml}',
            hint=(
                f'To resolve this try to factor out the mutation '
                f'expression into the top-level WITH block.'
            ),
            span=expr.span,
        )

    _protect_expr(expr.subject, ctx=ctx)

    # Record this node in the list of DML statements.
    ctx.env.dml_exprs.append(expr)

    with ctx.subquery() as ictx:
        stmt = irast.UpdateStmt(
            span=expr.span,
            sql_mode_link_only=expr.sql_mode_link_only,
        )
        init_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

        with ictx.new() as ectx:
            ectx.expr_exposed = context.Exposure.UNEXPOSED
            subject = dispatch.compile(expr.subject, ctx=ectx)
        assert isinstance(subject, irast.Set)

        subj_type = setgen.get_set_type(subject, ctx=ictx)
        if not isinstance(subj_type, s_objtypes.ObjectType):
            raise errors.QueryError(
                f'cannot update non-ObjectType objects',
                span=expr.subject.span
            )

        if subj_type.is_free_object_type(ctx.env.schema):
            raise errors.QueryError(
                f'free objects cannot be updated',
                span=expr.subject.span)

        mat_stype = schemactx.concretify(subj_type, ctx=ctx)

        if _is_forbidden_stdlib_type_for_mod(mat_stype, ctx):
            raise errors.QueryError(
                f'cannot update standard library type '
                f'{subj_type.get_displayname(ctx.env.schema)}',
                span=expr.subject.span)

        stmt._material_type = typeutils.type_to_typeref(
            ctx.env.schema,
            mat_stype,
            include_children=True,
            include_ancestors=True,
            cache=ctx.env.type_ref_cache,
        )

        ictx.partial_path_prefix = subject

        stmt.where = clauses.compile_where_clause(expr.where, ctx=ictx)

        with ictx.new() as bodyctx:
            bodyctx.class_view_overrides = ictx.class_view_overrides.copy()
            bodyctx.implicit_id_in_shapes = False
            bodyctx.implicit_tid_in_shapes = False
            bodyctx.implicit_tname_in_shapes = False
            bodyctx.implicit_limit = 0

            stmt.subject = compile_query_subject(
                subject,
                shape=expr.shape,
                view_rptr=ctx.view_rptr,
                compile_views=True,
                exprtype=s_types.ExprType.Update,
                ctx=bodyctx,
                span=expr.span,
            )
            # If we are doing a SQL-mode link only update (that is,
            # we are doing a SQL INSERT or DELETE to a link table),
            # disable rewrites.
            # HACK: This is a really ass-backwards way to accomplish that.
            if stmt.sql_mode_link_only:
                ctx.env.dml_rewrites.pop(stmt.subject, None)

        result = setgen.class_set(
            mat_stype, path_id=stmt.subject.path_id, ctx=ctx,
        )

        with ictx.new() as resultctx:
            stmt.result = compile_query_subject(
                result,
                view_scls=ctx.view_scls,
                view_name=ctx.toplevel_result_view_name,
                compile_views=ictx.stmt is ictx.toplevel_stmt,
                ctx=resultctx,
                span=expr.span,
            )

        for dtype in schemactx.get_all_concrete(mat_stype, ctx=ctx):
            if read_pol := policies.compile_dml_read_policies(
                dtype, result, mode=qltypes.AccessKind.UpdateRead, ctx=ictx
            ):
                stmt.read_policies[dtype.id] = read_pol
            if write_pol := policies.compile_dml_write_policies(
                dtype, result, mode=qltypes.AccessKind.UpdateWrite, ctx=ictx
            ):
                stmt.write_policies[dtype.id] = write_pol

        stmt.conflict_checks = conflicts.compile_inheritance_conflict_checks(
            stmt, mat_stype, ctx=ictx)

        result = fini_stmt(stmt, ctx=ictx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.DeleteQuery)
def compile_DeleteQuery(
    expr: qlast.DeleteQuery, *, ctx: context.ContextLevel
) -> irast.Set:

    if ctx.disallow_dml:
        raise errors.QueryError(
            f'DELETE statements cannot be used {ctx.disallow_dml}',
            hint=(
                f'To resolve this try to factor out the mutation '
                f'expression into the top-level WITH block.'
            ),
            span=expr.span,
        )

    _protect_expr(expr.subject, ctx=ctx)

    # Record this node in the list of potential DML expressions.
    ctx.env.dml_exprs.append(expr)

    with ctx.subquery() as ictx:
        stmt = irast.DeleteStmt(span=expr.span)
        # Expand the DELETE from sugar into full DELETE (SELECT ...)
        # form, if there's any additional clauses.
        if any([expr.where, expr.orderby, expr.offset, expr.limit]):
            if expr.offset or expr.limit:
                subjql = qlast.SelectQuery(
                    result=qlast.SelectQuery(
                        result=expr.subject,
                        where=expr.where,
                        orderby=expr.orderby,
                        span=expr.span,
                        implicit=True,
                    ),
                    limit=expr.limit,
                    offset=expr.offset,
                    span=expr.span,
                )
            else:
                subjql = qlast.SelectQuery(
                    result=expr.subject,
                    where=expr.where,
                    orderby=expr.orderby,
                    offset=expr.offset,
                    limit=expr.limit,
                    span=expr.span,
                )

            expr = qlast.DeleteQuery(
                aliases=expr.aliases,
                span=expr.span,
                subject=subjql,
            )

        init_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

        # DELETE Expr is a delete(SET OF X), so we need a scope fence.
        with ictx.newscope(fenced=True) as scopectx:
            scopectx.implicit_limit = 0
            scopectx.expr_exposed = context.Exposure.UNEXPOSED
            subject = setgen.scoped_set(
                dispatch.compile(expr.subject, ctx=scopectx), ctx=scopectx)

        subj_type = setgen.get_set_type(subject, ctx=ictx)
        if not isinstance(subj_type, s_objtypes.ObjectType):
            raise errors.QueryError(
                f'cannot delete non-ObjectType objects',
                span=expr.subject.span
            )

        if subj_type.is_free_object_type(ctx.env.schema):
            raise errors.QueryError(
                f'free objects cannot be deleted',
                span=expr.subject.span)

        mat_stype = schemactx.concretify(subj_type, ctx=ctx)

        if _is_forbidden_stdlib_type_for_mod(mat_stype, ctx):
            raise errors.QueryError(
                f'cannot delete standard library type '
                f'{subj_type.get_displayname(ctx.env.schema)}',
                span=expr.subject.span)

        stmt._material_type = typeutils.type_to_typeref(
            ctx.env.schema,
            mat_stype,
            include_children=True,
            include_ancestors=True,
            cache=ctx.env.type_ref_cache,
        )

        with ictx.new() as bodyctx:
            bodyctx.implicit_id_in_shapes = False
            bodyctx.implicit_tid_in_shapes = False
            bodyctx.implicit_tname_in_shapes = False

            stmt.subject = compile_query_subject(
                subject,
                shape=None,
                exprtype=s_types.ExprType.Delete,
                ctx=bodyctx,
                span=expr.span,
            )

        result = setgen.class_set(
            mat_stype, path_id=stmt.subject.path_id, ctx=ctx
        )

        with ictx.new() as resultctx:
            stmt.result = compile_query_subject(
                result,
                view_scls=ctx.view_scls,
                view_name=ctx.toplevel_result_view_name,
                compile_views=ictx.stmt is ictx.toplevel_stmt,
                ctx=resultctx,
                span=expr.span,
            )

        for dtype in schemactx.get_all_concrete(mat_stype, ctx=ctx):
            # Compile policies for every concrete type
            if pol_cond := policies.compile_dml_read_policies(
                dtype, result, mode=qltypes.AccessKind.Delete, ctx=ictx
            ):
                stmt.read_policies[dtype.id] = pol_cond

            schema = ctx.env.schema
            # And find any pointers to delete
            ptrs = []
            for ptr in dtype.get_pointers(schema).objects(schema):
                # If there is a pointer that has a real table and doesn't
                # have a special ON SOURCE DELETE policy, arrange to
                # delete it in the query itself.
                if not ptr.is_pure_computable(schema) and (
                    not ptr.singular(schema)
                    or ptr.has_user_defined_properties(schema)
                ) and (
                    not isinstance(ptr, s_links.Link)
                    or ptr.get_on_source_delete(schema) ==
                    s_links.LinkSourceDeleteAction.Allow
                ):
                    ptrs.append(typegen.ptr_to_ptrref(ptr, ctx=ctx))

            stmt.links_to_delete[dtype.id] = tuple(ptrs)

        result = fini_stmt(stmt, ctx=ictx, parent_ctx=ctx)

    return result


@dispatch.compile.register
def compile_DescribeStmt(
    ql: qlast.DescribeStmt, *, ctx: context.ContextLevel
) -> irast.Set:
    with ctx.subquery() as ictx:
        stmt = irast.SelectStmt()
        init_stmt(stmt, ql, ctx=ictx, parent_ctx=ctx)

        if ql.object is qlast.DescribeGlobal.Schema:
            if ql.language is qltypes.DescribeLanguage.DDL:
                # DESCRIBE SCHEMA AS DDL
                text = s_ddl.ddl_text_from_schema(
                    ctx.env.schema,
                )
            elif ql.language is qltypes.DescribeLanguage.SDL:
                # DESCRIBE SCHEMA AS SDL
                text = s_ddl.sdl_text_from_schema(
                    ctx.env.schema,
                )
            else:
                raise errors.QueryError(
                    f'cannot describe full schema as {ql.language}')

            ct = typegen.type_to_typeref(
                ctx.env.get_schema_type_and_track(
                    s_name.QualName('std', 'str')),
                env=ctx.env,
            )

            stmt.result = setgen.ensure_set(
                irast.StringConstant(value=text, typeref=ct),
                ctx=ictx,
            )

        elif ql.object is qlast.DescribeGlobal.DatabaseConfig:
            if ql.language is qltypes.DescribeLanguage.DDL:
                stmt.result = config_desc.compile_describe_config(
                    qltypes.ConfigScope.DATABASE, ctx=ictx)
            else:
                raise errors.QueryError(
                    f'cannot describe config as {ql.language}')

        elif ql.object is qlast.DescribeGlobal.InstanceConfig:
            if ql.language is qltypes.DescribeLanguage.DDL:
                stmt.result = config_desc.compile_describe_config(
                    qltypes.ConfigScope.INSTANCE, ctx=ictx)
            else:
                raise errors.QueryError(
                    f'cannot describe config as {ql.language}')

        elif ql.object is qlast.DescribeGlobal.Roles:
            if ql.language is qltypes.DescribeLanguage.DDL:
                function_call = dispatch.compile(
                    qlast.FunctionCall(
                        func=('sys', '_describe_roles_as_ddl'),
                    ),
                    ctx=ictx)
                stmt.result = function_call
            else:
                raise errors.QueryError(
                    f'cannot describe roles as {ql.language}')

        else:
            assert isinstance(ql.object, qlast.ObjectRef), ql.object
            modules = []
            items: DefaultDict[str, List[s_name.Name]] = defaultdict(list)
            referenced_classes: List[s_obj.ObjectMeta] = []

            objref = ql.object
            itemclass = objref.itemclass

            if itemclass is qltypes.SchemaObjectClass.MODULE:
                mod = s_name.UnqualName(str(s_utils.ast_ref_to_name(objref)))
                if not ctx.env.schema.get_global(
                        s_mod.Module, mod, None):
                    raise errors.InvalidReferenceError(
                        f"module '{mod}' does not exist",
                        span=objref.span,
                    )

                modules.append(mod)
            else:
                itemtype: Optional[Type[s_obj.Object]] = None

                name = s_utils.ast_ref_to_name(objref)
                if itemclass is not None:
                    if itemclass is qltypes.SchemaObjectClass.ALIAS:
                        # Look for underlying derived type.
                        itemtype = s_types.Type
                    else:
                        itemtype = (
                            s_obj.ObjectMeta.get_schema_metaclass_for_ql_class(
                                itemclass)
                        )

                last_exc = None
                # Search in the current namespace AND in std. We do
                # this to avoid masking a `std` object/function by one
                # in a default module.
                search_ns = [ictx.modaliases]
                # Only check 'std' separately if the current
                # modaliases don't already include it.
                if ictx.modaliases.get(None, 'std') != 'std':
                    search_ns.append({None: 'std'})

                # Search in the current namespace AND in std.
                for aliases in search_ns:
                    # Use the specific modaliases instead of the
                    # context ones.
                    with ictx.subquery() as newctx:
                        newctx.modaliases = aliases
                        # Get the default module name
                        modname = aliases[None]
                        # Is the current item a function
                        is_function = (itemclass is
                                       qltypes.SchemaObjectClass.FUNCTION)

                        # We need to check functions if we're looking for them
                        # specifically or if this is a broad search. They are
                        # handled separately because they allow multiple
                        # matches for the same name.
                        if (itemclass is None or is_function):
                            try:
                                funcs: Tuple[s_func.Function, ...] = (
                                    newctx.env.schema.get_functions(
                                        name,
                                        module_aliases=aliases)
                                )
                            except errors.InvalidReferenceError:
                                pass
                            else:
                                for func in funcs:
                                    items[f'function_{modname}'].append(
                                        func.get_name(newctx.env.schema))

                        # Also find an object matching the name as long as
                        # it's not a function we're looking for specifically.
                        if not is_function:
                            try:
                                if itemclass is not \
                                        qltypes.SchemaObjectClass.ALIAS:
                                    condition = None
                                    label = None
                                else:
                                    condition = (
                                        lambda obj:
                                        obj.get_alias_is_persistent(
                                            ctx.env.schema
                                        )
                                    )
                                    label = 'alias'
                                obj = schemactx.get_schema_object(
                                    objref,
                                    item_type=itemtype,
                                    condition=condition,
                                    label=label,
                                    ctx=newctx,
                                )
                                items[f'other_{modname}'].append(
                                    obj.get_name(newctx.env.schema))
                            except errors.InvalidReferenceError as exc:
                                # Record the exception to be possibly
                                # raised if no matches are found
                                last_exc = exc

                # If we already have some results, suppress the exception,
                # otherwise raise the recorded exception.
                if not items and last_exc:
                    raise last_exc

                if not items:
                    raise errors.InvalidReferenceError(
                        f"{str(itemclass).lower()} '{objref.name}' "
                        f"does not exist",
                        span=objref.span,
                    )

            verbose = ql.options.get_flag('VERBOSE')

            method: Any
            if ql.language is qltypes.DescribeLanguage.DDL:
                method = s_ddl.ddl_text_from_schema
            elif ql.language is qltypes.DescribeLanguage.SDL:
                method = s_ddl.sdl_text_from_schema
            elif ql.language is qltypes.DescribeLanguage.TEXT:
                method = s_ddl.descriptive_text_from_schema
                if not verbose.val:
                    referenced_classes = [s_links.Link, s_props.Property]
            else:
                raise errors.InternalServerError(
                    f'cannot handle describe language {ql.language}'
                )

            # Based on the items found generate main text and a
            # potential comment about masked items.
            defmod = ictx.modaliases.get(None, 'std')
            default_items = []
            masked_items = set()
            for objtype in ['function', 'other']:
                defkey = f'{objtype}_{defmod}'
                mskkey = f'{objtype}_std'

                default_items += items.get(defkey, [])
                if defkey in items and mskkey in items:
                    # We have a match in default module and some masked.
                    masked_items.update(items.get(mskkey, []))
                else:
                    default_items += items.get(mskkey, [])

            # Throw out anything in the masked set that's already in
            # the default.
            masked_items.difference_update(default_items)

            text = method(
                ctx.env.schema,
                included_modules=modules,
                included_items=default_items,
                included_ref_classes=referenced_classes,
                include_module_ddl=False,
                include_std_ddl=True,
            )
            if masked_items:
                text += ('\n\n'
                         '# The following builtins are masked by the above:'
                         '\n\n')
                masked = method(
                    ctx.env.schema,
                    included_modules=modules,
                    included_items=masked_items,
                    included_ref_classes=referenced_classes,
                    include_module_ddl=False,
                    include_std_ddl=True,
                )
                masked = textwrap.indent(masked, '# ')
                text += masked

            ct = typegen.type_to_typeref(
                ctx.env.get_schema_type_and_track(
                    s_name.QualName('std', 'str')),
                env=ctx.env,
            )

            stmt.result = setgen.ensure_set(
                irast.StringConstant(value=text, typeref=ct),
                ctx=ictx,
            )

        result = fini_stmt(stmt, ctx=ictx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.Shape)
def compile_Shape(
    shape: qlast.Shape, *, ctx: context.ContextLevel
) -> irast.Set:

    if ctx.no_factoring and not shape.allow_factoring:
        return dispatch.compile(
            qlast.SelectQuery(result=shape, implicit=True),
            ctx=ctx,
        )

    if ctx.warn_factoring and not shape.allow_factoring:
        with ctx.newscope(fenced=False) as subctx:
            subctx.path_scope.warn = True
            _protect_expr(shape, ctx=ctx)
            return dispatch.compile(
                shape, ctx=subctx
            )

    shape_expr = shape.expr or qlutils.FREE_SHAPE_EXPR
    with ctx.new() as subctx:
        subctx.qlstmt = astutils.ensure_ql_query(shape)
        subctx.stmt = stmt = irast.SelectStmt()
        ctx.env.compiled_stmts[subctx.qlstmt] = stmt
        subctx.class_view_overrides = subctx.class_view_overrides.copy()

        with subctx.new() as exposed_ctx:
            exposed_ctx.expr_exposed = context.Exposure.UNEXPOSED
            expr = dispatch.compile(shape_expr, ctx=exposed_ctx)

        expr_stype = setgen.get_set_type(expr, ctx=ctx)
        if not isinstance(expr_stype, s_objtypes.ObjectType):
            raise errors.QueryError(
                f'shapes cannot be applied to '
                f'{expr_stype.get_verbosename(ctx.env.schema)}',
                span=shape.span,
            )

        stmt.result = compile_query_subject(
            expr,
            shape=shape.elements,
            compile_views=False,
            ctx=subctx,
            span=expr.span)

        ir_result = setgen.ensure_set(stmt, ctx=subctx)

    return ir_result


def init_stmt(
    irstmt: irast.Stmt,
    qlstmt: qlast.Statement,
    *,
    ctx: context.ContextLevel,
    parent_ctx: context.ContextLevel,
) -> None:

    ctx.env.compiled_stmts[qlstmt] = irstmt

    if isinstance(irstmt, irast.MutatingStmt):
        # This is some kind of mutation, so we need to check if it is
        # allowed.
        if ctx.env.options.in_ddl_context_name is not None:
            raise errors.SchemaDefinitionError(
                f'mutations are invalid in '
                f'{ctx.env.options.in_ddl_context_name}',
                span=qlstmt.span,
            )
        elif (
            (dv := ctx.defining_view) is not None
            and dv.get_expr_type(ctx.env.schema) is s_types.ExprType.Select
            and not (
                # We allow DML in trivial *top-level* free objects
                ctx.partial_path_prefix
                and irutils.is_trivial_free_object(
                    irutils.unwrap_set(ctx.partial_path_prefix))
                # Find the enclosing context at the point the free object
                # was defined.
                and (outer_ctx := next((
                    x for x in reversed(ctx._stack.stack)
                    if isinstance(x, context.ContextLevel)
                    and x.partial_path_prefix != ctx.partial_path_prefix
                ), None))
                and outer_ctx.expr_exposed
            )
        ):
            # This is some shape in a regular query. Although
            # DML is not allowed in the computable, but it may
            # be possible to refactor it.
            raise errors.QueryError(
                f"mutations are invalid in a shape's computed expression",
                hint=(
                    f'To resolve this try to factor out the mutation '
                    f'expression into the top-level WITH block.'
                ),
                span=qlstmt.span,
            )

    ctx.stmt = irstmt
    ctx.qlstmt = qlstmt
    if ctx.toplevel_stmt is None:
        parent_ctx.toplevel_stmt = ctx.toplevel_stmt = irstmt

    ctx.path_scope = parent_ctx.path_scope.attach_fence()
    ctx.path_scope.warn = ctx.warn_factoring

    pending_own_ns = parent_ctx.pending_stmt_own_path_id_namespace
    if pending_own_ns:
        ctx.path_scope.add_namespaces(pending_own_ns)

    pending_full_ns = parent_ctx.pending_stmt_full_path_id_namespace
    if pending_full_ns:
        ctx.path_id_namespace |= pending_full_ns

    irstmt.parent_stmt = parent_ctx.stmt

    irstmt.bindings = process_with_block(
        qlstmt, ctx=ctx, parent_ctx=parent_ctx)

    if isinstance(irstmt, irast.MutatingStmt):
        ctx.path_scope.factoring_fence = True
        ctx.path_scope.factoring_allowlist.update(ctx.iterator_path_ids)


def fini_stmt(
    irstmt: Union[irast.Stmt, irast.Set],
    *,
    ctx: context.ContextLevel,
    parent_ctx: context.ContextLevel,
) -> irast.Set:

    view_name = parent_ctx.toplevel_result_view_name
    t = setgen.get_expr_type(irstmt, ctx=ctx)

    view: Optional[s_types.Type]
    path_id: Optional[irast.PathId]

    if isinstance(irstmt, irast.MutatingStmt):
        ctx.env.dml_stmts.append(irstmt)
        irstmt.rewrites = ctx.env.dml_rewrites.pop(irstmt.subject, None)

    if (isinstance(t, s_pseudo.PseudoType)
            and t.is_any(ctx.env.schema)):
        # Need to produce something valid. Should get caught as an
        # error later.
        view = None
        path_id = None

    elif t.get_name(ctx.env.schema) == view_name:
        # The view statement did contain a view declaration and
        # generated a view class with the requested name.
        view = t
        path_id = pathctx.get_path_id(view, ctx=parent_ctx)
    elif view_name is not None:
        # The view statement did _not_ contain a view declaration,
        # but we still want the correct path_id.
        view_obj = ctx.env.schema.get(view_name, None)
        if view_obj is not None:
            assert isinstance(view_obj, s_types.Type)
            view = view_obj
        else:
            view = schemactx.derive_view(
                t, derived_name=view_name, preserve_shape=True, ctx=parent_ctx)
        path_id = pathctx.get_path_id(view, ctx=parent_ctx)
    else:
        view = None
        path_id = None

    type_override = view if view is not None else None
    result = setgen.scoped_set(
        irstmt, type_override=type_override, path_id=path_id, ctx=ctx)
    if irstmt.span and not result.span:
        result = setgen.new_set_from_set(
            result, span=irstmt.span, ctx=ctx)

    if view is not None:
        parent_ctx.view_sets[view] = result

    return result


def process_with_block(
    edgeql_tree: qlast.Statement,
    *,
    ctx: context.ContextLevel,
    parent_ctx: context.ContextLevel,
) -> list[tuple[irast.Set, qltypes.Volatility]]:
    if edgeql_tree.aliases is None:
        return []

    had_materialized = False
    results = []
    for with_entry in edgeql_tree.aliases:
        if isinstance(with_entry, qlast.ModuleAliasDecl):
            ctx.modaliases[with_entry.alias] = with_entry.module

        elif isinstance(with_entry, qlast.AliasedExpr):
            with ctx.new() as scopectx:
                if scopectx.expr_exposed:
                    scopectx.expr_exposed = context.Exposure.BINDING
                binding = stmtctx.declare_view(
                    with_entry.expr,
                    s_name.UnqualName(with_entry.alias),
                    binding_kind=irast.BindingKind.With,
                    ctx=scopectx,
                )
                volatility = inference.infer_volatility(
                    binding, ctx.env, exclude_dml=True
                )
                results.append((binding, volatility))

                if reason := setgen.should_materialize(binding, ctx=ctx):
                    had_materialized = True
                    typ = setgen.get_set_type(binding, ctx=ctx)
                    ctx.env.materialized_sets[typ] = edgeql_tree, reason
                    setgen.maybe_materialize(typ, binding, ctx=ctx)

        else:
            raise RuntimeError(
                f'unexpected expression in WITH block: {with_entry}')

    if had_materialized:
        # If we had to materialize, put the body of the statement into
        # its own fence, to avoid potential spurious factoring when we
        # compile view sets for materialized sets.
        # (We could just *always* do this, but don't, to avoid cluttering
        # up the scope tree more.)
        ctx.path_scope = ctx.path_scope.attach_fence()

    return results


def compile_result_clause(
    result: qlast.Expr,
    *,
    view_scls: Optional[s_types.Type] = None,
    view_rptr: Optional[context.ViewRPtr] = None,
    view_name: Optional[s_name.QualName] = None,
    exprtype: s_types.ExprType = s_types.ExprType.Select,
    result_alias: Optional[str] = None,
    forward_rptr: bool = False,
    ctx: context.ContextLevel,
) -> irast.Set:
    with ctx.new() as sctx:
        if forward_rptr:
            sctx.view_rptr = view_rptr
            # sctx.view_scls = view_scls

        if result_alias:
            # `SELECT foo := expr` is equivalent to
            # `WITH foo := expr SELECT foo`
            rexpr = astutils.ensure_ql_select(result)

            stmtctx.declare_view(
                rexpr,
                alias=s_name.UnqualName(result_alias),
                binding_kind=irast.BindingKind.Select,
                ctx=sctx,
            )

            result = qlast.Path(
                steps=[qlast.ObjectRef(name=result_alias)],
                allow_factoring=True,
            )

        result_expr: qlast.Expr
        shape: Optional[Sequence[qlast.ShapeElement]]

        if isinstance(result, qlast.Shape):
            result_expr = result.expr or qlutils.FREE_SHAPE_EXPR
            shape = result.elements
        else:
            result_expr = result
            shape = None

        if astutils.is_ql_empty_set(result_expr):
            expr = setgen.new_empty_set(
                stype=sctx.empty_result_type_hint,
                alias=ctx.aliases.get('e'),
                ctx=sctx,
                span=result_expr.span,
            )
        elif astutils.is_ql_empty_array(result_expr):
            type_hint: Optional[s_types.Type] = None
            if (
                sctx.empty_result_type_hint is not None
                and sctx.empty_result_type_hint.is_array()
            ):
                type_hint = sctx.empty_result_type_hint

            expr = setgen.new_array_set(
                [],
                stype=type_hint,
                ctx=sctx,
                span=result_expr.span,
            )
        else:
            with sctx.new() as ectx:
                if shape is not None:
                    ectx.expr_exposed = context.Exposure.UNEXPOSED
                expr = dispatch.compile(result_expr, ctx=ectx)

        ctx.partial_path_prefix = expr

        ir_result = compile_query_subject(
            expr, shape=shape, view_rptr=view_rptr, view_name=view_name,
            forward_rptr=forward_rptr,
            result_alias=result_alias,
            view_scls=view_scls,
            allow_select_shape_inject=False,
            exprtype=exprtype,
            compile_views=ctx.stmt is ctx.toplevel_stmt,
            ctx=sctx,
            span=result.span
        )

        ctx.partial_path_prefix = ir_result

    return ir_result


def compile_query_subject(
        set: irast.Set,
        *,
        shape: Optional[List[qlast.ShapeElement]]=None,
        view_rptr: Optional[context.ViewRPtr]=None,
        view_name: Optional[s_name.QualName]=None,
        result_alias: Optional[str]=None,
        view_scls: Optional[s_types.Type]=None,
        compile_views: bool=True,
        exprtype: s_types.ExprType = s_types.ExprType.Select,
        allow_select_shape_inject: bool=True,
        forward_rptr: bool=False,
        span: Optional[qlast.Span],
        ctx: context.ContextLevel) -> irast.Set:

    set_stype = setgen.get_set_type(set, ctx=ctx)

    set_expr = set.expr
    while isinstance(set_expr, irast.TypeIntersectionPointer):
        set_expr = set_expr.source.expr

    is_ptr_alias = (
        view_rptr is not None
        and view_rptr.ptrcls is None
        and view_rptr.ptrcls_name is not None
        and isinstance(set_expr, irast.Pointer)
        and not isinstance(set_expr.source.expr, irast.Pointer)
        and (
            view_rptr.source.get_bases(ctx.env.schema).first(ctx.env.schema).id
            == set_expr.source.typeref.id
        )
        and (
            view_rptr.ptrcls_is_linkprop
            == (set_expr.ptrref.source_ptr is not None)
        )
    )

    if is_ptr_alias:
        assert view_rptr is not None
        set_rptr = cast(irast.Pointer, set_expr)
        # We are inside an expression that defines a link alias in
        # the parent shape, ie. Spam { alias := Spam.bar }, so
        # `Spam.alias` should be a subclass of `Spam.bar` inheriting
        # its properties.
        #
        # We also try to detect reverse aliases like `.<bar[IS Spam]`
        # and arange to inherit the linkprops from those if it resolves
        # to a unique type.
        ptrref = set_rptr.ptrref
        if (
            isinstance(set.expr, irast.Pointer)
            and isinstance(set.expr.ptrref, irast.TypeIntersectionPointerRef)
            and len(set.expr.ptrref.rptr_specialization) == 1
        ):
            ptrref = list(set.expr.ptrref.rptr_specialization)[0]

        if (
            set_rptr.direction is not s_pointers.PointerDirection.Outbound
            and ptrref.out_source.is_opaque_union
        ):
            base_ptrcls = None
        else:
            base_ptrcls = typegen.ptrcls_from_ptrref(ptrref, ctx=ctx)

        if isinstance(base_ptrcls, s_pointers.Pointer):
            view_rptr.base_ptrcls = base_ptrcls
            view_rptr.ptrcls_is_alias = True
            view_rptr.rptr_dir = set_rptr.direction

    if (
        (
            (
                ctx.expr_exposed >= context.Exposure.BINDING
                and allow_select_shape_inject

                and not forward_rptr
                and viewgen.has_implicit_type_computables(
                    set_stype,
                    is_mutation=exprtype.is_mutation(),
                    ctx=ctx,
                )
                and not set_stype.is_view(ctx.env.schema)
            )
            or exprtype.is_mutation()
            or (
                exprtype == s_types.ExprType.Group
                and not set_stype.is_view(ctx.env.schema)
            )
        )
        and set_stype.is_object_type()
        and shape is None
    ):
        # Force the subject to be compiled as a view in these cases:
        # a) a __tid__ insertion is anticipated (the actual
        #    decision about this is taken by the
        #    compile_view_shapes() flow);
        #    we also skip doing this when forward_rptr is true, because
        #    generating an extra type in those cases can cause issues,
        #    and we can just do the insertion on whatever the inner thing is
        #
        #    Note that we do this when exposed or when potentially exposed
        #    because we are in a binding. This is because types that
        #    appear in bindings might get put into the output
        #    and need a __tid__ injection without having a chance to have
        #    a shape put on them.
        # b) this is a mutation without an explicit shape,
        #    such as a DELETE, because mutation subjects are
        #    always expected to be derived types.
        shape = []

    if shape is not None and view_scls is None:
        if (view_name is None and
                isinstance(result_alias, s_name.QualName)):
            view_name = result_alias

        if not isinstance(set_stype, s_objtypes.ObjectType):
            raise errors.QueryError(
                f'shapes cannot be applied to '
                f'{set_stype.get_verbosename(ctx.env.schema)}',
                span=span,
            )

        view_scls, set = viewgen.process_view(
            set,
            stype=set_stype,
            elements=shape,
            view_rptr=view_rptr,
            view_name=view_name,
            exprtype=exprtype,
            ctx=ctx,
            span=span,
        )

    if view_scls is not None:
        set = setgen.ensure_set(set, type_override=view_scls, ctx=ctx)
        set_stype = view_scls

    if compile_views:
        viewgen.late_compile_view_shapes(set, ctx=ctx)

    if (shape is not None or view_scls is not None) and len(set.path_id) == 1:
        ctx.class_view_overrides[set.path_id.target.id] = set_stype

    return set


def maybe_add_view(ir: irast.Set, *, ctx: context.ContextLevel) -> irast.Set:
    """Possibly wrap ir in a new view, if needed for tid/tname injection

    This should be called by every ast leaf compilation that can originate
    an object type.
    """

    # We call compile_query_subject in order to create a new view for
    # injecting properties if needed. This will only happen if
    # expr_exposed, so stmt code paths that don't want a new view
    # created (because there is a shape already specified or because
    # it wants to create its own new view in its compile_query_subject call)
    # should make sure expr_exposed is false.
    #
    # The checks here are microoptimizations.
    if (
        ctx.expr_exposed >= context.Exposure.BINDING
        and ir.path_id.is_objtype_path()
    ):
        return compile_query_subject(
            ir, allow_select_shape_inject=True, compile_views=False, ctx=ctx,
            span=ir.span)
    else:
        return ir


def _is_forbidden_stdlib_type_for_mod(
    t: s_types.Type, ctx: context.ContextLevel
) -> bool:
    o = ctx.env.options
    if o.bootstrap_mode or o.schema_reflection_mode:
        return False

    schema = ctx.env.schema

    assert isinstance(t, s_objtypes.ObjectType)
    assert not t.is_view(schema)

    if intersection := t.get_intersection_of(schema):
        return all((_is_forbidden_stdlib_type_for_mod(it, ctx)
                    for it in intersection.objects(schema)))
    elif union := t.get_union_of(schema):
        return any((_is_forbidden_stdlib_type_for_mod(ut, ctx)
                    for ut in union.objects(schema)))

    name = t.get_name(schema)
    mod_name = name.get_module_name()

    if (
        mod_name == s_name.UnqualName('cfg')
        and o.in_server_config_op
    ):
        # Config ops include various internally generated statements for cfg::
        return False
    if name == s_name.QualName('std', 'Object'):
        # Allow people to mess with the baseclass of user-defined objects to
        # their hearts' content
        return False
    if mod_name == s_name.UnqualName('std::net::http'):
        # Allow users to insert net module types
        return False
    return mod_name in s_schema.STD_MODULES
