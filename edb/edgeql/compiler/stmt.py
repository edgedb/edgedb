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
from typing import *

from collections import defaultdict
import textwrap

from edb import errors
from edb.common import context as pctx

from edb.ir import ast as irast
from edb.ir import typeutils

from edb.schema import ddl as s_ddl
from edb.schema import functions as s_func
from edb.schema import links as s_links
from edb.schema import properties as s_props
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import pseudo as s_pseudo
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from edb.edgeql import ast as qlast
from edb.edgeql import utils as qlutils
from edb.edgeql import qltypes

from . import astutils
from . import clauses
from . import context
from . import dispatch
from . import inference
from . import pathctx
from . import setgen
from . import viewgen
from . import schemactx
from . import stmtctx
from . import typegen
from . import conflicts


@dispatch.compile.register(qlast.SelectQuery)
def compile_SelectQuery(
        expr: qlast.SelectQuery, *, ctx: context.ContextLevel) -> irast.Set:
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
        forward_rptr = bool(expr.offset or expr.limit)

        if (
            (ctx.expr_exposed or sctx.stmt is ctx.toplevel_stmt)
            and ctx.implicit_limit
            and expr.limit is None
            and not ctx.inhibit_implicit_limit
        ):
            expr.limit = qlast.IntegerConstant(value=str(ctx.implicit_limit))

        stmt.result = compile_result_clause(
            expr.result,
            view_scls=ctx.view_scls,
            view_rptr=ctx.view_rptr,
            result_alias=expr.result_alias,
            view_name=ctx.toplevel_result_view_name,
            forward_rptr=forward_rptr,
            ctx=sctx)

        clauses.compile_where_clause(
            stmt, expr.where, ctx=sctx)

        stmt.orderby = clauses.compile_orderby_clause(
            expr.orderby, ctx=sctx)

        stmt.offset = clauses.compile_limit_offset_clause(
            expr.offset, ctx=sctx)

        stmt.limit = clauses.compile_limit_offset_clause(
            expr.limit, ctx=sctx)

        result = fini_stmt(stmt, expr, ctx=sctx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.ForQuery)
def compile_ForQuery(
        qlstmt: qlast.ForQuery, *, ctx: context.ContextLevel) -> irast.Set:
    with ctx.subquery() as sctx:
        stmt = irast.SelectStmt(context=qlstmt.context)
        init_stmt(stmt, qlstmt, ctx=sctx, parent_ctx=ctx)

        # As an optimization, if the iterator is a singleton set, use
        # the element directly.
        iterator = qlstmt.iterator
        if isinstance(iterator, qlast.Set) and len(iterator.elements) == 1:
            iterator = iterator.elements[0]

        contains_dml = qlutils.contains_dml(qlstmt.result)
        # If the body contains DML, then we need to prohibit
        # correlation between the iterator and the enclosing
        # query, since the correlation imposes compilation issues
        # we aren't willing to tackle.
        if contains_dml:
            sctx.path_scope.factoring_allowlist.update(ctx.iterator_path_ids)
        iterator_view = stmtctx.declare_view(
            iterator,
            s_name.UnqualName(qlstmt.iterator_alias),
            factoring_fence=contains_dml,
            path_id_namespace=sctx.path_id_namespace,
            is_for_view=True,
            ctx=sctx,
        )

        iterator_stmt = setgen.new_set_from_set(
            iterator_view, preserve_scope_ns=True, ctx=sctx)
        stmt.iterator_stmt = iterator_stmt

        iterator_type = setgen.get_set_type(iterator_stmt, ctx=ctx)
        anytype = iterator_type.find_any(ctx.env.schema)
        if anytype is not None:
            raise errors.QueryError(
                'FOR statement has iterator of indeterminate type',
                context=ctx.env.type_origins.get(anytype),
            )

        view_scope_info = sctx.path_scope_map[iterator_view]

        pathctx.register_set_in_scope(
            iterator_stmt,
            path_scope=sctx.path_scope,
            ctx=sctx,
        )

        # Iterator symbol is, by construction, outside of the scope
        # of the UNION argument, but is perfectly legal to be referenced
        # inside a factoring fence that is an immediate child of this
        # scope.
        sctx.path_scope.factoring_allowlist.add(stmt.iterator_stmt.path_id)
        sctx.iterator_path_ids |= {stmt.iterator_stmt.path_id}
        node = sctx.path_scope.find_descendant(iterator_stmt.path_id)
        if node is not None:
            # See above about why we need a factoring fence.
            # We need to do this again when we move the branch so
            # as to preserve the fencing.
            # Do this by sticking the iterator subtree onto a branch
            # with a factoring fence.
            if contains_dml:
                node = node.attach_branch()
                node.factoring_fence = True
                node = node.attach_branch()

            node.attach_subtree(view_scope_info.path_scope,
                                context=iterator.context)

        # Compile the body
        with sctx.newscope(fenced=True) as bctx:
            stmt.result = setgen.scoped_set(
                compile_result_clause(
                    # Make sure it is a stmt, so that shapes inside the body
                    # get resolved there.
                    astutils.ensure_qlstmt(qlstmt.result),
                    view_scls=ctx.view_scls,
                    view_rptr=ctx.view_rptr,
                    result_alias=qlstmt.result_alias,
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
                qlast.IntegerConstant(value=str(ctx.implicit_limit)),
                ctx=sctx,
            )

        result = fini_stmt(stmt, qlstmt, ctx=sctx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.GroupQuery)
def compile_GroupQuery(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Set:

    raise errors.UnsupportedFeatureError(
        "'GROUP' statement is not currently implemented",
        context=expr.context)


@dispatch.compile.register(qlast.InsertQuery)
def compile_InsertQuery(
        expr: qlast.InsertQuery, *,
        ctx: context.ContextLevel) -> irast.Set:

    if ctx.in_conditional is not None:
        raise errors.QueryError(
            'INSERT statements cannot be used inside conditional '
            'expressions',
            context=expr.context,
        )

    # Record this node in the list of potential DML expressions.
    ctx.env.dml_exprs.append(expr)

    with ctx.subquery() as ictx:
        stmt = irast.InsertStmt(context=expr.context)
        init_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

        subject = dispatch.compile(expr.subject, ctx=ictx)
        assert isinstance(subject, irast.Set)

        subject_stype = setgen.get_set_type(subject, ctx=ictx)
        if subject_stype.get_abstract(ctx.env.schema):
            raise errors.QueryError(
                f'cannot insert into abstract '
                f'{subject_stype.get_verbosename(ctx.env.schema)}',
                context=expr.subject.context)

        if subject_stype.is_view(ctx.env.schema):
            raise errors.QueryError(
                f'cannot insert into expression alias '
                f'{str(subject_stype.get_shortname(ctx.env.schema))!r}',
                context=expr.subject.context)

        with ictx.new() as bodyctx:
            # Self-references in INSERT are prohibited.
            bodyctx.banned_paths = ictx.banned_paths.copy()
            pathctx.ban_path(subject.path_id, ctx=bodyctx)

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
                result_alias=expr.subject_alias,
                is_insert=True,
                ctx=bodyctx)

        stmt_subject_stype = setgen.get_set_type(subject, ctx=ictx)
        assert isinstance(stmt_subject_stype, s_objtypes.ObjectType)

        stmt.conflict_checks = conflicts.compile_inheritance_conflict_checks(
            stmt, stmt_subject_stype, ctx=ictx)

        if not ctx.path_scope.in_temp_scope():
            ctx.env.dml_stmts.add(stmt)

        if expr.unless_conflict is not None:
            constraint_spec, else_branch = expr.unless_conflict

            if constraint_spec:
                stmt.on_conflict = conflicts.compile_insert_unless_conflict_on(
                    stmt, stmt_subject_stype, constraint_spec, else_branch,
                    ctx=ictx)
            else:
                stmt.on_conflict = conflicts.compile_insert_unless_conflict(
                    stmt, stmt_subject_stype, ctx=ictx)

        result = setgen.class_set(
            schemactx.get_material_type(stmt_subject_stype, ctx=ctx),
            path_id=stmt.subject.path_id,
            ctx=ctx,
        )

        with ictx.new() as resultctx:
            if ictx.stmt is ctx.toplevel_stmt:
                resultctx.expr_exposed = True

            stmt.result = compile_query_subject(
                result,
                view_scls=ctx.view_scls,
                view_name=ctx.toplevel_result_view_name,
                compile_views=ictx.stmt is ictx.toplevel_stmt,
                ctx=resultctx,
            )

        result = fini_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

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
                resultctx.expr_exposed = True
                result = compile_query_subject(
                    result,
                    view_name=ctx.toplevel_result_view_name,
                    compile_views=ictx.stmt is ctx.toplevel_stmt,
                    ctx=resultctx,
                    parser_context=result.context)

    return result


def _get_dunder_type_ptrref(ctx: context.ContextLevel) -> irast.PointerRef:
    return typeutils.lookup_obj_ptrref(
        ctx.env.schema,
        s_name.QualName('std', 'BaseObject'),
        s_name.UnqualName('__type__'),
        cache=ctx.env.ptr_ref_cache,
        typeref_cache=ctx.env.type_ref_cache,
    )


@dispatch.compile.register(qlast.UpdateQuery)
def compile_UpdateQuery(
        expr: qlast.UpdateQuery, *, ctx: context.ContextLevel) -> irast.Set:

    if ctx.in_conditional is not None:
        raise errors.QueryError(
            'UPDATE statements cannot be used inside conditional expressions',
            context=expr.context,
        )

    # Record this node in the list of potential DML expressions.
    ctx.env.dml_exprs.append(expr)

    with ctx.subquery() as ictx:
        stmt = irast.UpdateStmt(
            context=expr.context,
            dunder_type_ptrref=_get_dunder_type_ptrref(ctx),
        )
        init_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

        subject = dispatch.compile(expr.subject, ctx=ictx)
        assert isinstance(subject, irast.Set)

        subj_type = inference.infer_type(subject, ictx.env)
        if not isinstance(subj_type, s_objtypes.ObjectType):
            raise errors.QueryError(
                f'cannot update non-ObjectType objects',
                context=expr.subject.context
            )

        ictx.partial_path_prefix = subject

        clauses.compile_where_clause(
            stmt, expr.where, ctx=ictx)

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
                result_alias=expr.subject_alias,
                is_update=True,
                ctx=bodyctx)

        stmt_subject_stype = setgen.get_set_type(subject, ctx=ictx)
        assert isinstance(stmt_subject_stype, s_objtypes.ObjectType)

        if not ctx.path_scope.in_temp_scope():
            ctx.env.dml_stmts.add(stmt)

        result = setgen.class_set(
            schemactx.get_material_type(stmt_subject_stype, ctx=ctx),
            path_id=stmt.subject.path_id,
            ctx=ctx,
        )

        with ictx.new() as resultctx:
            if ictx.stmt is ctx.toplevel_stmt:
                resultctx.expr_exposed = True

            stmt.result = compile_query_subject(
                result,
                view_scls=ctx.view_scls,
                view_name=ctx.toplevel_result_view_name,
                compile_views=ictx.stmt is ictx.toplevel_stmt,
                ctx=resultctx,
            )

        stmt.conflict_checks = conflicts.compile_inheritance_conflict_checks(
            stmt, stmt_subject_stype, ctx=ictx)

        result = fini_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.DeleteQuery)
def compile_DeleteQuery(
        expr: qlast.DeleteQuery, *, ctx: context.ContextLevel) -> irast.Set:

    if ctx.in_conditional is not None:
        raise errors.QueryError(
            'DELETE statements cannot be used inside conditional expressions',
            context=expr.context,
        )

    # Record this node in the list of potential DML expressions.
    ctx.env.dml_exprs.append(expr)

    with ctx.subquery() as ictx:
        stmt = irast.DeleteStmt(context=expr.context)
        # Expand the DELETE from sugar into full DELETE (SELECT ...)
        # form, if there's any additional clauses.
        if any([expr.where, expr.orderby, expr.offset, expr.limit]):
            if expr.offset or expr.limit:
                subjql = qlast.SelectQuery(
                    result=qlast.SelectQuery(
                        result=expr.subject,
                        result_alias=expr.subject_alias,
                        where=expr.where,
                        orderby=expr.orderby,
                        context=expr.context,
                        implicit=True,
                    ),
                    limit=expr.limit,
                    offset=expr.offset,
                    context=expr.context,
                )
            else:
                subjql = qlast.SelectQuery(
                    result=expr.subject,
                    result_alias=expr.subject_alias,
                    where=expr.where,
                    orderby=expr.orderby,
                    offset=expr.offset,
                    limit=expr.limit,
                    context=expr.context,
                )

            expr = qlast.DeleteQuery(
                aliases=expr.aliases,
                context=expr.context,
                subject=subjql,
            )

        init_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

        # DELETE Expr is a delete(SET OF X), so we need a scope fence.
        with ictx.newscope(fenced=True) as scopectx:
            scopectx.implicit_limit = 0
            subject = setgen.scoped_set(
                dispatch.compile(expr.subject, ctx=scopectx), ctx=scopectx)

        subj_type = inference.infer_type(subject, ictx.env)
        if not isinstance(subj_type, s_objtypes.ObjectType):
            raise errors.QueryError(
                f'cannot delete non-ObjectType objects',
                context=expr.subject.context
            )

        with ictx.new() as bodyctx:
            bodyctx.implicit_id_in_shapes = False
            bodyctx.implicit_tid_in_shapes = False
            bodyctx.implicit_tname_in_shapes = False
            stmt.subject = compile_query_subject(
                subject,
                shape=None,
                is_delete=True,
                ctx=bodyctx,
            )

        stmt_subject_stype = setgen.get_set_type(subject, ctx=ictx)
        result = setgen.class_set(
            schemactx.get_material_type(stmt_subject_stype, ctx=ctx),
            path_id=stmt.subject.path_id,
            ctx=ctx,
        )

        with ictx.new() as resultctx:
            if ictx.stmt is ctx.toplevel_stmt:
                resultctx.expr_exposed = True

            stmt.result = compile_query_subject(
                result,
                view_scls=ctx.view_scls,
                view_name=ctx.toplevel_result_view_name,
                compile_views=ictx.stmt is ictx.toplevel_stmt,
                ctx=resultctx,
            )

        result = fini_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

    return result


@dispatch.compile.register
def compile_DescribeStmt(
        ql: qlast.DescribeStmt, *, ctx: context.ContextLevel) -> irast.Set:
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
                ctx.env.get_track_schema_type(
                    s_name.QualName('std', 'str')),
                env=ctx.env,
            )

            stmt.result = setgen.ensure_set(
                irast.StringConstant(value=text, typeref=ct),
                ctx=ictx,
            )

        elif ql.object is qlast.DescribeGlobal.DatabaseConfig:
            if ql.language is qltypes.DescribeLanguage.DDL:
                function_call = dispatch.compile(
                    qlast.FunctionCall(
                        func=('cfg', '_describe_database_config_as_ddl'),
                    ),
                    ctx=ictx)
                assert isinstance(function_call, irast.Set), function_call
                stmt.result = function_call
            else:
                raise errors.QueryError(
                    f'cannot describe config as {ql.language}')

        elif ql.object is qlast.DescribeGlobal.InstanceConfig:
            if ql.language is qltypes.DescribeLanguage.DDL:
                function_call = dispatch.compile(
                    qlast.FunctionCall(
                        func=('cfg', '_describe_system_config_as_ddl'),
                    ),
                    ctx=ictx)
                assert isinstance(function_call, irast.Set), function_call
                stmt.result = function_call
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
                assert isinstance(function_call, irast.Set), function_call
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
                modules.append(s_utils.ast_ref_to_unqualname(objref))
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
                ctx.env.get_track_schema_type(
                    s_name.QualName('std', 'str')),
                env=ctx.env,
            )

            stmt.result = setgen.ensure_set(
                irast.StringConstant(value=text, typeref=ct),
                ctx=ictx,
            )

        result = fini_stmt(stmt, ql, ctx=ictx, parent_ctx=ctx)

    return result


@dispatch.compile.register(qlast.Shape)
def compile_Shape(
        shape: qlast.Shape, *, ctx: context.ContextLevel) -> irast.Set:
    shape_expr = shape.expr or qlutils.FREE_SHAPE_EXPR
    with ctx.new() as subctx:
        subctx.qlstmt = astutils.ensure_qlstmt(shape)
        subctx.stmt = stmt = irast.SelectStmt()
        ctx.env.compiled_stmts[subctx.qlstmt] = stmt
        subctx.class_view_overrides = subctx.class_view_overrides.copy()

        with ctx.new() as exposed_ctx:
            exposed_ctx.expr_exposed = False
            expr = dispatch.compile(shape_expr, ctx=exposed_ctx)

        expr_stype = setgen.get_set_type(expr, ctx=ctx)
        if not isinstance(expr_stype, s_objtypes.ObjectType):
            raise errors.QueryError(
                f'shapes cannot be applied to '
                f'{expr_stype.get_verbosename(ctx.env.schema)}',
                context=shape.context,
            )
        view_type = viewgen.process_view(
            stype=expr_stype, path_id=expr.path_id,
            elements=shape.elements, parser_context=shape.context, ctx=subctx)

        stmt.result = compile_query_subject(
            expr,
            view_scls=view_type,
            compile_views=False,
            ctx=subctx,
            parser_context=expr.context)

        ir_result = setgen.ensure_set(
            stmt, type_override=view_type, ctx=subctx)

    return ir_result


def init_stmt(
        irstmt: irast.Stmt, qlstmt: qlast.Statement, *,
        ctx: context.ContextLevel, parent_ctx: context.ContextLevel) -> None:

    ctx.env.compiled_stmts[qlstmt] = irstmt

    if isinstance(irstmt, irast.MutatingStmt):
        # This is some kind of mutation, so we need to check if it is
        # allowed.
        if ctx.env.options.in_ddl_context_name is not None:
            raise errors.SchemaDefinitionError(
                f'mutations are invalid in '
                f'{ctx.env.options.in_ddl_context_name}',
                context=qlstmt.context,
            )
        elif ((dv := ctx.defining_view) is not None and
                dv.get_expr_type(ctx.env.schema) is s_types.ExprType.Select and
                not ctx.env.options.allow_top_level_shape_dml):
            # This is some shape in a regular query. Although
            # DML is not allowed in the computable, but it may
            # be possible to refactor it.
            raise errors.QueryError(
                f"mutations are invalid in a shape's computed expression",
                hint=(
                    f'To resolve this try to factor out the mutation '
                    f'expression into the top-level WITH block.'
                ),
                context=qlstmt.context,
            )

    ctx.stmt = irstmt
    ctx.qlstmt = qlstmt
    if ctx.toplevel_stmt is None:
        parent_ctx.toplevel_stmt = ctx.toplevel_stmt = irstmt

    ctx.path_scope = parent_ctx.path_scope.attach_fence()

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
        parent_ctx.path_scope.factoring_allowlist.update(ctx.iterator_path_ids)


def fini_stmt(
        irstmt: Union[irast.Stmt, irast.Set],
        qlstmt: qlast.Statement, *,
        ctx: context.ContextLevel,
        parent_ctx: context.ContextLevel) -> irast.Set:

    view_name = parent_ctx.toplevel_result_view_name
    t = inference.infer_type(irstmt, ctx.env)

    view: Optional[s_types.Type]
    path_id: Optional[irast.PathId]

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
    if irstmt.context and not result.context:
        result = setgen.new_set_from_set(
            result, context=irstmt.context, ctx=ctx)

    if isinstance(irstmt, irast.Stmt) and irstmt.bindings:
        # If a binding is in a branched-but-not-fenced subnode, we
        # want to hoist it up to the current scope. This is important
        # for materialization-related cases where WITH+tuple is being
        # used like FOR.
        # See test_edgeql_volatility_hack_0{3,4}b
        # XXX: To be honest I do not 100% remember why this makes sense.
        for binding in irstmt.bindings:
            if node := ctx.path_scope.find_child(
                    binding.path_id, in_branches=True):
                node.remove()
                ctx.path_scope.attach_subtree(node)

    if view is not None:
        parent_ctx.view_sets[view] = result

    return result


def process_with_block(
        edgeql_tree: qlast.Statement, *,
        ctx: context.ContextLevel,
        parent_ctx: context.ContextLevel) -> List[irast.Set]:
    if edgeql_tree.aliases is None:
        return []

    had_materialized = False
    results = []
    for with_entry in edgeql_tree.aliases:
        if isinstance(with_entry, qlast.ModuleAliasDecl):
            ctx.modaliases[with_entry.alias] = with_entry.module

        elif isinstance(with_entry, qlast.AliasedExpr):
            with ctx.new() as scopectx:
                scopectx.expr_exposed = False
                binding = stmtctx.declare_view(
                    with_entry.expr,
                    s_name.UnqualName(with_entry.alias),
                    must_be_used=True,
                    ctx=scopectx,
                )
                results.append(binding)

                if reason := setgen.should_materialize(binding, ctx=ctx):
                    had_materialized = True
                    typ = setgen.get_set_type(binding, ctx=ctx)
                    ctx.env.materialized_sets[typ] = edgeql_tree, reason
                    assert binding.expr
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
        result: qlast.Expr, *,
        view_scls: Optional[s_types.Type]=None,
        view_rptr: Optional[context.ViewRPtr]=None,
        view_name: Optional[s_name.QualName]=None,
        result_alias: Optional[str]=None,
        forward_rptr: bool=False,
        ctx: context.ContextLevel) -> irast.Set:
    with ctx.new() as sctx:
        if sctx.stmt is ctx.toplevel_stmt:
            sctx.expr_exposed = True

        if forward_rptr:
            sctx.view_rptr = view_rptr
            # sctx.view_scls = view_scls

        if result_alias:
            # `SELECT foo := expr` is equivalent to
            # `WITH foo := expr SELECT foo`
            rexpr = astutils.ensure_ql_select(result)
            if (
                sctx.implicit_limit
                and rexpr.limit is None
                and not sctx.inhibit_implicit_limit
            ):
                # Inline alias is special: it's both "exposed",
                # but also subject for further processing, so
                # make sure we don't mangle it with an implicit
                # limit.
                rexpr.limit = qlast.TypeCast(
                    expr=qlast.Set(elements=[]),
                    type=qlast.TypeName(
                        maintype=qlast.ObjectRef(
                            module='__std__',
                            name='int64',
                        )
                    )
                )

            stmtctx.declare_view(
                rexpr,
                alias=s_name.UnqualName(result_alias),
                ctx=sctx,
            )

            result = qlast.Path(
                steps=[qlast.ObjectRef(name=result_alias)]
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
                srcctx=result_expr.context,
            )
        else:
            with sctx.new() as ectx:
                if shape is not None:
                    ectx.expr_exposed = False
                expr = dispatch.compile(result_expr, ctx=ectx)

        ctx.partial_path_prefix = expr

        ir_result = compile_query_subject(
            expr, shape=shape, view_rptr=view_rptr, view_name=view_name,
            forward_rptr=forward_rptr,
            result_alias=result_alias,
            view_scls=view_scls,
            compile_views=ctx.stmt is ctx.toplevel_stmt,
            ctx=sctx,
            parser_context=result.context)

        ctx.partial_path_prefix = ir_result

    return ir_result


def compile_query_subject(
        expr: irast.Set, *,
        shape: Optional[List[qlast.ShapeElement]]=None,
        view_rptr: Optional[context.ViewRPtr]=None,
        view_name: Optional[s_name.QualName]=None,
        result_alias: Optional[str]=None,
        view_scls: Optional[s_types.Type]=None,
        compile_views: bool=True,
        is_insert: bool=False,
        is_update: bool=False,
        is_delete: bool=False,
        forward_rptr: bool=False,
        parser_context: Optional[pctx.ParserContext]=None,
        ctx: context.ContextLevel) -> irast.Set:

    expr_stype = setgen.get_set_type(expr, ctx=ctx)
    expr_rptr = expr.rptr

    while isinstance(expr_rptr, irast.TypeIntersectionPointer):
        expr_rptr = expr_rptr.source.rptr

    is_ptr_alias = (
        view_rptr is not None
        and view_rptr.ptrcls is None
        and view_rptr.ptrcls_name is not None
        and expr_rptr is not None
        and expr_rptr.direction is s_pointers.PointerDirection.Outbound
        and expr_rptr.source.rptr is None
        and (
            view_rptr.source.get_bases(ctx.env.schema).first(ctx.env.schema).id
            == expr_rptr.source.typeref.id
        )
        and (
            view_rptr.ptrcls_is_linkprop
            == (expr_rptr.ptrref.source_ptr is not None)
        )
    )

    if is_ptr_alias:
        assert view_rptr is not None
        assert expr_rptr is not None
        # We are inside an expression that defines a link alias in
        # the parent shape, ie. Spam { alias := Spam.bar }, so
        # `Spam.alias` should be a subclass of `Spam.bar` inheriting
        # its properties.
        base_ptrcls = typegen.ptrcls_from_ptrref(expr_rptr.ptrref, ctx=ctx)
        if isinstance(base_ptrcls, s_pointers.Pointer):
            view_rptr.base_ptrcls = base_ptrcls
            view_rptr.ptrcls_is_alias = True

    is_mutation = is_insert or is_update or is_delete

    if (
        (
            (
                ctx.expr_exposed
                and expr_stype.is_object_type()
                and not forward_rptr
                and (
                    viewgen.has_implicit_type_computables(
                        expr_stype,
                        is_mutation=is_mutation,
                        ctx=ctx,
                    )
                    or expr_stype in ctx.env.materialized_sets
                )
            )
            or is_mutation
        )
        and shape is None
        and (
            expr_stype not in ctx.env.view_shapes
            or expr_stype in ctx.env.materialized_sets
        )
    ):
        # Force the subject to be compiled as a view in these cases:
        # a) a __tid__ insertion is anticipated (the actual
        #    decision about this is taken by the
        #    compile_view_shapes() flow);
        #    we also skip doing this when forward_rptr is true, because
        #    generating an extra type in those cases can cause issues,
        #    and we can just do the insertion on whatever the inner thing is
        # b) this is a mutation without an explicit shape,
        #    such as a DELETE, because mutation subjects are
        #    always expected to be derived types.
        # c) this is a use of a type we think we are materializing,
        #    which hacks around issues like
        #    test_edgeql_volatility_select_hard_objects_09 where we
        #    can't rely on the serialization done at an inner location.
        #    (This is a hack, because I don't think it can generalize
        #     to tuples/arrays containing objects)
        shape = []

    if shape is not None and view_scls is None:
        if (view_name is None and
                isinstance(result_alias, s_name.QualName)):
            view_name = result_alias

        if not isinstance(expr_stype, s_objtypes.ObjectType):
            raise errors.QueryError(
                f'shapes cannot be applied to '
                f'{expr_stype.get_verbosename(ctx.env.schema)}',
                context=parser_context,
            )

        view_scls = viewgen.process_view(
            stype=expr_stype,
            path_id=expr.path_id,
            elements=shape,
            view_rptr=view_rptr,
            view_name=view_name,
            is_insert=is_insert,
            is_update=is_update,
            is_delete=is_delete,
            parser_context=expr.context,
            ctx=ctx,
        )

    if view_scls is not None:
        expr = setgen.ensure_set(expr, type_override=view_scls, ctx=ctx)
        expr_stype = view_scls

    if compile_views or (
            view_scls and setgen.should_materialize_type(view_scls, ctx=ctx)):
        rptr = view_rptr.rptr if view_rptr is not None else None
        if is_update:
            with ctx.new() as subctx:
                subctx.compiling_update_shape = True
                viewgen.compile_view_shapes(expr, rptr=rptr, ctx=subctx)
        else:
            viewgen.compile_view_shapes(expr, rptr=rptr, ctx=ctx)

    if (shape is not None or view_scls is not None) and len(expr.path_id) == 1:
        ctx.class_view_overrides[expr.path_id.target.id] = expr_stype

    return expr


def compile_groupby_clause(
        groupexprs: Iterable[qlast.Base], *,
        ctx: context.ContextLevel) -> List[irast.Set]:
    result: List[irast.Set] = []
    if not groupexprs:
        return result

    with ctx.new() as sctx:
        ir_groupexprs = []
        for groupexpr in groupexprs:
            with sctx.newscope(fenced=True) as scopectx:
                ir_groupexpr = setgen.scoped_set(
                    dispatch.compile(groupexpr, ctx=scopectx), ctx=scopectx)
                ir_groupexpr.context = groupexpr.context
                ir_groupexprs.append(ir_groupexpr)

    return ir_groupexprs
