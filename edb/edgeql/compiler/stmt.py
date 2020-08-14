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
import textwrap

from collections import defaultdict
from edb import errors

from edb.ir import ast as irast
from edb.ir import staeval as ireval
from edb.ir import typeutils

from edb.schema import ddl as s_ddl
from edb.schema import functions as s_func
from edb.schema import links as s_links
from edb.schema import lproperties as s_lprops
from edb.schema import modules as s_mod
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import pseudo as s_pseudo
from edb.schema import types as s_types

from edb.edgeql import ast as qlast
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

if TYPE_CHECKING:
    from edb.schema import constraints as s_constr
    from edb.schema import schema as s_schema


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

        if expr.limit is not None:
            sctx.inhibit_implicit_limit = True
        elif ((ctx.expr_exposed or sctx.stmt is ctx.toplevel_stmt)
                and ctx.implicit_limit and not sctx.inhibit_implicit_limit):
            expr.limit = qlast.IntegerConstant(value=str(ctx.implicit_limit))
            sctx.inhibit_implicit_limit = True

        stmt.result = compile_result_clause(
            expr.result,
            view_scls=ctx.view_scls,
            view_rptr=ctx.view_rptr,
            result_alias=expr.result_alias,
            view_name=ctx.toplevel_result_view_name,
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
        stmt = irast.SelectStmt()
        init_stmt(stmt, qlstmt, ctx=sctx, parent_ctx=ctx)

        with sctx.newscope(fenced=True) as scopectx:
            iterator_ctx = None
            if (ctx.expr_exposed and ctx.iterator_ctx is not None
                    and ctx.iterator_ctx is not sctx):
                iterator_ctx = ctx.iterator_ctx

            if iterator_ctx is not None:
                iterator_scope_parent = iterator_ctx.path_scope
                path_id_ns = iterator_ctx.path_id_namespace
            else:
                iterator_scope_parent = sctx.path_scope
                path_id_ns = sctx.path_id_namespace

            iterator = qlstmt.iterator
            if isinstance(iterator, qlast.Set) and len(iterator.elements) == 1:
                iterator = iterator.elements[0]

            iterator_view = stmtctx.declare_view(
                iterator, qlstmt.iterator_alias,
                path_id_namespace=path_id_ns, ctx=scopectx)

            iterator_stmt = setgen.new_set_from_set(
                iterator_view, preserve_scope_ns=True, ctx=scopectx)

            iterator_type = setgen.get_set_type(iterator_stmt, ctx=ctx)
            anytype = iterator_type.find_any(ctx.env.schema)
            if anytype is not None:
                raise errors.QueryError(
                    'FOR statement has iterator of indeterminate type',
                    context=ctx.env.type_origins.get(anytype),
                )

            if iterator_ctx is not None and iterator_ctx.stmt is not None:
                iterator_ctx.stmt.hoisted_iterators.append(iterator_stmt)

            stmt.iterator_stmt = iterator_stmt

            view_scope_info = scopectx.path_scope_map[iterator_view]
            iterator_scope = view_scope_info.path_scope

            for cb in view_scope_info.tentative_work:
                stmtctx.at_stmt_fini(cb, ctx=ctx)

            view_scope_info.tentative_work[:] = []

        pathctx.register_set_in_scope(
            iterator_stmt,
            path_scope=iterator_scope_parent,
            ctx=sctx,
        )
        # Iterator symbol is, by construction, outside of the scope
        # of the UNION argument, but is perfectly legal to be referenced
        # inside a factoring fence that is an immediate child of this
        # scope.
        iterator_scope_parent.factoring_allowlist.add(
            stmt.iterator_stmt.path_id)
        node = iterator_scope_parent.find_descendant(iterator_stmt.path_id)
        if node is not None:
            node.attach_subtree(iterator_scope)

        stmt.result = compile_result_clause(
            qlstmt.result,
            view_scls=ctx.view_scls,
            view_rptr=ctx.view_rptr,
            result_alias=qlstmt.result_alias,
            view_name=ctx.toplevel_result_view_name,
            forward_rptr=True,
            ctx=sctx)

        if ((ctx.expr_exposed or sctx.stmt is ctx.toplevel_stmt)
                and ctx.implicit_limit):
            stmt.limit = setgen.ensure_set(
                dispatch.compile(
                    qlast.IntegerConstant(value=str(ctx.implicit_limit)),
                    ctx=sctx,
                ),
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


def simple_stmt_eq(lhs: irast.Base, rhs: irast.Base,
                   schema: s_schema.Schema) -> bool:
    if (
        isinstance(lhs, irast.BaseConstant)
        and isinstance(rhs, irast.BaseConstant)
        and (ireval.evaluate_to_python_val(lhs, schema) ==
             ireval.evaluate_to_python_val(rhs, schema))
    ):
        return True
    elif (
        isinstance(lhs, irast.Parameter)
        and isinstance(rhs, irast.Parameter)
        and lhs.name == rhs.name
    ):
        return True
    else:
        return False


def compile_insert_unless_conflict(
    stmt: irast.InsertStmt,
    constraint_spec: qlast.Expr,
    else_branch: Optional[qlast.Expr],
    *, ctx: context.ContextLevel,
) -> irast.ConstraintRef:
    ctx.partial_path_prefix = stmt.subject

    if else_branch:
        raise errors.UnsupportedFeatureError(
            'UNLESS CONFLICT ... ELSE currently unimplemented',
            context=else_branch.context,
        )

    # We compile the name here so we can analyze it, but we don't do
    # anything else with it.
    cspec_res = setgen.ensure_set(dispatch.compile(
        constraint_spec, ctx=ctx), ctx=ctx)

    stmtctx.enforce_singleton(cspec_res, ctx=ctx)

    if not cspec_res.rptr:
        raise errors.QueryError(
            'ON CONFLICT argument must be a property',
            context=constraint_spec.context,
        )

    if cspec_res.rptr.source.path_id != stmt.subject.path_id:
        raise errors.QueryError(
            'ON CONFLICT argument must be a property of the '
            'type being inserted',
            context=constraint_spec.context,
        )

    schema = ctx.env.schema
    schema, ptr = (
        typeutils.ptrcls_from_ptrref(cspec_res.rptr.ptrref,
                                     schema=schema))
    if not isinstance(ptr, s_pointers.Pointer):
        raise errors.QueryError(
            'ON CONFLICT property must be a property',
            context=constraint_spec.context,
        )

    ptr = ptr.get_nearest_non_derived_parent(schema)

    exclusive_constr: s_constr.Constraint = schema.get('std::exclusive')
    ex_cnstrs = [c for c in ptr.get_constraints(schema).objects(schema)
                 if c.issubclass(schema, exclusive_constr)]

    if len(ex_cnstrs) != 1:
        raise errors.QueryError(
            'ON CONFLICT property must have a single exclusive constraint',
            context=constraint_spec.context,
        )

    module_id = schema.get_global(
        s_mod.Module, ptr.get_name(schema).module).id

    ctx.env.schema = schema

    return irast.ConstraintRef(
        id=ex_cnstrs[0].id, module_id=module_id)


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

    with ctx.subquery() as ictx:
        stmt = irast.InsertStmt()
        init_stmt(stmt, expr, ctx=ictx, parent_ctx=ctx)

        subject = dispatch.compile(expr.subject, ctx=ictx)
        assert isinstance(subject, irast.Set)

        subject_stype = setgen.get_set_type(subject, ctx=ictx)
        if subject_stype.get_is_abstract(ctx.env.schema):
            raise errors.QueryError(
                f'cannot insert into abstract '
                f'{subject_stype.get_verbosename(ctx.env.schema)}',
                context=expr.subject.context)

        if subject_stype.is_view(ctx.env.schema):
            raise errors.QueryError(
                f'cannot insert into expression alias '
                f'{subject_stype.get_shortname(ctx.env.schema)!r}',
                context=expr.subject.context)

        with ictx.new() as bodyctx:
            # Self-references in INSERT are prohibited.
            bodyctx.banned_paths = ictx.banned_paths.copy()
            pathctx.ban_path(subject.path_id, ctx=bodyctx)

            bodyctx.implicit_id_in_shapes = False
            bodyctx.implicit_tid_in_shapes = False
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

        result = setgen.class_set(
            schemactx.get_material_type(stmt_subject_stype, ctx=ctx),
            path_id=stmt.subject.path_id,
            ctx=ctx,
        )

        if expr.unless_conflict is not None:
            constraint_spec, else_branch = expr.unless_conflict

            if constraint_spec:
                with ictx.new() as constraint_ctx:
                    stmt.on_conflict = compile_insert_unless_conflict(
                        stmt, constraint_spec, else_branch, ctx=constraint_ctx)
            else:
                stmt.on_conflict = True

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


@dispatch.compile.register(qlast.UpdateQuery)
def compile_UpdateQuery(
        expr: qlast.UpdateQuery, *, ctx: context.ContextLevel) -> irast.Set:

    if ctx.in_conditional is not None:
        raise errors.QueryError(
            'UPDATE statements cannot be used inside conditional expressions',
            context=expr.context,
        )

    with ctx.subquery() as ictx:
        stmt = irast.UpdateStmt()
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
            bodyctx.implicit_id_in_shapes = False
            bodyctx.implicit_tid_in_shapes = False
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


@dispatch.compile.register(qlast.DeleteQuery)
def compile_DeleteQuery(
        expr: qlast.DeleteQuery, *, ctx: context.ContextLevel) -> irast.Set:

    if ctx.in_conditional is not None:
        raise errors.QueryError(
            'DELETE statements cannot be used inside conditional expressions',
            context=expr.context,
        )

    with ctx.subquery() as ictx:
        stmt = irast.DeleteStmt()
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

        if ql.object == qlast.DescribeGlobal.Schema:
            if ql.language is qltypes.DescribeLanguage.DDL:
                # DESCRIBE SCHEMA
                text = s_ddl.ddl_text_from_schema(
                    ctx.env.schema,
                )
            else:
                raise errors.QueryError(
                    f'cannot describe full schema as {ql.language}')

            ct = typegen.type_to_typeref(
                ctx.env.get_track_schema_type('std::str'),
                env=ctx.env,
            )

            stmt.result = setgen.ensure_set(
                irast.StringConstant(value=text, typeref=ct),
                ctx=ictx,
            )

        elif ql.object == qlast.DescribeGlobal.SystemConfig:
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
        elif ql.object == qlast.DescribeGlobal.Roles:
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
            items: DefaultDict[str, List[str]] = defaultdict(list)
            referenced_classes: List[s_obj.ObjectMeta] = []

            objref = ql.object
            itemclass = objref.itemclass

            if itemclass is qltypes.SchemaObjectClass.MODULE:
                modules.append(objref.name)
            else:
                itemtype: Optional[Type[s_obj.Object]] = None

                name: str
                if objref.module:
                    name = s_name.Name(module=objref.module, name=objref.name)
                else:
                    name = objref.name

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

            if ql.language is qltypes.DescribeLanguage.DDL:
                method = s_ddl.ddl_text_from_schema
            elif ql.language is qltypes.DescribeLanguage.SDL:
                method = s_ddl.sdl_text_from_schema
            elif ql.language is qltypes.DescribeLanguage.TEXT:
                method = s_ddl.descriptive_text_from_schema
                if not verbose.val:
                    referenced_classes = [s_links.Link, s_lprops.Property]
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
                ctx.env.get_track_schema_type('std::str'),
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
    expr = setgen.ensure_set(dispatch.compile(shape.expr, ctx=ctx), ctx=ctx)
    expr_stype = setgen.get_set_type(expr, ctx=ctx)
    if not isinstance(expr_stype, s_objtypes.ObjectType):
        raise errors.QueryError(
            f'shapes cannot be applied to '
            f'{expr_stype.get_verbosename(ctx.env.schema)}'
        )
    view_type = viewgen.process_view(
        stype=expr_stype, path_id=expr.path_id,
        elements=shape.elements, ctx=ctx)

    return setgen.ensure_set(expr, type_override=view_type, ctx=ctx)


def init_stmt(
        irstmt: irast.Stmt, qlstmt: qlast.Statement, *,
        ctx: context.ContextLevel, parent_ctx: context.ContextLevel) -> None:

    ctx.stmt = irstmt
    if ctx.toplevel_stmt is None:
        parent_ctx.toplevel_stmt = ctx.toplevel_stmt = irstmt

    ctx.path_scope = parent_ctx.path_scope.attach_fence()

    pending_own_ns = parent_ctx.pending_stmt_own_path_id_namespace
    if pending_own_ns:
        ctx.path_scope.add_namespaces(pending_own_ns)

    pending_full_ns = parent_ctx.pending_stmt_full_path_id_namespace
    if pending_full_ns:
        ctx.path_id_namespace |= pending_full_ns

    metadata = ctx.stmt_metadata.get(qlstmt)
    if metadata is not None:
        if metadata.is_unnest_fence:
            ctx.path_scope.unnest_fence = True
        if metadata.iterator_target:
            ctx.iterator_ctx = ctx

    if isinstance(irstmt, irast.MutatingStmt):
        ctx.path_scope.factoring_fence = True
        ctx.iterator_ctx = None

    irstmt.parent_stmt = parent_ctx.stmt

    process_with_block(qlstmt, ctx=ctx, parent_ctx=parent_ctx)


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

    if view is not None:
        parent_ctx.view_sets[view] = result

    return result


def process_with_block(
        edgeql_tree: qlast.Statement, *,
        ctx: context.ContextLevel, parent_ctx: context.ContextLevel) -> None:
    for with_entry in edgeql_tree.aliases:
        if isinstance(with_entry, qlast.ModuleAliasDecl):
            ctx.modaliases[with_entry.alias] = with_entry.module

        elif isinstance(with_entry, qlast.AliasedExpr):
            with ctx.new() as scopectx:
                scopectx.expr_exposed = False
                stmtctx.declare_view(
                    with_entry.expr,
                    with_entry.alias,
                    must_be_used=True,
                    ctx=scopectx)

        else:
            raise RuntimeError(
                f'unexpected expression in WITH block: {with_entry}')


def compile_result_clause(
        result: qlast.Expr, *,
        view_scls: Optional[s_types.Type]=None,
        view_rptr: Optional[context.ViewRPtr]=None,
        view_name: Optional[s_name.SchemaName]=None,
        result_alias: Optional[str]=None,
        forward_rptr: bool=False,
        ctx: context.ContextLevel) -> irast.Set:
    with ctx.new() as sctx:
        if sctx.stmt is ctx.toplevel_stmt:
            sctx.expr_exposed = True

        if forward_rptr:
            sctx.view_rptr = view_rptr
            # sctx.view_scls = view_scls

        result_expr: qlast.Expr
        shape: Optional[Sequence[qlast.ShapeElement]]

        if isinstance(result, qlast.Shape):
            result_expr = result.expr
            shape = result.elements
        else:
            result_expr = result
            shape = None

        if result_alias:
            # `SELECT foo := expr` is equivalent to
            # `WITH foo := expr SELECT foo`
            stmtctx.declare_view(result_expr, alias=result_alias, ctx=sctx)

            result_expr = qlast.Path(
                steps=[qlast.ObjectRef(name=result_alias)]
            )

        if (view_rptr is not None and
                (view_rptr.is_insert or view_rptr.is_update) and
                view_rptr.ptrcls is not None) and False:
            # If we have an empty set assigned to a pointer in an INSERT
            # or UPDATE, there's no need to explicitly specify the
            # empty set type and it can be assumed to match the pointer
            # target type.
            target_t = view_rptr.ptrcls.get_target(ctx.env.schema)

            if astutils.is_ql_empty_set(result_expr):
                expr = setgen.new_empty_set(
                    stype=target_t,
                    alias=ctx.aliases.get('e'),
                    ctx=sctx,
                    srcctx=result_expr.context,
                )
            else:
                with sctx.new() as exprctx:
                    exprctx.empty_result_type_hint = target_t
                    expr = setgen.ensure_set(
                        dispatch.compile(result_expr, ctx=exprctx),
                        ctx=exprctx)
        else:
            if astutils.is_ql_empty_set(result_expr):
                expr = setgen.new_empty_set(
                    stype=sctx.empty_result_type_hint,
                    alias=ctx.aliases.get('e'),
                    ctx=sctx,
                    srcctx=result_expr.context,
                )
            else:
                expr = setgen.ensure_set(
                    dispatch.compile(result_expr, ctx=sctx), ctx=sctx)

        ctx.partial_path_prefix = expr

        ir_result = compile_query_subject(
            expr, shape=shape, view_rptr=view_rptr, view_name=view_name,
            result_alias=result_alias,
            view_scls=view_scls,
            compile_views=ctx.stmt is ctx.toplevel_stmt,
            ctx=sctx)

        ctx.partial_path_prefix = ir_result

    return ir_result


def compile_query_subject(
        expr: irast.Set, *,
        shape: Optional[List[qlast.ShapeElement]]=None,
        view_rptr: Optional[context.ViewRPtr]=None,
        view_name: Optional[s_name.SchemaName]=None,
        result_alias: Optional[str]=None,
        view_scls: Optional[s_types.Type]=None,
        compile_views: bool=True,
        is_insert: bool=False,
        is_update: bool=False,
        is_delete: bool=False,
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
        and (
            view_rptr.ptrcls_is_linkprop
            == (expr_rptr.ptrref.source_ptr is not None)
        )
    )

    if is_ptr_alias:
        assert view_rptr is not None
        # We are inside an expression that defines a link alias in
        # the parent shape, ie. Spam { alias := Spam.bar }, so
        # `Spam.alias` should be a subclass of `Spam.bar` inheriting
        # its properties.
        base_ptrcls = typegen.ptrcls_from_ptrref(expr_rptr.ptrref, ctx=ctx)
        if isinstance(base_ptrcls, s_pointers.Pointer):
            view_rptr.base_ptrcls = base_ptrcls
            view_rptr.ptrcls_is_alias = True

    if (
        ctx.expr_exposed
        and viewgen.has_implicit_tid(
            expr_stype,
            is_mutation=is_insert or is_update or is_delete,
            ctx=ctx,
        )
        and shape is None
        and expr_stype not in ctx.env.view_shapes
    ):
        # Force the subject to be compiled as a view if a __tid__
        # insertion is anticipated (the actual decision is taken
        # by the compile_view_shapes() flow).
        shape = []

    if shape is not None and view_scls is None:
        if (view_name is None and
                isinstance(result_alias, s_name.SchemaName)):
            view_name = result_alias

        if not isinstance(expr_stype, s_objtypes.ObjectType):
            raise errors.QueryError(
                f'shapes cannot be applied to '
                f'{expr_stype.get_verbosename(ctx.env.schema)}'
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
            ctx=ctx,
        )

    if view_scls is not None:
        expr = setgen.ensure_set(expr, type_override=view_scls, ctx=ctx)
        expr_stype = view_scls

    if compile_views:
        rptr = view_rptr.rptr if view_rptr is not None else None
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
