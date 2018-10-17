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


"""EdgeQL shape compilation functions."""


import functools
import typing

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import links as s_links
from edb.lang.schema import name as sn
from edb.lang.schema import nodes as s_nodes
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import sources as s_sources
from edb.lang.schema import types as s_types

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import errors

from edb.lang.common import exceptions as edgedb_error

from . import astutils
from . import context
from . import dispatch
from . import pathctx
from . import schemactx
from . import setgen


def process_view(
        *,
        scls: s_nodes.Node,
        path_id: irast.PathId,
        elements: typing.List[qlast.ShapeElement],
        view_rptr: typing.Optional[context.ViewRPtr]=None,
        view_name: typing.Optional[sn.SchemaName]=None,
        is_insert: bool=False,
        is_update: bool=False,
        ctx: context.CompilerContext) -> s_nodes.Node:

    cache_key = tuple(elements)
    view_scls = ctx.shape_type_cache.get(cache_key)
    if view_scls is not None:
        return view_scls

    with ctx.newscope(fenced=True, temporary=True) as scopectx:
        scopectx.path_scope.attach_path(path_id)

        view_scls = _process_view(
            scls=scls, path_id=path_id, elements=elements,
            view_rptr=view_rptr, view_name=view_name,
            is_insert=is_insert, is_update=is_update,
            ctx=scopectx
        )

    ctx.shape_type_cache[cache_key] = view_scls

    return view_scls


def _process_view(
        *,
        scls: s_nodes.Node,
        path_id: irast.PathId,
        elements: typing.List[qlast.ShapeElement],
        view_rptr: typing.Optional[context.ViewRPtr]=None,
        view_name: typing.Optional[sn.SchemaName]=None,
        is_insert: bool=False,
        is_update: bool=False,
        ctx: context.CompilerContext) -> s_nodes.Node:
    view_scls = schemactx.derive_view(
        scls, is_insert=is_insert, is_update=is_update,
        derived_name=view_name, ctx=ctx)
    is_mutation = is_insert or is_update
    is_defining_shape = ctx.expr_exposed or is_mutation

    pointers = []

    for shape_el in elements:
        with ctx.newscope(fenced=True) as scopectx:
            pointers.append(_normalize_view_ptr_expr(
                shape_el, view_scls, path_id=path_id,
                is_insert=is_insert, is_update=is_update,
                view_rptr=view_rptr, ctx=scopectx))

    if is_insert:
        explicit_ptrs = {ptrcls.shortname for ptrcls in pointers}

        for pn, ptrcls in scls.pointers.items():
            if (not ptrcls.default or
                    pn in explicit_ptrs or
                    ptrcls.is_pure_computable()):
                continue

            default_ql = qlast.ShapeElement(expr=qlast.Path(steps=[
                qlast.Ptr(ptr=qlast.ObjectRef(name=ptrcls.shortname.name,
                                              module=ptrcls.shortname.module))
            ]))

            with ctx.newscope(fenced=True) as scopectx:
                pointers.append(_normalize_view_ptr_expr(
                    default_ql, view_scls, path_id=path_id,
                    is_insert=is_insert, is_update=is_update,
                    view_rptr=view_rptr, ctx=scopectx))

    # Check if the view shape includes _only_ the link properties.
    # If so, we do not need to derive a new target view.
    lprops_only = True
    for ptrcls in pointers:
        if not ptrcls.is_link_property():
            lprops_only = False
            break

    if lprops_only:
        view_scls = scls

    for ptrcls in pointers:
        if ptrcls.is_link_property():
            source = view_rptr.derived_ptrcls
        else:
            source = view_scls

        if ptrcls.source is source and isinstance(source, s_sources.Source):
            # source may be an ScalarType in shapes that reference __type__,
            # hence the isinstance check.
            source.add_pointer(ptrcls, replace=True)

        if is_defining_shape:
            if source is None:
                # The nested shape is merely selecting the pointer,
                # so the link class has not been derived.  But for
                # the purposes of shape tracking, we must derive it
                # still.  The derived pointer must have the same
                # name as the original, as this is not a new computable,
                # and both `Foo.ptr` and `Foo { ptr }` are the same path.
                source = derive_ptrcls(
                    view_rptr, target_scls=view_scls,
                    inherit_name=True, ctx=ctx)

            ctx.class_shapes[source].append(ptrcls)

    if (view_rptr is not None and view_rptr.derived_ptrcls is not None and
            view_scls is not scls):
        view_scls.rptr = view_rptr.derived_ptrcls

    return view_scls


def _normalize_view_ptr_expr(
        shape_el: qlast.ShapeElement,
        view_scls: s_nodes.Node, *,
        path_id: irast.PathId,
        is_insert: bool=False,
        is_update: bool=False,
        view_rptr: typing.Optional[context.ViewRPtr]=None,
        ctx: context.CompilerContext) -> s_pointers.Pointer:
    steps = shape_el.expr.steps
    is_linkprop = False
    is_mutation = is_insert or is_update
    # Pointers may be qualified by the explicit source
    # class, which is equivalent to Expr[IS Type].
    is_polymorphic = isinstance(steps[0], qlast.TypeExpr)
    scls = view_scls.peel_view()
    ptrsource = scls
    qlexpr = None

    if is_polymorphic:
        ptype = steps[0]
        source = qlast.TypeFilter(
            expr=qlast.Path(steps=[qlast.Source()]),
            type=ptype)
        lexpr = steps[1]
        ptrsource = schemactx.get_schema_type(ptype.maintype, ctx=ctx)
    elif len(steps) == 1:
        # regular shape
        lexpr = steps[0]
        is_linkprop = lexpr.type == 'property'
        if is_linkprop:
            if view_rptr is None:
                raise errors.EdgeQLError(
                    'invalid reference to link property '
                    'in top level shape', context=lexpr.context)
            if view_rptr.ptrcls is None:
                derive_ptrcls(view_rptr, target_scls=view_scls, ctx=ctx)
            ptrsource = scls = view_rptr.ptrcls
        source = qlast.Source()
    else:
        raise RuntimeError(
            f'unexpected path length in view shape: {len(steps)}')

    ptrname = (lexpr.ptr.module, lexpr.ptr.name)
    ptrcls_is_derived = False

    compexpr = shape_el.compexpr
    if compexpr is None and is_insert and shape_el.elements:
        # Nested insert short form:
        #     INSERT Foo { bar: Spam { name := 'name' }}
        # Expand to:
        #     INSERT Foo { bar := (INSERT Spam { name := 'name' }) }
        if lexpr.target is not None:
            ptr_target = schemactx.get_schema_type(
                lexpr.target.maintype, ctx=ctx)
        else:
            ptr_target = None

        base_ptrcls = ptrcls = setgen.resolve_ptr(
            ptrsource, ptrname, s_pointers.PointerDirection.Outbound,
            target=ptr_target, ctx=ctx)

        compexpr = qlast.InsertQuery(
            subject=qlast.Path(
                steps=[
                    qlast.ObjectRef(name=ptrcls.target.name.name,
                                    module=ptrcls.target.name.module)
                ]
            ),
            shape=shape_el.elements
        )

    if compexpr is None:
        if lexpr.target is not None:
            ptr_target = schemactx.get_schema_type(
                lexpr.target.maintype, ctx=ctx)
        else:
            ptr_target = None

        base_ptrcls = ptrcls = setgen.resolve_ptr(
            ptrsource, ptrname, s_pointers.PointerDirection.Outbound,
            target=ptr_target, ctx=ctx)

        base_ptr_is_computable = ptrcls in ctx.source_map

        if ptr_target is not None and ptr_target != base_ptrcls.target:
            # This happens when a union type target is narrowed by an
            # [IS Type] construct.  Since the derived pointer will have
            # the correct target, we don't need to do anything, but
            # remove the [IS] qualifier to prevent recursion.
            lexpr.target = None
        else:
            ptr_target = ptrcls.target

        if ptrcls in ctx.pending_cardinality:
            # We do not know the parent's pointer cardinality yet.
            ptr_cardinality = None
        else:
            ptr_cardinality = ptrcls.cardinality

        if shape_el.elements:
            sub_view_rptr = context.ViewRPtr(
                ptrsource if is_linkprop else view_scls, ptrcls=ptrcls,
                is_insert=is_insert, is_update=is_update)

            sub_path_id = path_id.extend(ptrcls, target=ptrcls.target)
            ctx.path_scope.attach_path(sub_path_id)

            if is_update:
                for subel in shape_el.elements or []:
                    is_prop = (
                        isinstance(subel.expr.steps[0], qlast.Ptr) and
                        subel.expr.steps[0].type == 'property'
                    )
                    if not is_prop:
                        raise errors.EdgeQLError(
                            'only references to link properties are allowed '
                            'in nested UPDATE shapes', context=subel.context)

                ptr_target = _process_view(
                    scls=ptr_target, path_id=sub_path_id,
                    view_rptr=sub_view_rptr,
                    elements=shape_el.elements, is_update=True, ctx=ctx)
            else:
                ptr_target = _process_view(
                    scls=ptr_target, path_id=sub_path_id,
                    view_rptr=sub_view_rptr,
                    elements=shape_el.elements, ctx=ctx)

            ptrcls = sub_view_rptr.derived_ptrcls
            if ptrcls is None:
                ptrcls_is_derived = False
                ptrcls = sub_view_rptr.ptrcls
            else:
                ptrcls_is_derived = True

        if (shape_el.where or shape_el.orderby or
                shape_el.offset or shape_el.limit or
                base_ptr_is_computable or
                is_polymorphic):

            if qlexpr is None:
                qlexpr = qlast.Path(steps=[source, lexpr])

            qlexpr = astutils.ensure_qlstmt(qlexpr)
            qlexpr.where = shape_el.where
            qlexpr.orderby = shape_el.orderby
            qlexpr.offset = shape_el.offset
            qlexpr.limit = shape_el.limit
    else:
        try:
            base_ptrcls = ptrcls = setgen.resolve_ptr(
                ptrsource, ptrname, s_pointers.PointerDirection.Outbound,
                ctx=ctx)

            ptr_name = ptrcls.shortname
        except errors.EdgeQLReferenceError:
            if is_mutation:
                raise

            base_ptrcls = ptrcls = None

            ptr_module = (
                ptrname[0] or
                ctx.derived_target_module or
                scls.name.module
            )

            ptr_name = sn.SchemaName(module=ptr_module, name=ptrname[1])

        qlexpr = astutils.ensure_qlstmt(compexpr)

        with ctx.newscope(fenced=True) as shape_expr_ctx:
            # Put current pointer class in context, so
            # that references to link properties in sub-SELECT
            # can be resolved.  This is necessary for proper
            # evaluation of link properties on computable links,
            # most importantly, in INSERT/UPDATE context.
            shape_expr_ctx.view_rptr = context.ViewRPtr(
                ptrsource if is_linkprop else view_scls,
                ptrcls=ptrcls, ptrcls_name=ptr_name,
                ptrcls_is_linkprop=is_linkprop,
                is_insert=is_insert, is_update=is_update)

            shape_expr_ctx.path_scope.unnest_fence = True

            if is_mutation:
                shape_expr_ctx.expr_exposed = True
                shape_expr_ctx.empty_result_type_hint = ptrcls.target

            irexpr = dispatch.compile(qlexpr, ctx=shape_expr_ctx)

            irexpr.context = compexpr.context

            if base_ptrcls is None:
                base_ptrcls = ptrcls = shape_expr_ctx.view_rptr.ptrcls

            derived_ptrcls = shape_expr_ctx.view_rptr.derived_ptrcls
            if derived_ptrcls is not None:
                ptrcls_is_derived = True
                ptrcls = derived_ptrcls

        ptr_cardinality = None

        ptr_target = irutils.infer_type(irexpr, ctx.schema)
        if ptr_target is None:
            msg = 'cannot determine expression result type'
            raise errors.EdgeQLError(msg, context=shape_el.context)

        if is_mutation and not ptr_target.assignment_castable_to(
                base_ptrcls.target, schema=ctx.schema):
            # Validate that the insert/update expression is
            # of the correct class.
            lname = f'({ptrsource.name}).{ptrcls.shortname.name}'
            expected = [repr(str(base_ptrcls.target.name))]
            raise edgedb_error.InvalidPointerTargetError(
                f'invalid target for link {str(lname)!r}: '
                f'{str(ptr_target.name)!r} (expecting '
                f'{" or ".join(expected)})'
            )

    if qlexpr is not None or ptr_target is not ptrcls.target:
        if not ptrcls_is_derived:
            if is_linkprop:
                rptrcls = view_rptr.derived_ptrcls
                if rptrcls is None:
                    rptrcls = derive_ptrcls(
                        view_rptr, target_scls=view_scls, ctx=ctx)

                src_scls = rptrcls
            else:
                src_scls = view_scls

            if qlexpr is None:
                # This is not a computable, just a pointer
                # to a nested shape.  Have it reuse the original
                # pointer name so that in `Foo.ptr.name` and
                # `Foo { ptr: {name}}` are the same path.
                derived_name = ptrcls.name
            else:
                derived_name = None

            ptrcls = schemactx.derive_view(
                ptrcls, src_scls, ptr_target,
                is_insert=is_insert, is_update=is_update,
                derived_name=derived_name,
                derived_name_quals=[view_scls.name], ctx=ctx)

        if qlexpr is not None:
            ctx.source_map[ptrcls] = (qlexpr, ctx, path_id)
            ptrcls.computable = True

    if not is_mutation:
        if ptr_cardinality is None:
            if compexpr is not None:
                ctx.pending_cardinality.add(ptrcls)
            elif ptrcls is not base_ptrcls:
                ctx.pointer_derivation_map[base_ptrcls].append(ptrcls)

            ptrcls.cardinality = None
        else:
            ptrcls.cardinality = ptr_cardinality

    if ptrcls.is_protected_pointer() and qlexpr is not None:
        if is_polymorphic:
            msg = (f'cannot access {ptrcls.shortname.name} on a polymorphic '
                   f'shape element')
        else:
            msg = f'cannot assign to {ptrcls.shortname.name}'
        raise errors.EdgeQLError(msg, context=shape_el.context)

    return ptrcls


def derive_ptrcls(
        view_rptr: context.ViewRPtr, *,
        target_scls: s_nodes.Node,
        inherit_name: bool=False,
        ctx: context.ContextLevel) -> s_pointers.Pointer:

    if view_rptr.ptrcls is None:
        if view_rptr.base_ptrcls is not None:
            if inherit_name:
                derived_name = view_rptr.base_ptrcls.name
            else:
                derived_name = schemactx.derive_view_name(
                    view_rptr.base_ptrcls,
                    derived_name_base=view_rptr.ptrcls_name,
                    derived_name_quals=(view_rptr.source.name,),
                    ctx=ctx)

            view_rptr.ptrcls = schemactx.derive_view(
                view_rptr.base_ptrcls, view_rptr.source, target_scls,
                derived_name=derived_name,
                is_insert=view_rptr.is_insert,
                is_update=view_rptr.is_update,
                ctx=ctx
            )

            view_rptr.derived_ptrcls = view_rptr.ptrcls
        else:
            raise RuntimeError(
                'ViewRPtr does not define ptrcls or base_ptrcls')

    else:
        derived_name = view_rptr.ptrcls.name if inherit_name else None
        view_rptr.derived_ptrcls = schemactx.derive_view(
            view_rptr.ptrcls, view_rptr.source, target_scls,
            derived_name=derived_name,
            is_insert=view_rptr.is_insert,
            is_update=view_rptr.is_update,
            ctx=ctx
        )

    return view_rptr.derived_ptrcls


def _link_has_shape(
        ptrcls: s_pointers.Pointer, *,
        ctx: context.ContextLevel) -> bool:
    if not isinstance(ptrcls, s_links.Link):
        return False

    for p in ptrcls.pointers.values():
        if p.is_special_pointer() or p not in ctx.class_shapes[ptrcls]:
            continue
        else:
            return True

    return False


def _get_shape_configuration(
        ir_set: irast.Set, *,
        rptr: typing.Optional[irast.Pointer]=None,
        parent_view_type: typing.Optional[s_types.ViewType]=None,
        ctx: context.ContextLevel) \
        -> typing.List[typing.Tuple[irast.Set, s_pointers.Pointer]]:

    """Return a list of (source_set, ptrcls) pairs as a shape for a given set.
    """

    scls = ir_set.scls

    sources = []
    link_view = False
    is_objtype = ir_set.path_id.is_objtype_path()

    if rptr is None:
        rptr = ir_set.rptr

    link_view = (
        rptr is not None and
        not rptr.ptrcls.is_link_property() and
        _link_has_shape(rptr.ptrcls, ctx=ctx)
    )

    if is_objtype or not link_view:
        sources.append(scls)

    if link_view:
        sources.append(rptr.ptrcls)

    shape_ptrs = []

    id_present_in_shape = False

    for source in sources:
        for ptr in ctx.class_shapes[source]:
            if (ptr.is_link_property() and
                    ir_set.path_id != rptr.target.path_id):
                path_tip = rptr.target
            else:
                path_tip = ir_set

            shape_ptrs.append((path_tip, ptr))

            if source is scls and ptr.is_id_pointer():
                id_present_in_shape = True

    implicit_id = (
        # source expression is an object
        is_objtype and
        (
            # shape is unspecified
            not shape_ptrs or
            # implicit ids are always wanted
            ctx.implicit_id_in_shapes or
            # we are inside an UPDATE shape and this is
            # an explicit expression (link target update)
            (parent_view_type == s_types.ViewType.Update and
             ir_set.expr is not None)
        )
    )

    if implicit_id and not id_present_in_shape:
        # We want the id in this shape and it's not already there,
        # so insert it in the first position.
        for ptr in scls.pointers.values():
            if ptr.is_id_pointer():
                ctx.class_shapes[scls].insert(0, ptr)
                shape_ptrs.insert(0, (ir_set, ptr))
                break

    return shape_ptrs


@functools.singledispatch
def compile_view_shapes(
        expr: irast.Base, *,
        rptr: typing.Optional[irast.Pointer]=None,
        parent_view_type: typing.Optional[s_types.ViewType]=None,
        ctx: context.ContextLevel) -> None:
    pass


@compile_view_shapes.register(irast.Set)
def _compile_view_shapes_in_set(
        ir_set: irast.Set, *,
        rptr: typing.Optional[irast.Pointer]=None,
        parent_view_type: typing.Optional[s_types.ViewType]=None,
        ctx: context.ContextLevel) -> None:

    scls = ir_set.scls

    is_mutation = scls.view_type in (
        s_types.ViewType.Update, s_types.ViewType.Insert)

    shape_ptrs = _get_shape_configuration(
        ir_set, rptr=rptr, parent_view_type=parent_view_type, ctx=ctx)

    if shape_ptrs:
        if (isinstance(ir_set.expr, irast.SelectStmt) and
                (ir_set.expr.offset is not None or
                 ir_set.expr.limit is not None)):
            # The OFFSET/LIMIT query is a wrapper set up to
            # track the scope correctly, make sure we don't
            # mess that scope up, as the shape's source expression
            # should remain behind the SET OF fence of LIMIT.
            with ctx.new() as scopectx:
                ol_scope = pathctx.get_set_scope(
                    ir_set.expr.result, ctx=scopectx)
                scopectx.path_scope = ol_scope
                pathctx.register_set_in_scope(ir_set, ctx=scopectx)
        else:
            pathctx.register_set_in_scope(ir_set, ctx=ctx)

        for path_tip, ptr in shape_ptrs:
            element = setgen.extend_path(
                path_tip, ptr, force_computable=is_mutation,
                unnest_fence=True, ctx=ctx)

            element_scope = pathctx.get_set_scope(element, ctx=ctx)

            if element_scope is None:
                element_scope = ctx.path_scope.attach_fence()
                pathctx.assign_set_scope(element, element_scope, ctx=ctx)

            if element_scope.namespaces:
                element.path_id = element.path_id.merge_namespace(
                    element_scope.namespaces)

            with ctx.new() as scopectx:
                scopectx.path_scope = element_scope
                compile_view_shapes(
                    element, parent_view_type=scls.view_type, ctx=scopectx)

            ir_set.shape.append(element)

    elif ir_set.expr is not None:
        set_scope = pathctx.get_set_scope(ir_set, ctx=ctx)
        if set_scope is not None:
            with ctx.new() as scopectx:
                scopectx.path_scope = set_scope
                compile_view_shapes(ir_set.expr, ctx=scopectx)
        else:
            compile_view_shapes(ir_set.expr, ctx=ctx)


@compile_view_shapes.register(irast.SelectStmt)
def _compile_view_shapes_in_select(
        stmt: irast.SelectStmt, *,
        rptr: typing.Optional[irast.Pointer]=None,
        parent_view_type: typing.Optional[s_types.ViewType]=None,
        ctx: context.ContextLevel) -> None:
    compile_view_shapes(stmt.result, ctx=ctx)


@compile_view_shapes.register(irast.FunctionCall)
def _compile_view_shapes_in_fcall(
        expr: irast.FunctionCall, *,
        rptr: typing.Optional[irast.Pointer]=None,
        parent_view_type: typing.Optional[s_types.ViewType]=None,
        ctx: context.ContextLevel) -> None:
    funcobj = expr.func

    preserves_type = (
        any(irutils.is_polymorphic_type(p.type)
            for p in funcobj.params) and
        irutils.is_polymorphic_type(funcobj.return_type)
    )

    if preserves_type:
        for arg in expr.args:
            arg_scope = pathctx.get_set_scope(arg, ctx=ctx)
            if arg_scope is not None:
                with ctx.new() as scopectx:
                    scopectx.path_scope = arg_scope
                    compile_view_shapes(arg, ctx=scopectx)
            else:
                compile_view_shapes(arg, ctx=ctx)


@compile_view_shapes.register(irast.Tuple)
def _compile_view_shapes_in_tuple(
        expr: irast.Tuple, *,
        rptr: typing.Optional[irast.Pointer]=None,
        parent_view_type: typing.Optional[s_types.ViewType]=None,
        ctx: context.ContextLevel) -> None:
    for element in expr.elements:
        compile_view_shapes(element.val, ctx=ctx)
