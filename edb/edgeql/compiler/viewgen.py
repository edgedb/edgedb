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


from __future__ import annotations

import collections
import functools
from typing import *

from edb import errors
from edb.common import context as pctx
from edb.common.typeutils import not_none

from edb.ir import ast as irast
from edb.ir import typeutils

from edb.schema import links as s_links
from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import objects as s_objects
from edb.schema import pointers as s_pointers
from edb.schema import types as s_types

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import astutils
from . import context
from . import dispatch
from . import eta_expand
from . import inference
from . import pathctx
from . import schemactx
from . import setgen
from . import typegen

if TYPE_CHECKING:
    from edb.schema import properties as s_props
    from edb.schema import sources as s_sources

    ShapePtr = Tuple[
        irast.Set, s_pointers.Pointer, qlast.ShapeOp, Optional[irast.Set]
    ]


def process_view(
    ir_set: irast.Set,
    *,
    stype: s_objtypes.ObjectType,
    elements: List[qlast.ShapeElement],
    view_rptr: Optional[context.ViewRPtr] = None,
    view_name: Optional[sn.QualName] = None,
    exprtype: s_types.ExprType = s_types.ExprType.Select,
    parser_context: Optional[pctx.ParserContext],
    ctx: context.ContextLevel,
) -> Tuple[s_objtypes.ObjectType, irast.Set]:

    cache_key = (stype, exprtype, tuple(elements))
    view_scls = ctx.shape_type_cache.get(cache_key)
    if view_scls is not None:
        return view_scls, ir_set

    # XXX: This is an unfortunate hack to ensure that "cannot
    # reference correlated set" errors get produced correctly,
    # since there needs to be an intervening branch for a
    # factoring fence to be respected.
    hackscope = ctx.path_scope.attach_branch()
    pathctx.register_set_in_scope(ir_set, path_scope=hackscope, ctx=ctx)
    hackscope.remove()
    ctx.path_scope.attach_subtree(hackscope)

    # Make a snapshot of aliased_views that can't be mutated
    # in any parent scopes.
    ctx.aliased_views = collections.ChainMap(dict(ctx.aliased_views))

    view_scls, ir = _process_view(
        ir_set,
        stype=stype,
        elements=elements,
        view_rptr=view_rptr,
        view_name=view_name,
        exprtype=exprtype,
        parser_context=parser_context,
        ctx=ctx,
    )

    ctx.shape_type_cache[cache_key] = view_scls

    return view_scls, ir


def _process_view(
    ir_set: irast.Set,
    *,
    stype: s_objtypes.ObjectType,
    path_id_namespace: Optional[irast.Namespace] = None,
    elements: Optional[Sequence[qlast.ShapeElement]],
    view_rptr: Optional[context.ViewRPtr] = None,
    view_name: Optional[sn.QualName] = None,
    exprtype: s_types.ExprType = s_types.ExprType.Select,
    parser_context: Optional[pctx.ParserContext],
    ctx: context.ContextLevel,
) -> Tuple[s_objtypes.ObjectType, irast.Set]:
    path_id = ir_set.path_id

    needs_real_name = view_name is None and ctx.env.options.schema_view_mode
    generated_name = None
    if needs_real_name and view_rptr is not None:
        # Make sure persistent schema expression aliases have properly formed
        # names as opposed to the usual mangled form of the ephemeral
        # aliases.  This is needed for introspection readability, as well
        # as helps in maintaining proper type names for schema
        # representations that require alphanumeric names, such as
        # GraphQL.
        #
        # We use the name of the source together with the name
        # of the inbound link to form the name, so in e.g.
        #    CREATE ALIAS V := (SELECT Foo { bar: { baz: { ... } })
        # The name of the innermost alias would be "__V__bar__baz".
        source_name = view_rptr.source.get_name(ctx.env.schema).name
        if not source_name.startswith('__'):
            source_name = f'__{source_name}'
        if view_rptr.ptrcls_name is not None:
            ptr_name = view_rptr.ptrcls_name.name
        elif view_rptr.ptrcls is not None:
            ptr_name = view_rptr.ptrcls.get_shortname(ctx.env.schema).name
        else:
            raise errors.InternalServerError(
                '_process_view in schema mode received view_rptr with '
                'neither ptrcls_name, not ptrcls'
            )

        generated_name = f'{source_name}__{ptr_name}'
    elif needs_real_name and ctx.env.alias_result_view_name:
        # If this is a persistent schema expression but we aren't just
        # obviously sitting on an rptr (e.g CREATE ALIAS V := (Foo { x }, 10)),
        # we create a name like __V__Foo__2.
        source_name = ctx.env.alias_result_view_name.name
        type_name = stype.get_name(ctx.env.schema).name
        generated_name = f'__{source_name}__{type_name}'

    if generated_name:
        # If there are multiple, we want to stick a number on, but we'd
        # like to skip the number if there aren't.
        name = ctx.aliases.get(
            generated_name).replace('~1', '').replace('~', '__')
        view_name = sn.QualName(
            module=ctx.derived_target_module or '__derived__',
            name=name,
        )

    view_scls = schemactx.derive_view(
        stype,
        exprtype=exprtype,
        derived_name=view_name,
        ctx=ctx,
    )
    assert isinstance(view_scls, s_objtypes.ObjectType), view_scls
    is_mutation = exprtype.is_insert() or exprtype.is_update()
    is_defining_shape = ctx.expr_exposed or is_mutation

    orig_ir_set = ir_set
    ir_set = setgen.ensure_set(ir_set, type_override=view_scls, ctx=ctx)
    # Materialize based on the original base pointer.
    # This seems like a hack.
    if orig_ir_set.rptr and orig_ir_set.rptr.ptrref.base_ptr:
        ctx.env.schema, root_old_ptrcls = typeutils.ptrcls_from_ptrref(
            orig_ir_set.rptr.ptrref.base_ptr, schema=ctx.env.schema
        )
        setgen.maybe_materialize(root_old_ptrcls, ir_set, ctx=ctx)

    if view_rptr is not None and view_rptr.ptrcls is None:
        derive_ptrcls(
            view_rptr, target_scls=view_scls,
            transparent=True, ctx=ctx)

    pointers: List[s_pointers.Pointer] = []
    pointer_entries = []

    elements = elements or ()
    for shape_el in elements:
        with ctx.new() as scopectx:
            pointer, ptr_set = _normalize_view_ptr_expr(
                ir_set, shape_el, view_scls, path_id=path_id,
                path_id_namespace=path_id_namespace,
                exprtype=exprtype,
                view_rptr=view_rptr,
                pending_pointers=pointers,
                ctx=scopectx)

            pointers.append(pointer)
            pointer_entries.append((pointer, ptr_set))

    # If we are not defining a shape (so we might care about
    # materialization), look through our parent view (if one exists)
    # for materialized properties that are not present in this shape.
    # If any are found, inject them.
    # (See test_edgeql_volatility_rebind_flat_01 for an example.)
    schema = ctx.env.schema
    base = view_scls.get_bases(schema).objects(schema)[0]
    base_ptrs = (view_scls.get_pointers(schema).objects(schema)
                 if not is_defining_shape else ())
    for ptrcls in base_ptrs:
        if ptrcls in pointers or base not in ctx.env.view_shapes:
            continue
        pptr = ptrcls.get_bases(schema).objects(schema)[0]
        if (pptr, qlast.ShapeOp.MATERIALIZE) not in ctx.env.view_shapes[base]:
            continue

        # Make up a dummy shape element
        name = ptrcls.get_shortname(schema).name
        dummy_el = qlast.ShapeElement(expr=qlast.Path(
            steps=[qlast.Ptr(ptr=qlast.ObjectRef(name=name))]))

        with ctx.new() as scopectx:
            pointer, ptr_set = _normalize_view_ptr_expr(
                ir_set, dummy_el, view_scls, path_id=path_id,
                path_id_namespace=path_id_namespace,
                exprtype=exprtype,
                view_rptr=view_rptr,
                ctx=scopectx)

        pointers.append(pointer)
        pointer_entries.append((pointer, ptr_set))

    if exprtype.is_insert():
        explicit_ptrs = {
            ptrcls.get_local_name(ctx.env.schema)
            for ptrcls in pointers
        }
        scls_pointers = stype.get_pointers(ctx.env.schema)
        for pn, ptrcls in scls_pointers.items(ctx.env.schema):
            if (pn in explicit_ptrs or
                    ptrcls.is_pure_computable(ctx.env.schema)):
                continue

            default_expr = ptrcls.get_default(ctx.env.schema)
            if not default_expr:
                if (
                    ptrcls.get_required(ctx.env.schema)
                    and pn != sn.UnqualName('__type__')
                ):
                    if ptrcls.is_property(ctx.env.schema):
                        # If the target is a sequence, there's no need
                        # for an explicit value.
                        ptrcls_target = ptrcls.get_target(ctx.env.schema)
                        assert ptrcls_target is not None
                        if ptrcls_target.issubclass(
                                ctx.env.schema,
                                ctx.env.schema.get(
                                    'std::sequence',
                                    type=s_objects.SubclassableObject)):
                            continue
                    vn = ptrcls.get_verbosename(
                        ctx.env.schema, with_parent=True)
                    raise errors.MissingRequiredError(
                        f'missing value for required {vn}')
                else:
                    continue

            ptrcls_sn = ptrcls.get_shortname(ctx.env.schema)
            default_ql = qlast.ShapeElement(
                expr=qlast.Path(
                    steps=[
                        qlast.Ptr(
                            ptr=qlast.ObjectRef(
                                name=ptrcls_sn.name,
                                module=ptrcls_sn.module,
                            ),
                        ),
                    ],
                ),
                compexpr=qlast.DetachedExpr(
                    expr=default_expr.qlast,
                ),
            )

            with ctx.new() as scopectx:
                pointer_entries.append(
                    _normalize_view_ptr_expr(
                        ir_set,
                        default_ql,
                        view_scls,
                        path_id=path_id,
                        path_id_namespace=path_id_namespace,
                        exprtype=exprtype,
                        from_default=True,
                        view_rptr=view_rptr,
                        ctx=scopectx,
                    ),
                )

    set_shape = []
    shape_ptrs: List[ShapePtr] = []

    for ptrcls, ptr_set in pointer_entries:
        source: Union[s_types.Type, s_pointers.PointerLike]

        if ptrcls.is_link_property(ctx.env.schema):
            assert view_rptr is not None and view_rptr.ptrcls is not None
            source = view_rptr.ptrcls
        else:
            source = view_scls

        if is_defining_shape:
            cinfo = ctx.source_map.get(ptrcls)
            if cinfo is not None:
                shape_op = cinfo.shape_op
            else:
                shape_op = qlast.ShapeOp.ASSIGN
        elif ptrcls.get_computable(ctx.env.schema):
            shape_op = qlast.ShapeOp.MATERIALIZE
        else:
            continue

        ctx.env.view_shapes[source].append((ptrcls, shape_op))
        shape_ptrs.append((ir_set, ptrcls, shape_op, ptr_set))

    rptrcls = view_rptr.ptrcls if view_rptr else None
    shape_ptrs = _get_early_shape_configuration(
        ir_set, shape_ptrs, rptrcls=rptrcls, ctx=ctx)

    # Produce the shape. The main thing here is that we need to fixup
    # all of the rptrs to properly point back at ir_set.
    for _, ptrcls, shape_op, ptr_set in shape_ptrs:
        srcctx = None
        if ptrcls in ctx.env.pointer_specified_info:
            _, _, srcctx = ctx.env.pointer_specified_info[ptrcls]

        if ptr_set:
            src_path_id = path_id
            if ptrcls.is_link_property(ctx.env.schema):
                src_path_id = src_path_id.ptr_path()

            ptr_set.path_id = pathctx.extend_path_id(
                src_path_id,
                ptrcls=ptrcls,
                ns=ctx.path_id_namespace,
                ctx=ctx,
            )
            ptr_set.rptr = irast.Pointer(
                source=ir_set,
                target=ptr_set,
                direction=s_pointers.PointerDirection.Outbound,
                ptrref=not_none(ptr_set.path_id.rptr()),
                is_definition=True,
            )
            ptr_set.context = srcctx

            _setup_shape_source(ptr_set, ctx=ctx)

        else:
            # The set must be something pretty trivial, so just do it
            ptr_set = setgen.extend_path(
                ir_set,
                ptrcls,
                same_computable_scope=True,
                srcctx=srcctx,
                ctx=ctx,
            )

        # HACK?: when we see linkprops being used on an intersection,
        # attach the flattened source path to make linkprops on
        # computed backlinks work
        if (
            isinstance(path_id.rptr(), irast.TypeIntersectionPointerRef)
            and ptrcls.is_link_property(ctx.env.schema)
        ):
            ctx.path_scope.attach_path(
                path_id, flatten_intersection=True, context=None)

        set_shape.append((ptr_set, shape_op))

    ir_set.shape = tuple(set_shape)

    if (view_rptr is not None and view_rptr.ptrcls is not None and
            view_scls != stype):
        ctx.env.schema = view_scls.set_field_value(
            ctx.env.schema, 'rptr', view_rptr.ptrcls)

    return view_scls, ir_set


def _setup_shape_source(cur_set: irast.Set, ctx: context.ContextLevel) -> None:
    """Set up shape source for a shape element.

    This is basically all so that nested link properties get set up properly.
    XXX: There ought to be a better way.
    """

    if (isinstance(cur_set.expr, irast.SelectStmt)
        and (setgen.get_set_type(cur_set, ctx=ctx) ==
             setgen.get_set_type(cur_set.expr.result, ctx=ctx))):
        child = cur_set.expr.result
        _setup_shape_source(child, ctx=ctx)
        cur_set.shape_source = (
            child if child.shape else child.shape_source)

    # To get the linkprops to line up for inserts we unfortunately need to
    # pull linkprop shape elements up into the top element.
    if not cur_set.shape and isinstance(cur_set.expr, irast.InsertStmt):
        ptr_shape = []
        for sub_set, sub_op in cur_set.expr.subject.shape:
            if not sub_set.path_id.is_linkprop_path():
                continue
            sub_rptr = irast.Pointer(
                source=cur_set,
                target=sub_set,
                direction=s_pointers.PointerDirection.Outbound,
                ptrref=not_none(sub_set.path_id.rptr()),
                is_definition=True,
            )
            sub_set = setgen.new_set_from_set(sub_set, rptr=sub_rptr, ctx=ctx)
            sub_rptr.target = sub_set
            ptr_shape.append((sub_set, sub_op))
        cur_set.shape = tuple(ptr_shape)


def _compile_qlexpr(
    ir_source: irast.Set,
    qlexpr: qlast.Base,
    view_scls: s_objtypes.ObjectType,
    *,
    ptrcls: Optional[s_pointers.Pointer],
    ptrsource: s_sources.Source,
    path_id: irast.PathId,
    ptr_name: sn.QualName,
    exprtype: s_types.ExprType = s_types.ExprType.Select,
    is_linkprop: bool,

    ctx: context.ContextLevel,
) -> Tuple[irast.Set, context.ViewRPtr]:

    with ctx.newscope(fenced=True) as shape_expr_ctx:
        # Put current pointer class in context, so
        # that references to link properties in sub-SELECT
        # can be resolved.  This is necessary for proper
        # evaluation of link properties on computable links,
        # most importantly, in INSERT/UPDATE context.
        shape_expr_ctx.view_rptr = context.ViewRPtr(
            ptrsource if is_linkprop else view_scls,
            ptrcls=ptrcls,
            ptrcls_name=ptr_name,
            ptrcls_is_linkprop=is_linkprop,
            exprtype=exprtype,
        )

        shape_expr_ctx.defining_view = view_scls
        shape_expr_ctx.path_scope.unnest_fence = True
        source_set = setgen.fixup_computable_source_set(ir_source, ctx=ctx)
        shape_expr_ctx.partial_path_prefix = source_set

        if exprtype.is_mutation() and ptrcls is not None:
            shape_expr_ctx.expr_exposed = context.Exposure.EXPOSED
            shape_expr_ctx.empty_result_type_hint = \
                ptrcls.get_target(ctx.env.schema)

        shape_expr_ctx.view_map = ctx.view_map.new_child()
        setgen.update_view_map(
            source_set.path_id, source_set, ctx=shape_expr_ctx)

        irexpr = dispatch.compile(qlexpr, ctx=shape_expr_ctx)

    if ctx.expr_exposed:
        irexpr = eta_expand.eta_expand_ir(irexpr, ctx=ctx)

    return irexpr, shape_expr_ctx.view_rptr


def _normalize_view_ptr_expr(
        ir_source: irast.Set,
        shape_el: qlast.ShapeElement,
        view_scls: s_objtypes.ObjectType, *,
        path_id: irast.PathId,
        path_id_namespace: Optional[irast.Namespace]=None,
        exprtype: s_types.ExprType = s_types.ExprType.Select,
        from_default: bool=False,
        view_rptr: Optional[context.ViewRPtr]=None,
        pending_pointers: Collection[s_pointers.Pointer]=(),
        ctx: context.ContextLevel) -> Tuple[
            s_pointers.Pointer, Optional[irast.Set]]:
    steps = shape_el.expr.steps
    is_linkprop = False
    is_polymorphic = False
    is_mutation = exprtype.is_insert() or exprtype.is_update()
    materialized = None
    # Pointers may be qualified by the explicit source
    # class, which is equivalent to Expr[IS Type].
    plen = len(steps)
    ptrsource: s_sources.Source = view_scls
    qlexpr: Optional[qlast.Expr] = None
    target_typexpr = None
    source = []
    base_ptrcls_is_alias = False
    irexpr = None

    if plen >= 2 and isinstance(steps[-1], qlast.TypeIntersection):
        # Target type intersection: foo: Type
        target_typexpr = steps[-1].type
        plen -= 1
        steps = steps[:-1]

    if plen == 1:
        # regular shape
        lexpr = steps[0]
        assert isinstance(lexpr, qlast.Ptr)
        is_linkprop = lexpr.type == 'property'
        if is_linkprop:
            if view_rptr is None or view_rptr.ptrcls is None:
                raise errors.QueryError(
                    'invalid reference to link property '
                    'in top level shape', context=lexpr.context)
            assert isinstance(view_rptr.ptrcls, s_links.Link)
            ptrsource = view_rptr.ptrcls
    elif plen == 2 and isinstance(steps[0], qlast.TypeIntersection):
        # Source type intersection: [IS Type].foo
        source = [steps[0]]
        lexpr = steps[1]
        ptype = steps[0].type
        if not isinstance(ptype, qlast.TypeName):
            raise errors.QueryError(
                'complex type expressions are not supported here',
                context=ptype.context,
            )
        source_spec = schemactx.get_schema_type(ptype.maintype, ctx=ctx)
        if not isinstance(source_spec, s_objtypes.ObjectType):
            raise errors.QueryError(
                f'expected object type, got '
                f'{source_spec.get_verbosename(ctx.env.schema)}',
                context=ptype.context,
            )
        ptrsource = source_spec
        is_polymorphic = True
    else:  # pragma: no cover
        raise RuntimeError(
            f'unexpected path length in view shape: {len(steps)}')

    assert isinstance(lexpr, qlast.Ptr)
    ptrname = lexpr.ptr.name

    compexpr: Optional[qlast.Expr] = shape_el.compexpr
    if compexpr is None and is_mutation:
        raise errors.QueryError(
            "mutation queries must specify values with ':='",
            context=steps[-1].context,
        )

    ptrcls: Optional[s_pointers.Pointer]

    if compexpr is None:
        ptrcls = setgen.resolve_ptr(
            ptrsource, ptrname, track_ref=lexpr, ctx=ctx)
        if is_polymorphic:
            ptrcls = schemactx.derive_ptr(
                ptrcls, view_scls, ctx=ctx)

        base_ptrcls = ptrcls.get_bases(ctx.env.schema).first(ctx.env.schema)
        base_ptr_is_computable = base_ptrcls in ctx.source_map
        ptr_name = sn.QualName(
            module='__',
            name=ptrcls.get_shortname(ctx.env.schema).name,
        )

        # Schema computables that point to opaque unions will just have
        # BaseObject as their target, but in order to properly compile
        # it, we need to know the actual type here, so we recompute it.
        # XXX: This is a hack, though, and hopefully we can fix it once
        # the computable/alias rework lands.
        is_opaque_schema_computable = (
            ptrcls.is_pure_computable(ctx.env.schema)
            and (t := ptrcls.get_target(ctx.env.schema))
            and t.get_name(ctx.env.schema) == sn.QualName('std', 'BaseObject')
        )

        base_required = base_ptrcls.get_required(ctx.env.schema)
        base_cardinality = _get_base_ptr_cardinality(base_ptrcls, ctx=ctx)
        base_is_singleton = False
        if base_cardinality is not None and base_cardinality.is_known():
            base_is_singleton = base_cardinality.is_single()

        is_nontrivial = astutils.is_nontrivial_shape_element(shape_el)
        is_obj = not_none(ptrcls.get_target(ctx.env.schema)).is_object_type()

        if (
            is_obj
            or is_nontrivial
            or shape_el.elements

            or base_ptr_is_computable
            or is_polymorphic
            or target_typexpr is not None
            or (ctx.implicit_limit and not base_is_singleton)
            or is_opaque_schema_computable
        ):

            if target_typexpr is None:
                qlexpr = qlast.Path(steps=[*source, lexpr], partial=True)
            else:
                qlexpr = qlast.Path(steps=[
                    *source,
                    lexpr,
                    qlast.TypeIntersection(type=target_typexpr),
                ], partial=True)

            if shape_el.elements:
                qlexpr = qlast.Shape(expr=qlexpr, elements=shape_el.elements)

            qlexpr = astutils.ensure_qlstmt(qlexpr)
            assert isinstance(qlexpr, qlast.SelectQuery)
            qlexpr.where = shape_el.where
            qlexpr.orderby = shape_el.orderby

            if shape_el.offset or shape_el.limit:
                qlexpr = qlast.SelectQuery(result=qlexpr, implicit=True)
                qlexpr.offset = shape_el.offset
                qlexpr.limit = shape_el.limit

            if (
                (ctx.expr_exposed or ctx.stmt is ctx.toplevel_stmt)
                and not qlexpr.limit
                and ctx.implicit_limit
                and not base_is_singleton
            ):
                qlexpr = qlast.SelectQuery(result=qlexpr, implicit=True)
                qlexpr.limit = qlast.IntegerConstant(
                    value=str(ctx.implicit_limit),
                )

        if target_typexpr is not None:
            assert isinstance(target_typexpr, qlast.TypeName)
            intersector_type = schemactx.get_schema_type(
                target_typexpr.maintype, ctx=ctx)

            int_result = schemactx.apply_intersection(
                ptrcls.get_target(ctx.env.schema),  # type: ignore
                intersector_type,
                ctx=ctx,
            )

            ptr_target = int_result.stype
        else:
            _ptr_target = ptrcls.get_target(ctx.env.schema)
            assert _ptr_target
            ptr_target = _ptr_target

        ptr_required = base_required
        ptr_cardinality = base_cardinality
        if shape_el.where:
            # If the shape has a filter on it, we need to force a reinference
            # of the cardinality, to produce an error if needed.
            ptr_cardinality = None
        if ptr_cardinality is None or not ptr_cardinality.is_known():
            # We do not know the parent's pointer cardinality yet.
            ctx.env.pointer_derivation_map[base_ptrcls].append(ptrcls)
            ctx.env.pointer_specified_info[ptrcls] = (
                shape_el.cardinality, shape_el.required, shape_el.context)

        # If we generated qlexpr for the element, we process the
        # subview by just compiling the qlexpr. This is so that we can
        # figure out if it needs materialization and also so that
        # `qlexpr is not None` always implies that we did the
        # compilation.
        if qlexpr:
            qlptrcls = ptrcls
            qlptrsource = ptrsource

            irexpr, _ = _compile_qlexpr(
                ir_source, qlexpr, view_scls,
                ptrcls=qlptrcls, ptrsource=qlptrsource,
                path_id=path_id, ptr_name=ptr_name, is_linkprop=is_linkprop,
                exprtype=exprtype, ctx=ctx)
            materialized = setgen.should_materialize(
                irexpr, ptrcls=ptrcls,
                materialize_visible=True, skipped_bindings={path_id},
                ctx=ctx)
            ptr_target = inference.infer_type(irexpr, ctx.env)

    # compexpr is not None
    else:
        base_ptrcls = ptrcls = None

        if (is_mutation
                and ptrname not in ctx.special_computables_in_mutation_shape):
            # If this is a mutation, the pointer must exist.
            ptrcls = setgen.resolve_ptr(
                ptrsource, ptrname, track_ref=lexpr, ctx=ctx)
            if ptrcls.is_pure_computable(ctx.env.schema):
                ptr_vn = ptrcls.get_verbosename(ctx.env.schema,
                                                with_parent=True)
                raise errors.QueryError(
                    f'modification of computed {ptr_vn} is prohibited',
                    context=shape_el.context)

            base_ptrcls = ptrcls.get_bases(
                ctx.env.schema).first(ctx.env.schema)

            ptr_name = sn.QualName(
                module='__',
                name=ptrcls.get_shortname(ctx.env.schema).name,
            )

        else:
            ptr_name = sn.QualName(
                module='__',
                name=ptrname,
            )

            try:
                ptrcls = setgen.resolve_ptr(
                    ptrsource,
                    ptrname,
                    track_ref=False,
                    ctx=ctx,
                )

                base_ptrcls = ptrcls.get_bases(
                    ctx.env.schema).first(ctx.env.schema)
            except errors.InvalidReferenceError:
                # This is a NEW computable pointer, it's fine.
                pass

        qlexpr = astutils.ensure_qlstmt(compexpr)

        if ((ctx.expr_exposed or ctx.stmt is ctx.toplevel_stmt)
                and ctx.implicit_limit
                and isinstance(qlexpr, qlast.OffsetLimitMixin)
                and not qlexpr.limit):
            qlexpr = qlast.SelectQuery(result=qlexpr, implicit=True)
            qlexpr.limit = qlast.IntegerConstant(value=str(ctx.implicit_limit))

        irexpr, sub_view_rptr = _compile_qlexpr(
            ir_source, qlexpr, view_scls, ptrcls=ptrcls, ptrsource=ptrsource,
            path_id=path_id, ptr_name=ptr_name, is_linkprop=is_linkprop,
            exprtype=exprtype, ctx=ctx)
        materialized = setgen.should_materialize(
            irexpr, ptrcls=ptrcls,
            materialize_visible=True, skipped_bindings={path_id},
            ctx=ctx)
        ptr_target = inference.infer_type(irexpr, ctx.env)

        if (
            shape_el.operation.op is qlast.ShapeOp.APPEND
            or shape_el.operation.op is qlast.ShapeOp.SUBTRACT
        ):
            if not exprtype.is_update():
                op = (
                    '+=' if shape_el.operation.op is qlast.ShapeOp.APPEND
                    else '-='
                )
                raise errors.EdgeQLSyntaxError(
                    f"unexpected '{op}'",
                    context=shape_el.operation.context,
                )

        irexpr.context = compexpr.context

        is_inbound_alias = False
        if base_ptrcls is None:
            base_ptrcls = sub_view_rptr.base_ptrcls
            base_ptrcls_is_alias = sub_view_rptr.ptrcls_is_alias
            is_inbound_alias = (
                sub_view_rptr.rptr_dir is s_pointers.PointerDirection.Inbound)

        if ptrcls is not None:
            ctx.env.schema = ptrcls.set_field_value(
                ctx.env.schema, 'owned', True)

        ptr_cardinality = None
        ptr_required = False

        if (
            isinstance(ptr_target, s_types.Collection)
            and not ctx.env.orig_schema.get_by_id(ptr_target.id, default=None)
        ):
            # Record references to implicitly defined collection types,
            # so that the alias delta machinery can pick them up.
            ctx.env.created_schema_objects.add(ptr_target)

        anytype = ptr_target.find_any(ctx.env.schema)
        if anytype is not None:
            raise errors.QueryError(
                'expression returns value of indeterminate type',
                context=ctx.env.type_origins.get(anytype),
            )

        # Validate that the insert/update expression is
        # of the correct class.
        if is_mutation and ptrcls is not None:
            base_target = ptrcls.get_target(ctx.env.schema)
            assert base_target is not None
            if ptr_target.assignment_castable_to(
                    base_target,
                    schema=ctx.env.schema):
                # Force assignment casts if the target type is not a
                # subclass of the base type and the cast is not to an
                # object type.
                if not (
                    base_target.is_object_type()
                    or s_types.is_type_compatible(
                        base_target, ptr_target, schema=ctx.env.schema
                    )
                ):
                    qlexpr = astutils.ensure_qlstmt(qlast.TypeCast(
                        type=typegen.type_to_ql_typeref(base_target, ctx=ctx),
                        expr=compexpr,
                    ))
                    ptr_target = base_target
                    # We also need to compile the cast to IR.
                    with ctx.new() as subctx:
                        subctx.anchors = subctx.anchors.copy()
                        source_path = subctx.create_anchor(irexpr, 'a')
                        cast_qlexpr = astutils.ensure_qlstmt(qlast.TypeCast(
                            type=typegen.type_to_ql_typeref(
                                base_target, ctx=ctx),
                            expr=source_path,
                        ))

                        old_rptr = irexpr.rptr
                        irexpr.rptr = None
                        irexpr = dispatch.compile(cast_qlexpr, ctx=subctx)
                        irexpr.rptr = old_rptr

            else:
                expected = [
                    repr(str(base_target.get_displayname(ctx.env.schema)))
                ]

                ercls: Type[errors.EdgeDBError]
                if ptrcls.is_property(ctx.env.schema):
                    ercls = errors.InvalidPropertyTargetError
                else:
                    ercls = errors.InvalidLinkTargetError

                ptr_vn = ptrcls.get_verbosename(ctx.env.schema,
                                                with_parent=True)

                raise ercls(
                    f'invalid target for {ptr_vn}: '
                    f'{str(ptr_target.get_displayname(ctx.env.schema))!r} '
                    f'(expecting {" or ".join(expected)})'
                )

    # Common code for computed/not computed

    if ptrcls and ptrcls in pending_pointers:
        schema = ctx.env.schema
        vnp = ptrcls.get_verbosename(schema, with_parent=True)

        raise errors.QueryError(
            f'duplicate definition of {vnp}',
            context=shape_el.context)

    if qlexpr is not None or ptrcls is None:
        src_scls: s_sources.Source

        if is_linkprop:
            # Proper checking was done when is_linkprop is defined.
            assert view_rptr is not None
            assert isinstance(view_rptr.ptrcls, s_links.Link)
            src_scls = view_rptr.ptrcls
        else:
            src_scls = view_scls

        if ptr_target.is_object_type():
            base = ctx.env.get_track_schema_object(
                sn.QualName('std', 'link'), expr=None)
        else:
            base = ctx.env.get_track_schema_object(
                sn.QualName('std', 'property'), expr=None)

        if base_ptrcls is not None:
            derive_from = base_ptrcls
        else:
            derive_from = base

        derived_name = schemactx.derive_view_name(
            base_ptrcls,
            derived_name_base=ptr_name,
            derived_name_quals=[str(src_scls.get_name(ctx.env.schema))],
            ctx=ctx,
        )

        existing = ctx.env.schema.get(
            derived_name, default=None, type=s_pointers.Pointer)
        if existing is not None:
            existing_target = existing.get_target(ctx.env.schema)
            assert existing_target is not None
            if ctx.recompiling_schema_alias:
                ptr_cardinality = existing.get_cardinality(ctx.env.schema)
                ptr_required = existing.get_required(ctx.env.schema)
            if ptr_target == existing_target:
                ptrcls = existing
            elif ptr_target.implicitly_castable_to(
                    existing_target, ctx.env.schema):
                ctx.env.schema = existing.set_target(
                    ctx.env.schema, ptr_target)
                ptrcls = existing
            else:
                vnp = existing.get_verbosename(
                    ctx.env.schema, with_parent=True)

                t1_vn = existing_target.get_verbosename(ctx.env.schema)
                t2_vn = ptr_target.get_verbosename(ctx.env.schema)

                if compexpr is not None:
                    source_context = compexpr.context
                else:
                    source_context = shape_el.expr.steps[-1].context
                raise errors.SchemaError(
                    f'cannot redefine {vnp} as {t2_vn}',
                    details=f'{vnp} is defined as {t1_vn}',
                    context=source_context,
                )
        else:
            ptrcls = schemactx.derive_ptr(
                derive_from, src_scls, ptr_target,
                derive_backlink=is_inbound_alias,
                derived_name=derived_name,
                ctx=ctx)

    elif ptrcls.get_target(ctx.env.schema) != ptr_target:
        ctx.env.schema = ptrcls.set_target(ctx.env.schema, ptr_target)

    assert ptrcls is not None

    if materialized and is_mutation and any(
        x.is_binding == irast.BindingKind.With
        and x.expr
        and inference.infer_volatility(
            x.expr, ctx.env, for_materialization=True).is_volatile()

        for reason in materialized
        if isinstance(reason, irast.MaterializeVisible)
        for _, x in reason.sets
    ):
        raise errors.QueryError(
            f'cannot refer to volatile WITH bindings from DML',
            context=compexpr and compexpr.context,
        )

    if materialized and not is_mutation and ctx.qlstmt:
        assert ptrcls not in ctx.env.materialized_sets
        ctx.env.materialized_sets[ptrcls] = ctx.qlstmt, materialized

        if irexpr:
            setgen.maybe_materialize(ptrcls, irexpr, ctx=ctx)

    if qlexpr is None:
        # This is not a computable, just a pointer
        # to a nested shape.  Have it reuse the original
        # pointer name so that in `Foo.ptr.name` and
        # `Foo { ptr: {name}}` are the same path.
        path_id_name = base_ptrcls.get_name(ctx.env.schema)
        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema, 'path_id_name', path_id_name
        )

    if qlexpr is not None:
        ctx.source_map[ptrcls] = irast.ComputableInfo(
            qlexpr=qlexpr,
            irexpr=irexpr,
            context=ctx,
            path_id=path_id,
            path_id_ns=path_id_namespace,
            shape_op=shape_el.operation.op,
            should_materialize=materialized or [],
        )

    if compexpr is not None or is_polymorphic or materialized:
        if (old_ptrref := ctx.env.ptr_ref_cache.get(ptrcls)):
            old_ptrref.is_computable = True

        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema,
            'computable',
            True,
        )

        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema,
            'owned',
            True,
        )

    if ptr_cardinality is not None:
        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema, 'cardinality', ptr_cardinality)
        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema, 'required', ptr_required)
    else:
        if qlexpr is None and ptrcls is not base_ptrcls:
            ctx.env.pointer_derivation_map[base_ptrcls].append(ptrcls)

        base_cardinality = None
        base_required = None
        if base_ptrcls is not None and not base_ptrcls_is_alias:
            base_cardinality = _get_base_ptr_cardinality(base_ptrcls, ctx=ctx)
            base_required = base_ptrcls.get_required(ctx.env.schema)

        if base_cardinality is None or not base_cardinality.is_known():
            # If the base cardinality is not known the we can't make
            # any checks here and will rely on validation in the
            # cardinality inferer.
            specified_cardinality = shape_el.cardinality
            specified_required = shape_el.required
        else:
            specified_cardinality = base_cardinality

            # Inferred optionality overrides that of the base pointer
            # if base pointer is not `required`, hence the is True check.
            if shape_el.required is not None:
                specified_required = shape_el.required
            elif base_required is True:
                specified_required = base_required
            else:
                specified_required = None

            if (
                shape_el.cardinality is not None
                and base_ptrcls is not None
                and shape_el.cardinality != base_cardinality
            ):
                base_src = base_ptrcls.get_source(ctx.env.schema)
                assert base_src is not None
                base_src_name = base_src.get_verbosename(ctx.env.schema)
                raise errors.SchemaError(
                    f'cannot redefine the cardinality of '
                    f'{ptrcls.get_verbosename(ctx.env.schema)}: '
                    f'it is defined as {base_cardinality.as_ptr_qual()!r} '
                    f'in the base {base_src_name}',
                    context=compexpr and compexpr.context,
                )

            if (
                shape_el.required is False
                and base_ptrcls is not None
                and base_required
            ):
                base_src = base_ptrcls.get_source(ctx.env.schema)
                assert base_src is not None
                base_src_name = base_src.get_verbosename(ctx.env.schema)
                raise errors.SchemaError(
                    f'cannot redefine '
                    f'{ptrcls.get_verbosename(ctx.env.schema)} '
                    f'as optional: it is defined as required '
                    f'in the base {base_src_name}',
                    context=compexpr and compexpr.context,
                )

        ctx.env.pointer_specified_info[ptrcls] = (
            specified_cardinality, specified_required, shape_el.context)

        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema, 'cardinality', qltypes.SchemaCardinality.Unknown)

    if (
        ptrcls.is_protected_pointer(ctx.env.schema)
        and (compexpr is not None or is_polymorphic)
        and not from_default
        and not ctx.env.options.allow_writing_protected_pointers
    ):
        ptrcls_sn = ptrcls.get_shortname(ctx.env.schema)
        if is_polymorphic:
            msg = (f'cannot access {ptrcls_sn.name} on a polymorphic '
                   f'shape element')
        else:
            msg = f'cannot assign to {ptrcls_sn.name}'
        raise errors.QueryError(msg, context=shape_el.context)

    if exprtype.is_update() and ptrcls.get_readonly(ctx.env.schema):
        raise errors.QueryError(
            f'cannot update {ptrcls.get_verbosename(ctx.env.schema)}: '
            f'it is declared as read-only',
            context=compexpr and compexpr.context,
        )

    return ptrcls, irexpr


def derive_ptrcls(
        view_rptr: context.ViewRPtr, *,
        target_scls: s_types.Type,
        transparent: bool=False,
        ctx: context.ContextLevel) -> s_pointers.Pointer:

    if view_rptr.ptrcls is None:
        if view_rptr.base_ptrcls is None:
            transparent = False

            if target_scls.is_object_type():
                base = ctx.env.get_track_schema_object(
                    sn.QualName('std', 'link'), expr=None)
                view_rptr.base_ptrcls = cast(s_links.Link, base)
            else:
                base = ctx.env.get_track_schema_object(
                    sn.QualName('std', 'property'), expr=None)
                view_rptr.base_ptrcls = cast(s_props.Property, base)

        derived_name = schemactx.derive_view_name(
            view_rptr.base_ptrcls,
            derived_name_base=view_rptr.ptrcls_name,
            derived_name_quals=(
                str(view_rptr.source.get_name(ctx.env.schema)),
            ),
            ctx=ctx)

        attrs = {}
        if transparent and not view_rptr.ptrcls_is_alias:
            attrs['path_id_name'] = view_rptr.base_ptrcls.get_name(
                ctx.env.schema)

        is_inbound_alias = (
            view_rptr.rptr_dir is s_pointers.PointerDirection.Inbound)
        view_rptr.ptrcls = schemactx.derive_ptr(
            view_rptr.base_ptrcls, view_rptr.source, target_scls,
            derived_name=derived_name,
            derive_backlink=is_inbound_alias,
            attrs=attrs,
            ctx=ctx
        )

    else:
        attrs = {}
        if transparent and not view_rptr.ptrcls_is_alias:
            attrs['path_id_name'] = view_rptr.ptrcls.get_name(ctx.env.schema)

        view_rptr.ptrcls = schemactx.derive_ptr(
            view_rptr.ptrcls, view_rptr.source, target_scls,
            derived_name_quals=(
                str(view_rptr.source.get_name(ctx.env.schema)),
            ),
            attrs=attrs,
            ctx=ctx
        )

    return view_rptr.ptrcls


def _link_has_shape(
        ptrcls: s_pointers.PointerLike, *,
        ctx: context.ContextLevel) -> bool:
    if not isinstance(ptrcls, s_links.Link):
        return False

    ptr_shape = {p for p, _ in ctx.env.view_shapes[ptrcls]}
    for p in ptrcls.get_pointers(ctx.env.schema).objects(ctx.env.schema):
        if p.is_special_pointer(ctx.env.schema) or p not in ptr_shape:
            continue
        else:
            return True

    return False


def _get_base_ptr_cardinality(
    ptrcls: s_pointers.Pointer,
    *,
    ctx: context.ContextLevel,
) -> Optional[qltypes.SchemaCardinality]:
    ptr_name = ptrcls.get_name(ctx.env.schema)
    if ptr_name in {
        sn.QualName('std', 'link'),
        sn.QualName('std', 'property')
    }:
        return None
    else:
        return ptrcls.get_cardinality(ctx.env.schema)


def has_implicit_tid(
        stype: s_types.Type, *,
        is_mutation: bool,
        ctx: context.ContextLevel) -> bool:

    return (
        stype.is_object_type()
        and not is_mutation
        and ctx.implicit_tid_in_shapes
    )


def has_implicit_tname(
        stype: s_types.Type, *,
        is_mutation: bool,
        ctx: context.ContextLevel) -> bool:

    return (
        stype.is_object_type()
        and not is_mutation
        and ctx.implicit_tname_in_shapes
    )


def has_implicit_type_computables(
        stype: s_types.Type, *,
        is_mutation: bool,
        ctx: context.ContextLevel) -> bool:

    return (
        has_implicit_tid(stype, is_mutation=is_mutation, ctx=ctx)
        or has_implicit_tname(stype, is_mutation=is_mutation, ctx=ctx)
    )


def _inline_type_computable(
    ir_set: irast.Set,
    stype: s_objtypes.ObjectType,
    compname: str,
    propname: str,
    *,
    shape_ptrs: List[ShapePtr],
    ctx: context.ContextLevel,
) -> None:
    assert isinstance(stype, s_objtypes.ObjectType)
    # Injecting into non-view objects /almost/ works, but it fails if the
    # object is in the std library, and is dodgy always.
    # Prevent it in general to find bugs faster.
    assert stype.is_view(ctx.env.schema)

    ptr: Optional[s_pointers.Pointer]
    try:
        ptr = setgen.resolve_ptr(stype, compname, track_ref=None, ctx=ctx)
        # The pointer might exist on the base type. That doesn't count,
        # and we need to re-inject it.
        if ptr not in ctx.source_map:
            ptr = None
    except errors.InvalidReferenceError:
        ptr = None

    ptr_set = None
    if ptr is None:
        ql = qlast.ShapeElement(
            required=True,
            expr=qlast.Path(
                steps=[qlast.Ptr(
                    ptr=qlast.ObjectRef(name=compname),
                    direction=s_pointers.PointerDirection.Outbound,
                )],
            ),
            compexpr=qlast.Path(
                steps=[
                    qlast.Source(),
                    qlast.Ptr(
                        ptr=qlast.ObjectRef(name='__type__'),
                        direction=s_pointers.PointerDirection.Outbound,
                    ),
                    qlast.Ptr(
                        ptr=qlast.ObjectRef(name=propname),
                        direction=s_pointers.PointerDirection.Outbound,
                    )
                ]
            )
        )
        with ctx.new() as scopectx:
            scopectx.anchors = scopectx.anchors.copy()
            scopectx.anchors[qlast.Source().name] = ir_set
            ptr, ptr_set = _normalize_view_ptr_expr(
                ir_set, ql, stype, path_id=ir_set.path_id, ctx=scopectx)

    view_shape = ctx.env.view_shapes[stype]
    view_shape_ptrs = {p for p, _ in view_shape}
    if ptr not in view_shape_ptrs:
        view_shape.insert(0, (ptr, qlast.ShapeOp.ASSIGN))
        shape_ptrs.insert(0, (ir_set, ptr, qlast.ShapeOp.ASSIGN, ptr_set))


def _get_shape_configuration_inner(
    ir_set: irast.Set,
    shape_ptrs: List[ShapePtr],
    stype: s_types.Type,
    *,
    parent_view_type: Optional[s_types.ExprType]=None,
    ctx: context.ContextLevel
) -> None:
    is_objtype = ir_set.path_id.is_objtype_path()
    all_materialize = all(
        op == qlast.ShapeOp.MATERIALIZE for _, _, op, _ in shape_ptrs)

    if is_objtype:
        assert isinstance(stype, s_objtypes.ObjectType)

        view_type = stype.get_expr_type(ctx.env.schema)
        is_mutation = view_type in (s_types.ExprType.Insert,
                                    s_types.ExprType.Update)
        is_parent_update = parent_view_type is s_types.ExprType.Update

        implicit_id = (
            # shape is not specified at all
            not shape_ptrs
            # implicit ids are always wanted
            or (ctx.implicit_id_in_shapes and not is_mutation)
            # we are inside an UPDATE shape and this is
            # an explicit expression (link target update)
            or (is_parent_update and ir_set.expr is not None)
            or all_materialize
        )
        # We actually *always* inject an implicit id, but it's just
        # there in case materialization needs it, in many cases.
        implicit_op = qlast.ShapeOp.ASSIGN
        if not implicit_id:
            implicit_op = qlast.ShapeOp.MATERIALIZE

        # We want the id in this shape and it's not already there,
        # so insert it in the first position.
        pointers = stype.get_pointers(ctx.env.schema).objects(
            ctx.env.schema)
        view_shape = ctx.env.view_shapes[stype]
        view_shape_ptrs = {p for p, _ in view_shape}
        for ptr in pointers:
            if ptr.is_id_pointer(ctx.env.schema):
                if ptr not in view_shape_ptrs:
                    shape_metadata = ctx.env.view_shapes_metadata[stype]
                    view_shape.insert(0, (ptr, implicit_op))
                    shape_metadata.has_implicit_id = True
                    shape_ptrs.insert(0, (ir_set, ptr, implicit_op, None))
                break

    is_mutation = parent_view_type in {
        s_types.ExprType.Insert,
        s_types.ExprType.Update
    }

    if (
        stype is not None
        and has_implicit_tid(stype, is_mutation=is_mutation, ctx=ctx)
    ):
        assert isinstance(stype, s_objtypes.ObjectType)
        _inline_type_computable(
            ir_set, stype, '__tid__', 'id', ctx=ctx, shape_ptrs=shape_ptrs)

    if (
        stype is not None
        and has_implicit_tname(stype, is_mutation=is_mutation, ctx=ctx)
    ):
        assert isinstance(stype, s_objtypes.ObjectType)
        _inline_type_computable(
            ir_set, stype, '__tname__', 'name', ctx=ctx, shape_ptrs=shape_ptrs)


def _get_early_shape_configuration(
    ir_set: irast.Set,
    in_shape_ptrs: List[ShapePtr],
    *,
    rptrcls: Optional[s_pointers.Pointer],
    parent_view_type: Optional[s_types.ExprType]=None,
    ctx: context.ContextLevel
) -> List[ShapePtr]:
    """Return a list of (source_set, ptrcls) pairs as a shape for a given set.
    """

    stype = setgen.get_set_type(ir_set, ctx=ctx)

    # HACK: For some reason, all the link properties need to go last or
    # things choke in native output mode?
    shape_ptrs = sorted(
        in_shape_ptrs,
        key=lambda arg: arg[1].is_link_property(ctx.env.schema),
    )

    _get_shape_configuration_inner(
        ir_set, shape_ptrs, stype, parent_view_type=parent_view_type, ctx=ctx)

    return shape_ptrs


def _get_late_shape_configuration(
    ir_set: irast.Set,
    *,
    rptr: Optional[irast.Pointer]=None,
    parent_view_type: Optional[s_types.ExprType]=None,
    ctx: context.ContextLevel
) -> List[ShapePtr]:

    """Return a list of (source_set, ptrcls) pairs as a shape for a given set.
    """

    stype = setgen.get_set_type(ir_set, ctx=ctx)

    sources: List[Union[s_types.Type, s_pointers.PointerLike]] = []
    link_view = False
    is_objtype = ir_set.path_id.is_objtype_path()

    if rptr is None:
        rptr = ir_set.rptr

    rptrcls: Optional[s_pointers.PointerLike]
    if rptr is not None:
        rptrcls = typegen.ptrcls_from_ptrref(rptr.ptrref, ctx=ctx)
    else:
        rptrcls = None

    link_view = (
        rptrcls is not None and
        not rptrcls.is_link_property(ctx.env.schema) and
        _link_has_shape(rptrcls, ctx=ctx)
    )

    if is_objtype or not link_view:
        sources.append(stype)

    if link_view:
        assert rptrcls is not None
        sources.append(rptrcls)

    shape_ptrs: List[ShapePtr] = []

    for source in sources:
        for ptr, shape_op in ctx.env.view_shapes[source]:
            shape_ptrs.append((ir_set, ptr, shape_op, None))

    _get_shape_configuration_inner(
        ir_set, shape_ptrs, stype, parent_view_type=parent_view_type, ctx=ctx)

    return shape_ptrs


@functools.singledispatch
def late_compile_view_shapes(
        expr: irast.Base, *,
        rptr: Optional[irast.Pointer]=None,
        parent_view_type: Optional[s_types.ExprType]=None,
        ctx: context.ContextLevel) -> None:
    """Do a late insertion of any unprocessed shapes.

    We mainly compile shapes in process_view, but late_compile_view_shapes
    is responsible for compiling implicit exposed shapes (containing
    only id) and in cases like accessing a semi-joined shape.

    """
    pass


@late_compile_view_shapes.register(irast.Set)
def _late_compile_view_shapes_in_set(
        ir_set: irast.Set, *,
        rptr: Optional[irast.Pointer]=None,
        parent_view_type: Optional[s_types.ExprType]=None,
        ctx: context.ContextLevel) -> None:

    shape_ptrs = _get_late_shape_configuration(
        ir_set, rptr=rptr, parent_view_type=parent_view_type, ctx=ctx)

    # We want to push down the shape to better correspond with where it
    # appears in the query (rather than lifting it up to the first
    # place the view_type appears---this is a little hacky, because
    # letting it be lifted up is the natural thing with our view type-driven
    # shape compilation).
    #
    # This is to avoid losing subquery distinctions (in cases
    # like test_edgeql_scope_tuple_15), and generally seems more natural.
    if (isinstance(ir_set.expr, (irast.SelectStmt, irast.GroupStmt))
            and not (ir_set.rptr and not ir_set.rptr.is_definition)
            and (setgen.get_set_type(ir_set, ctx=ctx) ==
                 setgen.get_set_type(ir_set.expr.result, ctx=ctx))):
        child = ir_set.expr.result
        set_scope = pathctx.get_set_scope(ir_set, ctx=ctx)

        if shape_ptrs:
            pathctx.register_set_in_scope(ir_set, ctx=ctx)
        with ctx.new() as scopectx:
            if set_scope is not None:
                scopectx.path_scope = set_scope
            late_compile_view_shapes(
                child,
                rptr=rptr or ir_set.rptr,
                parent_view_type=parent_view_type,
                ctx=scopectx)

        ir_set.shape_source = child if child.shape else child.shape_source
        return

    if shape_ptrs:
        pathctx.register_set_in_scope(ir_set, ctx=ctx)
        stype = setgen.get_set_type(ir_set, ctx=ctx)

        # If the shape has already been populated (because the set is
        # referenced multiple times), then we've got nothing to do.
        if ir_set.shape:
            # We want to make sure anything inside of the shape gets
            # processed, though, so we do need to look through the
            # internals.
            for element, _ in ir_set.shape:
                element_scope = pathctx.get_set_scope(element, ctx=ctx)
                with ctx.new() as scopectx:
                    if element_scope:
                        scopectx.path_scope = element_scope
                    late_compile_view_shapes(
                        element,
                        parent_view_type=stype.get_expr_type(ctx.env.schema),
                        ctx=scopectx)

            return

        shape = []
        for path_tip, ptr, shape_op, _ in shape_ptrs:
            srcctx = None
            if ptr in ctx.env.pointer_specified_info:
                _, _, srcctx = ctx.env.pointer_specified_info[ptr]

            element = setgen.extend_path(
                path_tip,
                ptr,
                same_computable_scope=True,
                srcctx=srcctx,
                ctx=ctx,
            )

            # HACK?: when we see linkprops being used on an intersection,
            # attach the flattened source path to make linkprops on
            # computed backlinks work
            if (
                isinstance(
                    path_tip.path_id.rptr(), irast.TypeIntersectionPointerRef)
                and ptr.is_link_property(ctx.env.schema)
            ):
                ctx.path_scope.attach_path(
                    path_tip.path_id, flatten_intersection=True, context=None)

            element_scope = pathctx.get_set_scope(element, ctx=ctx)

            if element_scope is None:
                element_scope = ctx.path_scope.attach_fence()
                pathctx.assign_set_scope(element, element_scope, ctx=ctx)

            if element_scope.namespaces:
                element.path_id = element.path_id.merge_namespace(
                    element_scope.namespaces)

            with ctx.new() as scopectx:
                scopectx.path_scope = element_scope
                late_compile_view_shapes(
                    element,
                    parent_view_type=stype.get_expr_type(ctx.env.schema),
                    ctx=scopectx)

            shape.append((element, shape_op))

        ir_set.shape = tuple(shape)

    elif ir_set.expr is not None:
        set_scope = pathctx.get_set_scope(ir_set, ctx=ctx)
        if set_scope is not None:
            with ctx.new() as scopectx:
                scopectx.path_scope = set_scope
                late_compile_view_shapes(ir_set.expr, ctx=scopectx)
        else:
            late_compile_view_shapes(ir_set.expr, ctx=ctx)

    elif isinstance(ir_set.rptr, irast.TupleIndirectionPointer):
        late_compile_view_shapes(ir_set.rptr.source, ctx=ctx)


@late_compile_view_shapes.register(irast.SelectStmt)
def _late_compile_view_shapes_in_select(
        stmt: irast.SelectStmt, *,
        rptr: Optional[irast.Pointer]=None,
        parent_view_type: Optional[s_types.ExprType]=None,
        ctx: context.ContextLevel) -> None:
    late_compile_view_shapes(
        stmt.result, rptr=rptr, parent_view_type=parent_view_type, ctx=ctx)


@late_compile_view_shapes.register(irast.Call)
def _late_compile_view_shapes_in_call(
        expr: irast.Call, *,
        rptr: Optional[irast.Pointer]=None,
        parent_view_type: Optional[s_types.ExprType]=None,
        ctx: context.ContextLevel) -> None:

    if expr.func_polymorphic:
        for call_arg in expr.args:
            arg = call_arg.expr
            arg_scope = pathctx.get_set_scope(arg, ctx=ctx)
            if arg_scope is not None:
                with ctx.new() as scopectx:
                    scopectx.path_scope = arg_scope
                    late_compile_view_shapes(arg, ctx=scopectx)
            else:
                late_compile_view_shapes(arg, ctx=ctx)


@late_compile_view_shapes.register(irast.Tuple)
def _late_compile_view_shapes_in_tuple(
        expr: irast.Tuple, *,
        rptr: Optional[irast.Pointer]=None,
        parent_view_type: Optional[s_types.ExprType]=None,
        ctx: context.ContextLevel) -> None:
    for element in expr.elements:
        late_compile_view_shapes(element.val, ctx=ctx)


@late_compile_view_shapes.register(irast.Array)
def _late_compile_view_shapes_in_array(
        expr: irast.Array, *,
        rptr: Optional[irast.Pointer]=None,
        parent_view_type: Optional[s_types.ExprType]=None,
        ctx: context.ContextLevel) -> None:
    for element in expr.elements:
        late_compile_view_shapes(element, ctx=ctx)
