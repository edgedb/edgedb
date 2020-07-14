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

import functools
from typing import *

from edb import errors

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from edb.schema import links as s_links
from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import types as s_types

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import parser as qlparser

from . import astutils
from . import context
from . import dispatch
from . import inference
from . import pathctx
from . import schemactx
from . import setgen
from . import stmtctx
from . import typegen

if TYPE_CHECKING:
    from edb.schema import lproperties as s_props
    from edb.schema import sources as s_sources


def process_view(
    *,
    stype: s_objtypes.ObjectType,
    path_id: irast.PathId,
    elements: List[qlast.ShapeElement],
    view_rptr: Optional[context.ViewRPtr] = None,
    view_name: Optional[sn.SchemaName] = None,
    is_insert: bool = False,
    is_update: bool = False,
    is_delete: bool = False,
    ctx: context.ContextLevel,
) -> s_objtypes.ObjectType:

    cache_key = (stype, tuple(elements))
    view_scls = ctx.shape_type_cache.get(cache_key)
    if view_scls is not None:
        return view_scls

    with ctx.newscope(fenced=True, temporary=True) as scopectx:
        scopectx.path_scope.attach_path(path_id)
        view_path_id_ns = None
        if ctx.expr_exposed or is_insert or is_update:
            view_path_id_ns = irast.WeakNamespace(ctx.aliases.get('ns'))
            scopectx.path_id_namespace |= {view_path_id_ns}
            scopectx.path_scope.add_namespaces({view_path_id_ns})

        view_scls = _process_view(
            stype=stype,
            path_id=path_id,
            elements=elements,
            view_rptr=view_rptr,
            view_name=view_name,
            is_insert=is_insert,
            is_update=is_update,
            is_delete=is_delete,
            path_id_namespace=view_path_id_ns,
            ctx=scopectx,
        )

    ctx.shape_type_cache[cache_key] = view_scls

    return view_scls


def _process_view(
    *,
    stype: s_objtypes.ObjectType,
    path_id: irast.PathId,
    path_id_namespace: Optional[irast.WeakNamespace] = None,
    elements: List[qlast.ShapeElement],
    view_rptr: Optional[context.ViewRPtr] = None,
    view_name: Optional[sn.SchemaName] = None,
    is_insert: bool = False,
    is_update: bool = False,
    is_delete: bool = False,
    ctx: context.ContextLevel,
) -> s_objtypes.ObjectType:

    if (view_name is None and ctx.env.options.schema_view_mode
            and view_rptr is not None):
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

        name = f'{source_name}__{ptr_name}'
        view_name = sn.Name(
            module=ctx.derived_target_module or '__derived__',
            name=name,
        )

    view_scls = schemactx.derive_view(
        stype,
        is_insert=is_insert,
        is_update=is_update,
        is_delete=is_delete,
        derived_name=view_name,
        ctx=ctx,
    )
    assert isinstance(view_scls, s_objtypes.ObjectType)
    is_mutation = is_insert or is_update
    is_defining_shape = ctx.expr_exposed or is_mutation

    if view_rptr is not None and view_rptr.ptrcls is None:
        derive_ptrcls(
            view_rptr, target_scls=view_scls,
            transparent=True, ctx=ctx)

    pointers = []

    for shape_el in elements:
        with ctx.newscope(fenced=True) as scopectx:
            pointer = _normalize_view_ptr_expr(
                shape_el, view_scls, path_id=path_id,
                path_id_namespace=path_id_namespace,
                is_insert=is_insert, is_update=is_update,
                view_rptr=view_rptr, ctx=scopectx)

            if pointer in pointers:
                schema = ctx.env.schema
                vnp = pointer.get_verbosename(schema, with_parent=True)

                raise errors.QueryError(
                    f'duplicate definition of {vnp}',
                    context=shape_el.context)

            pointers.append(pointer)

    if is_insert:
        explicit_ptrs = {
            ptrcls.get_shortname(ctx.env.schema).name
            for ptrcls in pointers
        }
        scls_pointers = stype.get_pointers(ctx.env.schema)
        for pn, ptrcls in scls_pointers.items(ctx.env.schema):
            if (pn in explicit_ptrs or
                    ptrcls.is_pure_computable(ctx.env.schema)):
                continue

            default_expr = ptrcls.get_default(ctx.env.schema)
            if not default_expr:
                if ptrcls.get_required(ctx.env.schema):
                    if ptrcls.is_property(ctx.env.schema):
                        # If the target is a sequence, there's no need
                        # for an explicit value.
                        ptrcls_target = ptrcls.get_target(ctx.env.schema)
                        assert ptrcls_target is not None
                        if ptrcls_target.issubclass(
                                ctx.env.schema,
                                ctx.env.schema.get('std::sequence')):
                            continue

                        what = 'property'
                    else:
                        what = 'link'
                    raise errors.MissingRequiredError(
                        f'missing value for required {what} '
                        f'{stype.get_displayname(ctx.env.schema)}.'
                        f'{ptrcls.get_displayname(ctx.env.schema)}')
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

            with ctx.newscope(fenced=True) as scopectx:
                pointers.append(
                    _normalize_view_ptr_expr(
                        default_ql,
                        view_scls,
                        path_id=path_id,
                        path_id_namespace=path_id_namespace,
                        is_insert=is_insert,
                        is_update=is_update,
                        from_default=True,
                        view_rptr=view_rptr,
                        ctx=scopectx,
                    ),
                )

    elif (
        stype.get_name(ctx.env.schema).module == 'schema'
        and ctx.env.options.introspection_schema_rewrites
    ):
        explicit_ptrs = {
            ptrcls.get_shortname(ctx.env.schema).name
            for ptrcls in pointers
        }
        scls_pointers = stype.get_pointers(ctx.env.schema)
        for pn, ptrcls in scls_pointers.items(ctx.env.schema):
            if (
                pn in explicit_ptrs
                or ptrcls.is_pure_computable(ctx.env.schema)
            ):
                continue

            schema_deflt = ptrcls.get_schema_reflection_default(ctx.env.schema)
            if schema_deflt is None:
                continue

            with ctx.newscope(fenced=True) as scopectx:
                implicit_ql = qlast.ShapeElement(
                    expr=qlast.Path(
                        steps=[
                            qlast.Ptr(
                                ptr=qlast.ObjectRef(
                                    name=pn,
                                ),
                            ),
                        ],
                    ),
                    compexpr=qlast.BinOp(
                        left=qlast.Path(
                            partial=True,
                            steps=[
                                qlast.Ptr(
                                    ptr=qlast.ObjectRef(name=pn),
                                    direction=(
                                        s_pointers.PointerDirection.Outbound
                                    ),
                                )
                            ],
                        ),
                        right=qlparser.parse_fragment(schema_deflt),
                        op='??',
                    ),
                )

                # Note: we only need to record the schema default
                # as a computable, but not include it in the type
                # shape, so we ignore the return value.
                _normalize_view_ptr_expr(
                    implicit_ql,
                    view_scls,
                    path_id=path_id,
                    path_id_namespace=path_id_namespace,
                    is_insert=is_insert,
                    is_update=is_update,
                    view_rptr=view_rptr,
                    ctx=scopectx,
                )

    for ptrcls in pointers:
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

            ctx.env.view_shapes[source].append((ptrcls, shape_op))

    if (view_rptr is not None and view_rptr.ptrcls is not None and
            view_scls is not stype):
        ctx.env.schema = view_scls.set_field_value(
            ctx.env.schema, 'rptr', view_rptr.ptrcls)

    return view_scls


def _normalize_view_ptr_expr(
        shape_el: qlast.ShapeElement,
        view_scls: s_objtypes.ObjectType, *,
        path_id: irast.PathId,
        path_id_namespace: Optional[irast.WeakNamespace]=None,
        is_insert: bool=False,
        is_update: bool=False,
        from_default: bool=False,
        view_rptr: Optional[context.ViewRPtr]=None,
        ctx: context.ContextLevel) -> s_pointers.Pointer:
    steps = shape_el.expr.steps
    is_linkprop = False
    is_polymorphic = False
    is_mutation = is_insert or is_update
    # Pointers may be qualified by the explicit source
    # class, which is equivalent to Expr[IS Type].
    plen = len(steps)
    ptrsource: s_sources.Source = view_scls
    qlexpr = None
    target_typexpr = None
    source: qlast.Base
    base_ptrcls_is_alias = False

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
        source = qlast.Source()
    elif plen == 2 and isinstance(steps[0], qlast.TypeIntersection):
        # Source type intersection: [IS Type].foo
        source = qlast.Path(steps=[
            qlast.Source(),
            steps[0],
        ])
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

    compexpr = shape_el.compexpr
    if compexpr is None and is_insert and shape_el.elements:
        # Short shape form in INSERT, e.g
        #     INSERT Foo { bar: Spam { name := 'name' }}
        # is prohibited.
        raise errors.EdgeQLSyntaxError(
            "unexpected ':'", context=steps[-1].context)

    if compexpr is None:
        ptrcls = setgen.resolve_ptr(ptrsource, ptrname, ctx=ctx)
        if is_polymorphic:
            ptrcls = schemactx.derive_ptr(
                ptrcls, view_scls,
                is_insert=is_insert,
                is_update=is_update,
                ctx=ctx)

        base_ptrcls = ptrcls.get_bases(ctx.env.schema).first(ctx.env.schema)
        base_ptr_is_computable = base_ptrcls in ctx.source_map
        ptr_name = sn.Name(
            module='__',
            name=ptrcls.get_shortname(ctx.env.schema).name,
        )

        base_cardinality = base_ptrcls.get_cardinality(ctx.env.schema)
        base_is_singleton = base_cardinality is qltypes.SchemaCardinality.ONE

        if (
            shape_el.where
            or shape_el.orderby
            or shape_el.offset
            or shape_el.limit
            or base_ptr_is_computable
            or is_polymorphic
            or target_typexpr is not None
            or (ctx.implicit_limit and not base_is_singleton)
        ):

            if target_typexpr is None:
                qlexpr = qlast.Path(steps=[source, lexpr])
            else:
                qlexpr = qlast.Path(steps=[
                    source,
                    lexpr,
                    qlast.TypeIntersection(type=target_typexpr),
                ])

            qlexpr = astutils.ensure_qlstmt(qlexpr)
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
                qlexpr.limit = qlast.IntegerConstant(
                    value=str(ctx.implicit_limit),
                )

        if target_typexpr is not None:
            intersector_type = schemactx.get_schema_type(
                target_typexpr.maintype, ctx=ctx)

            int_result = schemactx.apply_intersection(
                ptrcls.get_target(ctx.env.schema),
                intersector_type,
                ctx=ctx,
            )

            ptr_target = int_result.stype
        else:
            ptr_target = ptrcls.get_target(ctx.env.schema)

        if base_ptrcls in ctx.pending_cardinality:
            # We do not know the parent's pointer cardinality yet.
            ptr_cardinality = None
            ctx.pointer_derivation_map[base_ptrcls].append(ptrcls)
            stmtctx.pend_pointer_cardinality_inference(
                ptrcls=ptrcls,
                specified_required=shape_el.required,
                specified_card=shape_el.cardinality,
                source_ctx=shape_el.context,
                ctx=ctx)
        else:
            ptr_cardinality = base_ptrcls.get_cardinality(ctx.env.schema)

        implicit_tid = has_implicit_tid(
            ptr_target,
            is_mutation=is_mutation,
            ctx=ctx,
        )

        if shape_el.elements or implicit_tid:
            sub_view_rptr = context.ViewRPtr(
                ptrsource if is_linkprop else view_scls,
                ptrcls=ptrcls,
                is_insert=is_insert,
                is_update=is_update)

            sub_path_id = pathctx.extend_path_id(
                path_id,
                ptrcls=base_ptrcls,
                ns=ctx.path_id_namespace,
                ctx=ctx)

            ctx.path_scope.attach_path(sub_path_id)

            if is_update:
                for subel in shape_el.elements or []:
                    is_prop = (
                        isinstance(subel.expr.steps[0], qlast.Ptr) and
                        subel.expr.steps[0].type == 'property'
                    )
                    if not is_prop:
                        raise errors.QueryError(
                            'only references to link properties are allowed '
                            'in nested UPDATE shapes', context=subel.context)

                ptr_target = _process_view(
                    stype=ptr_target, path_id=sub_path_id,
                    path_id_namespace=path_id_namespace,
                    view_rptr=sub_view_rptr,
                    elements=shape_el.elements, is_update=True, ctx=ctx)
            else:
                ptr_target = _process_view(
                    stype=ptr_target, path_id=sub_path_id,
                    path_id_namespace=path_id_namespace,
                    view_rptr=sub_view_rptr,
                    elements=shape_el.elements, ctx=ctx)

    else:
        base_ptrcls = ptrcls = None

        if (is_mutation
                and ptrname not in ctx.special_computables_in_mutation_shape):
            # If this is a mutation, the pointer must exist.
            ptrcls = setgen.resolve_ptr(ptrsource, ptrname, ctx=ctx)

            base_ptrcls = ptrcls.get_bases(
                ctx.env.schema).first(ctx.env.schema)

            ptr_name = sn.Name(
                module='__',
                name=ptrcls.get_shortname(ctx.env.schema).name,
            )

        else:
            ptr_name = sn.Name(
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
                # This is a NEW compitable pointer, it's fine.
                pass

        qlexpr = astutils.ensure_qlstmt(compexpr)

        if ((ctx.expr_exposed or ctx.stmt is ctx.toplevel_stmt)
                and ctx.implicit_limit
                and isinstance(qlexpr, qlast.OffsetLimitMixin)
                and not qlexpr.limit):
            qlexpr.limit = qlast.IntegerConstant(value=str(ctx.implicit_limit))

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
                is_insert=is_insert,
                is_update=is_update,
            )

            shape_expr_ctx.defining_view = view_scls
            shape_expr_ctx.path_scope.unnest_fence = True
            shape_expr_ctx.partial_path_prefix = setgen.class_set(
                view_scls.get_bases(ctx.env.schema).first(ctx.env.schema),
                path_id=path_id, ctx=shape_expr_ctx)
            prefix_rptrref = path_id.rptr()
            if prefix_rptrref is not None:
                # Source path seems to contain multiple steps,
                # so set up a rptr for abbreviated link property
                # paths.
                src_path_id = path_id.src_path()
                assert src_path_id is not None
                ctx.env.schema, src_t = irtyputils.ir_typeref_to_type(
                    shape_expr_ctx.env.schema,
                    src_path_id.target,
                )
                prefix_rptr = irast.Pointer(
                    source=setgen.class_set(
                        src_t,
                        path_id=src_path_id,
                        ctx=shape_expr_ctx,
                    ),
                    target=shape_expr_ctx.partial_path_prefix,
                    ptrref=prefix_rptrref,
                    direction=s_pointers.PointerDirection.Outbound,
                )
                shape_expr_ctx.partial_path_prefix.rptr = prefix_rptr

            if is_mutation and ptrcls is not None:
                shape_expr_ctx.expr_exposed = True
                shape_expr_ctx.empty_result_type_hint = \
                    ptrcls.get_target(ctx.env.schema)

            shape_expr_ctx.stmt_metadata[qlexpr] = context.StatementMetadata(
                iterator_target=True,
            )
            irexpr = dispatch.compile(qlexpr, ctx=shape_expr_ctx)

            if (
                shape_el.operation.op is qlast.ShapeOp.APPEND
                or shape_el.operation.op is qlast.ShapeOp.SUBTRACT
            ):
                if not is_update:
                    op = (
                        '+=' if shape_el.operation.op is qlast.ShapeOp.APPEND
                        else '-='
                    )
                    raise errors.EdgeQLSyntaxError(
                        f"unexpected '{op}'",
                        context=shape_el.operation.context,
                    )

            irexpr.context = compexpr.context

            if base_ptrcls is None:
                base_ptrcls = shape_expr_ctx.view_rptr.base_ptrcls
                base_ptrcls_is_alias = shape_expr_ctx.view_rptr.ptrcls_is_alias

            if ptrcls is not None:
                ctx.env.schema = ptrcls.set_field_value(
                    ctx.env.schema, 'is_owned', True)

        ptr_cardinality = None
        ptr_target = inference.infer_type(irexpr, ctx.env)

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
                    or schemactx.is_type_compatible(
                        base_target,
                        ptr_target,
                        ctx=ctx
                    )
                ):
                    qlexpr = astutils.ensure_qlstmt(qlast.TypeCast(
                        type=typegen.type_to_ql_typeref(base_target, ctx=ctx),
                        expr=compexpr,
                    ))
                    ptr_target = base_target

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
            base = ctx.env.get_track_schema_object('std::link')
        else:
            base = ctx.env.get_track_schema_object('std::property')

        if base_ptrcls is not None:
            derive_from = base_ptrcls
        else:
            derive_from = base

        derived_name = schemactx.derive_view_name(
            base_ptrcls,
            derived_name_base=ptr_name,
            derived_name_quals=[src_scls.get_name(ctx.env.schema)],
            ctx=ctx)

        existing = ctx.env.schema.get(derived_name, None)
        if existing is not None:
            assert isinstance(existing, s_pointers.Pointer)
            existing_target = existing.get_target(ctx.env.schema)
            assert existing_target is not None
            if ptr_target == existing_target:
                ptrcls = existing
            elif ptr_target.implicitly_castable_to(
                    existing_target, ctx.env.schema):
                ctx.env.schema = existing.set_target(
                    ctx.env.schema, ptr_target)
                ptrcls = existing
            else:
                target_rptr_set = (
                    ptr_target.get_rptr(ctx.env.schema) is not None
                )

                if target_rptr_set:
                    ctx.env.schema = ptr_target.set_field_value(
                        ctx.env.schema,
                        'rptr',
                        None,
                    )

                ctx.env.schema = existing.delete(ctx.env.schema)

                try:
                    ptrcls = schemactx.derive_ptr(
                        derive_from, src_scls, ptr_target,
                        is_insert=is_insert,
                        is_update=is_update,
                        derived_name=derived_name,
                        inheritance_merge=True,
                        ctx=ctx,
                    )
                except errors.SchemaError as e:
                    if compexpr is not None:
                        e.set_source_context(compexpr.context)
                    else:
                        e.set_source_context(shape_el.expr.steps[-1].context)
                    raise

                if target_rptr_set:
                    ctx.env.schema = ptr_target.set_field_value(
                        ctx.env.schema,
                        'rptr',
                        ptrcls,
                    )
        else:
            ptrcls = schemactx.derive_ptr(
                derive_from, src_scls, ptr_target,
                is_insert=is_insert,
                is_update=is_update,
                derived_name=derived_name,
                ctx=ctx)

    elif ptrcls.get_target(ctx.env.schema) != ptr_target:
        ctx.env.schema = ptrcls.set_target(ctx.env.schema, ptr_target)

    assert ptrcls is not None

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
        ctx.source_map[ptrcls] = context.ComputableInfo(
            qlexpr=qlexpr,
            context=ctx,
            path_id=path_id,
            path_id_ns=path_id_namespace,
            shape_op=shape_el.operation.op,
        )

    if compexpr is not None or is_polymorphic:
        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema,
            'computable',
            True,
        )

        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema,
            'is_owned',
            True,
        )

    if ptr_cardinality is not None:
        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema, 'cardinality', ptr_cardinality)
    else:
        if qlexpr is None and ptrcls is not base_ptrcls:
            ctx.pointer_derivation_map[base_ptrcls].append(ptrcls)

        base_cardinality = None
        base_required = False
        if base_ptrcls is not None and not base_ptrcls_is_alias:
            base_cardinality = base_ptrcls.get_cardinality(ctx.env.schema)
            base_required = base_ptrcls.get_required(ctx.env.schema)

        if base_cardinality is None:
            specified_cardinality = shape_el.cardinality
            specified_required = shape_el.required
        else:
            specified_cardinality = base_cardinality
            specified_required = base_required

            if (shape_el.cardinality is not None
                    and base_ptrcls is not None
                    and shape_el.cardinality != base_cardinality):
                base_src = base_ptrcls.get_source(ctx.env.schema)
                assert base_src is not None
                base_src_name = base_src.get_verbosename(ctx.env.schema)
                raise errors.SchemaError(
                    f'cannot redefine the cardinality of '
                    f'{ptrcls.get_verbosename(ctx.env.schema)}: '
                    f'it is defined as {base_cardinality.as_ptr_qual()!r} '
                    f'in the base {base_src_name}',
                    context=compexpr.context,
                )
            # The required flag may be inherited from the base
            specified_required = shape_el.required or base_required

        stmtctx.pend_pointer_cardinality_inference(
            ptrcls=ptrcls,
            specified_required=specified_required,
            specified_card=specified_cardinality,
            is_mut_assignment=is_mutation,
            shape_op=shape_el.operation.op,
            source_ctx=shape_el.context,
            ctx=ctx,
        )

        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema, 'cardinality', None)

    if (
        ptrcls.is_protected_pointer(ctx.env.schema)
        and qlexpr is not None
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

    if is_update and ptrcls.get_readonly(ctx.env.schema):
        raise errors.QueryError(
            f'cannot update {ptrcls.get_verbosename(ctx.env.schema)}: '
            f'it is declared as read-only',
            context=compexpr.context,
        )

    return ptrcls


def derive_ptrcls(
        view_rptr: context.ViewRPtr, *,
        target_scls: s_types.Type,
        transparent: bool=False,
        ctx: context.ContextLevel) -> s_pointers.Pointer:

    if view_rptr.ptrcls is None:
        if view_rptr.base_ptrcls is None:
            transparent = False

            if target_scls.is_object_type():
                base = ctx.env.get_track_schema_object('std::link')
                view_rptr.base_ptrcls = cast(s_links.Link, base)
            else:
                base = ctx.env.get_track_schema_object('std::property')
                view_rptr.base_ptrcls = cast(s_props.Property, base)

        derived_name = schemactx.derive_view_name(
            view_rptr.base_ptrcls,
            derived_name_base=view_rptr.ptrcls_name,
            derived_name_quals=(
                view_rptr.source.get_name(ctx.env.schema),
            ),
            ctx=ctx)

        attrs = {}
        if transparent and not view_rptr.ptrcls_is_alias:
            attrs['path_id_name'] = view_rptr.base_ptrcls.get_name(
                ctx.env.schema)

        view_rptr.ptrcls = schemactx.derive_ptr(
            view_rptr.base_ptrcls, view_rptr.source, target_scls,
            derived_name=derived_name,
            is_insert=view_rptr.is_insert,
            is_update=view_rptr.is_update,
            attrs=attrs,
            ctx=ctx
        )

    else:
        attrs = {}
        if transparent and not view_rptr.ptrcls_is_alias:
            attrs['path_id_name'] = view_rptr.ptrcls.get_name(ctx.env.schema)

        view_rptr.ptrcls = schemactx.derive_ptr(
            view_rptr.ptrcls, view_rptr.source, target_scls,
            derived_name_quals=(view_rptr.source.get_name(ctx.env.schema),),
            is_insert=view_rptr.is_insert,
            is_update=view_rptr.is_update,
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


def has_implicit_tid(
        stype: s_types.Type, *,
        is_mutation: bool,
        ctx: context.ContextLevel) -> bool:

    return (
        stype.is_object_type()
        and not is_mutation
        and ctx.implicit_tid_in_shapes
    )


def _get_shape_configuration(
    ir_set: irast.Set,
    *,
    rptr: Optional[irast.Pointer]=None,
    parent_view_type: Optional[s_types.ExprType]=None,
    ctx: context.ContextLevel
) -> List[Tuple[irast.Set, s_pointers.Pointer, qlast.ShapeOp]]:

    """Return a list of (source_set, ptrcls) pairs as a shape for a given set.
    """

    stype = setgen.get_set_type(ir_set, ctx=ctx)

    sources: List[
        Union[s_types.Type, s_pointers.PointerLike]] = []
    link_view = False
    is_objtype = ir_set.path_id.is_objtype_path()

    if rptr is None:
        rptr = ir_set.rptr

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
        sources.append(rptrcls)

    shape_ptrs = []

    for source in sources:
        for ptr, shape_op in ctx.env.view_shapes[source]:
            if (ptr.is_link_property(ctx.env.schema) and
                    ir_set.path_id != rptr.target.path_id):
                path_tip = rptr.target
            else:
                path_tip = ir_set

            shape_ptrs.append((path_tip, ptr, shape_op))

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
        )

        if implicit_id:
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
                        view_shape.insert(0, (ptr, qlast.ShapeOp.ASSIGN))
                        shape_metadata.has_implicit_id = True
                        shape_ptrs.insert(
                            0, (ir_set, ptr, qlast.ShapeOp.ASSIGN))
                    break

    is_mutation = parent_view_type in {
        s_types.ExprType.Insert,
        s_types.ExprType.Update
    }

    implicit_tid = (
        stype is not None
        and has_implicit_tid(stype, is_mutation=is_mutation, ctx=ctx)
    )

    if implicit_tid:
        assert isinstance(stype, s_objtypes.ObjectType)

        try:
            ptr = setgen.resolve_ptr(stype, '__tid__', ctx=ctx)
        except errors.InvalidReferenceError:
            ql = qlast.ShapeElement(
                expr=qlast.Path(
                    steps=[qlast.Ptr(
                        ptr=qlast.ObjectRef(name='__tid__'),
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
                            ptr=qlast.ObjectRef(name='id'),
                            direction=s_pointers.PointerDirection.Outbound,
                        )
                    ]
                )
            )
            with ctx.newscope(fenced=True) as scopectx:
                scopectx.anchors = scopectx.anchors.copy()
                scopectx.anchors[qlast.Source().name] = ir_set
                ptr = _normalize_view_ptr_expr(
                    ql, stype, path_id=ir_set.path_id, ctx=scopectx)

        view_shape = ctx.env.view_shapes[stype]
        view_shape_ptrs = {p for p, _ in view_shape}
        if ptr not in view_shape_ptrs:
            view_shape.insert(0, (ptr, qlast.ShapeOp.ASSIGN))
            shape_ptrs.insert(0, (ir_set, ptr, qlast.ShapeOp.ASSIGN))

    return shape_ptrs


@functools.singledispatch
def compile_view_shapes(
        expr: irast.Base, *,
        rptr: Optional[irast.Pointer]=None,
        parent_view_type: Optional[s_types.ExprType]=None,
        ctx: context.ContextLevel) -> None:
    pass


@compile_view_shapes.register(irast.Set)
def _compile_view_shapes_in_set(
        ir_set: irast.Set, *,
        rptr: Optional[irast.Pointer]=None,
        parent_view_type: Optional[s_types.ExprType]=None,
        ctx: context.ContextLevel) -> None:

    shape_ptrs = _get_shape_configuration(
        ir_set, rptr=rptr, parent_view_type=parent_view_type, ctx=ctx)

    if shape_ptrs:
        pathctx.register_set_in_scope(ir_set, ctx=ctx)
        stype = setgen.get_set_type(ir_set, ctx=ctx)

        for path_tip, ptr, shape_op in shape_ptrs:
            element = setgen.extend_path(
                path_tip,
                ptr,
                unnest_fence=True,
                same_computable_scope=True,
                hoist_iterators=True,
                ctx=ctx,
            )

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
                    element,
                    parent_view_type=stype.get_expr_type(ctx.env.schema),
                    ctx=scopectx)

            ir_set.shape.append((element, shape_op))

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
        rptr: Optional[irast.Pointer]=None,
        parent_view_type: Optional[s_types.ExprType]=None,
        ctx: context.ContextLevel) -> None:
    compile_view_shapes(stmt.result, ctx=ctx)


@compile_view_shapes.register(irast.FunctionCall)
def _compile_view_shapes_in_fcall(
        expr: irast.FunctionCall, *,
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
                    compile_view_shapes(arg, ctx=scopectx)
            else:
                compile_view_shapes(arg, ctx=ctx)


@compile_view_shapes.register(irast.Tuple)
def _compile_view_shapes_in_tuple(
        expr: irast.Tuple, *,
        rptr: Optional[irast.Pointer]=None,
        parent_view_type: Optional[s_types.ExprType]=None,
        ctx: context.ContextLevel) -> None:
    for element in expr.elements:
        compile_view_shapes(element.val, ctx=ctx)


@compile_view_shapes.register(irast.Array)
def _compile_view_shapes_in_array(
        expr: irast.Array, *,
        rptr: Optional[irast.Pointer]=None,
        parent_view_type: Optional[s_types.ExprType]=None,
        ctx: context.ContextLevel) -> None:
    for element in expr.elements:
        compile_view_shapes(element, ctx=ctx)
