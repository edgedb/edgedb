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


"""EdgeQL set compilation functions."""


from __future__ import annotations

from typing import *

import contextlib

from edb import errors

from edb.common import levenshtein
from edb.common import parsing
from edb.common.typeutils import downcast, not_none

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.schema import links as s_links
from edb.schema import name as s_name
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import pseudo as s_pseudo
from edb.schema import scalars as s_scalars
from edb.schema import sources as s_sources
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import parser as qlparser

from . import astutils
from . import casts
from . import context
from . import dispatch
from . import inference
from . import pathctx
from . import schemactx
from . import stmtctx
from . import typegen

if TYPE_CHECKING:
    from edb.schema import objects as s_obj


PtrDir = s_pointers.PointerDirection


def new_set(
    *,
    stype: s_types.Type,
    ctx: context.ContextLevel,
    ircls: Type[irast.Set] = irast.Set,
    **kwargs: Any,
) -> irast.Set:
    """Create a new ir.Set instance with given attributes.

    Absolutely all ir.Set instances must be created using this
    constructor.
    """
    if (
        stype not in ctx.type_rewrites
        and isinstance(stype, s_objtypes.ObjectType)
        and ctx.env.options.apply_query_rewrites
        and (filters := stype.get_access_policy_filters(ctx.env.schema))
    ):
        qry = qlast.SelectQuery(
            result=qlast.Path(
                steps=[s_utils.name_to_ast_ref(stype.get_name(ctx.env.schema))]
            ),
        )
        for f in filters:
            assert isinstance(f.qlast, qlast.Expr)
            qry.where = astutils.extend_binop(qry.where, f.qlast)

        with ctx.detached() as subctx:
            subctx.expr_exposed = False
            subctx.path_scope = subctx.env.path_scope.root
            # Put a placeholder to prevent recursion.
            subctx.type_rewrites[stype] = irast.Set()  # type: ignore
            filtered_set = dispatch.compile(qry, ctx=subctx)
            assert isinstance(filtered_set, irast.Set)
            subctx.type_rewrites[stype] = filtered_set

    typeref = typegen.type_to_typeref(stype, env=ctx.env)
    ir_set = ircls(typeref=typeref, **kwargs)
    ctx.env.set_types[ir_set] = stype
    return ir_set


def new_empty_set(*, stype: Optional[s_types.Type]=None, alias: str,
                  ctx: context.ContextLevel,
                  srcctx: Optional[
                      parsing.ParserContext]=None) -> irast.Set:
    if stype is None:
        stype = s_pseudo.PseudoType.get(ctx.env.schema, 'anytype')
        if srcctx is not None:
            ctx.env.type_origins[stype] = srcctx

    typeref = typegen.type_to_typeref(stype, env=ctx.env)
    path_id = pathctx.get_expression_path_id(stype, alias, ctx=ctx)
    ir_set = irast.EmptySet(path_id=path_id, typeref=typeref)
    ctx.env.set_types[ir_set] = stype
    return ir_set


def get_set_type(
        ir_set: irast.Set, *,
        ctx: context.ContextLevel) -> s_types.Type:
    return ctx.env.set_types[ir_set]


def new_set_from_set(
        ir_set: irast.Set, *,
        preserve_scope_ns: bool=False,
        path_id: Optional[irast.PathId]=None,
        stype: Optional[s_types.Type]=None,
        rptr: Optional[irast.Pointer]=None,
        context: Optional[parsing.ParserContext]=None,
        is_binding: Optional[irast.BindingKind]=None,
        ctx: context.ContextLevel) -> irast.Set:
    """Create a new ir.Set from another ir.Set.

    The new Set inherits source Set's scope, schema item, expression,
    and, if *preserve_scope_ns* is set, path_id.  If *preserve_scope_ns*
    is False, the new Set's path_id will be namespaced with the currently
    active scope namespace.
    """
    if path_id is None:
        path_id = ir_set.path_id
    if not preserve_scope_ns:
        path_id = path_id.merge_namespace(ctx.path_id_namespace)
    if stype is None:
        stype = get_set_type(ir_set, ctx=ctx)
    if rptr is None:
        rptr = ir_set.rptr
    if context is None:
        context = ir_set.context
    if is_binding is None:
        is_binding = ir_set.is_binding
    return new_set(
        path_id=path_id,
        path_scope_id=ir_set.path_scope_id,
        stype=stype,
        expr=ir_set.expr,
        rptr=rptr,
        context=context,
        is_binding=is_binding,
        ircls=type(ir_set),
        ctx=ctx,
    )


def new_tuple_set(
        elements: List[irast.TupleElement], *,
        named: bool,
        ctx: context.ContextLevel) -> irast.Set:

    dummy_typeref = cast(irast.TypeRef, None)
    tup = irast.Tuple(elements=elements, named=named, typeref=dummy_typeref)
    stype = inference.infer_type(tup, env=ctx.env)
    result_path_id = pathctx.get_expression_path_id(stype, ctx=ctx)

    final_elems = []
    for elem in elements:
        elem_path_id = pathctx.get_tuple_indirection_path_id(
            result_path_id, elem.name, get_set_type(elem.val, ctx=ctx),
            ctx=ctx)
        final_elems.append(irast.TupleElement(
            name=elem.name,
            val=elem.val,
            path_id=elem_path_id,
        ))

    typeref = typegen.type_to_typeref(stype, env=ctx.env)
    final_tup = irast.Tuple(elements=final_elems, named=named, typeref=typeref)
    return ensure_set(final_tup, path_id=result_path_id,
                      type_override=stype, ctx=ctx)


def new_array_set(
        elements: Sequence[irast.Set], *,
        ctx: context.ContextLevel,
        srcctx: Optional[parsing.ParserContext]=None) -> irast.Set:

    dummy_typeref = cast(irast.TypeRef, None)
    arr = irast.Array(elements=elements, typeref=dummy_typeref)
    if elements:
        stype = inference.infer_type(arr, env=ctx.env)
    else:
        anytype = s_pseudo.PseudoType.get(ctx.env.schema, 'anytype')
        ctx.env.schema, stype = s_types.Array.from_subtypes(
            ctx.env.schema, [anytype])
        if srcctx is not None:
            ctx.env.type_origins[anytype] = srcctx

    typeref = typegen.type_to_typeref(stype, env=ctx.env)
    arr = irast.Array(elements=elements, typeref=typeref)
    return ensure_set(arr, type_override=stype, ctx=ctx)


def compile_path(expr: qlast.Path, *, ctx: context.ContextLevel) -> irast.Set:
    """Create an ir.Set representing the given EdgeQL path expression."""
    anchors = ctx.anchors

    if expr.partial:
        if ctx.partial_path_prefix is not None:
            path_tip = ctx.partial_path_prefix
        else:
            raise errors.QueryError(
                'could not resolve partial path ',
                context=expr.context)

    computables = []
    path_sets = []

    for i, step in enumerate(expr.steps):
        if isinstance(step, qlast.SpecialAnchor):
            path_tip = resolve_special_anchor(step, ctx=ctx)

        elif isinstance(step, qlast.ObjectRef):
            if i > 0:  # pragma: no cover
                raise RuntimeError(
                    'unexpected ObjectRef as a non-first path item')

            refnode = None

            if (
                not step.module
                and s_name.UnqualName(step.name) not in ctx.aliased_views
            ):
                # Check if the starting path label is a known anchor
                refnode = anchors.get(step.name)

            if refnode is not None:
                path_tip = new_set_from_set(
                    refnode, preserve_scope_ns=True, ctx=ctx)
            else:
                stype = schemactx.get_schema_type(
                    step,
                    condition=lambda o: (
                        isinstance(o, s_types.Type)
                        and (
                            o.is_object_type() or
                            o.is_view(ctx.env.schema) or
                            o.is_enum(ctx.env.schema)
                        )
                    ),
                    label='object type or alias',
                    item_type=s_types.QualifiedType,
                    srcctx=step.context,
                    ctx=ctx,
                )

                if (stype.is_enum(ctx.env.schema) and
                        not stype.is_view(ctx.env.schema)):
                    return compile_enum_path(expr, source=stype, ctx=ctx)

                if (stype.get_expr_type(ctx.env.schema) is not None and
                        stype.get_name(ctx.env.schema) not in ctx.view_nodes):
                    # This is a schema-level view, as opposed to
                    # a WITH-block or inline alias view.
                    stype = stmtctx.declare_view_from_schema(stype, ctx=ctx)

                view_set = ctx.view_sets.get(stype)
                if view_set is not None:
                    view_scope_info = ctx.path_scope_map[view_set]
                    is_binding = (
                        irast.BindingKind.For if view_scope_info.is_for_view
                        else irast.BindingKind.With
                    )
                    path_tip = new_set_from_set(
                        view_set,
                        preserve_scope_ns=(
                            view_scope_info.pinned_path_id_ns is not None
                        ),
                        is_binding=is_binding,
                        ctx=ctx,
                    )

                    maybe_materialize(stype, path_tip, ctx=ctx)

                else:
                    path_tip = class_set(stype, ctx=ctx)

                view_scls = ctx.class_view_overrides.get(stype.id)
                if (view_scls is not None
                        and view_scls != get_set_type(path_tip, ctx=ctx)):
                    path_tip = ensure_set(
                        path_tip, type_override=view_scls, ctx=ctx)

        elif isinstance(step, qlast.Ptr):
            # Pointer traversal step
            ptr_expr = step
            if ptr_expr.direction is not None:
                direction = s_pointers.PointerDirection(ptr_expr.direction)
            else:
                direction = s_pointers.PointerDirection.Outbound

            ptr_name = ptr_expr.ptr.name

            source: s_obj.Object
            ptr: s_pointers.PointerLike

            if ptr_expr.type == 'property':
                # Link property reference; the source is the
                # link immediately preceding this step in the path.
                if path_tip.rptr is None:
                    raise errors.EdgeQLSyntaxError(
                        f"unexpected reference to link property {ptr_name!r} "
                        "outside of a path expression",
                        context=ptr_expr.ptr.context,
                    )

                if isinstance(path_tip.rptr.ptrref,
                              irast.TypeIntersectionPointerRef):
                    ind_prefix, ptrs = typegen.collapse_type_intersection_rptr(
                        path_tip,
                        ctx=ctx,
                    )

                    assert ind_prefix.rptr is not None
                    prefix_type = get_set_type(ind_prefix.rptr.source, ctx=ctx)
                    assert isinstance(prefix_type, s_objtypes.ObjectType)

                    if not ptrs:
                        tip_type = get_set_type(path_tip, ctx=ctx)
                        s_vn = prefix_type.get_verbosename(ctx.env.schema)
                        t_vn = tip_type.get_verbosename(ctx.env.schema)
                        pn = ind_prefix.rptr.ptrref.shortname.name
                        if direction is s_pointers.PointerDirection.Inbound:
                            s_vn, t_vn = t_vn, s_vn
                        raise errors.InvalidReferenceError(
                            f"property '{ptr_name}' does not exist because"
                            f" there are no '{pn}' links between"
                            f" {s_vn} and {t_vn}",
                            context=ptr_expr.ptr.context,
                        )

                    prefix_ptr_name = (
                        next(iter(ptrs)).get_local_name(ctx.env.schema))

                    ptr = schemactx.get_union_pointer(
                        ptrname=prefix_ptr_name,
                        source=prefix_type,
                        direction=ind_prefix.rptr.direction,
                        components=ptrs,
                        ctx=ctx,
                    )
                else:
                    ptr = typegen.ptrcls_from_ptrref(
                        path_tip.rptr.ptrref, ctx=ctx)

                if isinstance(ptr, s_links.Link):
                    source = ptr
                else:
                    raise errors.QueryError(
                        'improper reference to link property on '
                        'a non-link object',
                        context=step.context,
                    )
            else:
                source = get_set_type(path_tip, ctx=ctx)

            # If this is followed by type intersections, collect
            # them up, since we need them in ptr_step_set.
            upcoming_intersections = []
            for j in range(i + 1, len(expr.steps)):
                nstep = expr.steps[j]
                if (isinstance(nstep, qlast.TypeIntersection)
                        and isinstance(nstep.type, qlast.TypeName)):
                    upcoming_intersections.append(
                        schemactx.get_schema_type(
                            nstep.type.maintype, ctx=ctx))
                else:
                    break

            if isinstance(source, s_types.Tuple):
                path_tip = tuple_indirection_set(
                    path_tip, source=source, ptr_name=ptr_name,
                    source_context=step.context, ctx=ctx)

            else:
                path_tip = ptr_step_set(
                    path_tip, expr=step, source=source, ptr_name=ptr_name,
                    direction=direction,
                    upcoming_intersections=upcoming_intersections,
                    ignore_computable=True,
                    source_context=step.context, ctx=ctx)

                assert path_tip.rptr is not None
                ptrcls = typegen.ptrcls_from_ptrref(
                    path_tip.rptr.ptrref, ctx=ctx)
                if _is_computable_ptr(ptrcls, ctx=ctx):
                    computables.append(path_tip)

        elif isinstance(step, qlast.TypeIntersection):
            arg_type = inference.infer_type(path_tip, ctx.env)
            if not isinstance(arg_type, s_objtypes.ObjectType):
                raise errors.QueryError(
                    f'cannot apply type intersection operator to '
                    f'{arg_type.get_verbosename(ctx.env.schema)}: '
                    f'it is not an object type',
                    context=step.context)

            if not isinstance(step.type, qlast.TypeName):
                raise errors.QueryError(
                    f'complex type expressions are not supported here',
                    context=step.context,
                )

            typ = schemactx.get_schema_type(step.type.maintype, ctx=ctx)

            try:
                path_tip = type_intersection_set(
                    path_tip, typ, optional=False, ctx=ctx)
            except errors.SchemaError as e:
                e.set_source_context(step.type.context)
                raise

        else:
            # Arbitrary expression
            if i > 0:  # pragma: no cover
                raise RuntimeError(
                    'unexpected expression as a non-first path item')

            # We need to fence this if the head is a mutating
            # statement, to make sure that the factoring allowlist
            # works right.
            is_subquery = isinstance(step, qlast.Statement)
            with ctx.newscope(fenced=is_subquery) as subctx:
                path_tip = dispatch.compile(step, ctx=subctx)

                # If the head of the path is a direct object
                # reference, wrap it in an expression set to give it a
                # new path id. This prevents the object path from being
                # spuriously visible to computable paths defined in a shape
                # at the root of a path. (See test_edgeql_select_tvariant_04
                # for an example).
                if (
                    path_tip.path_id.is_objtype_path()
                    and not path_tip.path_id.is_view_path()
                    and path_tip.path_id.src_path() is None
                ):
                    path_tip = expression_set(
                        ensure_stmt(path_tip, ctx=subctx),
                        ctx=subctx)

                if path_tip.path_id.is_type_intersection_path():
                    assert path_tip.rptr is not None
                    scope_set = path_tip.rptr.source
                else:
                    scope_set = path_tip

                scope_set = scoped_set(scope_set, ctx=subctx)

        # We compile computables under namespaces, but we need to have
        # the source of the computable *not* under that namespace (and
        # potentially with the source's expr/rptr/etc?), so we need to
        # do some remapping. This is a little fiddly, since we may have
        # picked up *additional* namespaces.
        key = path_tip.path_id.strip_namespace(path_tip.path_id.namespace)
        entries = ctx.view_map.get(key, ())
        for inner_path_id, mapped in entries:
            fixed_inner = inner_path_id.merge_namespace(ctx.path_id_namespace)
            if fixed_inner == path_tip.path_id:
                path_tip = new_set(
                    path_id=mapped.path_id,
                    stype=get_set_type(path_tip, ctx=ctx),
                    expr=mapped.expr,
                    rptr=mapped.rptr,
                    ctx=ctx)
                break

        if pathctx.path_is_banned(path_tip.path_id, ctx=ctx):
            dname = stype.get_displayname(ctx.env.schema)
            raise errors.QueryError(
                f'invalid reference to {dname}: '
                f'self-referencing INSERTs are not allowed',
                hint=(
                    f'Use DETACHED if you meant to refer to an '
                    f'uncorrelated {dname} set'
                ),
                context=step.context,
            )

        path_sets.append(path_tip)

    path_tip.context = expr.context
    # Since we are attaching the computable scopes as siblings to
    # the subpaths they're computing, we must make sure that the
    # actual path head is not visible from inside the computable scope.
    #
    # Example:
    # type Tree {
    #   multi link children -> Tree;
    #   parent := .<children[IS Tree];
    # }
    # `SELECT Tree.parent` should generate rougly the following scope tree:
    #
    # (test::Tree).>parent[IS test::Tree]: {
    #    "BRANCH": {
    #       "(test::Tree)"
    #    },
    #    "FENCE": {
    #        "ns@(test::Tree).<children": {
    #            "(test::Tree) 0x7f30c7885d90"
    #        }
    #    },
    # }
    #
    # Note that we use an unfenced BRANCH node to isolate the path head,
    # to make sure it is still properly factorable.
    # The branch insertion is handled automatically by attach_path, and
    # we temporarily flip the branch to be a full fence for the compilation
    # of the computable.
    fences = pathctx.register_set_in_scope(
        path_tip,
        ctx=ctx,
    )

    for fence in fences:
        fence.fenced = True

    for ir_set in computables:
        scope = ctx.path_scope.find_descendant(ir_set.path_id)
        if scope is None:
            scope = ctx.path_scope.find_visible(ir_set.path_id)
        # We skip recompiling if we can't find a scope for it.
        # This whole mechanism seems a little sketchy, unfortunately.
        if scope is None:
            continue

        with ctx.new() as subctx:
            subctx.path_scope = scope
            assert ir_set.rptr is not None
            comp_ir_set = computable_ptr_set(ir_set.rptr, ctx=subctx)
            i = path_sets.index(ir_set)
            if i != len(path_sets) - 1:
                prptr = path_sets[i + 1].rptr
                assert prptr is not None
                prptr.source = comp_ir_set
            else:
                path_tip = comp_ir_set
            path_sets[i] = comp_ir_set

    for fence in fences:
        fence.fenced = False

    return path_tip


def resolve_special_anchor(
        anchor: qlast.SpecialAnchor, *,
        ctx: context.ContextLevel) -> irast.Set:

    # '__source__' and '__subject__` can only appear as the
    # starting path label syntactically and must be pre-populated
    # by the compile() caller.

    if isinstance(anchor, qlast.SpecialAnchor):
        token = anchor.name
    else:
        raise errors.InternalServerError(
            f'unexpected special anchor kind: {anchor!r}'
        )

    anchors = ctx.anchors
    path_tip = anchors.get(token)

    if path_tip is None:
        raise errors.InvalidReferenceError(
            f'{token} cannot be used in this expression',
            context=anchor.context,
        )

    return path_tip


def ptr_step_set(
        path_tip: irast.Set, *,
        upcoming_intersections: Sequence[s_types.Type] = (),
        source: s_obj.Object,
        expr: Optional[qlast.Base],
        ptr_name: str,
        direction: PtrDir = PtrDir.Outbound,
        source_context: Optional[parsing.ParserContext],
        ignore_computable: bool=False,
        ctx: context.ContextLevel) -> irast.Set:
    ptrcls = resolve_ptr(
        source,
        ptr_name,
        upcoming_intersections=upcoming_intersections,
        track_ref=expr,
        direction=direction,
        source_context=source_context,
        ctx=ctx)

    return extend_path(
        path_tip, ptrcls, direction,
        ignore_computable=ignore_computable, ctx=ctx)


def resolve_ptr(
    near_endpoint: s_obj.Object,
    pointer_name: str,
    *,
    upcoming_intersections: Sequence[s_types.Type] = (),
    far_endpoints: Iterable[s_obj.Object] = (),
    direction: s_pointers.PointerDirection = (
        s_pointers.PointerDirection.Outbound
    ),
    source_context: Optional[parsing.ParserContext] = None,
    track_ref: Optional[Union[qlast.Base, Literal[False]]],
    ctx: context.ContextLevel,
) -> s_pointers.Pointer:

    if not isinstance(near_endpoint, s_sources.Source):
        # Reference to a property on non-object
        msg = 'invalid property reference on a primitive type expression'
        raise errors.InvalidReferenceError(msg, context=source_context)

    ptr: Optional[s_pointers.Pointer] = None

    if direction is s_pointers.PointerDirection.Outbound:
        ptr = near_endpoint.maybe_get_ptr(
            ctx.env.schema,
            s_name.UnqualName(pointer_name),
        )

        if ptr is not None:
            ref = ptr.get_nearest_non_derived_parent(ctx.env.schema)
            if track_ref is not False:
                ctx.env.add_schema_ref(ref, track_ref)

    else:
        ptrs = near_endpoint.getrptrs(ctx.env.schema, pointer_name,
                                      sources=far_endpoints)
        if ptrs:
            # If this reverse pointer access is followed by
            # intersections, we filter out any pointers that
            # couldn't be picked up by the intersections. This avoids
            # creating spurious dependencies when reverse
            # links are used in schemas.
            dep_ptrs = {
                ptr for ptr in ptrs
                if (src := ptr.get_source(ctx.env.schema))
                and all(
                    src.issubclass(ctx.env.schema, typ)
                    or any(
                        dsrc.issubclass(ctx.env.schema, typ)
                        for dsrc in src.descendants(ctx.env.schema)
                    )
                    for typ in upcoming_intersections
                )
            }

            if track_ref is not False:
                for p in dep_ptrs:
                    ctx.env.add_schema_ref(
                        p.get_nearest_non_derived_parent(ctx.env.schema),
                        track_ref)

            # We can only compute backlinks for non-computed pointers,
            # but we need to make sure that a computed pointer doesn't
            # break properly-filtered backlinks.
            concrete_ptrs = [
                ptr for ptr in ptrs
                if not ptr.is_pure_computable(ctx.env.schema)]

            for ptr in ptrs:
                if (
                    ptr.is_pure_computable(ctx.env.schema)
                    and (ptr in dep_ptrs or not concrete_ptrs)
                ):
                    vname = ptr.get_verbosename(ctx.env.schema,
                                                with_parent=True)
                    raise errors.InvalidReferenceError(
                        f'cannot follow backlink {pointer_name!r} because '
                        f'{vname} is computed',
                        context=source_context
                    )

            opaque = not far_endpoints
            ctx.env.schema, ptr = s_pointers.get_or_create_union_pointer(
                ctx.env.schema,
                ptrname=s_name.UnqualName(pointer_name),
                source=near_endpoint,
                direction=direction,
                components=concrete_ptrs,
                opaque=opaque,
                modname=ctx.derived_target_module,
            )

    if ptr is not None:
        return ptr

    if isinstance(near_endpoint, s_links.Link):
        vname = near_endpoint.get_verbosename(ctx.env.schema, with_parent=True)
        msg = f'{vname} has no property {pointer_name!r}'

    elif direction == s_pointers.PointerDirection.Outbound:
        msg = (f'{near_endpoint.get_verbosename(ctx.env.schema)} '
               f'has no link or property {pointer_name!r}')

    else:
        nep_name = near_endpoint.get_displayname(ctx.env.schema)
        path = f'{nep_name}.{direction}{pointer_name}'
        msg = f'{path!r} does not resolve to any known path'

    err = errors.InvalidReferenceError(msg, context=source_context)

    if direction is s_pointers.PointerDirection.Outbound:
        near_enpoint_pointers = near_endpoint.get_pointers(ctx.env.schema)
        s_utils.enrich_schema_lookup_error(
            err,
            s_name.UnqualName(pointer_name),
            modaliases=ctx.modaliases,
            item_type=s_pointers.Pointer,
            collection=near_enpoint_pointers.objects(ctx.env.schema),
            schema=ctx.env.schema,
        )

    raise err


def extend_path(
    source_set: irast.Set,
    ptrcls: s_pointers.Pointer,
    direction: PtrDir = PtrDir.Outbound,
    *,
    ignore_computable: bool = False,
    same_computable_scope: bool = False,
    srcctx: Optional[parsing.ParserContext]=None,
    ctx: context.ContextLevel,
) -> irast.Set:
    """Return a Set node representing the new path tip."""

    if ptrcls.is_link_property(ctx.env.schema):
        src_path_id = source_set.path_id.ptr_path()
    else:
        if direction is not s_pointers.PointerDirection.Inbound:
            source = ptrcls.get_near_endpoint(ctx.env.schema, direction)
            assert isinstance(source, s_types.Type)
            stype = get_set_type(source_set, ctx=ctx)
            if not stype.issubclass(ctx.env.schema, source):
                # Polymorphic link reference
                source_set = type_intersection_set(
                    source_set, source, optional=True, ctx=ctx)

        src_path_id = source_set.path_id

    path_id = pathctx.extend_path_id(
        src_path_id,
        ptrcls=ptrcls,
        direction=direction,
        ns=ctx.path_id_namespace,
        ctx=ctx,
    )

    target = ptrcls.get_far_endpoint(ctx.env.schema, direction)
    assert isinstance(target, s_types.Type)
    target_set = new_set(
        stype=target, path_id=path_id, context=srcctx, ctx=ctx)

    ptr = irast.Pointer(
        source=source_set,
        target=target_set,
        direction=direction,
        ptrref=not_none(path_id.rptr()),
    )

    target_set.rptr = ptr
    is_computable = _is_computable_ptr(ptrcls, ctx=ctx)
    if not ignore_computable and is_computable:
        target_set = computable_ptr_set(
            ptr,
            same_computable_scope=same_computable_scope,
            srcctx=srcctx,
            ctx=ctx,
        )

    return target_set


def is_injected_computable_ptr(
    ptrcls: s_pointers.PointerLike,
    *,
    ctx: context.ContextLevel,
) -> bool:
    return (
        ctx.env.options.apply_query_rewrites
        and ptrcls not in ctx.disable_shadowing
        and bool(ptrcls.get_schema_reflection_default(ctx.env.schema))
    )


def _is_computable_ptr(
    ptrcls: s_pointers.PointerLike,
    *,
    ctx: context.ContextLevel,
) -> bool:
    try:
        qlexpr = ctx.source_map[ptrcls].qlexpr
    except KeyError:
        pass
    else:
        return qlexpr is not None

    return (
        ptrcls.is_pure_computable(ctx.env.schema)
        or is_injected_computable_ptr(ptrcls, ctx=ctx)
    )


def compile_enum_path(
        expr: qlast.Path,
        *,
        source: s_types.Type,
        ctx: context.ContextLevel) -> irast.Set:

    assert isinstance(source, s_scalars.ScalarType)
    enum_values = source.get_enum_values(ctx.env.schema)
    assert enum_values

    nsteps = len(expr.steps)
    if nsteps == 1:
        raise errors.QueryError(
            f"'{source.get_displayname(ctx.env.schema)}' enum "
            f"path expression lacks an enum member name, as in "
            f"'{source.get_displayname(ctx.env.schema)}.{enum_values[0]}'",
            context=expr.steps[0].context,
        )

    step2 = expr.steps[1]
    if not isinstance(step2, qlast.Ptr):
        raise errors.QueryError(
            f"an enum member name must follow enum type name in the path, "
            f"as in "
            f"'{source.get_displayname(ctx.env.schema)}.{enum_values[0]}'",
            context=step2.context,
        )

    ptr_name = step2.ptr.name

    step2_direction = s_pointers.PointerDirection.Outbound
    if step2.direction is not None:
        step2_direction = s_pointers.PointerDirection(step2.direction)
    if step2_direction is not s_pointers.PointerDirection.Outbound:
        raise errors.QueryError(
            f"enum types do not support backlink navigation",
            context=step2.context,
        )
    if step2.type == 'property':
        raise errors.QueryError(
            f"unexpected reference to link property '{ptr_name}' "
            f"outside of a path expression",
            context=step2.context,
        )

    if nsteps > 2:
        raise errors.QueryError(
            f"invalid property reference on a primitive type expression",
            context=expr.steps[2].context,
        )

    if ptr_name not in enum_values:
        rec_name = sorted(
            enum_values,
            key=lambda name: levenshtein.distance(name, ptr_name)
        )[0]
        src_name = source.get_displayname(ctx.env.schema)
        raise errors.InvalidReferenceError(
            f"'{src_name}' enum has no member called {ptr_name!r}",
            hint=f"did you mean {rec_name!r}?",
            context=step2.context,
        )

    return enum_indirection_set(
        source=source,
        ptr_name=step2.ptr.name,
        source_context=expr.context,
        ctx=ctx,
    )


def enum_indirection_set(
        *,
        source: s_types.Type,
        ptr_name: str,
        source_context: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> irast.Set:

    strref = typegen.type_to_typeref(
        ctx.env.get_track_schema_type(s_name.QualName('std', 'str')),
        env=ctx.env,
    )

    return casts.compile_cast(
        irast.StringConstant(value=ptr_name, typeref=strref),
        source,
        srcctx=source_context,
        ctx=ctx,
    )


def tuple_indirection_set(
        path_tip: irast.Set, *,
        source: s_types.Type,
        ptr_name: str,
        source_context: Optional[parsing.ParserContext] = None,
        ctx: context.ContextLevel) -> irast.Set:

    assert isinstance(source, s_types.Tuple)

    el_name = ptr_name
    el_norm_name = source.normalize_index(ctx.env.schema, el_name)
    el_type = source.get_subtype(ctx.env.schema, el_name)

    path_id = pathctx.get_tuple_indirection_path_id(
        path_tip.path_id, el_norm_name, el_type, ctx=ctx)

    ti_set = new_set(stype=el_type, path_id=path_id, ctx=ctx)

    ptr = irast.TupleIndirectionPointer(
        source=path_tip,
        target=ti_set,
        ptrref=downcast(path_id.rptr(), irast.TupleIndirectionPointerRef),
        direction=not_none(path_id.rptr_dir()),
    )

    ti_set.rptr = ptr

    return ti_set


def type_intersection_set(
    source_set: irast.Set,
    stype: s_types.Type,
    *,
    optional: bool,
    ctx: context.ContextLevel,
) -> irast.Set:
    """Return an interesection of *source_set* with type *stype*."""

    arg_type = get_set_type(source_set, ctx=ctx)

    result = schemactx.apply_intersection(arg_type, stype, ctx=ctx)
    if result.stype == arg_type:
        return source_set

    poly_set = new_set(stype=result.stype, ctx=ctx)
    rptr = source_set.rptr
    rptr_specialization = []

    if rptr is not None and rptr.ptrref.union_components:
        # This is a type intersection of a union pointer, most likely
        # a reverse link path specification.  If so, test the union
        # components against the type expression and record which
        # components match.  This information will be used later
        # when evaluating the path cardinality, as well as to
        # route link property references accordingly.
        for component in rptr.ptrref.union_components:
            component_endpoint_ref = component.dir_target(rptr.direction)
            ctx.env.schema, component_endpoint = irtyputils.ir_typeref_to_type(
                ctx.env.schema, component_endpoint_ref)
            if component_endpoint.issubclass(ctx.env.schema, stype):
                assert isinstance(component, irast.PointerRef)
                rptr_specialization.append(component)
            elif stype.issubclass(ctx.env.schema, component_endpoint):
                assert isinstance(stype, s_objtypes.ObjectType)
                if rptr.direction is s_pointers.PointerDirection.Inbound:
                    narrow_ptr = stype.getptr(
                        ctx.env.schema,
                        component.shortname.get_local_name(),
                    )
                    rptr_specialization.append(
                        irtyputils.ptrref_from_ptrcls(
                            schema=ctx.env.schema,
                            ptrcls=narrow_ptr,
                            cache=ctx.env.ptr_ref_cache,
                            typeref_cache=ctx.env.type_ref_cache,
                        ),
                    )
                else:
                    assert isinstance(component, irast.PointerRef)
                    rptr_specialization.append(component)

    ptrcls = irast.TypeIntersectionLink(
        arg_type,
        result.stype,
        optional=optional,
        is_empty=result.is_empty,
        is_subtype=result.is_subtype,
        rptr_specialization=rptr_specialization,
        # The type intersection cannot increase the cardinality
        # of the input set, so semantically, the cardinality
        # of the type intersection "link" is, at most, ONE.
        cardinality=qltypes.SchemaCardinality.One,
    )

    ptrref = irtyputils.ptrref_from_ptrcls(
        schema=ctx.env.schema,
        ptrcls=ptrcls,
        cache=ctx.env.ptr_ref_cache,
        typeref_cache=ctx.env.type_ref_cache,
    )

    poly_set.path_id = source_set.path_id.extend(ptrref=ptrref)

    ptr = irast.TypeIntersectionPointer(
        source=source_set,
        target=poly_set,
        ptrref=downcast(ptrref, irast.TypeIntersectionPointerRef),
        direction=not_none(poly_set.path_id.rptr_dir()),
        optional=optional,
    )

    poly_set.rptr = ptr

    return poly_set


def class_set(
        stype: s_types.Type, *,
        path_id: Optional[irast.PathId]=None,
        ctx: context.ContextLevel) -> irast.Set:

    if path_id is None:
        path_id = pathctx.get_path_id(stype, ctx=ctx)
    return new_set(path_id=path_id, stype=stype, ctx=ctx)


def expression_set(
        expr: irast.Expr,
        path_id: Optional[irast.PathId]=None, *,
        type_override: Optional[s_types.Type]=None,
        ctx: context.ContextLevel) -> irast.Set:

    if isinstance(expr, irast.Set):  # pragma: no cover
        raise errors.InternalServerError(f'{expr!r} is already a Set')

    if type_override is not None:
        stype = type_override
    else:
        stype = inference.infer_type(expr, ctx.env)

    if path_id is None:
        path_id = getattr(expr, 'path_id', None)
        if path_id is None:
            path_id = pathctx.get_expression_path_id(stype, ctx=ctx)

    return new_set(
        path_id=path_id,
        stype=stype,
        expr=expr,
        context=expr.context,
        ctx=ctx
    )


def scoped_set(
        expr: Union[irast.Set, irast.Expr], *,
        type_override: Optional[s_types.Type]=None,
        typehint: Optional[s_types.Type]=None,
        path_id: Optional[irast.PathId]=None,
        force_reassign: bool=False,
        ctx: context.ContextLevel) -> irast.Set:

    if not isinstance(expr, irast.Set):
        ir_set = expression_set(
            expr, type_override=type_override,
            path_id=path_id, ctx=ctx)
        pathctx.assign_set_scope(ir_set, ctx.path_scope, ctx=ctx)
    else:
        if typehint is not None or type_override is not None:
            ir_set = ensure_set(
                expr, typehint=typehint,
                type_override=type_override,
                path_id=path_id, ctx=ctx)
        else:
            ir_set = expr

        if ir_set.path_scope_id is None or force_reassign:
            if ctx.path_scope.find_child(ir_set.path_id) and path_id is None:
                # Protect from scope recursion in the common case by
                # wrapping the set into a subquery.
                ir_set = expression_set(
                    ensure_stmt(ir_set, ctx=ctx),
                    type_override=type_override,
                    ctx=ctx)

            pathctx.assign_set_scope(ir_set, ctx.path_scope, ctx=ctx)

    return ir_set


def ensure_set(
        expr: Union[irast.Set, irast.Expr], *,
        type_override: Optional[s_types.Type]=None,
        typehint: Optional[s_types.Type]=None,
        path_id: Optional[irast.PathId]=None,
        srcctx: Optional[parsing.ParserContext]=None,
        ctx: context.ContextLevel) -> irast.Set:

    if not isinstance(expr, irast.Set):
        ir_set = expression_set(
            expr, type_override=type_override,
            path_id=path_id, ctx=ctx)
    else:
        ir_set = expr

    stype = get_set_type(ir_set, ctx=ctx)

    if type_override is not None and stype != type_override:
        ir_set = new_set_from_set(
            ir_set, stype=type_override, preserve_scope_ns=True, ctx=ctx)

        stype = type_override

    if srcctx is not None:
        ir_set = new_set_from_set(
            ir_set, preserve_scope_ns=True, context=srcctx, ctx=ctx)

    if (isinstance(ir_set, irast.EmptySet)
            and (stype is None or stype.is_any(ctx.env.schema))
            and typehint is not None):
        inference.amend_empty_set_type(ir_set, typehint, env=ctx.env)
        stype = get_set_type(ir_set, ctx=ctx)

    if (typehint is not None and
            not stype.implicitly_castable_to(typehint, ctx.env.schema)):
        raise errors.QueryError(
            f'expecting expression of type '
            f'{typehint.get_displayname(ctx.env.schema)}, '
            f'got {stype.get_displayname(ctx.env.schema)}',
            context=expr.context
        )

    return ir_set


def ensure_stmt(
    expr: Union[irast.Set, irast.Expr], *,
    ctx: context.ContextLevel
) -> irast.Stmt:
    if not isinstance(expr, irast.Stmt):
        expr = irast.SelectStmt(
            result=ensure_set(expr, ctx=ctx),
            implicit_wrapper=True,
        )
    return expr


def computable_ptr_set(
    rptr: irast.Pointer,
    *,
    same_computable_scope: bool=False,
    srcctx: Optional[parsing.ParserContext]=None,
    ctx: context.ContextLevel,
) -> irast.Set:
    """Return ir.Set for a pointer defined as a computable."""
    ptrcls = typegen.ptrcls_from_ptrref(rptr.ptrref, ctx=ctx)
    source_set = rptr.source
    source_scls = get_set_type(source_set, ctx=ctx)
    ptrcls_to_shadow = None

    # process_view() may generate computable pointer expressions
    # in the form "self.linkname".  To prevent infinite recursion,
    # self must resolve to the parent type of the view NOT the view
    # type itself.  Similarly, when resolving computable link properties
    # make sure that we use the parent of derived ptrcls.
    if source_scls.is_view(ctx.env.schema):
        source_set_stype = source_scls.peel_view(ctx.env.schema)
        source_set = new_set_from_set(
            source_set, stype=source_set_stype,
            preserve_scope_ns=True, ctx=ctx)
        source_set.shape = ()
        if source_set.rptr is not None:
            source_rptrref = source_set.rptr.ptrref
            if source_rptrref.base_ptr is not None:
                source_rptrref = source_rptrref.base_ptr
            source_set.rptr = irast.Pointer(
                source=source_set.rptr.source,
                target=source_set,
                ptrref=source_rptrref,
                direction=source_set.rptr.direction,
            )

    qlctx: Optional[context.ContextLevel]
    inner_source_path_id: Optional[irast.PathId]

    try:
        comp_info = ctx.source_map[ptrcls]
        qlexpr = comp_info.qlexpr
        assert isinstance(comp_info.context, context.ContextLevel)
        qlctx = comp_info.context
        inner_source_path_id = comp_info.path_id
        path_id_ns = comp_info.path_id_ns
    except KeyError:
        comp_expr = ptrcls.get_expr(ctx.env.schema)
        schema_qlexpr: Optional[qlast.Expr] = None
        if comp_expr is None and ctx.env.options.apply_query_rewrites:
            schema_deflt = ptrcls.get_schema_reflection_default(ctx.env.schema)
            if schema_deflt is not None:
                assert isinstance(ptrcls, s_pointers.Pointer)
                ptrcls_n = ptrcls.get_shortname(ctx.env.schema).name
                schema_qlexpr = qlast.BinOp(
                    left=qlast.Path(
                        steps=[
                            qlast.Source(),
                            qlast.Ptr(
                                ptr=qlast.ObjectRef(name=ptrcls_n),
                                direction=s_pointers.PointerDirection.Outbound,
                                type=(
                                    'property'
                                    if ptrcls.is_link_property(ctx.env.schema)
                                    else None
                                )
                            )
                        ],
                    ),
                    right=qlparser.parse_fragment(schema_deflt),
                    op='??',
                )

                # Is this is a view, we want to shadow the underlying
                # ptrcls, since otherwise we will generate this default
                # code *twice*.
                if rptr.ptrref.base_ptr:
                    ptrcls_to_shadow = typegen.ptrcls_from_ptrref(
                        rptr.ptrref.base_ptr, ctx=ctx)

        if schema_qlexpr is None:
            if comp_expr is None:
                ptrcls_sn = ptrcls.get_shortname(ctx.env.schema)
                raise errors.InternalServerError(
                    f'{ptrcls_sn!r} is not a computed pointer')

            comp_qlexpr = qlparser.parse(comp_expr.text)
            assert isinstance(comp_qlexpr, qlast.Expr), 'expected qlast.Expr'
            schema_qlexpr = comp_qlexpr

        # NOTE: Validation of the expression type is not the concern
        # of this function. For any non-object pointer target type,
        # the default expression must be assignment-cast into that
        # type.
        target_scls = ptrcls.get_target(ctx.env.schema)
        assert target_scls is not None
        if not target_scls.is_object_type():
            schema_qlexpr = qlast.TypeCast(
                type=typegen.type_to_ql_typeref(
                    target_scls, ctx=ctx),
                expr=schema_qlexpr,
            )
        qlexpr = astutils.ensure_qlstmt(schema_qlexpr)
        qlctx = None
        inner_source_path_id = None
        path_id_ns = None

    newctx: Callable[[], ContextManager[context.ContextLevel]]

    if qlctx is None:
        # Schema-level computable, completely detached context
        newctx = ctx.detached
    else:
        newctx = _get_computable_ctx(
            rptr=rptr,
            source=source_set,
            source_scls=source_scls,
            inner_source_path_id=inner_source_path_id,
            path_id_ns=path_id_ns,
            same_scope=same_computable_scope,
            qlctx=qlctx,
            ctx=ctx)

    if ptrcls.is_link_property(ctx.env.schema):
        source_path_id = rptr.source.path_id.ptr_path()
    else:
        src_path = rptr.target.path_id.src_path()
        assert src_path is not None
        source_path_id = src_path

    result_path_id = pathctx.extend_path_id(
        source_path_id,
        ptrcls=ptrcls,
        ns=ctx.path_id_namespace,
        ctx=ctx,
    )

    result_stype = ptrcls.get_target(ctx.env.schema)
    base_object = ctx.env.schema.get('std::BaseObject', type=s_types.Type)
    with newctx() as subctx:
        subctx.disable_shadowing.add(ptrcls)
        if ptrcls_to_shadow:
            subctx.disable_shadowing.add(ptrcls_to_shadow)
        if result_stype != base_object:
            subctx.view_scls = result_stype
        subctx.view_rptr = context.ViewRPtr(
            source_scls, ptrcls=ptrcls, rptr=rptr)  # type: ignore
        subctx.anchors[qlast.Source().name] = source_set
        subctx.empty_result_type_hint = ptrcls.get_target(ctx.env.schema)
        subctx.partial_path_prefix = source_set
        # On a mutation, make the expr_exposed. This corresponds with
        # a similar check on is_mutation in _normalize_view_ptr_expr.
        if (source_scls.get_expr_type(ctx.env.schema)
                != s_types.ExprType.Select):
            subctx.expr_exposed = True

        comp_ir_set = dispatch.compile(qlexpr, ctx=subctx)

    comp_ir_set = new_set_from_set(
        comp_ir_set, path_id=result_path_id, rptr=rptr, context=srcctx,
        ctx=ctx)

    rptr.target = comp_ir_set

    maybe_materialize(ptrcls, comp_ir_set, ctx=ctx)

    return comp_ir_set


def _get_computable_ctx(
    *,
    rptr: irast.Pointer,
    source: irast.Set,
    source_scls: s_types.Type,
    inner_source_path_id: Optional[irast.PathId],
    path_id_ns: Optional[irast.Namespace],
    same_scope: bool,
    qlctx: context.ContextLevel,
    ctx: context.ContextLevel
) -> Callable[[], ContextManager[context.ContextLevel]]:

    @contextlib.contextmanager
    def newctx() -> Iterator[context.ContextLevel]:
        with ctx.new() as subctx:
            subctx.class_view_overrides = {}
            subctx.partial_path_prefix = None

            subctx.modaliases = qlctx.modaliases.copy()
            subctx.aliased_views = qlctx.aliased_views.new_child()
            source_stype = get_set_type(source, ctx=ctx)

            if source_scls.is_view(ctx.env.schema):
                scls_name = source_stype.get_name(ctx.env.schema)
                subctx.aliased_views[scls_name] = None
            subctx.source_map = qlctx.source_map.copy()
            subctx.view_nodes = qlctx.view_nodes.copy()
            subctx.view_map = ctx.view_map.new_child()

            source_scope = pathctx.get_set_scope(rptr.source, ctx=ctx)
            if source_scope and source_scope.namespaces:
                subctx.path_id_namespace |= source_scope.namespaces

            if path_id_ns is not None:
                subctx.path_id_namespace |= {path_id_ns}

            pending_pid_ns = {ctx.aliases.get('ns')}

            if path_id_ns is not None and same_scope:
                pending_pid_ns.add(path_id_ns)

            subctx.pending_stmt_own_path_id_namespace = (
                frozenset(pending_pid_ns))

            subns = set(pending_pid_ns)

            self_view = ctx.view_sets.get(source_stype)
            if self_view:
                subns.update(self_view.path_id.namespace)
            else:
                subns.update(source.path_id.namespace)

            if inner_source_path_id is not None:
                # The path id recorded in the source map may
                # contain namespaces referring to a temporary
                # scope subtree used by `process_view()`.
                # Since we recompile the computable expression
                # using the current path id namespace, the
                # original source path id needs to be fixed.
                inner_path_id = inner_source_path_id \
                    .strip_namespace(qlctx.path_id_namespace) \
                    .merge_namespace(subctx.path_id_namespace)
            else:
                inner_path_id = pathctx.get_path_id(
                    source_stype, ctx=subctx)

            inner_path_id = inner_path_id.merge_namespace(subns)

            subctx.pending_stmt_full_path_id_namespace = frozenset(subns)

            remapped_source = new_set_from_set(
                rptr.source, rptr=rptr.source.rptr,
                preserve_scope_ns=True, ctx=ctx)
            key = inner_path_id.strip_namespace(inner_path_id.namespace)
            old = subctx.view_map.get(key, ())
            subctx.view_map[key] = ((inner_path_id, remapped_source),) + old

            yield subctx

    return newctx


def maybe_materialize(
    stype: Union[s_types.Type, s_pointers.PointerLike],
    ir: irast.Set,
    *,
    ctx: context.ContextLevel,
) -> None:
    if isinstance(stype, s_pointers.PseudoPointer):
        return

    # Search for a materialized_sets entry
    while True:
        if mat_entry := ctx.env.materialized_sets.get(stype):
            break
        # Search up for parent pointers, if applicable
        if not isinstance(stype, s_pointers.Pointer):
            return
        bases = stype.get_bases(ctx.env.schema).objects(ctx.env.schema)
        if not bases:
            return
        stype = bases[0]

    # We've found an entry, populate it.
    mat_qlstmt, reason = mat_entry
    materialize_in_stmt = ctx.env.compiled_stmts[mat_qlstmt]
    if materialize_in_stmt.materialized_sets is None:
        materialize_in_stmt.materialized_sets = {}

    assert not isinstance(stype, s_pointers.PseudoPointer)
    if stype.id not in materialize_in_stmt.materialized_sets:
        saved = (
            None if isinstance(stype, s_pointers.Pointer) and not ir.rptr
            else ir
        )
        materialize_in_stmt.materialized_sets[stype.id] = (
            irast.MaterializedSet(
                materialized=saved, reason=reason, use_sets=[]))

    mat_set = materialize_in_stmt.materialized_sets[stype.id]
    mat_set.use_sets.append(ir)


def should_materialize(
    ir: irast.Base, *,
    ptrcls: Optional[s_pointers.Pointer]=None,
    materialize_visible: bool=False,
    skipped_bindings: AbstractSet[irast.PathId]=frozenset(),
    ctx: context.ContextLevel,
) -> Sequence[irast.MaterializeReason]:
    volatility = inference.infer_volatility(
        ir, ctx.env, for_materialization=True)
    reasons: List[irast.MaterializeReason] = []

    if volatility.is_volatile():
        reasons.append(irast.MaterializeVolatile())

    if not isinstance(ir, irast.Set):
        return reasons

    typ = get_set_type(ir, ctx=ctx)

    # For shape elements, we need to materialize when they reference
    # bindings that are visible from that point. This means that doing
    # WITH/FOR bindings internally is fine, but referring to
    # externally bound things needs materialization. We can't actually
    # do this visibility analysis until we are done, though, so
    # instead we just store the bindings.
    if (
        materialize_visible
        and (vis := irutils.find_potentially_visible(
            ir, ctx.path_scope, ctx.env.scope_tree_nodes, skipped_bindings))
    ):
        reasons.append(irast.MaterializeVisible(vis))

    if ptrcls and ptrcls in ctx.source_map:
        reasons += ctx.source_map[ptrcls].should_materialize

    reasons += should_materialize_type(typ, ctx=ctx)

    return reasons


def should_materialize_type(
    typ: s_types.Type, *, ctx: context.ContextLevel
) -> List[irast.MaterializeReason]:
    schema = ctx.env.schema
    reasons: List[irast.MaterializeReason] = []
    if isinstance(
            typ, (s_objtypes.ObjectType, s_pointers.Pointer)):
        for pointer in typ.get_pointers(schema).objects(schema):
            if pointer in ctx.source_map:
                reasons += ctx.source_map[pointer].should_materialize
    elif isinstance(typ, s_types.Collection):
        for sub in typ.get_subtypes(schema):
            reasons += should_materialize_type(sub, ctx=ctx)

    return reasons
