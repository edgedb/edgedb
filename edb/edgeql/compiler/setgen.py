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
import enum

from edb import errors

from edb.common import levenshtein
from edb.common import parsing
from edb.common.typeutils import downcast, not_none

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.schema import constraints as s_constr
from edb.schema import globals as s_globals
from edb.schema import indexes as s_indexes
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

    skip_subtypes: bool = kwargs.get('skip_subtypes', False)
    ignore_rewrites: bool = kwargs.get('ignore_rewrites', False)
    rw_key = (stype, skip_subtypes)

    if not ignore_rewrites and ctx.suppress_rewrites:
        from . import policies
        ignore_rewrites = kwargs['ignore_rewrites'] = (
            policies.should_ignore_rewrite(stype, ctx=ctx))

    if (
        not ignore_rewrites
        and rw_key not in ctx.env.type_rewrites
        and isinstance(stype, s_objtypes.ObjectType)
        and ctx.env.options.apply_query_rewrites
    ):
        from . import policies
        policies.try_type_rewrite(stype, skip_subtypes=skip_subtypes, ctx=ctx)

    typeref = typegen.type_to_typeref(stype, env=ctx.env)
    ir_set = ircls(typeref=typeref, **kwargs)
    ctx.env.set_types[ir_set] = stype
    return ir_set


def new_empty_set(*, stype: Optional[s_types.Type]=None, alias: str='e',
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


class KeepCurrentT(enum.Enum):
    KeepCurrent = 0


KeepCurrent: Final = KeepCurrentT.KeepCurrent


def new_set_from_set(
        ir_set: irast.Set, *,
        merge_current_ns: bool=False,
        path_scope_id: Optional[int | KeepCurrentT]=KeepCurrent,
        path_id: Optional[irast.PathId]=None,
        stype: Optional[s_types.Type]=None,
        rptr: Optional[irast.Pointer | KeepCurrentT]=KeepCurrent,
        expr: Optional[irast.Expr | KeepCurrentT]=KeepCurrent,
        context: Optional[parsing.ParserContext]=None,
        is_binding: Optional[irast.BindingKind]=None,
        is_materialized_ref: Optional[bool]=None,
        is_visible_binding_ref: Optional[bool]=None,
        skip_subtypes: Optional[bool]=None,
        ignore_rewrites: Optional[bool]=None,
        ctx: context.ContextLevel) -> irast.Set:
    """Create a new ir.Set from another ir.Set.

    The new Set inherits source everything from the old set that
    is not overriden.

    If *merge_current_ns* is set, the new Set's path_id will be
    namespaced with the currently active scope namespace.
    """
    if path_id is None:
        path_id = ir_set.path_id
    if merge_current_ns:
        path_id = path_id.merge_namespace(ctx.path_id_namespace)
    if stype is None:
        stype = get_set_type(ir_set, ctx=ctx)
    if path_scope_id == KeepCurrent:
        path_scope_id = ir_set.path_scope_id
    if rptr == KeepCurrent:
        rptr = ir_set.rptr
    if expr == KeepCurrent:
        expr = ir_set.expr
    if context is None:
        context = ir_set.context
    if is_binding is None:
        is_binding = ir_set.is_binding
    if is_materialized_ref is None:
        is_materialized_ref = ir_set.is_materialized_ref
    if is_visible_binding_ref is None:
        is_visible_binding_ref = ir_set.is_visible_binding_ref
    if skip_subtypes is None:
        skip_subtypes = ir_set.skip_subtypes
    if ignore_rewrites is None:
        ignore_rewrites = ir_set.ignore_rewrites
    return new_set(
        path_id=path_id,
        path_scope_id=path_scope_id,
        stype=stype,
        expr=expr,
        rptr=rptr,
        context=context,
        is_binding=is_binding,
        is_materialized_ref=is_materialized_ref,
        is_visible_binding_ref=is_visible_binding_ref,
        skip_subtypes=skip_subtypes,
        ignore_rewrites=ignore_rewrites,
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


def raise_self_insert_error(
    stype: s_obj.Object, source_context: Optional[parsing.ParserContext], *,
    ctx: context.ContextLevel,
) -> NoReturn:
    dname = stype.get_displayname(ctx.env.schema)
    raise errors.QueryError(
        f'invalid reference to {dname}: '
        f'self-referencing INSERTs are not allowed',
        hint=(
            f'Use DETACHED if you meant to refer to an '
            f'uncorrelated {dname} set'
        ),
        context=source_context,
    )


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

    computables: list[irast.Set] = []
    path_sets: list[irast.Set] = []

    for i, step in enumerate(expr.steps):
        is_computable = False

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
                path_tip = new_set_from_set(refnode, ctx=ctx)
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
                    if not stype.get_expr(ctx.env.schema):
                        raise errors.InvalidReferenceError(
                            f"cannot refer to alias link helper type "
                            f"'{stype.get_name(ctx.env.schema)}'",
                            context=step.context,
                        )

                    # This is a schema-level view, as opposed to
                    # a WITH-block or inline alias view.
                    stype = stmtctx.declare_view_from_schema(stype, ctx=ctx)

                view_set = ctx.view_sets.get(stype)
                if view_set is not None:
                    view_scope_info = ctx.env.path_scope_map[view_set]
                    path_tip = new_set_from_set(
                        view_set,
                        merge_current_ns=(
                            view_scope_info.pinned_path_id_ns is None
                        ),
                        is_binding=view_scope_info.binding_kind,
                        context=step.context,
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
                if _is_computable_ptr(ptrcls, direction, ctx=ctx):
                    is_computable = True

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
                    path_tip, typ, optional=False, source_context=step.context,
                    ctx=ctx)
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
            is_subquery = isinstance(step, qlast.Query)
            with ctx.newscope(fenced=is_subquery) as subctx:
                subctx.view_rptr = None
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
        # the source of the computable *not* under that namespace,
        # so we need to do some remapping.
        if mapped := get_view_map_remapping(path_tip.path_id, ctx):
            path_tip = new_set_from_set(
                path_tip, path_id=mapped.path_id, ctx=ctx)
            if path_tip.rptr:
                path_tip.rptr = path_tip.rptr.replace(target=path_tip)
            # If we are remapping a source path, then we know that
            # the path is visible, so we shouldn't recompile it
            # if it is a computable path.
            is_computable = False

        if is_computable:
            computables.append(path_tip)

        if pathctx.path_is_inserting(path_tip.path_id, ctx=ctx):
            raise_self_insert_error(stype, step.context, ctx=ctx)

        # Don't track this step of the path if it didn't change the set
        # (probably because of do-nothing intersection)
        if not path_sets or path_sets[-1] != path_tip:
            path_sets.append(path_tip)

    if expr.context:
        path_tip.context = expr.context
    pathctx.register_set_in_scope(path_tip, ctx=ctx)

    for ir_set in computables:
        # Compile the computables in sibling scopes to the subpaths
        # they are computing. Note that the path head will be visible
        # from inside the computable scope. That's fine.

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
            comp_ir_set = computable_ptr_set(
                ir_set.rptr, srcctx=ir_set.context, ctx=subctx)
            i = path_sets.index(ir_set)
            if i != len(path_sets) - 1:
                prptr = path_sets[i + 1].rptr
                assert prptr is not None
                prptr.source = comp_ir_set
            else:
                path_tip = comp_ir_set
            path_sets[i] = comp_ir_set

    return path_tip


def resolve_special_anchor(
        anchor: qlast.SpecialAnchor, *,
        ctx: context.ContextLevel) -> irast.Set:

    # '__source__' and '__subject__` can only appear as the
    # starting path label syntactically and must be pre-populated
    # by the compile() caller.

    assert isinstance(anchor, qlast.SpecialAnchor)
    token = anchor.name

    path_tip = ctx.anchors.get(token)

    if not path_tip:
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
    ptrcls, path_id_ptrcls = resolve_ptr_with_intersections(
        source,
        ptr_name,
        upcoming_intersections=upcoming_intersections,
        track_ref=expr,
        direction=direction,
        source_context=source_context,
        ctx=ctx)

    return extend_path(
        path_tip, ptrcls, direction,
        path_id_ptrcls=path_id_ptrcls,
        ignore_computable=ignore_computable, srcctx=source_context,
        ctx=ctx)


def _add_target_schema_refs(
    stype: Optional[s_obj.Object],
    ctx: context.ContextLevel,
) -> None:
    """Add the appropriate schema dependencies for a pointer target.

    The only annoying bit is we need to handle unions/intersections also."""
    if not isinstance(stype, s_objtypes.ObjectType):
        return
    ctx.env.add_schema_ref(stype, None)
    schema = ctx.env.schema
    for obj in (
        stype.get_union_of(schema).objects(schema) +
        stype.get_intersection_of(schema).objects(schema)
    ):
        ctx.env.add_schema_ref(obj, None)


def resolve_ptr(
    near_endpoint: s_obj.Object,
    pointer_name: str,
    *,
    direction: s_pointers.PointerDirection = (
        s_pointers.PointerDirection.Outbound
    ),
    source_context: Optional[parsing.ParserContext] = None,
    track_ref: Optional[Union[qlast.Base, Literal[False]]],
    ctx: context.ContextLevel,
) -> s_pointers.Pointer:
    return resolve_ptr_with_intersections(
        near_endpoint, pointer_name,
        direction=direction, source_context=source_context,
        track_ref=track_ref, ctx=ctx)[0]


def resolve_ptr_with_intersections(
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
) -> tuple[s_pointers.Pointer, s_pointers.Pointer]:
    """Resolve a pointer, taking into account upcoming intersections.

    The key trickiness here is that *two* pointers are returned:
      * one that (for backlinks) includes just the pointers that actually
        may be used
      * one for use in path ids, that does not do that filtering, so that
        path factoring works properly.
    """

    if not isinstance(near_endpoint, s_sources.Source):
        # Reference to a property on non-object
        msg = 'invalid property reference on a primitive type expression'
        raise errors.InvalidReferenceError(msg, context=source_context)

    ptr: Optional[s_pointers.Pointer] = None

    if direction is s_pointers.PointerDirection.Outbound:
        path_id_ptr = ptr = near_endpoint.maybe_get_ptr(
            ctx.env.schema,
            s_name.UnqualName(pointer_name),
        )

        # If we couldn't anything, but the source is a computed backlink,
        # look for a link property on the reverse side of it. This allows
        # us to access link properties in both directions on links, including
        # when the backlink has been stuck in a computed.
        if (
            ptr is None
            and isinstance(near_endpoint, s_pointers.Pointer)
            and (back := near_endpoint.get_computed_backlink(ctx.env.schema))
            and isinstance(back, s_links.Link)
            and (nptr := back.maybe_get_ptr(
                ctx.env.schema,
                s_name.UnqualName(pointer_name),
            ))
            # We can't handle computeds yet, since we would need to switch
            # around a bunch of stuff inside them.
            and not nptr.is_pure_computable(ctx.env.schema)
        ):
            ptr = schemactx.derive_ptr(nptr, near_endpoint, ctx=ctx)
            path_id_ptr = ptr

        if ptr is not None:
            ref = ptr.get_nearest_non_derived_parent(ctx.env.schema)
            if track_ref is not False:
                ctx.env.add_schema_ref(ref, track_ref)
                _add_target_schema_refs(
                    ref.get_target(ctx.env.schema), ctx=ctx)

    else:
        assert isinstance(near_endpoint, s_types.Type)
        concrete_near_endpoint = schemactx.concretify(near_endpoint, ctx=ctx)
        ptrs = concrete_near_endpoint.getrptrs(
            ctx.env.schema, pointer_name, sources=far_endpoints)
        if ptrs:
            # If this reverse pointer access is followed by
            # intersections, we filter out any pointers that
            # couldn't be picked up by the intersections.
            # If a pointer doesn't get picked up, we look to see
            # if any of its children might.
            #
            # This both allows us to avoid creating spurious
            # dependencies when reverse links are used in schemas
            # and to generate a precise set of possible pointers.
            dep_ptrs = set()
            wl = list(ptrs)
            while wl:
                ptr = wl.pop()
                if (src := ptr.get_source(ctx.env.schema)):
                    if all(
                        src.issubclass(ctx.env.schema, typ)
                        for typ in upcoming_intersections
                    ):
                        dep_ptrs.add(ptr)
                    else:
                        wl.extend(ptr.children(ctx.env.schema))

            if track_ref is not False:
                for p in dep_ptrs:
                    p = p.get_nearest_non_derived_parent(ctx.env.schema)
                    ctx.env.add_schema_ref(p, track_ref)
                    _add_target_schema_refs(
                        p.get_source(ctx.env.schema), ctx=ctx)

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
            concrete_ptr = schemactx.get_union_pointer(
                ptrname=s_name.UnqualName(pointer_name),
                source=near_endpoint,
                direction=direction,
                components=concrete_ptrs,
                opaque=opaque,
                modname=ctx.derived_target_module,
                ctx=ctx,
            )
            path_id_ptr = ptr = concrete_ptr
            # If we have an upcoming intersection that has actual
            # pointer targets, we want to put the filtered down
            # version into the AST, so that we can more easily use
            # that information in compilation.  But we still need the
            # *full* union in the path_ids, for factoring.
            if dep_ptrs and upcoming_intersections:
                ptr = schemactx.get_union_pointer(
                    ptrname=s_name.UnqualName(pointer_name),
                    source=near_endpoint,
                    direction=direction,
                    components=dep_ptrs,
                    opaque=opaque,
                    modname=ctx.derived_target_module,
                    ctx=ctx,
                )

    if ptr and path_id_ptr:
        return ptr, path_id_ptr

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

    if (
        direction is s_pointers.PointerDirection.Outbound
        # In some call sites, we call resolve_ptr "experimentally",
        # not tracking references and swallowing failures. Don't do an
        # expensive (30% of compilation time in some benchmarks!)
        # error enrichment for cases that won't really error.
        and track_ref is not False
    ):
        s_utils.enrich_schema_lookup_error(
            err,
            s_name.UnqualName(pointer_name),
            modaliases=ctx.modaliases,
            item_type=s_pointers.Pointer,
            pointer_parent=near_endpoint,
            schema=ctx.env.schema,
        )

    raise err


def extend_path(
    source_set: irast.Set,
    ptrcls: s_pointers.Pointer,
    direction: PtrDir = PtrDir.Outbound,
    *,
    path_id_ptrcls: Optional[s_pointers.Pointer] = None,
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
                    source_set, source, optional=True, source_context=srcctx,
                    ctx=ctx)

        src_path_id = source_set.path_id

    orig_ptrcls = ptrcls

    # If there is a particular specified ptrcls for the pathid, use
    # it, otherwise use the actual ptrcls. This comes up with
    # intersections on backlinks, where we want to use a precise ptr
    # in the IR for compilation reasons but need a path_id that is
    # independent of intersections.
    path_id_ptrcls = path_id_ptrcls or ptrcls

    # Find the pointer definition site.
    # This makes it so that views don't change path ids unless they are
    # introducing some computation.
    ptrcls = ptrcls.get_nearest_defined(ctx.env.schema)
    path_id_ptrcls = path_id_ptrcls.get_nearest_defined(ctx.env.schema)

    path_id = pathctx.extend_path_id(
        src_path_id,
        ptrcls=path_id_ptrcls,
        direction=direction,
        ns=ctx.path_id_namespace,
        ctx=ctx,
    )

    target = orig_ptrcls.get_far_endpoint(ctx.env.schema, direction)
    assert isinstance(target, s_types.Type)
    target_set = new_set(
        stype=target, path_id=path_id, context=srcctx, ctx=ctx)

    ptr = irast.Pointer(
        source=source_set,
        target=target_set,
        direction=direction,
        ptrref=typegen.ptr_to_ptrref(ptrcls, ctx=ctx),
        is_definition=False,
    )

    target_set.rptr = ptr
    is_computable = _is_computable_ptr(ptrcls, direction, ctx=ctx)
    if not ignore_computable and is_computable:
        target_set = computable_ptr_set(
            ptr,
            same_computable_scope=same_computable_scope,
            srcctx=srcctx,
            ctx=ctx,
        )

    return target_set


def needs_rewrite_existence_assertion(
    ptrcls: s_pointers.PointerLike,
    direction: PtrDir,
    *,
    ctx: context.ContextLevel,
) -> bool:
    """Determines if we need to inject an assert_exists for a pointer

    Required pointers to types with access policies need to have an
    assert_exists added
    """

    return bool(
        not ctx.suppress_rewrites
        and ptrcls.get_required(ctx.env.schema)
        and direction == PtrDir.Outbound
        and (target := ptrcls.get_target(ctx.env.schema))
        and ctx.env.type_rewrites.get((target, False))
        and ptrcls.get_shortname(ctx.env.schema).name != '__type__'
    )


def is_injected_computable_ptr(
    ptrcls: s_pointers.PointerLike,
    direction: PtrDir,
    *,
    ctx: context.ContextLevel,
) -> bool:
    return (
        ctx.env.options.apply_query_rewrites
        and ptrcls not in ctx.active_computeds
        and (
            bool(ptrcls.get_schema_reflection_default(ctx.env.schema))
            or needs_rewrite_existence_assertion(ptrcls, direction, ctx=ctx)
        )
    )


def _is_computable_ptr(
    ptrcls: s_pointers.PointerLike,
    direction: PtrDir,
    *,
    ctx: context.ContextLevel,
) -> bool:
    try:
        qlexpr = ctx.env.source_map[ptrcls].qlexpr
    except KeyError:
        pass
    else:
        return qlexpr is not None

    return (
        bool(ptrcls.get_expr(ctx.env.schema))
        or is_injected_computable_ptr(ptrcls, direction, ctx=ctx)
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
        ctx.env.get_schema_type_and_track(s_name.QualName('std', 'str')),
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
        ptrref=downcast(irast.TupleIndirectionPointerRef, path_id.rptr()),
        direction=not_none(path_id.rptr_dir()),
    )

    ti_set.rptr = ptr

    return ti_set


def type_intersection_set(
    source_set: irast.Set,
    stype: s_types.Type,
    *,
    optional: bool,
    source_context: Optional[parsing.ParserContext] = None,
    ctx: context.ContextLevel,
) -> irast.Set:
    """Return an interesection of *source_set* with type *stype*."""

    arg_type = get_set_type(source_set, ctx=ctx)

    result = schemactx.apply_intersection(arg_type, stype, ctx=ctx)
    if result.stype == arg_type:
        return source_set

    poly_set = new_set(stype=result.stype, context=source_context, ctx=ctx)
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
                    # assert isinstance(component, irast.PointerRef)
                    # rptr_specialization.append(component)

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
        ptrref=downcast(irast.TypeIntersectionPointerRef, ptrref),
        direction=not_none(poly_set.path_id.rptr_dir()),
        optional=optional,
    )

    poly_set.rptr = ptr

    return poly_set


def class_set(
        stype: s_types.Type, *,
        path_id: Optional[irast.PathId]=None,
        skip_subtypes: bool=False,
        ignore_rewrites: bool=False,
        ctx: context.ContextLevel) -> irast.Set:

    if path_id is None:
        path_id = pathctx.get_path_id(stype, ctx=ctx)
    return new_set(
        path_id=path_id, stype=stype,
        skip_subtypes=skip_subtypes, ignore_rewrites=ignore_rewrites, ctx=ctx)


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
        ir_set = new_set_from_set(ir_set, stype=type_override, ctx=ctx)

        stype = type_override

    if srcctx is not None:
        ir_set = new_set_from_set(ir_set, context=srcctx, ctx=ctx)

    if (isinstance(ir_set, irast.EmptySet)
            and (stype is None or stype.is_any(ctx.env.schema))
            and typehint is not None):
        inference.amend_empty_set_type(ir_set, typehint, env=ctx.env)
        stype = get_set_type(ir_set, ctx=ctx)

    if (
        typehint is not None
        and stype != typehint
        and not stype.implicitly_castable_to(typehint, ctx.env.schema)
    ):
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


def fixup_computable_source_set(
    source_set: irast.Set,
    *,
    ctx: context.ContextLevel,
) -> irast.Set:
    source_scls = get_set_type(source_set, ctx=ctx)
    # process_view() may generate computable pointer expressions
    # in the form "self.linkname".  To prevent infinite recursion,
    # self must resolve to the parent type of the view NOT the view
    # type itself.  Similarly, when resolving computable link properties
    # make sure that we use the parent of derived ptrcls.
    if source_scls.is_view(ctx.env.schema):
        source_set_stype = source_scls.peel_view(ctx.env.schema)
        source_set = new_set_from_set(
            source_set, stype=source_set_stype, ctx=ctx)
        source_set.shape = ()
        if source_set.rptr is not None:
            source_rptrref = source_set.rptr.ptrref
            if source_rptrref.base_ptr is not None:
                source_rptrref = source_rptrref.base_ptr
            source_set.rptr = source_set.rptr.replace(
                target=source_set,
                ptrref=source_rptrref,
                is_definition=True,
            )
    return source_set


def computable_ptr_set(
    rptr: irast.Pointer,
    *,
    same_computable_scope: bool=False,
    srcctx: Optional[parsing.ParserContext]=None,
    ctx: context.ContextLevel,
) -> irast.Set:
    """Return ir.Set for a pointer defined as a computable."""
    ptrcls = typegen.ptrcls_from_ptrref(rptr.ptrref, ctx=ctx)
    source_scls = get_set_type(rptr.source, ctx=ctx)
    source_set = fixup_computable_source_set(rptr.source, ctx=ctx)
    ptrcls_to_shadow = None

    qlctx: Optional[context.ContextLevel]

    try:
        comp_info = ctx.env.source_map[ptrcls]
        qlexpr = comp_info.qlexpr
        assert isinstance(comp_info.context, context.ContextLevel)
        qlctx = comp_info.context
        inner_source_path_id = comp_info.path_id
        path_id_ns = comp_info.path_id_ns
    except KeyError:
        comp_expr = ptrcls.get_expr(ctx.env.schema)
        schema_qlexpr: Optional[qlast.Expr] = None
        if comp_expr is None and ctx.env.options.apply_query_rewrites:
            assert isinstance(ptrcls, s_pointers.Pointer)
            ptrcls_n = ptrcls.get_shortname(ctx.env.schema).name
            path = qlast.Path(
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
            )

            schema_deflt = ptrcls.get_schema_reflection_default(ctx.env.schema)
            if schema_deflt is not None:
                schema_qlexpr = qlast.BinOp(
                    left=path,
                    right=qlparser.parse_fragment(schema_deflt),
                    op='??',
                )

            if needs_rewrite_existence_assertion(
                    ptrcls, PtrDir.Outbound, ctx=ctx):
                # Wrap it in a dummy select so that we can't optimize away
                # the assert_exists.
                # TODO: do something less bad
                arg = qlast.SelectQuery(
                    result=path, where=qlast.BooleanConstant(value='true'))
                vname = ptrcls.get_verbosename(
                    ctx.env.schema, with_parent=True)
                msg = f'required {vname} is hidden by access policy'
                if ctx.active_computeds:
                    cur = next(reversed(ctx.active_computeds))
                    vname = cur.get_verbosename(
                        ctx.env.schema, with_parent=True)
                    msg += f' (while evaluating computed {vname})'

                schema_qlexpr = qlast.FunctionCall(
                    func=('__std__', 'assert_exists'),
                    args=[arg],
                    kwargs={'message': qlast.StringConstant(value=msg)},
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

            comp_qlexpr = comp_expr.qlast
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
        qlexpr = astutils.ensure_ql_query(schema_qlexpr)
        qlctx = None
        path_id_ns = None

    newctx: Callable[[], ContextManager[context.ContextLevel]]

    if qlctx is None:
        # Schema-level computed link or property, the context should
        # still have a source.
        newctx = _get_schema_computed_ctx(
            rptr=rptr,
            source=source_set,
            ctx=ctx)

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
        assert isinstance(source_scls, s_sources.Source)
        assert isinstance(ptrcls, s_pointers.Pointer)

        subctx.active_computeds = subctx.active_computeds.copy()
        if ptrcls_to_shadow:
            assert isinstance(ptrcls_to_shadow, s_pointers.Pointer)
            subctx.active_computeds.add(ptrcls_to_shadow)
        subctx.active_computeds.add(ptrcls)
        if result_stype != base_object:
            subctx.view_scls = result_stype
        subctx.view_rptr = context.ViewRPtr(
            source=source_scls, ptrcls=ptrcls)
        subctx.anchors[qlast.Source().name] = source_set
        subctx.empty_result_type_hint = ptrcls.get_target(ctx.env.schema)
        subctx.partial_path_prefix = source_set
        # On a mutation, make the expr_exposed. This corresponds with
        # a similar check on is_mutation in _normalize_view_ptr_expr.
        if (source_scls.get_expr_type(ctx.env.schema)
                != s_types.ExprType.Select):
            subctx.expr_exposed = context.Exposure.EXPOSED

        comp_ir_set = dispatch.compile(qlexpr, ctx=subctx)

    comp_ir_set = new_set_from_set(
        comp_ir_set, path_id=result_path_id, rptr=rptr, context=srcctx,
        merge_current_ns=True,
        ctx=ctx)

    rptr.target = comp_ir_set

    maybe_materialize(ptrcls, comp_ir_set, ctx=ctx)

    return comp_ir_set


def _get_schema_computed_ctx(
    *,
    rptr: irast.Pointer,
    source: irast.Set,
    ctx: context.ContextLevel
) -> Callable[[], ContextManager[context.ContextLevel]]:

    @contextlib.contextmanager
    def newctx() -> Iterator[context.ContextLevel]:
        with ctx.detached() as subctx:
            source_scope = pathctx.get_set_scope(rptr.source, ctx=ctx)
            if source_scope and source_scope.namespaces:
                subctx.path_id_namespace |= source_scope.namespaces

            # Get the type of the actual location where the computed pointer
            # was defined in the schema, since that is the type that must
            # be used in the view map, since that is the type that might
            # be *referenced in the definition*.
            ptr = typegen.ptrcls_from_ptrref(rptr.ptrref, ctx=ctx)
            assert isinstance(ptr, s_pointers.Pointer)
            ptr = ptr.maybe_get_topmost_concrete_base(ctx.env.schema) or ptr
            src = ptr.get_source(ctx.env.schema)

            # If the source is an abstract pointer, then we don't have
            # a full path to bind in the computed. Otherwise use a
            # path derived from the pointer source.
            if not (
                isinstance(src, s_pointers.Pointer)
                and src.generic(ctx.env.schema)
            ):
                inner_path_id = not_none(irast.PathId.from_pointer(
                    ctx.env.schema, ptr, namespace=subctx.path_id_namespace,
                ).src_path())
                remapped_source = new_set_from_set(
                    rptr.source, rptr=rptr.source.rptr, ctx=ctx
                )
                update_view_map(inner_path_id, remapped_source, ctx=subctx)

            yield subctx

    return newctx


def update_view_map(
    path_id: irast.PathId,
    remapped_source: irast.Set,
    *,
    ctx: context.ContextLevel
) -> None:
    ctx.view_map = ctx.view_map.new_child()
    key = path_id.strip_namespace(path_id.namespace)
    old = ctx.view_map.get(key, ())
    ctx.view_map[key] = ((path_id, remapped_source),) + old


def get_view_map_remapping(
    path_id: irast.PathId, ctx: context.ContextLevel
) -> Optional[irast.Set]:
    """Perform path_id remapping based on outer views

    This is a little fiddly, since we may have
    picked up *additional* namespaces.
    """
    key = path_id.strip_namespace(path_id.namespace)
    entries = ctx.view_map.get(key, ())
    fixed_path_id = path_id.merge_namespace(ctx.path_id_namespace, deep=True)
    for inner_path_id, mapped in entries:
        fixed_inner = inner_path_id.merge_namespace(
            ctx.path_id_namespace, deep=True)

        if fixed_inner == fixed_path_id:
            return mapped
    return None


def remap_path_id(
    path_id: irast.PathId, ctx: context.ContextLevel
) -> irast.PathId:
    """Remap a path_id based on the view_map, one step at a time.

    This is intended to mirror what happens to paths in compile_path.
    """
    new_id = None
    hit = False
    for prefix in path_id.iter_prefixes():
        if not new_id:
            new_id = prefix
        else:
            nrptr, dir = prefix.rptr(), prefix.rptr_dir()
            assert nrptr and dir
            new_id = new_id.extend(
                ptrref=nrptr, direction=dir, ns=prefix.namespace)

        if mapped := get_view_map_remapping(new_id, ctx):
            hit = True
            new_id = mapped.path_id

    assert new_id and (new_id == path_id or hit)
    return new_id


def _get_computable_ctx(
    *,
    rptr: irast.Pointer,
    source: irast.Set,
    source_scls: s_types.Type,
    inner_source_path_id: irast.PathId,
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

            subctx.view_nodes = qlctx.view_nodes.copy()
            subctx.view_map = ctx.view_map.new_child()
            subctx.view_sets = ctx.view_sets.copy()

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
            subns.add(ctx.aliases.get('ns'))

            # Include the namespace from the source in the namespace
            # we compile under. This helps make sure the remapping
            # lines up.
            subns |= qlctx.path_id_namespace

            subctx.pending_stmt_full_path_id_namespace = frozenset(subns)

            # If one of the sources present at the definition site is still
            # visible, make sure to hang on to the remapping.
            for entry in qlctx.view_map.values():
                for map_path_id, remapped in entry:
                    if subctx.path_scope.is_visible(map_path_id):
                        update_view_map(map_path_id, remapped, ctx=subctx)

            inner_path_id = inner_source_path_id.merge_namespace(subns)
            with subctx.new() as remapctx:
                remapctx.path_id_namespace |= subns
                # We need to run the inner_path_id through the same
                # remapping process that happens in compile_path, or
                # else the path id won't match, since the prefix will
                # get remapped first.
                inner_path_id = remap_path_id(inner_path_id, remapctx)

            remapped_source = new_set_from_set(
                rptr.source, rptr=rptr.source.rptr, ctx=ctx)
            update_view_map(inner_path_id, remapped_source, ctx=subctx)

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
        materialize_in_stmt.materialized_sets[stype.id] = (
            irast.MaterializedSet(
                materialized=ir, reason=reason, use_sets=[]))

    mat_set = materialize_in_stmt.materialized_sets[stype.id]
    mat_set.use_sets.append(ir)


def should_materialize(
    ir: irast.Base, *,
    ptrcls: Optional[s_pointers.Pointer]=None,
    materialize_visible: bool=False,
    skipped_bindings: AbstractSet[irast.PathId]=frozenset(),
    ctx: context.ContextLevel,
) -> Sequence[irast.MaterializeReason]:
    volatility = inference.infer_volatility(ir, ctx.env, exclude_dml=True)
    reasons: List[irast.MaterializeReason] = []

    if volatility.is_volatile():
        reasons.append(irast.MaterializeVolatile())

    if not isinstance(ir, irast.Set):
        return reasons

    if irtyputils.is_free_object(ir.typeref):
        reasons.append(irast.MaterializeVolatile())

    typ = get_set_type(ir, ctx=ctx)

    assert ir.path_scope_id is not None

    # For shape elements, we need to materialize when they reference
    # bindings that are visible from that point. This means that doing
    # WITH/FOR bindings internally is fine, but referring to
    # externally bound things needs materialization. We can't actually
    # do this visibility analysis until we are done, though, so
    # instead we just store the bindings.
    if (
        materialize_visible
        and (vis := irutils.find_potentially_visible(
            ir,
            ctx.env.scope_tree_nodes[ir.path_scope_id],
            ctx.env.scope_tree_nodes, skipped_bindings))
    ):
        reasons.append(irast.MaterializeVisible(
            sets=vis, path_scope_id=ir.path_scope_id))

    if ptrcls and ptrcls in ctx.env.source_map:
        reasons += ctx.env.source_map[ptrcls].should_materialize

    for r in should_materialize_type(typ, ctx=ctx):
        # Rewrite visibility reasons from the typ to reflect this,
        # the real bind point.
        if isinstance(r, irast.MaterializeVolatile):
            reasons.append(r)
        else:
            reasons.append(
                irast.MaterializeVisible(
                    sets=r.sets, path_scope_id=ir.path_scope_id))

    return reasons


def should_materialize_type(
    typ: s_types.Type, *, ctx: context.ContextLevel
) -> List[irast.MaterializeReason]:
    schema = ctx.env.schema
    reasons: List[irast.MaterializeReason] = []
    if isinstance(
            typ, (s_objtypes.ObjectType, s_pointers.Pointer)):
        for pointer in typ.get_pointers(schema).objects(schema):
            if pointer in ctx.env.source_map:
                reasons += ctx.env.source_map[pointer].should_materialize
    elif isinstance(typ, s_types.Collection):
        for sub in typ.get_subtypes(schema):
            reasons += should_materialize_type(sub, ctx=ctx)

    return reasons


def get_global_param(
        glob: s_globals.Global, * , ctx: context.ContextLevel) -> irast.Global:
    name = glob.get_name(ctx.env.schema)

    if name not in ctx.env.query_globals:
        param_name = f'__edb_global_{len(ctx.env.query_globals)}__'

        target = glob.get_target(ctx.env.schema)
        target_typeref = typegen.type_to_typeref(target, env=ctx.env)

        ctx.env.query_globals[name] = irast.Global(
            name=param_name,
            required=False,
            schema_type=target,
            ir_type=target_typeref,
            global_name=name,
            has_present_arg=glob.needs_present_arg(ctx.env.schema),
        )

    return ctx.env.query_globals[name]


def get_global_param_sets(
    glob: s_globals.Global, *, ctx: context.ContextLevel,
    is_implicit_global: bool=False,
) -> Tuple[irast.Set, Optional[irast.Set]]:
    param = get_global_param(glob, ctx=ctx)
    default = glob.get_default(ctx.env.schema)

    param_set = ensure_set(
        irast.Parameter(
            name=param.name,
            required=param.required and not bool(default),
            typeref=param.ir_type,
            is_implicit_global=is_implicit_global,
        ),
        ctx=ctx,
    )
    if glob.needs_present_arg(ctx.env.schema):
        present_set = ensure_set(
            irast.Parameter(
                name=param.name + "present__",
                required=True,
                typeref=typegen.type_to_typeref(
                    ctx.env.schema.get('std::bool', type=s_types.Type),
                    env=ctx.env),
                is_implicit_global=is_implicit_global,
            ),
            ctx=ctx,
        )
    else:
        present_set = None

    return param_set, present_set


def get_func_global_json_arg(
    *, ctx: context.ContextLevel
) -> irast.Set:
    json_type = ctx.env.schema.get('std::json', type=s_types.Type)
    json_typeref = typegen.type_to_typeref(json_type, env=ctx.env)
    name = '__edb_json_globals__'

    # If this is because we have json params, not because we're in a
    # function, we need to register it.
    if ctx.env.options.json_parameters:
        qname = s_name.QualName('__', name)
        ctx.env.query_globals[qname] = irast.Global(
            name=name,
            required=False,
            schema_type=json_type,
            ir_type=json_typeref,
            global_name=qname,
            has_present_arg=False,
        )

    return ensure_set(
        irast.Parameter(
            name=name,
            required=True,
            typeref=json_typeref,
        ),
        ctx=ctx,
    )


def get_func_global_param_sets(
    glob: s_globals.Global, *,
    ctx: context.ContextLevel,
) -> Tuple[qlast.Expr, Optional[qlast.Expr]]:
    # NB: updates ctx anchors

    if ctx.env.options.func_params is not None:
        # Make sure that we properly track the globals we use in functions
        get_global_param(glob, ctx=ctx)

    with ctx.new() as subctx:
        name = str(glob.get_name(ctx.env.schema))

        glob_set = get_func_global_json_arg(ctx=ctx)
        glob_anchor = qlast.FunctionCall(
            func=('__std__', 'json_get'),
            args=[
                subctx.create_anchor(glob_set, 'a'),
                qlast.StringConstant(value=str(name)),
            ],
        )

        target = glob.get_target(ctx.env.schema)
        type = typegen.type_to_ql_typeref(target, ctx=ctx)
        main_set = qlast.TypeCast(expr=glob_anchor, type=type)

        if glob.needs_present_arg(ctx.env.schema):
            present_set = qlast.UnaryOp(
                op='EXISTS',
                operand=glob_anchor,
            )
        else:
            present_set = None

    return main_set, present_set


def get_globals_as_json(
    globs: Sequence[s_globals.Global], *,
    ctx: context.ContextLevel,
    srcctx: Optional[parsing.ParserContext],
) -> irast.Set:
    """Build a json object that contains the values of `globs`

    The format of the object is simply
       {"<glob name 1>": <json>glob_val_1, ...},
    where values that are unset or set to {} are represented as null,
    with one catch:
       for globals that need "present" arguments (that is, optional globals
       with default values), we need to distinguish between the global
       being unset and being set to {}. In that case, we represent being
       set to {} with null and being unset by omitting it from the object.
    """
    # TODO: arrange to compute this once per query, in a CTE or some such?

    objctx = ctx.env.options.schema_object_context
    if globs and objctx in (s_constr.Constraint, s_indexes.Index):
        typname = objctx.get_schema_class_displayname()
        # XXX: or should we pass in empty globals, in this situation?
        raise errors.SchemaDefinitionError(
            f'functions that reference global variables cannot be called '
            f'from {typname}',
            context=srcctx)

    null_expr = qlast.FunctionCall(
        func=('__std__', 'to_json'),
        args=[qlast.StringConstant(value="null")],
    )

    with ctx.new() as subctx:
        subctx.anchors = subctx.anchors.copy()
        normal_els = []
        full_objs: list[qlast.Expr] = []

        json_type = qlast.TypeName(maintype=qlast.ObjectRef(
            module='__std__', name='json'))

        for glob in globs:
            param, present = get_global_param_sets(
                glob, is_implicit_global=True, ctx=ctx)
            # The name of the global isn't syntactically a valid identifier
            # for a namedtuple element but nobody can stop us!
            name = str(glob.get_name(ctx.env.schema))

            main_param = subctx.create_anchor(param, 'a')
            tuple_el = qlast.TupleElement(
                name=qlast.ObjectRef(name=name),
                val=qlast.BinOp(
                    op='??',
                    left=qlast.TypeCast(expr=main_param, type=json_type),
                    right=null_expr,
                )
            )

            if not present:
                # For normal globals, just stick the element in the tuple.
                normal_els.append(tuple_el)
            else:
                # For globals with a present arg, we conditionally
                # construct a one-element object if it is present
                # and an empty object if it is not. These are
                # be combined using ++.
                present_param = subctx.create_anchor(present, 'a')
                tup = qlast.TypeCast(
                    expr=qlast.NamedTuple(elements=[tuple_el]),
                    type=json_type,
                )

                full_objs.append(qlast.IfElse(
                    condition=present_param,
                    if_expr=tup,
                    else_expr=qlast.FunctionCall(
                        func=('__std__', 'to_json'),
                        args=[qlast.StringConstant(value="{}")],
                    )
                ))

        # If access policies are disabled, stick a value in the blob
        # to indicate that.  We do this using a full object so it
        # works in constraints and the like, where the tuple->json cast
        # isn't supported yet.
        if (
            not ctx.env.options.apply_user_access_policies
            or not ctx.env.options.apply_query_rewrites
        ):
            full_objs.append(qlast.FunctionCall(
                func=('__std__', 'to_json'),
                args=[qlast.StringConstant(
                    value='{"__disable_access_policies": true}'
                )],
            ))

        full_expr: qlast.Expr
        if not normal_els and not full_objs:
            full_expr = null_expr
        else:
            simple_obj = None
            if normal_els or not full_objs:
                simple_obj = qlast.TypeCast(
                    expr=qlast.NamedTuple(elements=normal_els),
                    type=json_type,
                )

            full_expr = astutils.extend_binop(simple_obj, *full_objs, op='++')

        return dispatch.compile(full_expr, ctx=subctx)
