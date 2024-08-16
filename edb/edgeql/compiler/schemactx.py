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


"""EdgeQL compiler schema helpers."""


from __future__ import annotations

from typing import (
    Any,
    Callable,
    Optional,
    Type,
    Union,
    Iterable,
    Sequence,
    Dict,
    NamedTuple,
    cast,
)

from edb import errors

from edb.common import parsing
from edb.ir import typeutils

from edb.schema import links as s_links
from edb.schema import name as sn
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import pseudo as s_pseudo
from edb.schema import scalars as s_scalars
from edb.schema import sources as s_sources
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import context


def get_schema_object(
    ref: qlast.BaseObjectRef,
    module: Optional[str]=None,
    *,
    item_type: Optional[Type[s_obj.Object]]=None,
    condition: Optional[Callable[[s_obj.Object], bool]]=None,
    label: Optional[str]=None,
    ctx: context.ContextLevel,
    span: Optional[parsing.Span] = None,
) -> s_obj.Object:

    if isinstance(ref, qlast.ObjectRef):
        if span is None:
            span = ref.span
        module = ref.module
        lname = ref.name
    elif isinstance(ref, qlast.PseudoObjectRef):
        return s_pseudo.PseudoType.get(ctx.env.schema, ref.name)
    else:
        raise AssertionError(f"Unhandled BaseObjectRef subclass: {ref!r}")

    name: sn.Name
    if module:
        name = sn.QualName(module=module, name=lname)
    else:
        name = sn.UnqualName(name=lname)

    try:
        stype = ctx.env.get_schema_object_and_track(
            name=name,
            expr=ref,
            modaliases=ctx.modaliases,
            type=item_type,
            condition=condition,
            label=label,
        )

    except errors.QueryError as e:
        s_utils.enrich_schema_lookup_error(
            e,
            name,
            modaliases=ctx.modaliases,
            schema=ctx.env.schema,
            item_type=item_type,
            pointer_parent=_get_partial_path_prefix_type(ctx),
            condition=condition,
            span=span,
        )
        raise

    if stype == ctx.defining_view:
        # stype is the view in process of being defined and as such is
        # not yet a valid schema object
        raise errors.SchemaDefinitionError(
            f'illegal self-reference in definition of {str(name)!r}',
            span=span)

    return stype


def _get_partial_path_prefix_type(
    ctx: context.ContextLevel,
) -> Optional[s_types.Type]:
    if ctx is None:
        return None
    ppp = ctx.partial_path_prefix
    if ppp is None or ppp.typeref is None:
        return None

    _, type = typeutils.ir_typeref_to_type(ctx.env.schema, ppp.typeref)
    return type


def get_schema_type(
    name: qlast.BaseObjectRef,
    module: Optional[str] = None,
    *,
    ctx: context.ContextLevel,
    label: Optional[str] = None,
    condition: Optional[Callable[[s_obj.Object], bool]] = None,
    item_type: Optional[Type[s_obj.Object]] = None,
    span: Optional[parsing.Span] = None,
) -> s_types.Type:
    if item_type is None:
        item_type = s_types.Type
    obj = get_schema_object(name, module, item_type=item_type,
                            condition=condition, label=label,
                            ctx=ctx, span=span)
    assert isinstance(obj, s_types.Type)
    return obj


def resolve_schema_name(
    name: str, module: str, *, ctx: context.ContextLevel
) -> Optional[sn.QualName]:
    schema_module = ctx.modaliases.get(module)
    if schema_module is None:
        return None
    else:
        return sn.QualName(name=name, module=schema_module)


def preserve_view_shape(
    base: Union[s_types.Type, s_pointers.Pointer],
    derived: Union[s_types.Type, s_pointers.Pointer],
    *,
    derived_name_base: Optional[sn.Name] = None,
    ctx: context.ContextLevel,
) -> None:
    """Copy a view shape to a child type, updating the pointers"""
    new = []
    schema = ctx.env.schema
    for ptr, op in ctx.env.view_shapes[base]:
        target = ptr.get_target(ctx.env.schema)
        assert target
        schema, nptr = ptr.get_derived(
            schema, cast(s_sources.Source, derived), target,
            derived_name_base=derived_name_base)
        new.append((nptr, op))
    ctx.env.view_shapes[derived] = new
    if isinstance(base, s_types.Type) and isinstance(derived, s_types.Type):
        ctx.env.view_shapes_metadata[derived] = (
            ctx.env.view_shapes_metadata[base]).replace()

    # All of the pointers should already exist, so nothing should have
    # been created.
    assert schema is ctx.env.schema


def derive_view(
    stype: s_types.Type,
    *,
    derived_name: Optional[sn.QualName] = None,
    derived_name_quals: Optional[Sequence[str]] = (),
    preserve_shape: bool = False,
    exprtype: s_types.ExprType = s_types.ExprType.Select,
    inheritance_merge: bool = True,
    attrs: Optional[Dict[str, Any]] = None,
    ctx: context.ContextLevel,
) -> s_types.Type:

    if derived_name is None:
        assert isinstance(stype, s_obj.DerivableObject)
        derived_name = derive_view_name(
            stype=stype, derived_name_quals=derived_name_quals,
            ctx=ctx)

    if attrs is None:
        attrs = {}
    else:
        attrs = dict(attrs)

    attrs['expr_type'] = exprtype

    derived: s_types.Type

    if isinstance(stype, s_types.Collection):
        ctx.env.schema, derived = stype.derive_subtype(
            ctx.env.schema,
            name=derived_name,
            attrs=attrs,
        )

    elif isinstance(stype, (s_objtypes.ObjectType, s_scalars.ScalarType)):
        existing = ctx.env.schema.get(
            derived_name, default=None, type=type(stype))
        if existing is not None:
            if ctx.recompiling_schema_alias:
                # When recompiling schema alias, we, essentially
                # re-derive the already-existing objects exactly.
                derived = existing
            else:
                raise AssertionError(
                    f'{type(stype).get_schema_class_displayname()}'
                    f' {derived_name!r} already exists',
                )
        else:
            ctx.env.schema, derived = stype.derive_subtype(
                ctx.env.schema,
                name=derived_name,
                inheritance_merge=inheritance_merge,
                inheritance_refdicts={'pointers'},
                mark_derived=True,
                transient=True,
                # When compiling aliases, we can't elide
                # @source/@target pointers, which normally we would
                # when creating a view.
                preserve_endpoint_ptrs=ctx.env.options.schema_view_mode,
                attrs=attrs,
                stdmode=ctx.env.options.bootstrap_mode,
            )

        if (
            stype.is_view(ctx.env.schema)
            # XXX: Previously, the main check here was just for
            # (not stype.is_non_concrete(...)). is_non_concrete isn't really the
            # right way to figure out if something is a view, since
            # some aliases will be generic. On changing it to is_view
            # instead, though, two GROUP BY tests that grouped
            # on the result of a group broke
            # (test_edgeql_group_by_group_by_03{a,b}).
            #
            # It's probably a bug that this matters in that case, and
            # it is an accident that group bindings are named in such
            # a way that they count as being generic, but for now
            # preserve that behavior.
            and not (
                stype.is_non_concrete(ctx.env.schema)
                and (view_ir := ctx.view_sets.get(stype))
                and (scope_info := ctx.env.path_scope_map.get(view_ir))
                and scope_info.binding_kind
            )
            and isinstance(derived, s_objtypes.ObjectType)
        ):
            assert isinstance(stype, s_objtypes.ObjectType)
            scls_pointers = stype.get_pointers(ctx.env.schema)
            derived_own_pointers = derived.get_pointers(ctx.env.schema)

            for pn, ptr in derived_own_pointers.items(ctx.env.schema):
                # This is a view of a view.  Make sure query-level
                # computable expressions for pointers are carried over.
                src_ptr = scls_pointers.get(ctx.env.schema, pn)
                computable_data = (
                    ctx.env.source_map.get(src_ptr) if src_ptr else None)
                if computable_data is not None:
                    ctx.env.source_map[ptr] = computable_data

                if src_ptr in ctx.env.pointer_specified_info:
                    ctx.env.pointer_derivation_map[src_ptr].append(ptr)

    else:
        raise TypeError("unsupported type in derive_view")

    ctx.view_nodes[derived.get_name(ctx.env.schema)] = derived

    if preserve_shape and stype in ctx.env.view_shapes:
        preserve_view_shape(stype, derived, ctx=ctx)

    return derived


def derive_ptr(
    ptr: s_pointers.Pointer,
    source: s_sources.Source,
    target: Optional[s_types.Type] = None,
    *qualifiers: str,
    derived_name: Optional[sn.QualName] = None,
    derived_name_quals: Optional[Sequence[str]] = (),
    preserve_shape: bool = False,
    derive_backlink: bool = False,
    inheritance_merge: bool = True,
    attrs: Optional[Dict[str, Any]] = None,
    ctx: context.ContextLevel,
) -> s_pointers.Pointer:

    if derived_name is None and ctx.derived_target_module:
        derived_name = derive_view_name(
            stype=ptr, derived_name_quals=derived_name_quals, ctx=ctx)

    if ptr.get_name(ctx.env.schema) == derived_name:
        qualifiers = qualifiers + (ctx.aliases.get('d'),)

    # If we are deriving a backlink, we just register that instead of
    # actually deriving from it.
    if derive_backlink:
        attrs = attrs.copy() if attrs else {}
        attrs['computed_link_alias'] = ptr
        attrs['computed_link_alias_is_backward'] = True
        ptr = ctx.env.schema.get('std::link', type=s_pointers.Pointer)

    ctx.env.schema, derived = ptr.derive_ref(
        ctx.env.schema,
        source,
        *qualifiers,
        target=target,
        name=derived_name,
        inheritance_merge=inheritance_merge,
        inheritance_refdicts={'pointers'},
        mark_derived=True,
        transient=True,
        # When compiling aliases, we can't elide
        # @source/@target pointers, which normally we would
        # when creating a view.
        preserve_endpoint_ptrs=ctx.env.options.schema_view_mode,
        attrs=attrs,
    )

    if not ptr.is_non_concrete(ctx.env.schema):
        if isinstance(derived, s_sources.Source):
            ptr = cast(s_links.Link, ptr)
            scls_pointers = ptr.get_pointers(ctx.env.schema)
            derived_own_pointers = derived.get_pointers(ctx.env.schema)

            for pn, ptr in derived_own_pointers.items(ctx.env.schema):
                # This is a view of a view.  Make sure query-level
                # computable expressions for pointers are carried over.
                src_ptr = scls_pointers.get(ctx.env.schema, pn)
                # mypy somehow loses the type argument in the
                # "pointers" ObjectIndex.
                assert isinstance(src_ptr, s_pointers.Pointer)
                computable_data = ctx.env.source_map.get(src_ptr)
                if computable_data is not None:
                    ctx.env.source_map[ptr] = computable_data

    if preserve_shape and ptr in ctx.env.view_shapes:
        preserve_view_shape(ptr, derived, ctx=ctx)

    return derived


def derive_view_name(
    stype: Optional[s_obj.DerivableObject],
    derived_name_quals: Optional[Sequence[str]] = (),
    derived_name_base: Optional[sn.Name] = None,
    *,
    ctx: context.ContextLevel,
) -> sn.QualName:
    if not derived_name_quals:
        derived_name_quals = (ctx.aliases.get('view'),)

    if ctx.derived_target_module:
        derived_name_module = ctx.derived_target_module
    else:
        derived_name_module = '__derived__'

    return s_obj.derive_name(
        ctx.env.schema,
        *derived_name_quals,
        module=derived_name_module,
        derived_name_base=derived_name_base,
        parent=stype,
    )


def get_union_type(
    types: Sequence[s_types.TypeT],
    *,
    opaque: bool = False,
    preserve_derived: bool = False,
    ctx: context.ContextLevel,
    span: Optional[parsing.Span] = None,
) -> s_types.TypeT:

    targets: Sequence[s_types.Type]
    if preserve_derived:
        targets = s_utils.simplify_union_types_preserve_derived(
            ctx.env.schema, types
        )
    else:
        targets = s_utils.simplify_union_types(
            ctx.env.schema, types
        )

    try:
        ctx.env.schema, union, _ = s_utils.ensure_union_type(
            ctx.env.schema, targets,
            opaque=opaque, transient=True)
    except errors.SchemaError as e:
        union_name = (
            '(' + ' | '.join(sorted(
            t.get_displayname(ctx.env.schema)
            for t in types
            )) + ')'
        )
        e.args = (
            (f'cannot create union {union_name} {e.args[0]}',)
            + e.args[1:]
        )
        e.set_span(span)
        raise e

    if (
        not isinstance(union, s_obj.QualifiedObject)
        or union.get_name(ctx.env.schema).module != '__derived__'
    ):
        ctx.env.add_schema_ref(union, expr=None)

    return cast(s_types.TypeT, union)


def get_intersection_type(
    types: Sequence[s_types.TypeT],
    *,
    ctx: context.ContextLevel,
) -> s_types.TypeT:

    targets: Sequence[s_types.Type]
    targets = s_utils.simplify_intersection_types(ctx.env.schema, types)
    ctx.env.schema, intersection = s_utils.ensure_intersection_type(
        ctx.env.schema, targets, transient=True
    )

    if (
        not isinstance(intersection, s_obj.QualifiedObject)
        or intersection.get_name(ctx.env.schema).module != '__derived__'
    ):
        ctx.env.add_schema_ref(intersection, expr=None)

    return cast(s_types.TypeT, intersection)


def get_material_type(
    t: s_types.TypeT,
    *,
    ctx: context.ContextLevel,
) -> s_types.TypeT:

    ctx.env.schema, mtype = t.material_type(ctx.env.schema)
    return mtype


def concretify(
    t: s_types.TypeT,
    *,
    ctx: context.ContextLevel,
) -> s_types.TypeT:
    """Produce a version of t with all views removed.

    This procedes recursively through unions and intersections,
    which can result in major simplifications with intersection types
    in particular.
    """
    t = get_material_type(t, ctx=ctx)
    if els := t.get_union_of(ctx.env.schema):
        ts = [concretify(e, ctx=ctx) for e in els.objects(ctx.env.schema)]
        return get_union_type(ts, ctx=ctx)
    if els := t.get_intersection_of(ctx.env.schema):
        ts = [concretify(e, ctx=ctx) for e in els.objects(ctx.env.schema)]
        return get_intersection_type(ts, ctx=ctx)
    return t


def get_all_concrete(
    stype: s_objtypes.ObjectType, *, ctx: context.ContextLevel
) -> set[s_objtypes.ObjectType]:
    if union := stype.get_union_of(ctx.env.schema):
        return {
            x
            for t in union.objects(ctx.env.schema)
            for x in get_all_concrete(t, ctx=ctx)
        }
    elif intersection := stype.get_intersection_of(ctx.env.schema):
        return set.intersection(*(
            get_all_concrete(t, ctx=ctx)
            for t in intersection.objects(ctx.env.schema)
        ))
    return {stype} | {
        x for x in stype.descendants(ctx.env.schema)
        if x.is_material_object_type(ctx.env.schema)
    }


class TypeIntersectionResult(NamedTuple):

    stype: s_types.Type
    is_empty: bool = False
    is_subtype: bool = False


def apply_intersection(
    left: s_types.Type, right: s_types.Type, *, ctx: context.ContextLevel
) -> TypeIntersectionResult:
    """Compute an intersection of two types: *left* and *right*.

    Returns:
        A :class:`~TypeIntersectionResult` named tuple containing the
        result intersection type, whether the type system considers
        the intersection empty and whether *left* is related to *right*
        (i.e either is a subtype of another).
    """

    if left.issubclass(ctx.env.schema, right):
        # The intersection type is a proper *superclass*
        # of the argument, then this is, effectively, a NOP.
        return TypeIntersectionResult(stype=left)

    if right.issubclass(ctx.env.schema, left):
        # The intersection type is a proper *subclass* and can be directly
        # narrowed.
        return TypeIntersectionResult(
            stype=right,
            is_empty=False,
            is_subtype=True,
        )

    if (
        left.get_is_opaque_union(ctx.env.schema)
        and (left_union := left.get_union_of(ctx.env.schema))
    ):
        # Expose any opaque union types before continuing with the intersection.
        # The schema does not yet fully implement type intersections since there
        # is no `IntersectionTypeShell`. As a result, some intersections
        # produced while compiling the standard library cannot be resolved.
        left = get_union_type(left_union.objects(ctx.env.schema), ctx=ctx)

    int_type: s_types.Type = get_intersection_type([left, right], ctx=ctx)
    is_empty: bool = (
        not s_utils.expand_type_expr_descendants(int_type, ctx.env.schema)
    )
    is_subtype: bool = int_type.issubclass(ctx.env.schema, left)

    return TypeIntersectionResult(
        stype=int_type,
        is_empty=is_empty,
        is_subtype=is_subtype,
    )


def derive_dummy_ptr(
    ptr: s_pointers.Pointer,
    *,
    ctx: context.ContextLevel,
) -> s_pointers.Pointer:
    stdobj = ctx.env.schema.get('std::BaseObject', type=s_objtypes.ObjectType)
    derived_obj_name = stdobj.get_derived_name(
        ctx.env.schema, stdobj, module='__derived__')
    derived_obj = ctx.env.schema.get(
        derived_obj_name, None, type=s_obj.QualifiedObject)
    if derived_obj is None:
        ctx.env.schema, derived_obj = stdobj.derive_subtype(
            ctx.env.schema, name=derived_obj_name)

    derived_name = ptr.get_derived_name(
        ctx.env.schema, derived_obj)

    derived: s_pointers.Pointer
    derived = cast(s_pointers.Pointer, ctx.env.schema.get(derived_name, None))
    if derived is None:
        ctx.env.schema, derived = ptr.derive_ref(
            ctx.env.schema,
            derived_obj,
            target=derived_obj,
            attrs={
                'cardinality': qltypes.SchemaCardinality.One,
            },
            name=derived_name,
            mark_derived=True,
        )

    return derived


def get_union_pointer(
    *,
    ptrname: sn.UnqualName,
    source: s_sources.Source,
    direction: s_pointers.PointerDirection,
    components: Iterable[s_pointers.Pointer],
    opaque: bool = False,
    modname: Optional[str] = None,
    ctx: context.ContextLevel,
) -> s_pointers.Pointer:

    ctx.env.schema, ptr = s_pointers.get_or_create_union_pointer(
        ctx.env.schema,
        ptrname,
        source,
        direction=direction,
        components=components,
        opaque=opaque,
        modname=modname,
        transient=True,
    )
    return ptr
