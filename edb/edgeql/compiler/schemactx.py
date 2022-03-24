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

from typing import *

from edb import errors

from edb.common import parsing

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
    srcctx: Optional[parsing.ParserContext] = None,
) -> s_obj.Object:

    if isinstance(ref, qlast.ObjectRef):
        if srcctx is None:
            srcctx = ref.context
        module = ref.module
        lname = ref.name
    elif isinstance(ref, qlast.AnyType):
        return s_pseudo.PseudoType.get(ctx.env.schema, 'anytype')
    elif isinstance(ref, qlast.AnyTuple):
        return s_pseudo.PseudoType.get(ctx.env.schema, 'anytuple')
    else:
        raise AssertionError(f"Unhandled BaseObjectRef subclass: {ref!r}")

    name: sn.Name
    if module:
        name = sn.QualName(module=module, name=lname)
    else:
        name = sn.UnqualName(name=lname)

    view = _get_type_variant(name, ctx)
    if view is not None:
        return view

    try:
        stype = ctx.env.get_track_schema_object(
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
            condition=condition,
            context=srcctx,
        )
        raise

    view = _get_type_variant(stype.get_name(ctx.env.schema), ctx)
    if view is not None:
        return view
    elif stype == ctx.defining_view:
        # stype is the view in process of being defined and as such is
        # not yet a valid schema object
        raise errors.SchemaDefinitionError(
            f'illegal self-reference in definition of {str(name)!r}',
            context=srcctx)
    else:
        return stype


def _get_type_variant(
    name: sn.Name,
    ctx: context.ContextLevel,
) -> Optional[s_obj.Object]:
    type_variant = ctx.aliased_views.get(name)
    if type_variant is not None:
        ctx.must_use_views[type_variant] = None
        return type_variant
    else:
        return None


def get_schema_type(
    name: qlast.BaseObjectRef,
    module: Optional[str] = None,
    *,
    ctx: context.ContextLevel,
    label: Optional[str] = None,
    condition: Optional[Callable[[s_obj.Object], bool]] = None,
    item_type: Optional[Type[s_obj.Object]] = None,
    srcctx: Optional[parsing.ParserContext] = None,
) -> s_types.Type:
    if item_type is None:
        item_type = s_types.Type
    obj = get_schema_object(name, module, item_type=item_type,
                            condition=condition, label=label,
                            ctx=ctx, srcctx=srcctx)
    assert isinstance(obj, s_types.Type)
    return obj


def resolve_schema_name(
        name: str, module: str, *,
        ctx: context.ContextLevel) -> Optional[sn.QualName]:
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
    preserve_path_id: bool = False,
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
                preserve_path_id=preserve_path_id,
                attrs=attrs,
            )

        if (not stype.generic(ctx.env.schema)
                and isinstance(derived, s_objtypes.ObjectType)):
            assert isinstance(stype, s_objtypes.ObjectType)
            scls_pointers = stype.get_pointers(ctx.env.schema)
            derived_own_pointers = derived.get_pointers(ctx.env.schema)

            for pn, ptr in derived_own_pointers.items(ctx.env.schema):
                # This is a view of a view.  Make sure query-level
                # computable expressions for pointers are carried over.
                src_ptr = scls_pointers.get(ctx.env.schema, pn)
                computable_data = (
                    ctx.source_map.get(src_ptr) if src_ptr else None)
                if computable_data is not None:
                    ctx.source_map[ptr] = computable_data

                if src_ptr in ctx.env.pointer_specified_info:
                    ctx.env.pointer_derivation_map[src_ptr].append(ptr)

    else:
        raise TypeError("unsupported type in derive_view")

    ctx.view_nodes[derived.get_name(ctx.env.schema)] = derived

    if preserve_shape and stype in ctx.env.view_shapes:
        preserve_view_shape(stype, derived, ctx=ctx)

    ctx.env.created_schema_objects.add(derived)

    return derived


def derive_ptr(
    ptr: s_pointers.Pointer,
    source: s_sources.Source,
    target: Optional[s_types.Type] = None,
    *qualifiers: str,
    derived_name: Optional[sn.QualName] = None,
    derived_name_quals: Optional[Sequence[str]] = (),
    preserve_shape: bool = False,
    preserve_path_id: bool = False,
    inheritance_merge: bool = True,
    attrs: Optional[Dict[str, Any]] = None,
    ctx: context.ContextLevel,
) -> s_pointers.Pointer:

    if derived_name is None and ctx.derived_target_module:
        derived_name = derive_view_name(
            stype=ptr, derived_name_quals=derived_name_quals, ctx=ctx)

    if ptr.get_name(ctx.env.schema) == derived_name:
        qualifiers = qualifiers + (ctx.aliases.get('d'),)

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
        preserve_path_id=preserve_path_id,
        attrs=attrs)

    if not ptr.generic(ctx.env.schema):
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
                computable_data = ctx.source_map.get(src_ptr)
                if computable_data is not None:
                    ctx.source_map[ptr] = computable_data

    if preserve_shape and ptr in ctx.env.view_shapes:
        preserve_view_shape(ptr, derived, ctx=ctx)

    ctx.env.created_schema_objects.add(derived)

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
    types: Iterable[s_types.Type],
    *,
    opaque: bool = False,
    preserve_derived: bool = False,
    ctx: context.ContextLevel,
) -> s_types.Type:

    ctx.env.schema, union, created = s_utils.ensure_union_type(
        ctx.env.schema, types,
        opaque=opaque, preserve_derived=preserve_derived)

    if created:
        ctx.env.created_schema_objects.add(union)
    elif (
        union not in ctx.env.created_schema_objects
        and (
            not isinstance(union, s_obj.QualifiedObject)
            or union.get_name(ctx.env.schema).module != '__derived__'
        )
    ):
        ctx.env.add_schema_ref(union, expr=None)

    return union


def get_intersection_type(
    types: Iterable[s_types.Type],
    *,
    ctx: context.ContextLevel,
) -> s_types.Type:

    ctx.env.schema, intersection, created = s_utils.ensure_intersection_type(
        ctx.env.schema, types)

    if created:
        ctx.env.created_schema_objects.add(intersection)
    elif (
        intersection not in ctx.env.created_schema_objects
        and (
            not isinstance(intersection, s_obj.QualifiedObject)
            or intersection.get_name(ctx.env.schema).module != '__derived__'
        )
    ):
        ctx.env.add_schema_ref(intersection, expr=None)

    return intersection


def get_material_type(
    t: s_types.TypeT,
    *,
    ctx: context.ContextLevel,
) -> s_types.TypeT:

    ctx.env.schema, mtype = t.material_type(ctx.env.schema)
    return mtype


class TypeIntersectionResult(NamedTuple):

    stype: s_types.Type
    is_empty: bool = False
    is_subtype: bool = False


def apply_intersection(
    left: s_types.Type,
    right: s_types.Type,
    *,
    ctx: context.ContextLevel
) -> TypeIntersectionResult:
    """Compute an intersection of two types: *left* and *right*.

    In theory, this should handle all combinations of unions and intersections
    recursively, but currently this handles only the common case of
    intersecting a regular type or a union type with a regular type.

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

    is_subtype = False
    empty_intersection = False
    union = left.get_union_of(ctx.env.schema)
    if union:
        # If the argument type is a union type, then we
        # narrow it by the intersection type.
        narrowed_union = []
        for component_type in union.objects(ctx.env.schema):
            if component_type.issubclass(ctx.env.schema, right):
                narrowed_union.append(component_type)
            elif right.issubclass(ctx.env.schema, component_type):
                narrowed_union.append(right)

        if len(narrowed_union) == 0:
            int_type = get_intersection_type((left, right), ctx=ctx)
            is_subtype = int_type.issubclass(ctx.env.schema, left)
            assert isinstance(right, s_obj.InheritingObject)
            empty_intersection = not any(
                c.issubclass(ctx.env.schema, left)
                for c in right.descendants(ctx.env.schema)
            )
        elif len(narrowed_union) == 1:
            int_type = narrowed_union[0]
            is_subtype = int_type.issubclass(ctx.env.schema, left)
        else:
            int_type = get_union_type(narrowed_union, ctx=ctx)
    else:
        is_subtype = right.issubclass(ctx.env.schema, left)
        empty_intersection = not is_subtype
        int_type = get_intersection_type((left, right), ctx=ctx)

    return TypeIntersectionResult(
        stype=int_type,
        is_empty=empty_intersection,
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
        ctx.env.created_schema_objects.add(derived_obj)

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
            transient=True,
        )
        ctx.env.created_schema_objects.add(derived)

    return derived


def get_union_pointer(
    *,
    ptrname: sn.UnqualName,
    source: s_sources.Source,
    direction: s_pointers.PointerDirection,
    components: Iterable[s_pointers.Pointer],
    ctx: context.ContextLevel,
) -> s_pointers.Pointer:

    ctx.env.schema, ptr = s_pointers.get_or_create_union_pointer(
        ctx.env.schema,
        ptrname,
        source,
        direction=direction,
        components=components,
    )

    ctx.env.created_schema_objects.add(ptr)

    return ptr
