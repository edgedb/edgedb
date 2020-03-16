#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2015-present MagicStack Inc. and the EdgeDB authors.
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

"""Utilities for IR type descriptors."""

from __future__ import annotations

from typing import *

from edb.edgeql import qltypes

from edb.schema import links as s_links
from edb.schema import lproperties as s_props
from edb.schema import modules as s_mod
from edb.schema import pointers as s_pointers
from edb.schema import pseudo as s_pseudo
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from . import ast as irast

if TYPE_CHECKING:
    import uuid

    from edb.schema import name as s_name
    from edb.schema import schema as s_schema

    PtrRefCacheKey = Tuple[s_pointers.PointerLike, s_pointers.PointerDirection]


def is_scalar(typeref: irast.TypeRef) -> bool:
    """Return True if *typeref* describes a scalar type."""
    return typeref.is_scalar


def is_object(typeref: irast.TypeRef) -> bool:
    """Return True if *typeref* describes an object type."""
    return (
        not is_scalar(typeref)
        and not is_collection(typeref)
        and not is_generic(typeref)
    )


def is_view(typeref: irast.TypeRef) -> bool:
    """Return True if *typeref* describes a view."""
    return typeref.is_view


def is_collection(typeref: irast.TypeRef) -> bool:
    """Return True if *typeref* describes an collection type."""
    return bool(typeref.collection)


def is_array(typeref: irast.TypeRef) -> bool:
    """Return True if *typeref* describes an array type."""
    return typeref.collection == s_types.Array.schema_name


def is_tuple(typeref: irast.TypeRef) -> bool:
    """Return True if *typeref* describes an tuple type."""
    return typeref.collection == s_types.Tuple.schema_name


def is_any(typeref: irast.TypeRef) -> bool:
    """Return True if *typeref* describes the ``anytype`` generic type."""
    return isinstance(typeref, irast.AnyTypeRef)


def is_anytuple(typeref: irast.TypeRef) -> bool:
    """Return True if *typeref* describes the ``anytuple`` generic type."""
    return isinstance(typeref, irast.AnyTupleRef)


def is_generic(typeref: irast.TypeRef) -> bool:
    """Return True if *typeref* describes a generic type."""
    if is_collection(typeref):
        return any(is_generic(st) for st in typeref.subtypes)
    else:
        return is_any(typeref) or is_anytuple(typeref)


def is_abstract(typeref: irast.TypeRef) -> bool:
    """Return True if *typeref* describes an abstract type."""
    return typeref.is_abstract


def type_to_typeref(
    schema: s_schema.Schema,
    t: s_types.Type,
    *,
    cache: Optional[Dict[uuid.UUID, irast.TypeRef]] = None,
    typename: Optional[s_name.Name] = None,
    _name: Optional[str] = None,
) -> irast.TypeRef:
    """Return an instance of :class:`ir.ast.TypeRef` for a given type.

    An IR TypeRef is an object that fully describes a schema type for
    the purposes of query compilation.

    Args:
        schema:
            A schema instance, in which the type *t* is defined.
        t:
            A schema type instance.
        cache:
            Optional mapping from (type UUID, typename) to cached IR TypeRefs.
        typename:
            Optional name hint to use for the type in the returned
            TypeRef.  If ``None``, the type name is used.
        _name:
            Optional subtype element name if this type is a collection within
            a Tuple,

    Returns:
        A ``TypeRef`` instance corresponding to the given schema type.
    """

    result: irast.TypeRef

    if cache is not None and typename is None:
        cached_result = cache.get(t.id)
        if cached_result is not None:
            # If the schema changed due to an ongoing compilation, the name
            # hint might be outdated.
            if cached_result.name_hint == t.get_name(schema):
                return cached_result

    if t.is_anytuple():
        result = irast.AnyTupleRef(
            id=t.id,
            name_hint=typename or t.get_name(schema),
        )
    elif t.is_any():
        result = irast.AnyTypeRef(
            id=t.id,
            name_hint=typename or t.get_name(schema),
        )
    elif not isinstance(t, s_types.Collection):
        assert isinstance(t, s_types.InheritingType)
        union_of = t.get_union_of(schema)
        if union_of:
            non_overlapping, union_is_concrete = (
                s_utils.get_non_overlapping_union(
                    schema,
                    union_of.objects(schema),
                )
            )
            union = frozenset(
                type_to_typeref(schema, c, cache=cache)
                for c in non_overlapping
            )
        else:
            union_is_concrete = False
            union = frozenset()

        intersection_of = t.get_intersection_of(schema)
        if intersection_of:
            intersection = frozenset(
                type_to_typeref(schema, c, cache=cache)
                for c in intersection_of.objects(schema)
            )
        else:
            intersection = frozenset()

        material_type = t.material_type(schema)

        material_typeref: Optional[irast.TypeRef]
        if material_type is not t:
            material_typeref = type_to_typeref(
                schema, material_type, cache=cache
            )
        else:
            material_typeref = None

        if (isinstance(material_type, s_scalars.ScalarType)
                and not material_type.get_is_abstract(schema)):
            base_type = material_type.get_topmost_concrete_base(schema)
            if base_type is material_type:
                base_typeref = None
            else:
                assert isinstance(base_type, s_types.Type)
                base_typeref = type_to_typeref(
                    schema, base_type, cache=cache
                )
        else:
            base_typeref = None

        if typename is not None:
            name = typename
        else:
            name = t.get_name(schema)
        module = schema.get_global(s_mod.Module, name.module)

        common_parent_ref: Optional[irast.TypeRef]
        if union_of:
            common_parent = s_utils.get_class_nearest_common_ancestor(
                schema, union_of.objects(schema))
            assert isinstance(common_parent, s_types.Type)
            common_parent_ref = type_to_typeref(
                schema, common_parent, cache=cache
            )
        else:
            common_parent_ref = None

        result = irast.TypeRef(
            id=t.id,
            module_id=module.id,
            name_hint=name,
            material_type=material_typeref,
            base_type=base_typeref,
            union=union,
            union_is_concrete=union_is_concrete,
            intersection=intersection,
            common_parent=common_parent_ref,
            element_name=_name,
            is_scalar=t.is_scalar(),
            is_abstract=t.get_is_abstract(schema),
            is_view=t.is_view(schema),
            is_opaque_union=t.get_is_opaque_union(schema),
        )
    elif isinstance(t, s_types.Tuple) and t.named:
        result = irast.TypeRef(
            id=t.id,
            name_hint=typename or t.get_name(schema),
            element_name=_name,
            collection=t.schema_name,
            in_schema=schema.get_by_id(t.id, None) is not None,
            subtypes=tuple(
                type_to_typeref(schema, st, _name=sn)  # note: no cache
                for sn, st in t.iter_subtypes(schema)
            )
        )
    else:
        result = irast.TypeRef(
            id=t.id,
            name_hint=typename or t.get_name(schema),
            element_name=_name,
            collection=t.schema_name,
            in_schema=schema.get_by_id(t.id, None) is not None,
            subtypes=tuple(
                type_to_typeref(schema, st, cache=cache)
                for st in t.get_subtypes(schema)
            )
        )

    if cache is not None and typename is None and _name is None:
        # Note: there is no cache for `_name` variants since they are only used
        # for Tuple subtypes and thus they will be cached on the outer level
        # anyway.
        # There's also no variant for types with custom typenames since they
        # proved to have a very low hit rate.
        # This way we save on the size of the key tuple.
        cache[t.id] = result
    return result


def ir_typeref_to_type(
    schema: s_schema.Schema,
    typeref: irast.TypeRef,
) -> s_types.Type:
    """Return a schema type for a given IR TypeRef.

    This is the reverse of :func:`~type_to_typeref`.

    Args:
        schema:
            A schema instance. The result type must exist in it.
        typeref:
            A :class:`ir.ast.TypeRef` instance for which to return
            the corresponding schema type.

    Returns:
        A :class:`schema.types.Type` instance corresponding to the
        given *typeref*.
    """
    if is_anytuple(typeref):
        return s_pseudo.AnyTuple.instance()

    elif is_any(typeref):
        return s_pseudo.Any.instance()

    elif is_tuple(typeref):
        named = False
        tuple_subtypes = {}
        for si, st in enumerate(typeref.subtypes):
            if st.element_name:
                named = True
                type_name = st.element_name
            else:
                type_name = str(si)

            tuple_subtypes[type_name] = ir_typeref_to_type(schema, st)

        return s_types.Tuple.from_subtypes(
            schema, tuple_subtypes, {'named': named})

    elif is_array(typeref):
        array_subtypes = []
        for st in typeref.subtypes:
            array_subtypes.append(ir_typeref_to_type(schema, st))

        return s_types.Array.from_subtypes(schema, array_subtypes)

    else:
        t = schema.get_by_id(typeref.id)
        assert isinstance(t, s_types.Type), 'expected a Type instance'
        return t


def ptrref_from_ptrcls(
    *,
    schema: s_schema.Schema,
    ptrcls: s_pointers.PointerLike,
    direction: s_pointers.PointerDirection = (
        s_pointers.PointerDirection.Outbound),
    cache: Optional[Dict[PtrRefCacheKey, irast.BasePointerRef]] = None,
    typeref_cache: Optional[Dict[uuid.UUID, irast.TypeRef]] = None,
    _include_descendants: bool = True,
) -> irast.BasePointerRef:
    """Return an IR pointer descriptor for a given schema pointer.

    An IR PointerRef is an object that fully describes a schema pointer for
    the purposes of query compilation.

    Args:
        schema:
            A schema instance, in which the type *t* is defined.
        ptrcls:
            A :class:`schema.pointers.Pointer` instance for which to
            return the PointerRef.
        direction:
            The direction of the pointer in the path expression.

    Returns:
        An instance of a subclass of :class:`ir.ast.BasePointerRef`
        corresponding to the given schema pointer.
    """

    if cache is not None:
        cached = cache.get((ptrcls, direction))
        if cached is not None:
            return cached

    kwargs: Dict[str, Any] = {}

    ircls: Type[irast.BasePointerRef]

    if isinstance(ptrcls, irast.TupleIndirectionLink):
        ircls = irast.TupleIndirectionPointerRef
    elif isinstance(ptrcls, irast.TypeIntersectionLink):
        ircls = irast.TypeIntersectionPointerRef
        kwargs['optional'] = ptrcls.is_optional()
        kwargs['is_empty'] = ptrcls.is_empty()
        kwargs['is_subtype'] = ptrcls.is_subtype()
        kwargs['rptr_specialization'] = ptrcls.get_rptr_specialization()
    elif isinstance(ptrcls, s_pointers.Pointer):
        ircls = irast.PointerRef
        kwargs['id'] = ptrcls.id
        name = ptrcls.get_name(schema)
        kwargs['module_id'] = schema.get_global(
            s_mod.Module, name.module).id
    else:
        raise AssertionError(f'unexpected pointer class: {ptrcls}')

    out_source: Optional[irast.TypeRef]

    target = ptrcls.get_far_endpoint(schema, direction)
    if isinstance(target, s_types.Type):
        target_ref = type_to_typeref(schema, target, cache=typeref_cache)
    else:
        target_ref = target

    source = ptrcls.get_near_endpoint(schema, direction)

    source_ptr: Optional[irast.BasePointerRef]
    if (isinstance(ptrcls, s_props.Property)
            and isinstance(source, s_links.Link)):
        source_ptr = ptrref_from_ptrcls(
            ptrcls=source,
            direction=direction,
            schema=schema,
            cache=cache,
            typeref_cache=typeref_cache,
        )
        source_ref = None
    else:
        if isinstance(source, s_types.Type):
            source_ref = type_to_typeref(schema, source, cache=typeref_cache)
        else:
            source_ref = source
        source_ptr = None

    if direction is s_pointers.PointerDirection.Inbound:
        out_source = target_ref
        out_target = source_ref
    else:
        out_source = source_ref
        out_target = target_ref

    out_cardinality = ptrcls.get_cardinality(schema)
    if out_cardinality is None:
        # The cardinality is not yet known.
        dir_cardinality = None
    elif ptrcls.singular(schema, direction):
        dir_cardinality = qltypes.Cardinality.ONE
    else:
        dir_cardinality = qltypes.Cardinality.MANY

    material_ptrcls = ptrcls.material_type(schema)
    material_ptr: Optional[irast.BasePointerRef]
    if material_ptrcls is not None and material_ptrcls is not ptrcls:
        material_ptr = ptrref_from_ptrcls(
            ptrcls=material_ptrcls,
            direction=direction,
            schema=schema,
            cache=cache,
            typeref_cache=typeref_cache,
        )
    else:
        material_ptr = None

    union_components: Set[irast.BasePointerRef] = set()
    union_of = ptrcls.get_union_of(schema)
    union_is_concrete = False
    if union_of:
        union_ptrs = set()

        for component in union_of.objects(schema):
            material_comp = component.material_type(schema)
            union_ptrs.add(material_comp)

        non_overlapping, union_is_concrete = s_utils.get_non_overlapping_union(
            schema,
            union_ptrs,
        )

        union_components = {
            ptrref_from_ptrcls(
                ptrcls=p,
                direction=direction,
                schema=schema,
                cache=cache,
                typeref_cache=typeref_cache,
            ) for p in non_overlapping
        }

    std_parent_name = None
    for ancestor in ptrcls.get_ancestors(schema).objects(schema):
        ancestor_name = ancestor.get_name(schema)
        if ancestor_name.module == 'std' and ancestor.generic(schema):
            std_parent_name = ancestor_name
            break

    is_derived = ptrcls.get_is_derived(schema)
    base_ptr: Optional[irast.BasePointerRef]
    if is_derived:
        base_ptrcls = ptrcls.get_bases(schema).first(schema)
        base_ptr = ptrref_from_ptrcls(
            ptrcls=base_ptrcls,
            direction=direction,
            schema=schema,
            cache=cache,
            typeref_cache=typeref_cache,
        )
    else:
        base_ptr = None

    kwargs.update(dict(
        out_source=out_source,
        out_target=out_target,
        name=ptrcls.get_name(schema),
        shortname=ptrcls.get_shortname(schema),
        path_id_name=ptrcls.get_path_id_name(schema),
        std_parent_name=std_parent_name,
        direction=direction,
        source_ptr=source_ptr,
        base_ptr=base_ptr,
        material_ptr=material_ptr,
        is_derived=ptrcls.get_is_derived(schema),
        is_computable=ptrcls.get_computable(schema),
        union_components=union_components,
        union_is_concrete=union_is_concrete,
        has_properties=ptrcls.has_user_defined_properties(schema),
        required=ptrcls.get_required(schema),
        dir_cardinality=dir_cardinality,
        out_cardinality=out_cardinality,
    ))

    ptrref = ircls(**kwargs)

    if cache is not None:
        cache[ptrcls, direction] = ptrref

    return ptrref


def ptrcls_from_ptrref(
    ptrref: irast.BasePointerRef, *,
    schema: s_schema.Schema,
) -> s_pointers.PointerLike:
    """Return a schema pointer for a given IR PointerRef.

    This is the reverse of :func:`~type_to_typeref`.

    Args:
        schema:
            A schema instance. The result type must exist in it.
        ptrref:
            A :class:`ir.ast.BasePointerRef` instance for which to return
            the corresponding schema pointer.

    Returns:
        A :class:`schema.pointers.PointerLike` instance corresponding to the
        given *ptrref*.
    """

    ptrcls: s_pointers.PointerLike

    if isinstance(ptrref, irast.TupleIndirectionPointerRef):
        ptrcls = irast.TupleIndirectionLink(
            source=ir_typeref_to_type(schema, ptrref.out_source),
            target=ir_typeref_to_type(schema, ptrref.out_target),
            element_name=ptrref.name.name,
        )
    elif isinstance(ptrref, irast.TypeIntersectionPointerRef):
        target = schema.get_by_id(ptrref.out_target.id)
        assert isinstance(target, s_types.Type)
        ptrcls = irast.TypeIntersectionLink(
            source=schema.get_by_id(ptrref.out_source.id),
            target=target,
            optional=ptrref.optional,
            is_empty=ptrref.is_empty,
            is_subtype=ptrref.is_subtype,
            cardinality=ptrref.out_cardinality,
        )
    elif isinstance(ptrref, irast.PointerRef):
        ptr = schema.get_by_id(ptrref.id)
        assert isinstance(ptr, s_pointers.Pointer)
        ptrcls = ptr
    else:
        raise TypeError(f'unexpected pointer ref type: {ptrref!r}')

    return ptrcls


def is_id_ptrref(ptrref: irast.BasePointerRef) -> bool:
    """Return True if *ptrref* describes the id property."""
    return (
        ptrref.std_parent_name == 'std::id'
    )


def is_inbound_ptrref(ptrref: irast.BasePointerRef) -> bool:
    """Return True if pointer described by *ptrref* is inbound."""
    return ptrref.direction is s_pointers.PointerDirection.Inbound


def is_computable_ptrref(ptrref: irast.BasePointerRef) -> bool:
    """Return True if pointer described by *ptrref* is computed."""
    return ptrref.is_computable


def type_contains(
    parent: irast.TypeRef,
    typeref: irast.TypeRef,
) -> bool:
    """Check if *parent* typeref contains the given *typeref*.

    *Containment* here means that either *parent* == *typeref* or, if
    *parent* is a compound type, *typeref* is properly contained within
    a compound type.
    """

    if typeref == parent:
        return True

    elif typeref.union:
        # A union is considered a subtype of a type, if
        # ALL its components are subtypes of that type.
        return all(
            type_contains(parent, component)
            for component in typeref.union
        )

    elif typeref.intersection:
        # An intersection is considered a subtype of a type, if
        # ANY of its components are subtypes of that type.
        return any(
            type_contains(parent, component)
            for component in typeref.intersection
        )

    elif parent.union:
        # A type is considered a subtype of a union type,
        # if it is a subtype of ANY of the union components.
        return any(
            type_contains(component, typeref)
            for component in parent.union
        )

    elif parent.intersection:
        # A type is considered a subtype of an intersection type,
        # if it is a subtype of ALL of the intersection components.
        return any(
            type_contains(component, typeref)
            for component in parent.intersection
        )

    else:
        return False
