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


from __future__ import annotations

import collections
import decimal
import itertools
from typing import *

from edb import errors

from edb.common import levenshtein
from edb.edgeql import ast as qlast

from . import name as sn
from . import objects as so

if TYPE_CHECKING:
    from . import schema as s_schema
    from . import types as s_types


def ast_objref_to_objref(
        node: qlast.ObjectRef, *,
        metaclass: Optional[so.ObjectMeta] = None,
        modaliases: Dict[Optional[str], str],
        schema: s_schema.Schema) -> so.Object:

    if metaclass is not None and issubclass(metaclass, so.GlobalObject):
        return schema.get_global(metaclass, node.name)

    nqname = node.name
    module = node.module
    if module is not None:
        lname = sn.Name(module=module, name=nqname)
    else:
        lname = nqname
    obj = schema.get(lname, module_aliases=modaliases, default=None)
    if obj is not None:
        actual_name = obj.get_name(schema)
        module = actual_name.module
    else:
        aliased_module = modaliases.get(module)
        if aliased_module is not None:
            module = aliased_module

    return so.ObjectRef(name=sn.Name(module=module, name=nqname),
                        origname=lname,
                        schemaclass=metaclass,
                        sourcectx=node.context)


def ast_to_typeref(
        node: qlast.TypeName, *,
        metaclass: Optional[so.ObjectMeta] = None,
        modaliases: Mapping[Optional[str], str],
        schema: s_schema.Schema) -> so.Object:

    if node.subtypes is not None and isinstance(node.maintype,
                                                qlast.ObjectRef) \
            and node.maintype.name == 'enum':
        from . import scalars as s_scalars

        return s_scalars.AnonymousEnumTypeRef(
            name='std::anyenum',
            elements=[st.val.value for st in cast(List[qlast.TypeExprLiteral],
                                                  node.subtypes)],
        )

    elif node.subtypes is not None:
        from . import types as s_types

        assert isinstance(node.maintype, qlast.ObjectRef)
        coll = s_types.Collection.get_class(node.maintype.name)

        if issubclass(coll, s_types.Tuple):
            # Note: if we used abc Tuple here, then we would need anyway
            # to assert it is an instance of s_types.Tuple to make mypy happy
            # (rightly so, because later we use from_subtypes method)

            subtypes: Dict[str, so.Object] \
                = collections.OrderedDict()
            # tuple declaration must either be named or unnamed, but not both
            named = None
            unnamed = None
            for si, st in enumerate(node.subtypes):
                if st.name:
                    named = True
                    type_name = st.name
                else:
                    unnamed = True
                    type_name = str(si)

                if named is not None and unnamed is not None:
                    raise errors.EdgeQLSyntaxError(
                        f'mixing named and unnamed tuple declaration '
                        f'is not supported',
                        context=node.subtypes[0].context,
                    )

                subtypes[type_name] = ast_to_typeref(
                    cast(qlast.TypeName, st),
                    modaliases=modaliases,
                    metaclass=metaclass,
                    schema=schema)

            try:
                return coll.from_subtypes(
                    schema,
                    cast(Mapping[str, s_types.Type], subtypes),
                    {'named': bool(named)})
            except errors.SchemaError as e:
                # all errors raised inside are pertaining to subtypes, so
                # the context should point to the first subtype
                e.set_source_context(node.subtypes[0].context)
                raise e

        else:
            subtypes_list: List[so.Object] = []
            for st in node.subtypes:
                subtypes_list.append(ast_to_typeref(
                    cast(qlast.TypeName, st),
                    modaliases=modaliases,
                    metaclass=metaclass,
                    schema=schema))

            try:
                return coll.from_subtypes(schema,
                                          cast(Sequence[s_types.Type],
                                               subtypes_list))
            except errors.SchemaError as e:
                e.set_source_context(node.context)
                raise e

    elif isinstance(node.maintype, qlast.AnyType):
        from . import pseudo as s_pseudo
        return s_pseudo.AnyObjectRef()

    elif isinstance(node.maintype, qlast.AnyTuple):
        from . import pseudo as s_pseudo
        return s_pseudo.AnyTupleRef()

    assert isinstance(node.maintype, qlast.ObjectRef)

    return ast_objref_to_objref(
        node.maintype, modaliases=modaliases,
        metaclass=metaclass, schema=schema)


def typeref_to_ast(schema: s_schema.Schema,
                   t: so.Object,
                   *,
                   _name: Optional[str] = None) -> qlast.TypeExpr:
    from . import types as s_types

    if isinstance(t, so.ObjectRef):
        # We want typenames like 'anytype` that are wrapped in an
        # ObjectRef to be unwrapped to proper types, so that we
        # can generate proper AST nodes for them (e.g. for `anytype` it
        # is `qlast.AnyType()`).
        t = t._resolve_ref(schema)

    result: qlast.TypeExpr
    components: Tuple[so.ObjectRef, ...]

    if t.is_type() and cast(s_types.Type, t).is_any():
        result = qlast.TypeName(name=_name, maintype=qlast.AnyType())
    elif t.is_type() and cast(s_types.Type, t).is_anytuple():
        result = qlast.TypeName(name=_name, maintype=qlast.AnyTuple())
    elif isinstance(t, s_types.Tuple) and t.named:
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                name=t.schema_name
            ),
            subtypes=[
                typeref_to_ast(schema, cast(so.ObjectRef, st), _name=sn)
                for sn, st in t.iter_subtypes(schema)
            ]
        )
    elif isinstance(t, (s_types.Array, s_types.Tuple)):
        # Here the concrete type Array is used because t.schema_name is used,
        # which is not defined for more generic collections and abcs
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                name=t.schema_name
            ),
            subtypes=[
                typeref_to_ast(schema, st)
                for st in t.get_subtypes(schema)
            ]
        )
    elif isinstance(t, s_types.UnionTypeRef):
        components = t.get_union_of(schema)
        result = typeref_to_ast(schema, components[0])
        for component in components[1:]:
            result = qlast.TypeOp(
                left=result,
                op='|',
                right=typeref_to_ast(schema, component),
            )
    elif t.is_type() and cast(s_types.Type, t).is_union_type(schema):
        object_set: Optional[so.ObjectSet[s_types.Type]] = \
            cast(s_types.Type, t).get_union_of(schema)
        assert object_set is not None

        components = tuple(object_set.objects(schema))
        result = typeref_to_ast(schema, components[0])
        for component in components[1:]:
            result = qlast.TypeOp(
                left=result,
                op='|',
                right=typeref_to_ast(schema, component),
            )
    else:
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                module=t.get_name(schema).module,
                name=t.get_name(schema).name
            )
        )

    return result


def reduce_to_typeref(schema: s_schema.Schema, t: so.Object) -> so.Object:
    ref, _ = t._reduce_to_ref(schema)
    return ref


def resolve_typeref(ref: so.Object, schema: s_schema.Schema) -> so.Object:
    return ref._resolve_ref(schema)


def is_nontrivial_container(value: Any) -> Optional[collections.abc.Iterable]:
    trivial_classes = (str, bytes, bytearray, memoryview)
    if (isinstance(value, collections.abc.Iterable) and
            not isinstance(value, trivial_classes)):
        return value
    else:
        return None


def get_class_nearest_common_ancestor(
    schema: s_schema.Schema,
    classes: Iterable[so.InheritingObjectBaseT]) \
        -> Optional[so.InheritingObjectBaseT]:
    # First, find the intersection of parents
    classes = list(classes)
    first = [classes[0]]
    first.extend(classes[0].get_ancestors(schema).objects(schema))
    common = set(first).intersection(
        *[set(c.get_ancestors(schema).objects(schema)) | {c}
          for c in classes[1:]])
    common_list = sorted(common, key=lambda i: first.index(i))
    if common_list:
        return common_list[0]
    else:
        return None


def minimize_class_set_by_most_generic(
    schema: s_schema.Schema,
    classes: Iterable[so.InheritingObjectBaseT]
) -> List[so.InheritingObjectBaseT]:
    """Minimize the given set of objects by filtering out all subclasses."""

    classes = list(classes)
    mros = [set(p.get_ancestors(schema).objects(schema)) for p in classes]
    count = len(classes)
    smap = itertools.starmap

    # Return only those entries that do not have other entries in their mro
    result = [
        scls for i, scls in enumerate(classes)
        if not any(smap(set.__contains__,
                        ((mros[i], classes[j])
                         for j in range(count) if j != i)))
    ]

    return result


def minimize_class_set_by_least_generic(
    schema: s_schema.Schema,
    classes: Iterable[so.InheritingObjectBaseT]
) -> List[so.InheritingObjectBaseT]:
    """Minimize the given set of objects by filtering out all superclasses."""

    classes = list(classes)
    mros = [set(p.get_ancestors(schema).objects(schema)) | {p}
            for p in classes]
    count = len(classes)
    smap = itertools.starmap

    # Return only those entries that are not present in other entries' mro
    result = [
        scls for i, scls in enumerate(classes)
        if not any(smap(set.__contains__,
                        ((mros[j], classes[i])
                         for j in range(count) if j != i)))
    ]

    return result


def merge_reduce(target: so.InheritingObjectBaseT,
                 sources: Iterable[so.InheritingObjectBaseT],
                 field_name: str,
                 *,
                 schema: s_schema.Schema,
                 f: Callable[[List[Any]], so.InheritingObjectBaseT]) \
        -> Optional[so.InheritingObjectBaseT]:
    values = []
    ours = target.get_explicit_local_field_value(schema, field_name, None)
    if ours is not None:
        values.append(ours)
    for source in sources:
        theirs = source.get_explicit_field_value(schema, field_name, None)
        if theirs is not None:
            values.append(theirs)

    if values:
        return f(values)
    else:
        return None


def merge_sticky_bool(target: so.InheritingObjectBaseT,
                      sources: Iterable[so.InheritingObjectBaseT],
                      field_name: str,
                      *,
                      schema: s_schema.Schema) \
        -> Optional[so.InheritingObjectBaseT]:
    return merge_reduce(target, sources, field_name, schema=schema, f=max)


def merge_weak_bool(target: so.InheritingObjectBaseT,
                    sources: Iterable[so.InheritingObjectBaseT],
                    field_name: str,
                    *,
                    schema: s_schema.Schema) \
        -> Optional[so.InheritingObjectBaseT]:
    return merge_reduce(target, sources, field_name, schema=schema, f=min)


def get_nq_name(schema: s_schema.Schema,
                item: so.Object) -> str:
    shortname = item.get_shortname(schema)
    if isinstance(shortname, sn.Name):
        return shortname.name
    else:
        return shortname


def find_item_suggestions(
        name: Optional[str],
        modaliases: Mapping[Optional[str], str],
        schema: s_schema.Schema,
        *,
        item_type: Optional[so.ObjectMeta] = None,
        limit: int = 3,
        collection: Optional[Iterable[so.Object]] = None,
        condition: Optional[Callable[[so.Object], bool]] = None
) -> List[so.Object]:
    from . import functions as s_func
    from . import modules as s_mod

    if isinstance(name, sn.Name):
        orig_modname: Optional[str] = name.module
        short_name: str = name.name
    else:
        assert name is not None, ("A name must be provided either "
                                  "as string or SchemaName")
        orig_modname = None
        short_name = name

    modname = modaliases.get(orig_modname, orig_modname)

    suggestions: List[so.Object] = []

    if collection is not None:
        suggestions.extend(collection)
    else:
        if modname:
            module = schema.get_global(s_mod.Module, modname, None)
            if module:
                suggestions.extend(
                    schema.get_objects(included_modules=[modname]))

        if not orig_modname:
            suggestions.extend(schema.get_objects(
                included_modules=['std']))

    filters = []

    if item_type:
        filters.append(lambda s: isinstance(s, item_type))

    if condition is not None:
        filters.append(condition)

    if not item_type:
        # When schema class is not specified, only suggest generic objects.
        filters.append(lambda s: not sn.is_fullname(s.get_name(schema)))
        filters.append(lambda s: not isinstance(s, s_func.CallableObject))

    # Never suggest object fragments.
    filters.append(lambda s: not isinstance(s, so.ObjectFragment))

    filtered_suggestions = filter(lambda s: all(f(s) for f in filters),
                                  suggestions)

    # Compute Levenshtein distance for each suggestion.
    with_distance: List[Tuple[so.Object, int]] = [
        (s, levenshtein.distance(short_name, get_nq_name(schema, s)))
        for s in filtered_suggestions
    ]

    # Filter out suggestions that are too dissimilar.
    max_distance = 3
    closest = list(filter(lambda s: s[1] < max_distance, with_distance))

    # Sort by proximity, then by whether the suggestion is contains
    # the source string at the beginning, then by suggestion name.
    closest.sort(
        key=lambda s: (
            s[1],
            not get_nq_name(schema, s[0]).startswith(short_name),
            s[0].get_displayname(schema)
        )
    )

    return [s[0] for s in closest[:limit]]


def enrich_schema_lookup_error(
        error: errors.EdgeDBError,
        item_name: Optional[str],
        modaliases: Mapping[Optional[str], str],
        schema: s_schema.Schema,
        *,
        item_type: Optional[so.ObjectMeta] = None,
        suggestion_limit: int = 3,
        name_template: Optional[str] = None,
        collection: Optional[Iterable[so.Object]] = None,
        condition: Optional[Callable[[so.Object], bool]] = None,
        context: Any = None) -> None:

    suggestions = find_item_suggestions(
        item_name, modaliases, schema,
        item_type=item_type, limit=suggestion_limit,
        collection=collection, condition=condition)

    if suggestions:
        names: Union[List[str], Set[str]] = []
        current_module_name = modaliases.get(None)

        for suggestion in suggestions:
            if (suggestion.get_name(schema).module == 'std' or
                    suggestion.get_name(schema).module == current_module_name):
                names.append(get_nq_name(schema, suggestion))
            else:
                names.append(str(suggestion.get_displayname(schema)))

        if name_template is not None:
            # Use a set() for names as there might be duplicates.
            # E.g. "to_datetime" function has multiple variants, and
            # we don't want to diplay "did you mean one of these:
            # to_datetime, to_datetime, to_datetime?"
            names = {name_template.format(name=name) for name in names}

        if len(names) > 1:
            hint = f'did you mean one of these: {", ".join(names)}?'
        else:
            hint = f'did you mean {names[0]!r}?'

        error.set_hint_and_details(hint=hint)

    if context is not None:
        error.set_source_context(context)


def ensure_union_type(
    schema: s_schema.Schema,
    types: Iterable[s_types.Type],
    *,
    opaque: bool = False,
    module: Optional[str] = None,
) -> Tuple[s_schema.Schema, s_types.Type, bool]:

    from edb.schema import objtypes as s_objtypes
    from edb.schema import types as s_types

    components: Set[s_types.Type] = set()
    for t in types:
        union_of = t.get_union_of(schema)
        if union_of:
            components.update(union_of.objects(schema))
        else:
            components.add(t)

    components_list = minimize_class_set_by_most_generic(schema, components)

    if len(components_list) == 1 and not opaque:
        return schema, next(iter(components_list)), False

    seen_scalars = False
    seen_objtypes = False
    created = False

    for component in components_list:
        if component.is_object_type():
            if seen_scalars:
                raise _union_error(schema, components_list)
            seen_objtypes = True
        else:
            if seen_objtypes:
                raise _union_error(schema, components_list)
            seen_scalars = True

    if seen_scalars:
        uniontype: s_types.Type = components_list[0]
        for t1 in components_list[1:]:

            common_type = uniontype.\
                find_common_implicitly_castable_type(t1, schema)

            if common_type is None:
                raise _union_error(schema, components_list)
            else:
                uniontype = common_type
    else:
        schema, uniontype, created = s_objtypes.get_or_create_union_type(
            schema,
            components=components_list,
            opaque=opaque,
            module=module)

    return schema, uniontype, created


def get_union_type(
    schema: s_schema.Schema,
    types: Iterable[s_types.Type],
    *,
    opaque: bool = False,
    module: Optional[str] = None,
) -> Tuple[s_schema.Schema, s_types.Type]:

    schema, union, _ = ensure_union_type(
        schema, types, opaque=opaque, module=module)

    return schema, union


def get_non_overlapping_union(
    schema: s_schema.Schema,
    objects: Iterable[so.InheritingObjectBaseT],
) -> Tuple[FrozenSet[so.InheritingObjectBaseT], bool]:

    all_objects: Set[so.InheritingObjectBaseT] = set(objects)
    non_unique_count = 0
    for obj in objects:
        descendants = obj.descendants(schema)
        non_unique_count += len(descendants) + 1
        all_objects.update(descendants)

    if non_unique_count == len(all_objects):
        # The input object set is already non-overlapping
        return frozenset(objects), False
    else:
        return frozenset(all_objects), True


def _union_error(schema: s_schema.Schema, components: Iterable[s_types.Type]) \
        -> errors.SchemaError:
    names = ', '.join(sorted(c.get_displayname(schema) for c in components))
    return errors.SchemaError(f'cannot create a union of {names}')


def ensure_intersection_type(
    schema: s_schema.Schema,
    types: Iterable[s_types.Type],
    *,
    module: Optional[str] = None,
) -> Tuple[s_schema.Schema, s_types.Type, bool]:

    from edb.schema import objtypes as s_objtypes

    components: Set[s_types.Type] = set()
    for t in types:
        intersection_of = t.get_intersection_of(schema)
        if intersection_of:
            components.update(intersection_of.objects(schema))
        else:
            components.add(t)

    components_list = minimize_class_set_by_least_generic(schema, components)

    if len(components_list) == 1:
        return schema, next(iter(components_list)), False

    seen_scalars = False
    seen_objtypes = False

    for component in components_list:
        if component.is_object_type():
            if seen_scalars:
                raise _intersection_error(schema, components_list)
            seen_objtypes = True
        else:
            if seen_objtypes:
                raise _intersection_error(schema, components_list)
            seen_scalars = True

    if seen_scalars:
        # Non-related scalars and collections cannot for intersection types.
        raise _intersection_error(schema, components_list)
    else:
        return s_objtypes.get_or_create_intersection_type(
            schema, components=cast(Iterable[s_objtypes.ObjectType],
                                    components_list), module=module)


def get_intersection_type(
    schema: s_schema.Schema,
    types: Iterable[s_types.Type],
    *,
    module: Optional[str] = None,
) -> Tuple[s_schema.Schema, s_types.Type]:

    schema, intersection, _ = ensure_intersection_type(
        schema, types, module=module)

    return schema, intersection


def _intersection_error(schema: s_schema.Schema,
                        components: Iterable[s_types.Type]) \
        -> errors.SchemaError:
    names = ', '.join(sorted(c.get_displayname(schema) for c in components))
    return errors.SchemaError(f'cannot create an intersection of {names}')


MAX_INT64 = 2 ** 63 - 1
MIN_INT64 = -2 ** 63


def const_ast_from_python(val: Any) -> qlast.BaseConstant:
    if isinstance(val, str):
        return qlast.StringConstant.from_python(val)
    elif isinstance(val, bool):
        return qlast.BooleanConstant(value='true' if val else 'false')
    elif isinstance(val, int):
        if MIN_INT64 <= val <= MAX_INT64:
            return qlast.IntegerConstant(value=str(val))
        else:
            raise ValueError(f'int64 value out of range: {val}')
    elif isinstance(val, decimal.Decimal):
        return qlast.DecimalConstant(value=f'{val}n')
    elif isinstance(val, float):
        return qlast.FloatConstant(value=str(val))
    elif isinstance(val, bytes):
        return qlast.BytesConstant.from_python(value=val)
    else:
        raise ValueError(f'unexpected constant type: {type(val)!r}')
