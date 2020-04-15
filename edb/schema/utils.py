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
from typing import *

import collections
import decimal
import itertools

from edb import errors

from edb.common import levenshtein
from edb.edgeql import ast as qlast

from . import name as sn
from . import objects as so

if TYPE_CHECKING:
    from . import objtypes as s_objtypes
    from . import schema as s_schema
    from . import types as s_types


def ast_objref_to_object_shell(
    node: qlast.ObjectRef, *,
    metaclass: Optional[Type[so.Object]] = None,
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
) -> so.ObjectShell:

    nqname = node.name
    module = node.module
    if module is not None:
        lname = sn.Name(module=module, name=nqname)
    else:
        lname = nqname
    obj = schema.get(lname, module_aliases=modaliases, default=None)

    if metaclass is not None and not issubclass(metaclass, so.QualifiedObject):
        name = nqname
    else:
        if obj is not None:
            actual_name = obj.get_name(schema)
            module = actual_name.module
        else:
            aliased_module = modaliases.get(module)
            if aliased_module is not None:
                module = aliased_module

        name = sn.Name(module=module, name=nqname)

    return so.ObjectShell(
        name=name,
        origname=lname,
        schemaclass=metaclass or so.Object,
        sourcectx=node.context,
    )


def ast_objref_to_type_shell(
    node: qlast.ObjectRef,
    *,
    metaclass: Optional[Type[s_types.Type]] = None,
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
) -> s_types.TypeShell:
    from . import types as s_types

    if metaclass is None:
        metaclass = s_types.Type

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

    return s_types.TypeShell(
        name=sn.Name(module=module, name=nqname),
        origname=lname,
        schemaclass=metaclass,
        sourcectx=node.context,
    )


def ast_to_type_shell(
    node: qlast.TypeName,
    *,
    metaclass: Optional[Type[s_types.Type]] = None,
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
) -> s_types.TypeShell:

    if (node.subtypes is not None
            and isinstance(node.maintype, qlast.ObjectRef)
            and node.maintype.name == 'enum'):
        from . import scalars as s_scalars

        return s_scalars.AnonymousEnumTypeShell(
            elements=[
                st.val.value
                for st in cast(List[qlast.TypeExprLiteral], node.subtypes)
            ],
        )

    elif node.subtypes is not None:
        from . import types as s_types

        assert isinstance(node.maintype, qlast.ObjectRef)
        coll = s_types.Collection.get_class(node.maintype.name)

        if issubclass(coll, s_types.Tuple):
            # Note: if we used abc Tuple here, then we would need anyway
            # to assert it is an instance of s_types.Tuple to make mypy happy
            # (rightly so, because later we use from_subtypes method)

            subtypes: Dict[str, s_types.TypeShell] = {}
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

                subtypes[type_name] = ast_to_type_shell(
                    cast(qlast.TypeName, st),
                    modaliases=modaliases,
                    metaclass=metaclass,
                    schema=schema,
                )

            try:
                return coll.create_shell(
                    schema,
                    subtypes=subtypes,
                    typemods={'named': bool(named)},
                )
            except errors.SchemaError as e:
                # all errors raised inside are pertaining to subtypes, so
                # the context should point to the first subtype
                e.set_source_context(node.subtypes[0].context)
                raise e

        elif issubclass(coll, s_types.Array):

            subtypes_list: List[s_types.TypeShell] = []
            for st in node.subtypes:
                subtypes_list.append(
                    ast_to_type_shell(
                        cast(qlast.TypeName, st),
                        modaliases=modaliases,
                        metaclass=metaclass,
                        schema=schema,
                    )
                )

            if len(subtypes_list) != 1:
                raise errors.SchemaError(
                    f'unexpected number of subtypes,'
                    f' expecting 1, got {len(subtypes_list)}',
                    context=node.context,
                )

            if isinstance(subtypes_list[0], s_types.ArrayTypeShell):
                raise errors.UnsupportedFeatureError(
                    'nested arrays are not supported',
                    context=node.subtypes[0].context,
                )

            try:
                return coll.create_shell(
                    schema,
                    subtypes=subtypes_list,
                )
            except errors.SchemaError as e:
                e.set_source_context(node.context)
                raise e

    elif isinstance(node.maintype, qlast.AnyType):
        from . import pseudo as s_pseudo
        return s_pseudo.PseudoTypeShell(name='anytype')

    elif isinstance(node.maintype, qlast.AnyTuple):
        from . import pseudo as s_pseudo
        return s_pseudo.PseudoTypeShell(name='anytuple')

    assert isinstance(node.maintype, qlast.ObjectRef)

    return ast_objref_to_type_shell(
        node.maintype,
        modaliases=modaliases,
        metaclass=metaclass,
        schema=schema,
    )


def ast_to_object_shell(
    node: Union[qlast.ObjectRef, qlast.TypeName],
    *,
    metaclass: Optional[Type[so.Object]] = None,
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
) -> so.ObjectShell:
    from . import types as s_types

    if isinstance(node, qlast.TypeName):
        if metaclass is not None and issubclass(metaclass, s_types.Type):
            return ast_to_type_shell(
                node,
                metaclass=metaclass,
                modaliases=modaliases,
                schema=schema,
            )
        else:
            objref = node.maintype
            if node.subtypes:
                raise AssertionError(
                    'must pass s_types.Type subclass as type when '
                    'creating a type shell from type AST'
                )
            assert isinstance(objref, qlast.ObjectRef)
            return ast_objref_to_object_shell(
                objref,
                modaliases=modaliases,
                metaclass=metaclass,
                schema=schema,
            )
    else:
        return ast_objref_to_object_shell(
            node,
            modaliases=modaliases,
            metaclass=metaclass,
            schema=schema,
        )


def typeref_to_ast(
    schema: s_schema.Schema,
    ref: Union[so.Object, so.ObjectShell],
    *,
    _name: Optional[str] = None,
) -> qlast.TypeExpr:
    from . import types as s_types

    if isinstance(ref, so.ObjectShell):
        t = ref.resolve(schema)
    else:
        t = ref

    result: qlast.TypeExpr
    components: Tuple[so.Object, ...]

    if t.is_type() and cast(s_types.Type, t).is_any(schema):
        result = qlast.TypeName(name=_name, maintype=qlast.AnyType())
    elif t.is_type() and cast(s_types.Type, t).is_anytuple(schema):
        result = qlast.TypeName(name=_name, maintype=qlast.AnyTuple())
    elif isinstance(t, s_types.Tuple) and t.is_named(schema):
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                name=t.schema_name
            ),
            subtypes=[
                typeref_to_ast(schema, st, _name=sn)
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
    elif t.is_type() and cast(s_types.Type, t).is_union_type(schema):
        object_set: Optional[so.ObjectSet[s_types.Type]] = \
            cast(s_types.Type, t).get_union_of(schema)
        assert object_set is not None

        component_objects = tuple(object_set.objects(schema))
        result = typeref_to_ast(schema, component_objects[0])
        for component_object in component_objects[1:]:
            result = qlast.TypeOp(
                left=result,
                op='|',
                right=typeref_to_ast(schema, component_object),
            )
    elif isinstance(t, so.QualifiedObject):
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                module=t.get_name(schema).module,
                name=t.get_name(schema).name
            )
        )
    else:
        raise NotImplementedError(f'cannot represent {t!r} as a shell')

    return result


def name_to_ast_ref(name: str) -> qlast.ObjectRef:
    if isinstance(name, sn.Name):
        return qlast.ObjectRef(
            module=name.module,
            name=name.name,
        )
    else:
        return qlast.ObjectRef(
            name=name,
        )


def ast_to_object(
    node: Union[qlast.TypeName, qlast.ObjectRef],
    *,
    metaclass: Optional[Type[so.Object]] = None,
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
) -> so.Object:

    ref = ast_to_object_shell(
        node,
        metaclass=metaclass,
        modaliases=modaliases,
        schema=schema,
    )

    return ref.resolve(schema)


@overload
def ast_to_type(  # NoQA: F811
    node: qlast.TypeName, *,
    metaclass: Type[s_types.TypeT],
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
) -> s_types.TypeT:
    ...


@overload
def ast_to_type(  # NoQA: F811
    node: qlast.TypeName, *,
    metaclass: None = None,
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
) -> s_types.Type:
    ...


def ast_to_type(  # NoQA: F811
    node: qlast.TypeName, *,
    metaclass: Optional[Type[s_types.TypeT]] = None,
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
) -> s_types.TypeT:

    ref = ast_to_type_shell(
        node,
        metaclass=metaclass,
        modaliases=modaliases,
        schema=schema,
    )

    return ref.resolve(schema)  # type: ignore


def is_nontrivial_container(value: Any) -> Optional[Iterable[Any]]:
    trivial_classes = (str, bytes, bytearray, memoryview)
    if (isinstance(value, collections.abc.Iterable) and
            not isinstance(value, trivial_classes)):
        return value
    else:
        return None


def get_class_nearest_common_ancestor(
    schema: s_schema.Schema,
    classes: Iterable[so.InheritingObjectT]
) -> Optional[so.InheritingObjectT]:
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
    classes: Iterable[so.InheritingObjectT]
) -> List[so.InheritingObjectT]:
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
    classes: Iterable[so.InheritingObjectT]
) -> List[so.InheritingObjectT]:
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


def merge_reduce(target: so.InheritingObjectT,
                 sources: Iterable[so.InheritingObjectT],
                 field_name: str,
                 *,
                 schema: s_schema.Schema,
                 f: Callable[[List[Any]], so.InheritingObjectT]) \
        -> Optional[so.InheritingObjectT]:
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


def merge_sticky_bool(target: so.InheritingObjectT,
                      sources: Iterable[so.InheritingObjectT],
                      field_name: str,
                      *,
                      schema: s_schema.Schema) \
        -> Optional[so.InheritingObjectT]:
    return merge_reduce(target, sources, field_name, schema=schema, f=max)


def merge_weak_bool(target: so.InheritingObjectT,
                    sources: Iterable[so.InheritingObjectT],
                    field_name: str,
                    *,
                    schema: s_schema.Schema) \
        -> Optional[so.InheritingObjectT]:
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

    if item_type is not None:
        it = item_type
        filters.append(lambda s: isinstance(s, it))

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
        names: Union[List[str]] = []
        cur_module_name = modaliases.get(None)

        for suggestion in suggestions:
            if (
                isinstance(suggestion, so.QualifiedObject)
                and (
                    suggestion.get_name(schema).module == 'std'
                    or suggestion.get_name(schema).module == cur_module_name
                )
            ):
                names.append(get_nq_name(schema, suggestion))
            else:
                names.append(str(suggestion.get_displayname(schema)))

        if name_template is not None:
            # Use a set() for names as there might be duplicates.
            # E.g. "to_datetime" function has multiple variants, and
            # we don't want to diplay "did you mean one of these:
            # to_datetime, to_datetime, to_datetime?"
            names = list({name_template.format(name=name) for name in names})

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

    components_list: Sequence[s_types.Type]

    if all(isinstance(c, s_types.InheritingType) for c in components):
        components_list = minimize_class_set_by_most_generic(
            schema,
            cast(Set[s_types.InheritingType], components),
        )
    else:
        components_list = list(components)

    if len(components_list) == 1 and not opaque:
        return schema, next(iter(components_list)), False

    seen_scalars = False
    seen_objtypes = False
    created = False

    for component in components_list:
        if isinstance(component, s_objtypes.ObjectType):
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

            schema, common_type = (
                uniontype.find_common_implicitly_castable_type(t1, schema)
            )

            if common_type is None:
                raise _union_error(schema, components_list)
            else:
                uniontype = common_type
    else:
        objtypes = cast(
            Sequence[s_objtypes.ObjectType],
            components_list,
        )
        schema, uniontype, created = s_objtypes.get_or_create_union_type(
            schema,
            components=objtypes,
            opaque=opaque,
            module=module,
        )

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
    objects: Iterable[so.InheritingObjectT],
) -> Tuple[FrozenSet[so.InheritingObjectT], bool]:

    all_objects: Set[so.InheritingObjectT] = set(objects)
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
    from edb.schema import types as s_types

    components: Set[s_types.Type] = set()
    for t in types:
        intersection_of = t.get_intersection_of(schema)
        if intersection_of:
            components.update(intersection_of.objects(schema))
        else:
            components.add(t)

    components_list: Sequence[s_types.Type]

    if all(isinstance(c, s_types.InheritingType) for c in components):
        components_list = minimize_class_set_by_least_generic(
            schema,
            cast(Set[s_types.InheritingType], components),
        )
    else:
        components_list = list(components)

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
            schema,
            components=cast(Iterable[s_objtypes.ObjectType], components_list),
            module=module,
        )


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
        return qlast.BytesConstant.from_python(val)
    else:
        raise ValueError(f'unexpected constant type: {type(val)!r}')


def get_config_type_shape(
    schema: s_schema.Schema,
    stype: s_objtypes.ObjectType,
    path: List[qlast.Base],
) -> List[qlast.ShapeElement]:
    shape = []
    seen: Set[str] = set()

    stypes = [stype] + list(stype.descendants(schema))

    for t in stypes:
        t_name = t.get_name(schema)

        for pn, p in t.get_pointers(schema).items(schema):
            if pn in ('id', '__type__') or pn in seen:
                continue

            elem_path: List[qlast.Base] = []

            if t is not stype:
                elem_path.append(
                    qlast.TypeIntersection(
                        type=qlast.TypeName(
                            maintype=qlast.ObjectRef(
                                module=t_name.module,
                                name=t_name.name,
                            ),
                        ),
                    ),
                )

            elem_path.append(qlast.Ptr(ptr=qlast.ObjectRef(name=pn)))

            ptype = p.get_target(schema)

            if ptype.is_object_type():
                subshape = get_config_type_shape(
                    schema, ptype, path + elem_path)
                subshape.append(
                    qlast.ShapeElement(
                        expr=qlast.Path(
                            steps=[
                                qlast.Ptr(
                                    ptr=qlast.ObjectRef(name='_tname'),
                                ),
                            ],
                        ),
                        compexpr=qlast.Path(
                            steps=path + elem_path + [
                                qlast.Ptr(
                                    ptr=qlast.ObjectRef(name='__type__')),
                                qlast.Ptr(
                                    ptr=qlast.ObjectRef(name='name')),
                            ],
                        ),
                    ),
                )
            else:
                subshape = []

            shape.append(
                qlast.ShapeElement(
                    expr=qlast.Path(steps=elem_path),
                    elements=subshape,
                ),
            )

            seen.add(pn)

    return shape
