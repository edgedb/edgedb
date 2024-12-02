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
from typing import (
    Any,
    Callable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    Iterable,
    Mapping,
    Sequence,
    Dict,
    List,
    Set,
    FrozenSet,
    cast,
    TYPE_CHECKING,
)

import collections
import decimal
import itertools

from edb import errors

from edb.common import levenshtein
from edb.edgeql import ast as qlast

from edb.ir import statypes

from . import name as sn
from . import objects as so
from . import expr as s_expr

if TYPE_CHECKING:
    from . import objtypes as s_objtypes
    from . import schema as s_schema
    from . import types as s_types
    from edb.common import parsing

    T = TypeVar('T')


def name_to_ast_ref(name: sn.Name) -> qlast.ObjectRef:
    if isinstance(name, sn.QualName):
        return qlast.ObjectRef(
            module=name.module,
            name=name.name,
        )
    else:
        return qlast.ObjectRef(
            name=name.name,
        )


def ast_ref_to_name(ref: qlast.ObjectRef) -> sn.Name:
    if ref.module:
        return sn.QualName(name=ref.name, module=ref.module)
    else:
        return sn.UnqualName(name=ref.name)


def ast_ref_to_unqualname(ref: qlast.ObjectRef) -> sn.UnqualName:
    if ref.module:
        raise errors.InternalServerError(
            f'unexpected fully-qualified name: {ast_ref_to_name(ref)}',
            span=ref.span,
        )
    else:
        return sn.UnqualName(name=ref.name)


def resolve_name(
    lname: sn.Name,
    *,
    metaclass: Optional[Type[so.Object]] = None,
    sourcectx: Optional[parsing.Span] = None,
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
) -> sn.Name:
    obj = schema.get(
        lname,
        type=metaclass,
        module_aliases=modaliases,
        default=None,
        sourcectx=sourcectx,
    )
    if obj is not None:
        name = obj.get_name(schema)
    elif isinstance(lname, sn.QualName):
        name = sn.QualName(
            module=modaliases.get(lname.module, lname.module),
            name=lname.name,
        )
    elif metaclass is not None and issubclass(metaclass, so.QualifiedObject):
        actual_module = modaliases.get(None)
        if actual_module is None:
            raise errors.InvalidReferenceError(
                'unqualified name and no default module alias set')
        name = sn.QualName(module=actual_module, name=lname.name)
    else:
        # Do not assume the name is fully-qualified unless asked
        # explicitly.
        name = lname

    return name


def ast_objref_to_object_shell(
    ref: qlast.ObjectRef,
    *,
    metaclass: Type[so.Object_T],
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
) -> so.ObjectShell[so.Object_T]:
    lname = ast_ref_to_name(ref)
    name = resolve_name(
        lname,
        metaclass=metaclass,
        modaliases=modaliases,
        schema=schema,
        sourcectx=ref.span,
    )

    return so.ObjectShell(
        name=name,
        origname=lname,
        schemaclass=metaclass,
        sourcectx=ref.span,
    )


def ast_objref_to_type_shell(
    ref: qlast.ObjectRef,
    *,
    metaclass: Type[s_types.TypeT],
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
) -> s_types.TypeShell[s_types.TypeT]:
    from . import types as s_types

    if metaclass is not s_types.Type:
        mcls = metaclass
    else:
        mcls = s_types.QualifiedType  # type: ignore

    lname = ast_ref_to_name(ref)
    name = resolve_name(
        lname,
        metaclass=mcls,
        modaliases=modaliases,
        schema=schema,
        sourcectx=ref.span,
    )

    return s_types.TypeShell(
        name=name,
        origname=lname,
        schemaclass=mcls,
        sourcectx=ref.span,
    )


def ast_to_type_shell(
    node: qlast.TypeExpr,
    *,
    metaclass: Type[s_types.TypeT_co],
    module: Optional[str] = None,
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
    allow_generalized_bases: bool = False,
) -> s_types.TypeShell[s_types.TypeT_co]:

    if isinstance(node, qlast.TypeOp):
        return type_op_ast_to_type_shell(
            node,
            metaclass=metaclass,
            module=module,
            modaliases=modaliases,
            schema=schema,
        )

    assert isinstance(node, qlast.TypeName)

    if (node.subtypes is not None
            and isinstance(node.maintype, qlast.ObjectRef)
            and node.maintype.name == 'enum'):
        from . import scalars as s_scalars
        from edb.pgsql import common as pg_common

        assert node.subtypes

        elements: List[str] = []
        element_spans: List[Optional[parsing.Span]] = []

        if isinstance(node.subtypes[0], qlast.TypeExprLiteral):
            # handling enums as literals
            # eg. enum<'A','B','C'>
            for subtype_expr_literal in cast(
                List[qlast.TypeExprLiteral], node.subtypes
            ):
                elements.append(subtype_expr_literal.val.value)
                element_spans.append(subtype_expr_literal.val.span)
        else:
            # handling enums as typenames
            # eg. enum<A,B,C>
            for subtype_type_name in cast(
                List[qlast.TypeName], node.subtypes
            ):
                if (
                    not isinstance(subtype_type_name, qlast.TypeName)
                    or not isinstance(
                        subtype_type_name.maintype, qlast.ObjectRef
                    )
                ):
                    raise errors.EdgeQLSyntaxError(
                        f'enums do not support mapped values',
                        span=subtype_type_name.span,
                    )
                elements.append(subtype_type_name.maintype.name)
                element_spans.append(subtype_type_name.maintype.span)

        for element, element_span in zip(elements, element_spans):
            if len(element) > pg_common.MAX_ENUM_LABEL_LENGTH:
                raise errors.SchemaDefinitionError(
                    f'enum labels cannot exceed '
                    f'{pg_common.MAX_ENUM_LABEL_LENGTH} characters',
                    span=element_span,
                )

        return s_scalars.AnonymousEnumTypeShell(  # type: ignore
            elements=elements
        )

    elif node.subtypes is not None:
        from . import types as s_types

        assert isinstance(node.maintype, qlast.ObjectRef)
        coll = None
        try:
            coll = s_types.Collection.get_class(node.maintype.name)
        except errors.SchemaError:
            if not allow_generalized_bases:
                raise

        subtypes_list: List[s_types.TypeShell[s_types.Type]] = []
        if coll is None:
            assert allow_generalized_bases
            res = ast_objref_to_type_shell(
                node.maintype,
                modaliases=modaliases,
                metaclass=metaclass,
                schema=schema,
            )
            res.extra_args = tuple(node.subtypes)
            return res

        elif issubclass(coll, s_types.Tuple):
            # Note: if we used abc Tuple here, then we would need anyway
            # to assert it is an instance of s_types.Tuple to make mypy happy
            # (rightly so, because later we use from_subtypes method)

            subtypes: Dict[str, s_types.TypeShell[s_types.Type]] = {}
            # tuple declaration must either be named or unnamed, but not both
            names = set()
            named = None
            unnamed = None
            for si, st in enumerate(node.subtypes):
                if st.name:
                    named = True
                    type_name = st.name

                    if type_name in names:
                        raise errors.SchemaError(
                            f"named tuple has duplicate field '{type_name}'",
                            span=st.span)
                    names.add(type_name)
                else:
                    unnamed = True
                    type_name = str(si)

                if named is not None and unnamed is not None:
                    raise errors.EdgeQLSyntaxError(
                        f'mixing named and unnamed tuple declaration '
                        f'is not supported',
                        span=node.subtypes[0].span,
                    )

                subtypes[type_name] = ast_to_type_shell(
                    cast(qlast.TypeName, st),
                    modaliases=modaliases,
                    metaclass=metaclass,
                    schema=schema,
                )

            try:
                return coll.create_shell(  # type: ignore
                    schema,
                    subtypes=subtypes,
                    typemods={'named': bool(named)},
                )
            except errors.SchemaError as e:
                # all errors raised inside are pertaining to subtypes, so
                # the context should point to the first subtype
                e.set_span(node.subtypes[0].span)
                raise e

        elif issubclass(coll, s_types.Array):
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
                    span=node.span,
                )

            if isinstance(subtypes_list[0], s_types.ArrayTypeShell):
                raise errors.UnsupportedFeatureError(
                    'nested arrays are not supported',
                    span=node.subtypes[0].span,
                )

            try:
                return coll.create_shell(  # type: ignore
                    schema,
                    subtypes=subtypes_list,
                )
            except errors.SchemaError as e:
                e.set_span(node.span)
                raise e

        elif issubclass(coll, (s_types.Range, s_types.MultiRange)):
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
                    span=node.span,
                )

            # FIXME: need to check that subtypes are only anypoint

            try:
                return coll.create_shell(  # type: ignore
                    schema,
                    subtypes=subtypes_list,
                )
            except errors.SchemaError as e:
                e.set_span(node.span)
                raise e

    elif isinstance(node.maintype, qlast.PseudoObjectRef):
        from . import pseudo as s_pseudo
        return s_pseudo.PseudoTypeShell(
            name=sn.UnqualName(node.maintype.name),
            sourcectx=node.maintype.span,
        )  # type: ignore

    assert isinstance(node.maintype, qlast.ObjectRef)

    return ast_objref_to_type_shell(
        node.maintype,
        modaliases=modaliases,
        metaclass=metaclass,
        schema=schema,
    )


def type_op_ast_to_type_shell(
    node: qlast.TypeOp,
    *,
    metaclass: Type[s_types.TypeT],
    module: Optional[str] = None,
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
) -> s_types.TypeExprShell[s_types.TypeT]:

    from . import types as s_types

    if node.op != '|':
        raise errors.UnsupportedFeatureError(
            f'unsupported type expression operator: {node.op}',
            span=node.span,
        )

    if module is None:
        module = modaliases.get(None)

    if module is None:
        raise errors.InternalServerError(
            'cannot determine module for derived compound type',
            span=node.span,
        )

    left = ast_to_type_shell(
        node.left,
        metaclass=metaclass,
        module=module,
        modaliases=modaliases,
        schema=schema,
    )
    right = ast_to_type_shell(
        node.right,
        metaclass=metaclass,
        module=module,
        modaliases=modaliases,
        schema=schema,
    )

    if isinstance(left, s_types.UnionTypeShell):
        if isinstance(right, s_types.UnionTypeShell):
            return s_types.UnionTypeShell(
                components=left.components + right.components,
                module=module,
                schemaclass=metaclass,
                sourcectx=node.span,
            )
        else:
            return s_types.UnionTypeShell(
                components=left.components + (right,),
                module=module,
                schemaclass=metaclass,
                sourcectx=node.span,
            )
    else:
        if isinstance(right, s_types.UnionTypeShell):
            return s_types.UnionTypeShell(
                components=(left,) + right.components,
                schemaclass=metaclass,
                module=module,
                sourcectx=node.span,
            )
        else:
            return s_types.UnionTypeShell(
                components=(left, right),
                module=module,
                schemaclass=metaclass,
                sourcectx=node.span,
            )


def ast_to_object_shell(
    node: Union[qlast.ObjectRef, qlast.TypeName],
    *,
    metaclass: Type[so.Object_T],
    module: Optional[str] = None,
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
) -> so.ObjectShell[so.Object_T]:
    from . import types as s_types

    if isinstance(node, qlast.TypeName):
        if issubclass(metaclass, s_types.Type):
            return ast_to_type_shell(  # type: ignore
                node,
                metaclass=metaclass,
                module=module,
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
    ref: Union[so.Object_T, so.ObjectShell[so.Object_T]],
    *,
    _name: Optional[str] = None,
    disambiguate_std: bool=False,
) -> qlast.TypeExpr:
    from . import types as s_types

    if isinstance(ref, so.ObjectShell):
        return shell_to_ast(schema, ref)
    else:
        t = ref

    result: qlast.TypeExpr

    if isinstance(t, s_types.Type) and t.is_any(schema):
        result = qlast.TypeName(
            name=_name, maintype=qlast.PseudoObjectRef(name='anytype')
        )
    elif isinstance(t, s_types.Type) and t.is_anytuple(schema):
        result = qlast.TypeName(
            name=_name, maintype=qlast.PseudoObjectRef(name='anytuple')
        )
    elif isinstance(t, s_types.Type) and t.is_anyobject(schema):
        result = qlast.TypeName(
            name=_name, maintype=qlast.PseudoObjectRef(name='anyobject')
        )
    elif isinstance(t, s_types.Tuple) and t.is_named(schema):
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                name=t.get_schema_name()
            ),
            subtypes=[
                typeref_to_ast(schema, st, _name=sn,
                               disambiguate_std=disambiguate_std)
                for sn, st in t.iter_subtypes(schema)
            ]
        )
    elif isinstance(t, (s_types.Array, s_types.Tuple,
                        s_types.Range, s_types.MultiRange)):
        # Here the concrete type Array is used because t.get_schema_name()
        # is used, which is not defined for more generic collections and abcs
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                name=t.get_schema_name()
            ),
            subtypes=[
                typeref_to_ast(schema, st,
                               disambiguate_std=disambiguate_std)
                for st in t.get_subtypes(schema)
            ]
        )
    elif isinstance(t, s_types.Type) and t.is_union_type(schema):
        object_set = t.get_union_of(schema)
        assert object_set is not None

        component_objects = tuple(object_set.objects(schema))
        result = typeref_to_ast(schema, component_objects[0],
                                disambiguate_std=disambiguate_std)
        for component_object in component_objects[1:]:
            result = qlast.TypeOp(
                left=result,
                op='|',
                right=typeref_to_ast(schema, component_object,
                                     disambiguate_std=disambiguate_std),
            )
    elif isinstance(t, so.QualifiedObject):
        t_name = t.get_name(schema)
        module = t_name.module
        if disambiguate_std and module == 'std':
            # If the type is defined in 'std::', replace the module to
            # '__std__' to handle cases where 'std' name is aliased to
            # another module.
            module = '__std__'
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                module=module,
                name=t_name.name
            )
        )
    else:
        raise NotImplementedError(f'cannot represent {t!r} as a shell')

    return result


def shell_to_ast(
    schema: s_schema.Schema,
    t: so.ObjectShell[so.Object],
    *,
    _name: Optional[str] = None,
) -> qlast.TypeExpr:
    from . import pseudo as s_pseudo
    from . import types as s_types
    from . import scalars as s_scalars

    result: qlast.TypeExpr
    qlref: qlast.BaseObjectRef

    if isinstance(t, s_pseudo.PseudoTypeShell):
        if t.name.name not in {'anytype', 'anytuple', 'anyobject'}:
            raise AssertionError(f'unexpected pseudo type shell: {t.name!r}')
        result = qlast.TypeName(
            name=_name, maintype=qlast.PseudoObjectRef(name=t.name.name)
        )
    elif isinstance(t, s_types.TupleTypeShell):
        if t.is_named():
            result = qlast.TypeName(
                name=_name,
                maintype=qlast.ObjectRef(
                    name='tuple',
                ),
                subtypes=[
                    shell_to_ast(schema, st, _name=sn)
                    for sn, st in t.iter_subtypes(schema)
                ]
            )
        else:
            result = qlast.TypeName(
                name=_name,
                maintype=qlast.ObjectRef(
                    name='tuple',
                ),
                subtypes=[
                    shell_to_ast(schema, st)
                    for st in t.get_subtypes(schema)
                ]
            )
    elif isinstance(t, s_types.ArrayTypeShell):
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                name='array',
            ),
            subtypes=[
                shell_to_ast(schema, st)
                for st in t.get_subtypes(schema)
            ]
        )
    elif isinstance(t, s_types.RangeTypeShell):
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                name='range',
            ),
            subtypes=[
                shell_to_ast(schema, st)
                for st in t.get_subtypes(schema)
            ]
        )
    elif isinstance(t, s_types.MultiRangeTypeShell):
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                name='multirange',
            ),
            subtypes=[
                shell_to_ast(schema, st)
                for st in t.get_subtypes(schema)
            ]
        )
    elif isinstance(t, s_types.UnionTypeShell):
        components = t.get_components(schema)
        result = typeref_to_ast(schema, components[0])
        for component in components[1:]:
            result = qlast.TypeOp(
                left=result,
                op='|',
                right=typeref_to_ast(schema, component),
            )
    elif isinstance(t, s_scalars.AnonymousEnumTypeShell):
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                name='enum',
            ),
            subtypes=[
                qlast.TypeName(maintype=qlast.ObjectRef(name=x))
                for x in t.elements
            ]
        )
    elif isinstance(t, so.ObjectShell):
        name = t.name
        if isinstance(name, sn.QualName):
            qlref = qlast.ObjectRef(
                module=name.module,
                name=name.name,
            )
        else:
            qlref = qlast.ObjectRef(
                module='',
                name=name.name,
            )
        result = qlast.TypeName(
            name=_name,
            maintype=qlref,
        )
    else:
        raise NotImplementedError(f'cannot represent {t!r} as a shell')

    return result


def is_nontrivial_container(value: Any) -> Optional[Iterable[Any]]:
    trivial_classes = (str, bytes, bytearray, memoryview)
    if (isinstance(value, collections.abc.Iterable) and
            not isinstance(value, trivial_classes)):
        return value
    else:
        return None


def get_class_nearest_common_ancestors(
    schema: s_schema.Schema, classes: Iterable[so.InheritingObjectT]
) -> List[so.InheritingObjectT]:
    # First, find the intersection of parents
    classes = list(classes)
    first = [classes[0]]
    first.extend(classes[0].get_ancestors(schema).objects(schema))
    common = set(first).intersection(
        *[set(c.get_ancestors(schema).objects(schema)) | {c}
          for c in classes[1:]])
    common_list = sorted(common, key=lambda i: first.index(i))
    nearests: List[so.InheritingObjectT] = []
    # Then find the common ancestors that don't have any subclasses that
    # are also nearest common ancestors.
    for anc in common_list:
        if not any(x.issubclass(schema, anc) for x in nearests):
            nearests.append(anc)

    return nearests


def minimize_class_set_by_most_generic(
    schema: s_schema.Schema, classes: Iterable[so.InheritingObjectT]
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
    schema: s_schema.Schema, classes: Iterable[so.InheritingObjectT]
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


def merge_reduce(
    target: so.InheritingObjectT,
    sources: Iterable[so.InheritingObjectT],
    field_name: str,
    *,
    ignore_local: bool,
    schema: s_schema.Schema,
    f: Callable[[T, T], T],
    type: Type[T],
) -> Optional[T]:
    values: list[tuple[T, str]] = []
    if not ignore_local:
        ours = target.get_explicit_local_field_value(schema, field_name, None)
        if ours is not None:
            vn = target.get_verbosename(schema, with_parent=True)
            values.append((ours, vn))
    for source in sources:
        theirs = source.get_explicit_field_value(schema, field_name, None)
        if theirs is not None:
            vn = source.get_verbosename(schema, with_parent=True)
            values.append((theirs, vn))

    if values:
        val = values[0][0]
        desc = values[0][1]
        cdn = target.get_schema_class_displayname()
        for other_val, other_desc in values[1:]:
            try:
                val = f(val, other_val)
            except Exception:
                raise errors.SchemaDefinitionError(
                    f'invalid {cdn} definition: {field_name} is defined '
                    f'as {val} in {desc}, but is defined as {other_val} '
                    f'in {other_desc}, which is incompatible'
                )

        return val
    else:
        return None


def get_nq_name(schema: s_schema.Schema, item: so.Object) -> str:
    shortname = item.get_shortname(schema)
    if isinstance(shortname, sn.QualName):
        return shortname.name
    else:
        return str(shortname)


def find_item_suggestions(
    name: sn.Name,
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
    *,
    item_type: Optional[so.ObjectMeta] = None,
    condition: Optional[Callable[[so.Object], bool]] = None,
) -> Iterable[Tuple[so.Object, str]]:
    from . import functions as s_func
    from . import properties as s_prop
    from . import links as s_link
    from . import modules as s_mod

    orig_modname = name.module if isinstance(name, sn.QualName) else None

    suggestions: List[so.Object] = []

    if modname := modaliases.get(orig_modname, None):
        if schema.get_global(s_mod.Module, modname, None):
            suggestions.extend(
                schema.get_objects(
                    included_modules=[sn.UnqualName(modname)],
                ),
            )

        modname = f'std::{modname}'
        if schema.get_global(s_mod.Module, modname, None):
            suggestions.extend(
                schema.get_objects(
                    included_modules=[sn.UnqualName(modname)],
                ),
            )

    if orig_modname:
        if schema.get_global(s_mod.Module, orig_modname, None):
            suggestions.extend(
                schema.get_objects(
                    included_modules=[sn.UnqualName(orig_modname)],
                ),
            )

        modname = f'std::{orig_modname}'
        if schema.get_global(s_mod.Module, modname, None):
            suggestions.extend(
                schema.get_objects(
                    included_modules=[sn.UnqualName(modname)],
                ),
            )

    else:
        suggestions.extend(
            schema.get_objects(
                included_modules=[sn.UnqualName("std")],
            ),
        )

    filters = []

    # links and properties are suggested by find_fields_suggestions
    filters.append(
        lambda s: not isinstance(s, s_prop.Property)
        and not isinstance(s, s_link.Link)
    )

    if condition is not None:
        filters.append(condition)

    if item_type is not None:
        it = item_type
        filters.append(lambda s: isinstance(s, it))
    else:
        # When schema class is not specified, only suggest generic objects.
        filters.append(lambda s: not sn.is_fullname(str(s.get_name(schema))))
        filters.append(lambda s: not isinstance(s, s_func.CallableObject))

    # Never suggest object fragments.
    filters.append(lambda s: not isinstance(s, so.ObjectFragment))

    filtered = filter(lambda s: all(f(s) for f in filters), suggestions)

    # Add display names
    cur_module_name = modaliases.get(None)

    def get_display_name(suggestion: so.Object) -> str:
        if isinstance(suggestion, so.QualifiedObject):
            mod = suggestion.get_name(schema).module
            if mod == "std" or mod == cur_module_name:
                return get_nq_name(schema, suggestion)

        return suggestion.get_displayname(schema)

    return ((s, get_display_name(s)) for s in filtered)


def find_pointer_suggestions(
    schema: s_schema.Schema,
    item_type: Optional[so.ObjectMeta],
    parent: Optional[so.Object],
) -> Iterable[Tuple[so.Object, str]]:
    from . import pointers as s_pointers

    """
    Suggests pointers (properties or links) from parent object type.
    If pointer type is not expected, use .name notation.
    """
    from . import sources as s_sources

    if not isinstance(parent, s_sources.Source):
        return ()

    pointers_with_names = parent.get_pointers(schema).items(schema)
    pointers = (pointer for _, pointer in pointers_with_names)

    suggestions = ((s, s.get_displayname(schema)) for s in pointers)

    if item_type is not s_pointers.Pointer:
        # Prefix with .
        suggestions = ((s, "." + n) for s, n in suggestions)

    return suggestions


def pick_closest_suggestions(
    name: sn.Name,
    schema: s_schema.Schema,
    suggestions: Iterable[Tuple[so.Object, str]],
    limit: int,
) -> List[Tuple[so.Object, str]]:
    local_name = name.name

    # Compute Levenshtein distance for each suggestion.
    with_distance: List[Tuple[so.Object, str, int]] = [
        (s, name, levenshtein.distance(local_name, get_nq_name(schema, s)))
        for s, name in suggestions
    ]

    # Filter out suggestions that are too dissimilar.
    max_distance = 3
    closest = list(filter(lambda s: s[2] < max_distance, with_distance))

    # Sort by proximity, then by whether the suggestion is contains
    # the source string at the beginning, then by suggestion name.
    closest.sort(
        key=lambda s: (
            s[2],
            not get_nq_name(schema, s[0]).startswith(local_name),
            s[1],
        )
    )

    return [(s[0], s[1]) for s in closest[:limit]]


def enrich_schema_lookup_error(
    error: errors.EdgeDBError,
    item_name: sn.Name,
    modaliases: Mapping[Optional[str], str],
    schema: s_schema.Schema,
    *,
    item_type: Optional[so.ObjectMeta] = None,
    suggestion_limit: int = 3,
    condition: Optional[Callable[[so.Object], bool]] = None,
    span: Optional[parsing.Span] = None,
    pointer_parent: Optional[so.Object] = None,
    hint_text: str = 'did you mean'
) -> None:

    all_suggestions = itertools.chain(
        find_item_suggestions(
            item_name,
            modaliases,
            schema,
            item_type=item_type,
            condition=condition,
        ),
        find_pointer_suggestions(schema, item_type, pointer_parent),
    )

    suggestions = pick_closest_suggestions(
        item_name, schema, all_suggestions, suggestion_limit
    )

    if suggestions:
        names = [name for _, name in suggestions]

        if len(names) > 1:
            hint = f'{hint_text} one of these: {", ".join(names)}?'
        else:
            hint = f'{hint_text} {names[0]!r}?'

        error.set_hint_and_details(hint=hint)

    if span is not None:
        error.set_span(span)


def ensure_union_type(
    schema: s_schema.Schema,
    types: Sequence[s_types.Type],
    *,
    opaque: bool = False,
    module: Optional[str] = None,
    transient: bool = False,
) -> Tuple[s_schema.Schema, s_types.Type, bool]:

    from edb.schema import objtypes as s_objtypes

    if len(types) == 1 and not opaque:
        return schema, next(iter(types)), False

    seen_scalars = False
    seen_objtypes = False
    created = False

    for t in types:
        if isinstance(t, s_objtypes.ObjectType):
            if seen_scalars:
                raise _union_error(schema, types)
            seen_objtypes = True
        else:
            if seen_objtypes:
                raise _union_error(schema, types)
            seen_scalars = True

    if seen_scalars:
        uniontype: s_types.Type = types[0]
        for t1 in types[1:]:

            schema, common_type = (
                uniontype.find_common_implicitly_castable_type(t1, schema)
            )

            if common_type is None:
                raise _union_error(schema, types)
            else:
                uniontype = common_type
    else:
        objtypes = cast(
            Sequence[s_objtypes.ObjectType],
            types,
        )
        schema, uniontype, created = s_objtypes.get_or_create_union_type(
            schema,
            components=objtypes,
            opaque=opaque,
            module=module,
            transient=transient,
        )

    return schema, uniontype, created


def simplify_union_types(
    schema: s_schema.Schema,
    types: Sequence[s_types.Type],
) -> Sequence[s_types.Type]:
    """Minimize the types used to create a union of types.

    Any unions types are unwrapped. Then, any unnecessary subclasses are
    removed.
    """

    from edb.schema import types as s_types

    components: Set[s_types.Type] = set()
    for t in types:
        union_of = t.get_union_of(schema)
        if union_of:
            components.update(union_of.objects(schema))
        else:
            components.add(t)

    if all(isinstance(c, s_types.InheritingType) for c in components):
        return list(minimize_class_set_by_most_generic(
            schema,
            cast(Set[s_types.InheritingType], components),
        ))
    else:
        return list(components)


def simplify_union_types_preserve_derived(
    schema: s_schema.Schema,
    types: Sequence[s_types.Type],
) -> Sequence[s_types.Type]:
    """Minimize the types used to create a union of types.

    Any unions types are unwrapped. Then, any unnecessary subclasses are
    removed.

    Derived types are always preserved for 'std::UNION', 'std::IF', and
    'std::??'.
    """

    from edb.schema import types as s_types

    components: Set[s_types.Type] = set()
    for t in types:
        union_of = t.get_union_of(schema)
        if union_of:
            components.update(union_of.objects(schema))
        else:
            components.add(t)

    derived = set(
        t
        for t in components
        if (
            isinstance(t, s_types.InheritingType)
            and t.get_is_derived(schema)
        )
    )

    nonderived: Sequence[s_types.Type] = [
        t
        for t in components
        if t not in derived
    ]
    nonderived = minimize_class_set_by_most_generic(
        schema,
        cast(Set[s_types.InheritingType], nonderived),
    )

    return list(nonderived) + list(derived)


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


def get_type_expr_non_overlapping_union(
    type: s_types.Type,
    schema: s_schema.Schema,
) -> Tuple[FrozenSet[s_types.Type], bool]:
    """Get a non-overlapping set of the type's descendants"""

    from edb.schema import types as s_types

    expanded_types = expand_type_expr_descendants(type, schema)

    # filter out subclasses
    expanded_types = {
        type
        for type in expanded_types
        if not any(
            type is not other and type.issubclass(schema, other)
            for other in expanded_types
        )
    }

    non_overlapping, union_is_exhaustive = get_non_overlapping_union(
        schema, cast(set[so.InheritingObject], expanded_types)
    )

    return cast(FrozenSet[s_types.Type], non_overlapping), union_is_exhaustive


def expand_type_expr_descendants(
    type: s_types.Type,
    schema: s_schema.Schema,
    *,
    expand_opaque_union: bool = True,
) -> set[s_types.Type]:
    """Expand types and type expressions to get descendants"""

    from edb.schema import types as s_types

    if sub_union := type.get_union_of(schema):
        # Expanding a union
        # Get the union of the component descendants
        return set.union(*(
            expand_type_expr_descendants(
                component, schema,
            )
            for component in sub_union.objects(schema)
        ))

    elif sub_intersection := type.get_intersection_of(schema):
        # Expanding an intersection
        # Get the intersection of component descendants
        return set.intersection(*(
            expand_type_expr_descendants(
                component, schema
            )
            for component in sub_intersection.objects(schema)
        ))

    elif type.is_view(schema):
        # When expanding a view, simply unpeel the view.
        return expand_type_expr_descendants(
            type.peel_view(schema), schema
        )

    # Return simple type and all its descendants.
    # Some types (eg. BaseObject) have non-simple descendants, filter them out.
    return {type} | {
        c for c in cast(
            set[s_types.Type],
            set(cast(so.InheritingObject, type).descendants(schema))
        )
        if (
            not c.is_union_type(schema)
            and not c.is_intersection_type(schema)
            and not c.is_view(schema)
        )
    }


def _union_error(
    schema: s_schema.Schema, components: Iterable[s_types.Type]
) -> errors.SchemaError:
    names = ', '.join(sorted(c.get_displayname(schema) for c in components))
    return errors.SchemaError(f'using incompatible types {names}')


def ensure_intersection_type(
    schema: s_schema.Schema,
    types: Sequence[s_types.Type],
    *,
    transient: bool = False,
    module: Optional[str] = None,
) -> Tuple[s_schema.Schema, s_types.Type]:

    from edb.schema import objtypes as s_objtypes

    if len(types) == 1:
        return schema, next(iter(types))

    seen_scalars = False
    seen_objtypes = False

    for t in types:
        if t.is_object_type():
            if seen_scalars:
                raise _intersection_error(schema, types)
            seen_objtypes = True
        else:
            if seen_objtypes:
                raise _intersection_error(schema, types)
            seen_scalars = True

    if seen_scalars:
        # Non-related scalars and collections cannot for intersection types.
        raise _intersection_error(schema, types)
    else:
        return s_objtypes.get_or_create_intersection_type(
            schema,
            components=cast(Iterable[s_objtypes.ObjectType], types),
            module=module,
            transient=transient,
        )


def simplify_intersection_types(
    schema: s_schema.Schema,
    types: Sequence[s_types.Type],
) -> Sequence[s_types.Type]:
    """Minimize the types used to create an intersection of types.

    Any intersection types are unwrapped. Then, any unnecessary superclasses are
    removed.
    """

    from edb.schema import types as s_types

    components: Set[s_types.Type] = set()
    for t in types:
        intersection_of = t.get_intersection_of(schema)
        if intersection_of:
            components.update(intersection_of.objects(schema))
        else:
            components.add(t)

    if all(isinstance(c, s_types.InheritingType) for c in components):
        return minimize_class_set_by_least_generic(
            schema,
            cast(Set[s_types.InheritingType], components),
        )
    else:
        return list(components)


def _intersection_error(
    schema: s_schema.Schema, components: Iterable[s_types.Type]
) -> errors.SchemaError:
    names = ', '.join(sorted(c.get_displayname(schema) for c in components))
    return errors.SchemaError(f'cannot create an intersection of {names}')


MAX_INT64 = 2 ** 63 - 1
MIN_INT64 = -2 ** 63


def const_ast_from_python(val: Any, with_secrets: bool=False) -> qlast.Expr:
    if isinstance(val, str):
        return qlast.Constant.string(val)
    elif isinstance(val, bool):
        return qlast.Constant.boolean(val)
    elif isinstance(val, int):
        if MIN_INT64 <= val <= MAX_INT64:
            return qlast.Constant.integer(val)
        else:
            raise ValueError(f'int64 value out of range: {val}')
    elif isinstance(val, decimal.Decimal):
        return qlast.Constant(value=f'{val}n', kind=qlast.ConstantKind.DECIMAL)
    elif isinstance(val, float):
        return qlast.Constant(value=str(val), kind=qlast.ConstantKind.FLOAT)
    elif isinstance(val, bytes):
        return qlast.BytesConstant(value=val)
    elif isinstance(val, statypes.Duration):
        return qlast.TypeCast(
            type=qlast.TypeName(
                maintype=qlast.ObjectRef(module='__std__', name='duration'),
            ),
            expr=qlast.Constant.string(value=val.to_iso8601()),
        )
    elif isinstance(val, statypes.CompositeType):
        return qlast.InsertQuery(
            subject=name_to_ast_ref(sn.name_from_string(val._tspec.name)),
            shape=[
                qlast.ShapeElement(
                    expr=qlast.Path(steps=[qlast.Ptr(name=ptr)]),
                    compexpr=const_ast_from_python(
                        getattr(val, ptr), with_secrets=with_secrets
                    ),
                )
                for ptr, typ in val._tspec.fields.items()
                if not (typ.secret and not with_secrets) and not typ.protected
            ],
        )
    elif isinstance(val, (set, frozenset)):
        return qlast.Set(elements=[
            const_ast_from_python(x, with_secrets=with_secrets) for x in val
        ])
    elif val is None:
        return qlast.Set(elements=[])
    else:
        raise ValueError(f'unexpected constant type: {type(val)!r}')


def get_config_type_shape(
    schema: s_schema.Schema,
    stype: s_objtypes.ObjectType,
    path: List[qlast.PathElement],
) -> List[qlast.ShapeElement]:
    from . import objtypes as s_objtypes
    shape = [
        qlast.ShapeElement(
            expr=qlast.Path(steps=[qlast.Ptr(name='_tname')], ),
            compexpr=qlast.Path(
                steps=path + [
                    qlast.Ptr(name='__type__'),
                    qlast.Ptr(name='name'),
                ],
            ),
        ),
    ]
    seen: Set[str] = set()

    stypes = [stype] + list(stype.ordered_descendants(schema))

    for t in stypes:
        t_name = t.get_name(schema)

        for unqual_pn, p in t.get_pointers(schema).items(schema):
            pn = str(unqual_pn)
            if pn in ('id', '__type__') or pn in seen:
                continue

            elem_path: List[qlast.PathElement] = []

            if t != stype:
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

            elem_path.append(qlast.Ptr(name=pn))

            ptype = p.get_target(schema)
            assert ptype is not None
            if str(ptype.get_name(schema)) == 'cfg::AbstractConfig':
                continue

            if isinstance(ptype, s_objtypes.ObjectType):
                subshape = get_config_type_shape(
                    schema, ptype, path + elem_path)
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


def type_shell_multi_substitute(
    mapping: Dict[sn.Name, s_types.TypeShell[s_types.TypeT_co]],
    typ: s_types.TypeShell[s_types.TypeT_co],
    schema: s_schema.Schema,
) -> s_types.TypeShell[s_types.TypeT_co]:
    for name, new in mapping.items():
        typ = type_shell_substitute(name, new, typ, schema)
    return typ


def type_shell_substitute(
    name: sn.Name,
    new: s_types.TypeShell[s_types.TypeT_co],
    typ: s_types.TypeShell[s_types.TypeT_co],
    schema: s_schema.Schema,
) -> s_types.TypeShell[s_types.TypeT_co]:
    from . import types as s_types

    # arguably this would be better done with a method on the types
    if typ.name == name:
        return new

    if isinstance(typ, s_types.UnionTypeShell):
        assert isinstance(typ.name, sn.QualName)
        return s_types.UnionTypeShell(
            module=typ.name.module,
            schemaclass=typ.schemaclass,
            opaque=typ.opaque,
            components=[
                type_shell_substitute(name, new, c, schema)
                for c in typ.components
            ],
        )
    elif isinstance(typ, s_types.IntersectionTypeShell):
        assert isinstance(typ.name, sn.QualName)
        return s_types.IntersectionTypeShell(
            module=typ.name.module,
            schemaclass=typ.schemaclass,
            components=[
                type_shell_substitute(name, new, c, schema)
                for c in typ.components
            ],
        )
    elif isinstance(typ, s_types.ArrayTypeShell):
        return s_types.ArrayTypeShell(
            name=None,
            expr=typ.expr,
            typemods=typ.typemods,
            schemaclass=typ.schemaclass,
            subtype=type_shell_substitute(name, new, typ.subtype, schema),
        )
    elif isinstance(typ, s_types.TupleTypeShell):
        return s_types.TupleTypeShell(
            name=None,
            typemods=typ.typemods,
            schemaclass=typ.schemaclass,
            subtypes={
                k: type_shell_substitute(name, new, v, schema)
                for k, v in typ.subtypes.items()
            },
        )
    elif isinstance(typ, s_types.RangeTypeShell):
        return s_types.RangeTypeShell(
            name=None,
            typemods=typ.typemods,
            schemaclass=typ.schemaclass,
            subtype=type_shell_substitute(name, new, typ.subtype, schema),
        )
    else:
        return typ


def try_compile_irast_to_sql_tree(
    compiled_expr: s_expr.CompiledExpression,
    span: Optional[parsing.Span]
) -> None:
    # compile the expression to sql to preempt errors downstream

    from edb.pgsql import compiler as pg_compiler

    try:
        pg_compiler.compile_ir_to_sql_tree(
            compiled_expr.irast,
            output_format=pg_compiler.OutputFormat.NATIVE,
            singleton_mode=True,
        )
    except errors.EdgeDBError as exception:
        exception.set_span(span)
        raise exception
    except:
        raise
