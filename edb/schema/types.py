#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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

import collections.abc
import enum
import types
import typing
import uuid

from edb import errors

from edb.common import checked
from edb.common import uuidgen

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import abc as s_abc
from . import delta as sd
from . import derivable
from . import expr as s_expr
from . import name as s_name
from . import objects as so
from . import schema as s_schema
from . import utils

if typing.TYPE_CHECKING:
    # We cannot use `from typing import *` in this file due to name conflict
    # with local Tuple and Type classes.
    from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional
    from typing import Sequence, Union
    from edb.common import parsing
    from edb.ir import ast as irast
    from edb.schema import scalars

TYPE_ID_NAMESPACE = uuidgen.UUID('00e50276-2502-11e7-97f2-27fe51238dbd')
MAX_TYPE_DISTANCE = 1_000_000_000


class ExprType(enum.IntEnum):
    """Enumeration to identify the type of an expression in aliases."""
    Select = enum.auto()
    Insert = enum.auto()
    Update = enum.auto()


TypeT = typing.TypeVar('TypeT', bound='Type')


class Type(so.InheritingObjectBase, derivable.DerivableObjectBase, s_abc.Type):
    """A schema item that is a valid *type*."""

    # If this type is an alias, expr will contain an expression that
    # defines it.
    expr = so.SchemaField(
        s_expr.Expression,
        default=None, coerce=True, compcoef=0.909)

    # For a type representing an expression alias, this would contain the
    # expressoin type.  Non-alias types have None here.
    expr_type = so.SchemaField(
        ExprType,
        default=None, compcoef=0.909)

    # True for aliases defined by CREATE ALIAS, false for local
    # aliases in queries.
    alias_is_persistent = so.SchemaField(
        bool,
        default=False, compcoef=None)

    # If this type is a view defined by a nested shape expression,
    # and the nested shape contains references to link properties,
    # rptr will contain the inbound pointer class.
    rptr = so.SchemaField(
        so.Object,
        weak_ref=True,
        default=None, compcoef=0.909)

    # The OID by which the backend refers to the type.
    backend_id = so.SchemaField(
        int,
        default=None, inheritable=False, introspectable=False)

    def is_blocking_ref(
        self, schema: s_schema.Schema, reference: so.Object
    ) -> bool:
        return reference is not self.get_rptr(schema)

    def derive_subtype(
        self: TypeT,
        schema: s_schema.Schema,
        *,
        name: s_name.Name,
        mark_derived: bool = False,
        attrs: Optional[Mapping] = None,
        inheritance_merge: bool = True,
        preserve_path_id: bool = False,
        refdict_whitelist=None,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, TypeT]:

        if self.get_name(schema) == name:
            raise errors.SchemaError(
                f'cannot derive {self!r}({name}) from itself')

        derived_attrs: Dict[str, object] = {}

        if attrs is not None:
            derived_attrs.update(attrs)

        derived_attrs['name'] = name
        derived_attrs['bases'] = so.ObjectList.create(schema, [self])

        cmdcls = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.CreateObject, type(self))

        cmd = cmdcls(classname=name)

        for k, v in derived_attrs.items():
            cmd.set_attribute_value(k, v)

        context = sd.CommandContext(
            modaliases={},
            schema=schema,
        )

        delta = sd.DeltaRoot()

        with context(sd.DeltaRootContext(schema=schema, op=delta)):
            if not inheritance_merge:
                context.current().inheritance_merge = False

            if refdict_whitelist is not None:
                context.current().inheritance_refdicts = refdict_whitelist

            if mark_derived:
                context.current().mark_derived = True

            if preserve_path_id:
                context.current().preserve_path_id = True

            delta.add(cmd)
            schema = delta.apply(schema, context)

        derived = typing.cast(TypeT, schema.get(name))

        return schema, derived

    def is_type(self) -> bool:
        return True

    def is_object_type(self) -> bool:
        return False

    def is_union_type(self, schema) -> bool:
        return False

    def is_intersection_type(self, schema) -> bool:
        return False

    def is_compound_type(self, schema) -> bool:
        return False

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return False

    def is_any(self) -> bool:
        return False

    def is_anytuple(self) -> bool:
        return False

    def find_any(self, schema: s_schema.Schema) -> Optional[Type]:
        if self.is_any():
            return self
        else:
            return None

    def contains_any(self, schema: s_schema.Schema) -> bool:
        return self.is_any()

    def is_scalar(self) -> bool:
        return False

    def is_collection(self) -> bool:
        return False

    def is_array(self) -> bool:
        return False

    def is_tuple(self) -> bool:
        return False

    def is_enum(self, schema):
        return False

    def test_polymorphic(self, schema: s_schema.Schema, poly: Type) -> bool:
        """Check if this type can be matched by a polymorphic type.

        Examples:

            - `array<anyscalar>`.test_polymorphic(`array<anytype>`) -> True
            - `array<str>`.test_polymorphic(`array<anytype>`) -> True
            - `array<int64>`.test_polymorphic(`anyscalar`) -> False
            - `float32`.test_polymorphic(`anyint`) -> False
            - `int32`.test_polymorphic(`anyint`) -> True
        """

        if not poly.is_polymorphic(schema):
            raise TypeError('expected a polymorphic type as a second argument')

        if poly.is_any():
            return True

        return self._test_polymorphic(schema, poly)

    def resolve_polymorphic(
        self, schema: s_schema.Schema, other: Type
    ) -> Optional[Type]:
        """Resolve the polymorphic type component.

        Examples:

            - `array<anytype>`.resolve_polymorphic(`array<int>`) -> `int`
            - `array<anytype>`.resolve_polymorphic(`tuple<int>`) -> None
        """
        if not self.is_polymorphic(schema):
            return None

        return self._resolve_polymorphic(schema, other)

    def to_nonpolymorphic(
        self, schema: s_schema.Schema, concrete_type: Type
    ) -> Type:
        """Produce an non-polymorphic version of self.

        Example:
            `array<anytype>`.to_nonpolymorphic(`int`) -> `array<int>`
            `tuple<int, anytype>`.to_nonpolymorphic(`str`) -> `tuple<int, str>`
        """
        if not self.is_polymorphic(schema):
            raise TypeError('non-polymorphic type')

        return self._to_nonpolymorphic(schema, concrete_type)

    def _test_polymorphic(self, schema: s_schema.Schema, other: Type) -> bool:
        return False

    def _resolve_polymorphic(self, schema, concrete_type: Type):
        raise NotImplementedError(
            f'{type(self)} does not support resolve_polymorphic()')

    def _to_nonpolymorphic(self, schema, concrete_type: Type):
        raise NotImplementedError(
            f'{type(self)} does not support to_nonpolymorphic()')

    def is_view(self, schema: s_schema.Schema) -> bool:
        return self.get_expr_type(schema) is not None

    def assignment_castable_to(
        self, other: Type, schema: s_schema.Schema
    ) -> bool:
        return self.implicitly_castable_to(other, schema)

    def implicitly_castable_to(self, other: Type, schema) -> bool:
        return False

    def get_implicit_cast_distance(
        self, other: Type, schema: s_schema.Schema
    ) -> int:
        return -1

    def find_common_implicitly_castable_type(
            self, other: Type, schema) -> Optional[Type]:
        return None

    def explicitly_castable_to(self, other: Type, schema) -> bool:
        if self.implicitly_castable_to(other, schema):
            return True

        if self.assignment_castable_to(other, schema):
            return True

        return False

    def get_union_of(
        self,
        schema: s_schema.Schema,
    ) -> Optional[so.ObjectSet[Type]]:
        return None

    def get_is_opaque_union(self, schema: s_schema.Schema) -> bool:
        return False

    def get_intersection_of(
        self,
        schema: s_schema.Schema,
    ) -> Optional[so.ObjectSet[Type]]:
        return None

    def material_type(
        self, schema: s_schema.Schema
    ) -> Type:
        return typing.cast(Type, self.get_nearest_non_derived_parent(schema))

    def peel_view(self, schema: s_schema.Schema) -> Type:
        # When self is a view, this returns the class the view
        # is derived from (which may be another view).  If no
        # parent class is available, returns self.
        if self.is_view(schema):
            return typing.cast(Type, self.get_bases(schema).first(schema))
        else:
            return self

    def get_common_parent_type_distance(
            self, other: Type, schema) -> int:
        if other.is_any() or self.is_any():
            return MAX_TYPE_DISTANCE

        if not isinstance(other, type(self)):
            return -1

        if self == other:
            return 0

        ancestor = utils.get_class_nearest_common_ancestor(
            schema, [self, other])

        if ancestor is self:
            return 0
        else:
            ancestors = list(self.get_ancestors(schema).objects(schema))
            return ancestors.index(ancestor) + 1

    def as_create_delta_for_compound_type(
        self,
        schema: s_schema.Schema,
    ) -> Optional[sd.CreateObject]:
        return None

    def allow_ref_propagation(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
    ) -> bool:
        return not self.is_view(schema)


TypeExprRefT = typing.TypeVar('TypeExprRefT', bound='TypeExprRef')


class TypeExprRef(so.Object, so.BaseObjectRef):
    _components: typing.Tuple[so.ObjectRef, ...]
    module: str

    def __init__(
        self,
        components: Iterable[so.ObjectRef], *,
        module: str,
    ) -> None:
        self.__dict__['_components'] = tuple(components)
        self.__dict__['module'] = module

    def resolve_components(self, schema) -> typing.Tuple[Type, ...]:
        return tuple(
            typing.cast(Type, c._resolve_ref(schema))
            for c in self._components
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        else:
            return self._components == other._components

    def __hash__(self) -> int:
        return hash((type(self), self._components))

    def _reduce_to_ref(
        self: TypeExprRefT, schema: s_schema.Schema
    ) -> typing.Tuple[TypeExprRefT, Any]:
        return self, self._components


class UnionTypeRef(TypeExprRef):

    def _resolve_ref(self, schema: s_schema.Schema) -> Type:
        components = self.resolve_components(schema)
        type_id, _ = get_union_type_id(schema, components, module=self.module)
        return typing.cast(Type, schema.get_by_id(type_id))

    def get_union_of(self, schema) -> typing.Tuple[so.ObjectRef, ...]:
        return self._components

    def __repr__(self) -> str:
        return f'<UnionTypeRef {self._components!r} at 0x{id(self):x}>'


# Breaking Liskov Substitution Principle
class ExistingUnionTypeRef(UnionTypeRef, so.ObjectRef):  # type: ignore

    def __init__(
        self,
        *,
        name: s_name.Name,
        components: Iterable[so.ObjectRef],
        origname: Optional[str]=None,
        schemaclass: Optional[typing.Type[so.Object]]=None,
        sourcectx=None,
    ) -> None:

        so.ObjectRef.__init__(
            self, name=name, origname=origname,
            schemaclass=schemaclass, sourcectx=sourcectx)

        UnionTypeRef.__init__(
            self, components=components, module=name.module)


class IntersectionTypeRef(TypeExprRef):

    def _resolve_ref(self, schema: s_schema.Schema) -> Type:
        components = self.resolve_components(schema)
        type_id, _ = get_intersection_type_id(
            schema, components, module=self.module)
        return typing.cast(Type, schema.get_by_id(type_id))

    def get_intersection_of(self, schema) -> typing.Tuple[so.ObjectRef, ...]:
        return self._components

    def __repr__(self) -> str:
        return f'<IntersectionTypeRef {self._components!r} at 0x{id(self):x}>'


# Breaking Liskov Substitution Principle
class ExistingIntersectionTypeRef(  # type: ignore
        IntersectionTypeRef, so.ObjectRef):

    def __init__(
        self,
        *,
        name: s_name.Name,
        components: Iterable[so.ObjectRef],
        origname: Optional[str]=None,
        schemaclass: Optional[typing.Type[so.Object]]=None,
        sourcectx=None,
    ) -> None:

        so.ObjectRef.__init__(
            self, name=name, origname=origname,
            schemaclass=schemaclass, sourcectx=sourcectx)

        IntersectionTypeRef.__init__(
            self, components=components, module=name.module)


class Collection(Type, s_abc.Collection):

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return any(st.is_polymorphic(schema)
                   for st in self.get_subtypes(schema))

    def find_any(self, schema: s_schema.Schema) -> Optional[Type]:
        for st in self.get_subtypes(schema):
            any_t = st.find_any(schema)
            if any_t is not None:
                return any_t

        return None

    def contains_any(self, schema):
        return any(st.contains_any(schema) for st in self.get_subtypes(schema))

    def contains_object(self, schema: s_schema.Schema) -> bool:
        return any(
            st.contains_object(schema) if isinstance(st, Collection)
            else st.is_object_type()
            for st in self.get_subtypes(schema)
        )

    def contains_array_of_tuples(self, schema):
        raise NotImplementedError

    def is_collection(self) -> bool:
        return True

    def get_common_parent_type_distance(
            self, other: Type, schema: s_schema.Schema) -> int:
        if other.is_any():
            return 1

        if other.__class__ is not self.__class__:
            return -1

        other = typing.cast(Collection, other)
        other_types = other.get_subtypes(schema)
        my_types = self.get_subtypes(schema)

        type_dist = 0
        for ot, my in zip(other_types, my_types):
            el_dist = my.get_common_parent_type_distance(ot, schema)
            if el_dist < 0:
                return -1
            else:
                type_dist += el_dist

        return type_dist

    def _issubclass(
        self, schema: s_schema.Schema, parent: so.InheritingObjectBase
    ) -> bool:
        if isinstance(parent, Type) and parent.is_any():
            return True

        if parent.__class__ is not self.__class__:
            return False

        # The cast below should not be necessary but Mypy does not believe
        # that a.__class__ == b.__class__ is enough.
        parent_types = typing.cast(Collection, parent).get_subtypes(schema)
        my_types = self.get_subtypes(schema)

        for pt, my in zip(parent_types, my_types):
            if not pt.is_any() and not my.issubclass(schema, pt):
                return False

        return True

    def issubclass(
        self,
        schema: s_schema.Schema,
        parent: Union[
            so.InheritingObjectBase,
            typing.Tuple[so.InheritingObjectBase, ...],
        ],
    ) -> bool:
        if isinstance(parent, tuple):
            return any(self.issubclass(schema, p) for p in parent)

        if isinstance(parent, Type) and parent.is_any():
            return True

        return self._issubclass(schema, parent)

    @classmethod
    def compare_values(cls, ours, theirs, *,
                       our_schema, their_schema, context, compcoef):
        if (ours is None) != (theirs is None):
            return compcoef
        elif ours is None:
            return 1.0

        if type(ours) is not type(theirs):
            basecoef = 0.2
        else:
            my_subtypes = ours.get_subtypes(our_schema)
            other_subtypes = theirs.get_subtypes(their_schema)

            similarity = []
            for i, st in enumerate(my_subtypes):
                similarity.append(
                    st.compare(
                        other_subtypes[i], our_schema=our_schema,
                        their_schema=their_schema, context=context))

            basecoef = sum(similarity) / len(similarity)

        return basecoef + (1 - basecoef) * compcoef

    def get_container(self):
        raise NotImplementedError

    def get_subtypes(self, schema: s_schema.Schema) -> typing.Tuple[Type, ...]:
        raise NotImplementedError

    def get_typemods(self, schema: s_schema.Schema):
        return ()

    @classmethod
    def get_class(
        cls, schema_name: str
    ) -> Union[typing.Type[Array], typing.Type[Tuple]]:
        if schema_name == 'array':
            return Array
        elif schema_name == 'tuple':
            return Tuple

        raise errors.SchemaError(
            'unknown collection type: {!r}'.format(schema_name))

    @classmethod
    def from_subtypes(
        cls,
        schema: s_schema.Schema,
        subtypes: Any,
        typemods: Any = None,
    ) -> Collection:
        raise NotImplementedError

    def _reduce_to_ref(
        self, schema: s_schema.Schema
    ) -> typing.Tuple[Collection, Any]:
        strefs = []

        for st in self.get_subtypes(schema):
            st_ref, _ = st._reduce_to_ref(schema)
            strefs.append(st_ref)

        return (
            self.__class__.from_subtypes(
                schema, strefs, typemods=self.get_typemods(schema)),
            (self.__class__, tuple(r.name for r in strefs))
        )

    def _resolve_ref(self, schema: s_schema.Schema) -> Collection:
        if any(hasattr(st, '_resolve_ref')
               for st in self.get_subtypes(schema)):

            subtypes = []
            for st in self.get_subtypes(schema):
                subtypes.append(st._resolve_ref(schema))

            return self.__class__.from_subtypes(
                schema, subtypes, typemods=self.get_typemods(schema))
        else:
            return self

    def __repr__(self) -> str:
        return (
            f'<{self.__class__.__name__} '
            f'{self.name} {self.id} '
            f'at 0x{id(self):x}>'
        )

    def dump(self, schema: s_schema.Schema) -> str:
        return repr(self)

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        *,
        view_name: Optional[str] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> sd.Command:
        raise NotImplementedError

    def as_delete_delta(
        self, schema: s_schema.Schema, *, view_name: str = None
    ) -> sd.Command:
        raise NotImplementedError


class EphemeralCollection(Collection):
    # Mypy is unhappy with fields being so.Fields here and not so.SchemaFields.
    # This forces us to liberally apply type-ignores for now.

    name = so.Field(  # type: ignore
        s_name.SchemaName,
        inheritable=False, compcoef=0.670)

    expr_type = so.Field(  # type: ignore
        ExprType,
        default=None, ephemeral=True)

    expr = so.Field(  # type: ignore
        s_expr.Expression,
        default=None, ephemeral=True)

    rptr = so.Field(  # type: ignore
        so.Object,
        default=None, ephemeral=True)

    def get_name(self, schema: s_schema.Schema) -> s_name.Name:
        return self.name  # type: ignore

    def get_shortname(self, schema: s_schema.Schema) -> s_name.Name:
        return self.name  # type: ignore

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return 'collection'

    def get_expr_type(self, schema: s_schema.Schema) -> None:
        return self.expr_type  # type: ignore

    def get_expr(self, schema: s_schema.Schema) -> s_expr.Expression:
        return self.expr  # type: ignore

    def get_rptr(self, schema: s_schema.Schema) -> so.Object:
        return self.rptr  # type: ignore


Dimensions = checked.FrozenCheckedList[int]
BaseArray_T = typing.TypeVar("BaseArray_T", bound="BaseArray")


class BaseArray(Collection, s_abc.Array):
    schema_name = 'array'

    @classmethod
    def create(
        cls: typing.Type[BaseArray_T],
        schema: s_schema.Schema,
        *,
        name: Optional[s_name.Name] = None,
        id: Union[uuid.UUID, so.NoDefaultT] = so.NoDefault,
        dimensions: Sequence[int] = (),
        element_type: Any,
        **kwargs: Any,
    ) -> BaseArray_T:
        if not dimensions:
            dimensions = [-1]

        if dimensions != [-1]:
            raise errors.UnsupportedFeatureError(
                f'multi-dimensional arrays are not supported')

        if id is so.NoDefault:
            id = generate_type_id(f'array-{element_type.id}-{dimensions}')

        if name is None:
            name = s_name.SchemaName(
                module='std',
                name=f'array<{element_type.get_name(schema)}>')

        return super()._create(
            schema, id=id, name=name, element_type=element_type,
            dimensions=dimensions, **kwargs)

    def contains_array_of_tuples(self, schema: s_schema.Schema) -> bool:
        return self.get_element_type(schema).is_tuple()

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return (
            f'array<{self.get_element_type(schema).get_displayname(schema)}>')

    def is_array(self) -> bool:
        return True

    def get_container(self):
        return tuple

    def derive_subtype(
        self,
        schema: s_schema.Schema,
        *,
        name: s_name.Name,
        attrs: Optional[Mapping] = None,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, Array]:
        assert not kwargs
        return schema, Array.from_subtypes(
            schema,
            [self.get_element_type(schema)],
            self.get_typemods(schema),
            name=name,
            **(attrs or {}))

    def get_element_type(self, schema):
        raise NotImplementedError

    def get_dimensions(self, schema):
        raise NotImplementedError

    def get_typemods(self, schema: s_schema.Schema) -> typing.Tuple[List[int]]:
        return (self.get_dimensions(schema),)

    def implicitly_castable_to(
        self, other: Type, schema: s_schema.Schema
    ) -> bool:
        if not isinstance(other, BaseArray):
            return False

        return self.get_element_type(schema).implicitly_castable_to(
            other.get_element_type(schema), schema)

    def get_implicit_cast_distance(
        self, other: Type, schema: s_schema.Schema
    ) -> int:
        if not isinstance(other, BaseArray):
            return -1

        return self.get_element_type(schema).get_implicit_cast_distance(
            other.get_element_type(schema), schema)

    def assignment_castable_to(self, other: Type, schema) -> bool:
        if not isinstance(other, BaseArray):
            return False

        return self.get_element_type(schema).assignment_castable_to(
            other.get_element_type(schema), schema)

    def find_common_implicitly_castable_type(
        self, other: Type, schema: s_schema.Schema
    ) -> Optional[Type]:

        if not isinstance(other, BaseArray):
            return None

        if self == other:
            return self

        my_el = self.get_element_type(schema)
        subtype = my_el.find_common_implicitly_castable_type(
            other.get_element_type(schema), schema)

        if subtype is None:
            return None

        return Array.from_subtypes(schema, [subtype])

    def _resolve_polymorphic(
        self, schema: s_schema.Schema, concrete_type: Type
    ) -> Optional[scalars.ScalarType]:
        if not isinstance(concrete_type, BaseArray):
            return None

        return self.get_element_type(schema).resolve_polymorphic(
            schema, concrete_type.get_element_type(schema))

    def _to_nonpolymorphic(
        self, schema: s_schema.Schema, concrete_type: Type
    ) -> Array:
        return Array.from_subtypes(schema, (concrete_type,))

    def _test_polymorphic(self, schema: s_schema.Schema, other: Type) -> bool:
        if other.is_any():
            return True

        if not isinstance(other, BaseArray):
            return False

        return self.get_element_type(schema).test_polymorphic(
            schema, other.get_element_type(schema))

    @classmethod
    def from_subtypes(
        cls: typing.Type[BaseArray_T],
        schema: s_schema.Schema,
        subtypes: Sequence[Type],
        typemods: Any = None,
        *,
        name: Optional[s_name.Name] = None,
        id: Union[uuid.UUID, so.NoDefaultT] = so.NoDefault,
    ) -> BaseArray_T:
        if len(subtypes) != 1:
            raise errors.SchemaError(
                f'unexpected number of subtypes, expecting 1: {subtypes!r}')
        stype = subtypes[0]

        if isinstance(stype, Array):
            raise errors.UnsupportedFeatureError(
                f'nested arrays are not supported')

        # One-dimensional unbounded array.
        dimensions = [-1]

        return cls.create(schema, element_type=stype,
                          dimensions=dimensions, name=name,
                          id=id)

    def material_type(
        self: BaseArray_T, schema: s_schema.Schema
    ) -> BaseArray_T:
        # We need to resolve material types based on the subtype recursively.
        new_material_type = False

        st = self.get_element_type(schema)
        subtypes = [st.material_type(schema)]
        if subtypes[0] is not st:
            new_material_type = True

        if new_material_type:
            return self.__class__.from_subtypes(
                schema, subtypes, typemods=self.get_typemods(schema))
        else:
            return self

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        *,
        view_name: Optional[str] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> sd.CommandGroup:
        ca: Union[CreateArray, CreateArrayExprAlias]
        cmd = sd.CommandGroup()
        if view_name is None:
            name = str(self.id)
            ca = CreateArray(
                classname=name,
            )
        else:
            name = view_name
            ca = CreateArrayExprAlias(
                classname=name,
            )

        el = self.get_element_type(schema)
        if el.is_collection() and schema.get_by_id(el.id, None) is None:
            cmd.add(el.as_create_delta(schema))

        ca.set_attribute_value('id', self.id)
        ca.set_attribute_value('name', name)
        ca.set_attribute_value('element_type', el)
        ca.set_attribute_value('dimensions', self.get_dimensions(schema))

        if attrs:
            for k, v in attrs.items():
                ca.set_attribute_value(k, v)

        cmd.add(ca)

        return cmd

    def as_delete_delta(
        self, schema: s_schema.Schema, *, view_name: str = None
    ) -> Union[DeleteArray, DeleteArrayExprAlias]:
        cmd: Union[DeleteArray, DeleteArrayExprAlias]
        if view_name is None:
            name = str(self.id)
            cmd = DeleteArray(
                classname=name,
            )
        else:
            name = view_name
            cmd = DeleteArrayExprAlias(
                classname=name,
            )

        el = self.get_element_type(schema)
        if (el.is_collection()
                and list(schema.get_referrers(el))[0].id == self.id):
            cmd.add(el.as_delete_delta(schema))

        return cmd


class Array(EphemeralCollection, BaseArray):

    element_type = so.Field(Type)
    dimensions = so.Field(Dimensions, coerce=True)

    def as_schema_coll(self, schema):

        existing = schema.get_by_id(self.id, None)
        if existing is not None:
            return schema, existing

        el_type = self.element_type
        if el_type.is_collection():
            schema, el_type = el_type.as_schema_coll(schema)

        return SchemaArray.create_in_schema(
            schema,
            id=self.id,
            name=str(self.id),
            element_type=el_type,
        )

    def get_element_type(self, schema: s_schema.Schema) -> Type:
        return self.element_type

    def get_subtypes(
        self, schema: s_schema.Schema
    ) -> typing.Tuple[Type]:
        return (self.element_type,)

    def get_dimensions(
        self, schema: s_schema.Schema
    ) -> checked.FrozenCheckedList[int]:
        return self.dimensions


class SchemaCollectionMeta(so.ObjectMeta):

    @property
    def is_schema_object(cls) -> bool:
        return True


class SchemaCollection(so.Object, metaclass=SchemaCollectionMeta):
    def __repr__(self):
        return (
            f'<{self.__class__.__name__} '
            f'{self.id} '
            f'at 0x{id(self):x}>'
        )


class CollectionExprAlias(SchemaCollection):

    @classmethod
    def get_schema_class_displayname(cls):
        return 'view'


class SchemaAnonymousCollection(so.UnqualifiedObject, SchemaCollection):
    name = so.SchemaField(
        str,  # type: ignore
        inheritable=False,
        # We want a low compcoef so that collections are *never* altered.
        compcoef=0,
    )


# Breaking Liskov Substitution Principle on _reduce_to_ref
class SchemaArrayRef(BaseArray, so.ObjectRef):  # type: ignore
    def _resolve_ref(self, schema: s_schema.Schema) -> SchemaArray:
        return schema.get_global(SchemaArray, self.get_name(schema))


class BaseSchemaArray(SchemaCollection, BaseArray):

    element_type = so.SchemaField(
        so.Object,
        # We want a low compcoef so that tuples are *never* altered.
        compcoef=0,
    )
    dimensions = so.SchemaField(
        Dimensions, default=None, coerce=True,
        # We want a low compcoef so that tuples are *never* altered.
        compcoef=0,
    )

    def get_subtypes(self, schema):
        return (self.get_element_type(schema),)

    # Breaking Liskov Substitution Principle
    def _reduce_to_ref(  # type: ignore
        self: BaseArray_T, schema: s_schema.Schema
    ) -> typing.Tuple[SchemaArrayRef, Any]:
        # Since this is already a schema object, we should just create
        # a reference by "name".
        sa_ref = SchemaArrayRef(name=self.get_name(schema))
        return (sa_ref, (self.__class__, sa_ref.name))


class SchemaArray(SchemaAnonymousCollection, BaseSchemaArray,
                  qlkind=qltypes.SchemaObjectClass.ARRAY_TYPE):
    pass


class ArrayExprAlias(CollectionExprAlias, BaseSchemaArray):
    pass


BaseTuple_T = typing.TypeVar('BaseTuple_T', bound='BaseTuple')


class BaseTuple(Collection, s_abc.Tuple):
    schema_name = 'tuple'

    named = so.Field(bool)
    element_types = so.Field[typing.Mapping[str, Type]](dict, coerce=True)

    @classmethod
    def create(
        cls: typing.Type[BaseTuple_T],
        schema: s_schema.Schema,
        *,
        name: Optional[s_name.Name] = None,
        id: Union[uuid.UUID, so.NoDefaultT] = so.NoDefault,
        element_types: Mapping[str, Type],
        named: bool = False,
        **kwargs,
    ) -> BaseTuple_T:
        element_types = types.MappingProxyType(element_types)
        if id is so.NoDefault:
            id_str = ','.join(
                f'{n}:{st.id}' for n, st in element_types.items())
            id_str = f'tuple-{id_str}'
            id = generate_type_id(id_str)

        if name is None:
            if named:
                st_names = ','.join(f'{sn}:={st.get_name(schema)}'
                                    for sn, st in element_types.items())
            else:
                st_names = ','.join(st.get_name(schema)
                                    for st in element_types.values())
            name = s_name.SchemaName(
                module='std',
                name=f'tuple<{st_names}>')

        return super()._create(
            schema, id=id, name=name, named=named,
            element_types=element_types, **kwargs)

    def get_displayname(self, schema: s_schema.Schema) -> str:
        st_names = ', '.join(st.get_displayname(schema)
                             for st in self.get_subtypes(schema))
        return f'tuple<{st_names}>'

    def is_tuple(self):
        return True

    def get_container(self):
        return dict

    def get_element_names(self, schema: s_schema.Schema) -> Sequence[str]:
        raise NotImplementedError

    def iter_subtypes(
        self, schema: s_schema.Schema
    ) -> Iterator[typing.Tuple[str, Type]]:
        raise NotImplementedError

    def is_named(self, schema) -> bool:
        raise NotImplementedError

    def normalize_index(self, schema: s_schema.Schema, field: str) -> str:
        if self.is_named(schema) and field.isdecimal():
            idx = int(field)
            el_names = self.get_element_names(schema)
            if idx >= 0 and idx < len(el_names):
                return el_names[idx]
            else:
                raise errors.InvalidReferenceError(
                    f'{field} is not a member of '
                    f'{self.get_displayname(schema)}')

        return field

    def index_of(self, schema, field: str) -> int:
        if field.isdecimal():
            idx = int(field)
            el_names = self.get_element_names(schema)
            if idx >= 0 and idx < len(el_names):
                if self.is_named(schema):
                    return el_names.index(field)
                else:
                    return idx
        elif self.is_named(schema):
            el_names = self.get_element_names(schema)
            try:
                return el_names.index(field)
            except ValueError:
                pass

        raise errors.InvalidReferenceError(
            f'{field} is not a member of {self.get_displayname(schema)}')

    def get_subtype(self, schema: s_schema.Schema, field: str) -> Type:
        # index can be a name or a position
        if field.isdecimal():
            idx = int(field)
            subtypes_l = list(self.get_subtypes(schema))
            if idx >= 0 and idx < len(subtypes_l):
                return subtypes_l[idx]

        elif self.is_named(schema):
            subtypes_d = dict(self.iter_subtypes(schema))
            if field in subtypes_d:
                return subtypes_d[field]

        raise errors.InvalidReferenceError(
            f'{field} is not a member of {self.get_displayname(schema)}')

    def get_subtypes(self, schema):
        raise NotImplementedError

    def derive_subtype(
        self,
        schema: s_schema.Schema,
        *,
        name: s_name.Name,
        attrs: Optional[Mapping] = None,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, Tuple]:
        assert not kwargs
        return schema, Tuple.from_subtypes(
            schema,
            dict(self.iter_subtypes(schema)),
            self.get_typemods(schema),
            name=name,
            **(attrs or {}))

    @classmethod
    def from_subtypes(
        cls: typing.Type[BaseTuple_T],
        schema: s_schema.Schema,
        subtypes: Union[Iterable[Type], Mapping[str, Type]],
        typemods: Any = None,
        *,
        name: Optional[s_name.Name] = None,
        id: Union[uuid.UUID, so.NoDefaultT] = so.NoDefault,
        **kwargs,
    ) -> BaseTuple_T:
        named = False
        if typemods is not None:
            named = typemods.get('named', False)

        types: Mapping[str, Type]
        if isinstance(subtypes, collections.abc.Mapping):
            types = subtypes
        else:
            types = {str(i): type for i, type in enumerate(subtypes)}
        return cls.create(schema, element_types=types, named=named,
                          name=name, id=id, **kwargs)

    def implicitly_castable_to(self, other: Type, schema) -> bool:
        if not isinstance(other, BaseTuple):
            return False

        self_subtypes = self.get_subtypes(schema)
        other_subtypes = other.get_subtypes(schema)

        if len(self_subtypes) != len(other_subtypes):
            return False

        for st, ot in zip(self_subtypes, other_subtypes):
            if not st.implicitly_castable_to(ot, schema):
                return False

        return True

    def get_implicit_cast_distance(self, other: Type, schema) -> int:
        if not isinstance(other, BaseTuple):
            return -1

        self_subtypes = self.get_subtypes(schema)
        other_subtypes = other.get_subtypes(schema)

        if len(self_subtypes) != len(other_subtypes):
            return -1

        total_dist = 0

        for st, ot in zip(self_subtypes, other_subtypes):
            dist = st.get_implicit_cast_distance(ot, schema)
            if dist < 0:
                return -1

            total_dist += dist

        return total_dist

    def assignment_castable_to(self, other: Type, schema) -> bool:
        if not isinstance(other, BaseTuple):
            return False

        self_subtypes = self.get_subtypes(schema)
        other_subtypes = other.get_subtypes(schema)

        if len(self_subtypes) != len(other_subtypes):
            return False

        for st, ot in zip(self_subtypes, other_subtypes):
            if not st.assignment_castable_to(ot, schema):
                return False

        return True

    def find_common_implicitly_castable_type(
            self, other: Type, schema) -> Optional[Type]:

        if not isinstance(other, BaseTuple):
            return None

        if self == other:
            return self

        subs = self.get_subtypes(schema)
        other_subs = other.get_subtypes(schema)

        if len(subs) != len(other_subs):
            return None

        new_types: List[Type] = []
        for st, ot in zip(subs, other_subs):
            nt = st.find_common_implicitly_castable_type(ot, schema)
            if nt is None:
                return None

            new_types.append(nt)

        if self.is_named(schema) and other.is_named(schema):
            my_names = self.get_element_names(schema)
            other_names = other.get_element_names(schema)
            if my_names == other_names:
                return Tuple.from_subtypes(
                    schema, dict(zip(my_names, new_types)), {"named": True}
                )

        return Tuple.from_subtypes(schema, new_types)

    def get_typemods(self, schema: s_schema.Schema) -> Dict[str, bool]:
        return {'named': self.is_named(schema)}

    def contains_array_of_tuples(self, schema: s_schema.Schema) -> bool:
        return any(
            st.contains_array_of_tuples(schema)
            if st.is_collection() else False
            for st in self.get_subtypes(schema)
        )

    def _resolve_polymorphic(self, schema, concrete_type: Type):
        if not isinstance(concrete_type, BaseTuple):
            return None

        self_subtypes = self.get_subtypes(schema)
        other_subtypes = concrete_type.get_subtypes(schema)

        if len(self_subtypes) != len(other_subtypes):
            return None

        for source, target in zip(self_subtypes, other_subtypes):
            if source.is_polymorphic(schema):
                return source.resolve_polymorphic(schema, target)

        return None

    def _to_nonpolymorphic(self, schema, concrete_type: Type):
        new_types: List[Type] = []
        for st in self.get_subtypes(schema):
            if st.is_polymorphic(schema):
                nst = st.to_nonpolymorphic(schema, concrete_type)
            else:
                nst = st
            new_types.append(nst)

        if self.is_named(schema):
            return Tuple.from_subtypes(
                schema,
                dict(zip(self.get_element_names(schema), new_types)),
                {"named": True},
            )

        return Tuple.from_subtypes(schema, new_types)

    def _test_polymorphic(self, schema, other: Type):
        if other.is_any() or other.is_anytuple():
            return True
        if not isinstance(other, BaseTuple):
            return False

        self_subtypes = self.get_subtypes(schema)
        other_subtypes = other.get_subtypes(schema)

        if len(self_subtypes) != len(other_subtypes):
            return False

        return all(st.test_polymorphic(schema, ot)
                   for st, ot in zip(self_subtypes, other_subtypes))

    def _reduce_to_ref(
        self, schema: s_schema.Schema
    ) -> typing.Tuple[BaseTuple, Any]:
        strefs: Dict[str, Type] = {}

        for n, st in self.iter_subtypes(schema):
            st_ref, _ = st._reduce_to_ref(schema)
            strefs[n] = typing.cast(Type, st_ref)

        return (
            self.__class__.from_subtypes(
                schema, strefs, typemods=self.get_typemods(schema)),
            (self.__class__, tuple(r.name for r in strefs.values()))
        )

    def _resolve_ref(
        self: BaseTuple_T, schema: s_schema.Schema
    ) -> BaseTuple_T:
        if any(hasattr(st, '_resolve_ref')
               for st in self.get_subtypes(schema)):
            subtypes: Dict[str, Type] = {}
            for st_name, st in self.iter_subtypes(schema):
                subtypes[st_name] = typing.cast(Type, st._resolve_ref(schema))

            return self.__class__.from_subtypes(
                schema, subtypes, typemods=self.get_typemods(schema))
        else:
            return self

    def material_type(
        self: BaseTuple_T, schema: s_schema.Schema
    ) -> BaseTuple_T:
        # We need to resolve material types of all the subtypes recursively.
        new_material_type = False
        subtypes = {}

        for st_name, st in self.iter_subtypes(schema):
            subtypes[st_name] = st.material_type(schema)
            if subtypes[st_name] is not st:
                new_material_type = True

        if new_material_type:
            return self.__class__.from_subtypes(
                schema, subtypes, typemods=self.get_typemods(schema))
        else:
            return self

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        *,
        view_name: Optional[str] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> sd.CommandGroup:
        ct: Union[CreateTuple, CreateTupleExprAlias]
        cmd = sd.CommandGroup()
        if view_name is None:
            name = str(self.id)
            ct = CreateTuple(
                classname=name,
            )
        else:
            name = view_name
            ct = CreateTupleExprAlias(
                classname=name,
            )

        for el in self.get_subtypes(schema):
            if el.is_collection() and schema.get_by_id(el.id, None) is None:
                cmd.add(el.as_create_delta(schema))

        ct.set_attribute_value('id', self.id)
        ct.set_attribute_value('name', name)
        ct.set_attribute_value('named', self.is_named(schema))
        ct.set_attribute_value(
            'element_types',
            so.ObjectDict.create(
                schema,
                dict(self.iter_subtypes(schema)),
            ))

        if attrs:
            for k, v in attrs.items():
                ct.set_attribute_value(k, v)

        cmd.add(ct)

        return cmd

    def as_delete_delta(
        self, schema: s_schema.Schema, *, view_name: str = None
    ) -> Union[DeleteTuple, DeleteTupleExprAlias]:
        cmd: Union[DeleteTuple, DeleteTupleExprAlias]
        if view_name is None:
            name = str(self.id)
            cmd = DeleteTuple(
                classname=name,
            )
        else:
            name = view_name
            cmd = DeleteTupleExprAlias(
                classname=name,
            )

        for el in self.get_subtypes(schema):
            if el.is_collection():
                refs = schema.get_referrers(el)
                if len(refs) == 1 and list(refs)[0].id == self.id:
                    cmd.add(el.as_delete_delta(schema))

        return cmd


class Tuple(EphemeralCollection, BaseTuple):

    named = so.Field(bool)
    element_types = so.Field(dict, coerce=True)

    def is_named(self, schema: s_schema.Schema) -> bool:
        return self.named

    def get_element_names(self, schema: s_schema.Schema) -> Sequence[str]:
        return list(self.element_types)

    def iter_subtypes(
        self, schema: s_schema.Schema
    ) -> Iterator[typing.Tuple[str, Type]]:
        yield from self.element_types.items()

    def get_subtypes(self, schema: s_schema.Schema) -> typing.Tuple[Type, ...]:
        if self.element_types:
            return tuple(self.element_types.values())
        else:
            return ()

    def as_schema_coll(self, schema):

        existing = schema.get_by_id(self.id, None)
        if existing is not None:
            return schema, existing

        el_types = {}
        for k, v in self.element_types.items():
            if v.is_collection():
                schema, v = v.as_schema_coll(schema)

            el_types[k] = v

        return SchemaTuple.create_in_schema(
            schema,
            id=self.id,
            name=str(self.id),
            named=self.named,
            element_types=so.ObjectDict.create(schema, el_types),
        )

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state['element_types'] = dict(state['element_types'])
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        state['element_types'] = types.MappingProxyType(state['element_types'])
        self.__dict__.update(state)


# Breaking Liskov Substitution Principle
class SchemaTupleRef(BaseTuple, so.ObjectRef):  # type: ignore
    def _resolve_ref(self, schema):
        return schema.get_global(SchemaTuple, self.get_name(schema))


class BaseSchemaTuple(SchemaCollection, BaseTuple):

    named = so.SchemaField(  # type: ignore
        bool,
        # We want a low compcoef so that tuples are *never* altered.
        compcoef=0,
    )

    element_types = so.SchemaField(  # type: ignore
        so.ObjectDict,
        coerce=True,
        # We want a low compcoef so that tuples are *never* altered.
        compcoef=0,
    )

    def is_named(self, schema: s_schema.Schema) -> bool:
        return self.get_named(schema)

    def get_element_names(self, schema: s_schema.Schema) -> Sequence[str]:
        return tuple(self.get_element_types(schema).keys(schema))

    def iter_subtypes(
        self, schema: s_schema.Schema
    ) -> Iterator[typing.Tuple[str, Type]]:
        # TODO: SchemaFields need to be made generic in the Mypy plugin
        yield from self.get_element_types(schema).items(schema)  # type: ignore

    def get_subtypes(self, schema: s_schema.Schema) -> typing.Tuple[Type, ...]:
        # TODO: SchemaFields need to be made generic in the Mypy plugin
        return self.get_element_types(schema).values(schema)  # type: ignore

        if self.element_types:
            return self.element_types.objects(schema)
        else:
            return []

    # Breaking Liskov Substitution Principle
    def _reduce_to_ref(  # type: ignore
        self, schema: s_schema.Schema
    ) -> typing.Tuple[SchemaTupleRef, Any]:
        # Since this is already a schema object, we should just create
        # a reference by "name".
        sa_ref = SchemaTupleRef(name=self.get_name(schema))
        return (sa_ref, (self.__class__, sa_ref.name))


class SchemaTuple(SchemaAnonymousCollection, BaseSchemaTuple,
                  qlkind=qltypes.SchemaObjectClass.TUPLE_TYPE):
    pass


class TupleExprAlias(CollectionExprAlias, BaseSchemaTuple):
    pass


def generate_type_id(id_str: str) -> uuid.UUID:
    return uuidgen.uuid5(TYPE_ID_NAMESPACE, id_str)


def get_union_type_id(
    schema,
    components: typing.Iterable[Type], *,
    module: typing.Optional[str]=None,
) -> typing.Tuple[uuid.UUID, s_name.Name]:

    name = s_name.Name(
        name=f"({' | '.join(sorted(str(t.id) for t in components))})",
        module=module or '__derived__',
    )

    return generate_type_id(name), name


def get_intersection_type_id(
    schema,
    components: typing.Iterable[Type], *,
    module: typing.Optional[str]=None,
) -> typing.Tuple[uuid.UUID, s_name.Name]:

    name = s_name.Name(
        name=f"({' & '.join(sorted(str(t.id) for t in components))})",
        module=module or '__derived__',
    )

    return generate_type_id(name), name


def ensure_schema_type_expr_type(
    schema: s_schema.Schema,
    type_ref: TypeExprRef,
    parent_cmd: sd.Command,
    *,
    src_context: typing.Optional[parsing.ParserContext] = None,
    context: sd.CommandContext,
) -> TypeExprRef:

    module = type_ref.module

    components = type_ref.resolve_components(schema)

    if isinstance(type_ref, UnionTypeRef):
        type_id, type_name = get_union_type_id(
            schema,
            components,
            module=module,
        )
    elif isinstance(type_ref, IntersectionTypeRef):
        type_id, type_name = get_union_type_id(
            schema,
            components,
            module=module,
        )
    else:
        raise AssertionError(f'unexpected typeref: {type_ref!r}')

    texpr_type = schema.get_by_id(type_id, None)

    if texpr_type is None:
        if isinstance(type_ref, UnionTypeRef):
            schema, texpr_type = utils.get_union_type(
                schema, components, module=module)
        else:
            schema, texpr_type = utils.get_intersection_type(
                schema, components, module=module)

        cmd = texpr_type.as_create_delta_for_compound_type(schema)
        if cmd is not None:
            parent_cmd.add_prerequisite(cmd)

    return type_ref


class TypeCommand(sd.ObjectCommand):
    @classmethod
    def _get_alias_expr(cls, astnode: qlast.CreateAlias) -> qlast.Expr:
        expr = qlast.get_ddl_field_value(astnode, 'expr')
        if expr is None:
            raise errors.InvalidAliasDefinitionError(
                f'missing required view expression', context=astnode.context)
        return expr

    @classmethod
    def _compile_view_expr(
        cls,
        expr: qlast.Base,
        classname: s_name.SchemaName,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> irast.Statement:
        from edb.edgeql import compiler as qlcompiler

        ir = context.get_cached((expr, classname))
        if ir is None:
            if not isinstance(expr, qlast.Statement):
                expr = qlast.SelectQuery(result=expr)
            ir = qlcompiler.compile_ast_to_ir(
                expr, schema, derived_target_module=classname.module,
                result_view_name=classname, modaliases=context.modaliases,
                schema_view_mode=True)
            context.cache_value((expr, classname), ir)

        return ir

    @classmethod
    def _handle_view_op(
        cls,
        schema: s_schema.Schema,
        cmd: sd.ObjectCommand,
        astnode: qlast.ObjectDDL,
        context: sd.CommandContext,
    ) -> sd.Command:
        from . import ordering as s_ordering

        view_expr = qlast.get_ddl_field_value(astnode, 'expr')
        if view_expr is None:
            return cmd

        classname = cmd.classname
        if not s_name.Name.is_qualified(classname):
            # Collection commands use unqualified names
            # because they use the type id in the general case,
            # but in the case of an explicit named view, we
            # still want a properly qualified name.
            fq_classname = sd.ObjectCommand._classname_from_ast(
                schema, astnode, context)
            assert isinstance(fq_classname, s_name.Name)
            cmd.classname = fq_classname

        expr = s_expr.Expression.from_ast(
            view_expr, schema, context.modaliases)

        ir = cls._compile_view_expr(expr.qlast, classname,
                                    schema, context)

        new_schema = ir.schema

        expr = s_expr.Expression.from_ir(expr, ir, schema=schema)

        coll_expr_aliases: List[Collection] = []
        prev_coll_expr_aliases: List[Collection] = []
        expr_aliases: List[Type] = []
        prev_expr_aliases: List[Type] = []
        prev_ir: Optional[irast.Statement] = None
        old_schema: Optional[s_schema.Schema] = None

        for vt in ir.views.values():
            if isinstance(vt, Collection):
                coll_expr_aliases.append(vt)
            else:
                new_schema = vt.set_field_value(
                    new_schema, 'alias_is_persistent', True)

                expr_aliases.append(vt)

        if isinstance(astnode, qlast.AlterObject):
            prev = typing.cast(Type, schema.get(classname))
            prev_expr = prev.get_expr(schema)
            assert prev_expr is not None
            prev_ir = cls._compile_view_expr(
                prev_expr.qlast, classname, schema, context)
            old_schema = prev_ir.schema
            for vt in prev_ir.views.values():
                if isinstance(vt, Collection):
                    prev_coll_expr_aliases.append(vt)
                else:
                    prev_expr_aliases.append(vt)

        derived_delta = sd.DeltaRoot()

        derived_delta.add(so.Object.delta_sets(
            prev_expr_aliases, expr_aliases,
            old_schema=old_schema, new_schema=new_schema))

        if prev_ir is not None:
            for vt in prev_coll_expr_aliases:
                dt = vt.as_delete_delta(prev_ir.schema, view_name=classname)
                derived_delta.prepend(dt)

        for vt in coll_expr_aliases:
            ct = vt.as_create_delta(
                new_schema, view_name=classname,
                attrs={
                    'id': uuidgen.uuid1mc(),
                    'expr': expr,
                    'alias_is_persistent': True,
                    'expr_type': ExprType.Select,
                })
            new_schema = ct.apply(new_schema, context)
            derived_delta.add(ct)

        derived_delta = s_ordering.linearize_delta(
            derived_delta, old_schema=old_schema, new_schema=new_schema)

        real_cmd = None
        for op in derived_delta.get_subcommands():
            assert isinstance(op, sd.ObjectCommand)
            if op.classname == classname:
                real_cmd = op
                break

        if real_cmd is None:
            raise RuntimeError(
                'view delta does not contain the expected '
                'view Create/Alter command')

        real_cmd.set_attribute_value('expr', expr)

        result = sd.CommandGroup()
        result.update(derived_delta.get_subcommands())
        result.canonical = True
        return result

    def _create_begin(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)
        assert isinstance(self.scls, Type)
        if not self.scls.is_view(schema):
            delta_root = context.top().op
            assert isinstance(delta_root, sd.DeltaRoot)
            delta_root.new_types.add(self.scls.id)
        return schema


class CollectionTypeCommandContext(sd.ObjectCommandContext):
    pass


class CollectionTypeCommand(sd.UnqualifiedObjectCommand,
                            TypeCommand,
                            context_class=CollectionTypeCommandContext):

    def get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        # CollectionTypeCommand cannot have its own AST because it is a
        # side-effect of some other command.
        return None


class CollectionExprAliasCommand(TypeCommand,
                                 context_class=CollectionTypeCommandContext):
    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.ObjectDDL)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, sd.ObjectCommand)
        cmd = cls._handle_view_op(schema, cmd, astnode, context)
        return cmd


class CreateCollectionType(CollectionTypeCommand, sd.CreateObject):
    pass


class DeleteCollectionType(CollectionTypeCommand, sd.DeleteObject):
    pass


class CreateCollectionExprAlias(CollectionExprAliasCommand, sd.CreateObject):
    pass


class DeleteCollectionExprAlias(CollectionExprAliasCommand, sd.DeleteObject):
    pass


class CreateTuple(CreateCollectionType, schema_metaclass=SchemaTuple):
    pass


class CreateTupleExprAlias(CreateCollectionExprAlias,
                           schema_metaclass=TupleExprAlias):
    def _get_ast_node(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> typing.Type[qlast.CreateAlias]:
        # Can't just use class-level astnode because that creates a
        # duplicate in ast -> command mapping.
        return qlast.CreateAlias


class CreateArray(CreateCollectionType, schema_metaclass=SchemaArray):
    pass


class CreateArrayExprAlias(CreateCollectionExprAlias,
                           schema_metaclass=ArrayExprAlias):
    def _get_ast_node(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> typing.Type[qlast.CreateAlias]:
        # Can't just use class-level astnode because that creates a
        # duplicate in ast -> command mapping.
        return qlast.CreateAlias


class DeleteTuple(DeleteCollectionType, schema_metaclass=SchemaTuple):
    pass


class DeleteTupleExprAlias(DeleteCollectionExprAlias,
                           schema_metaclass=TupleExprAlias):
    pass


class DeleteArray(DeleteCollectionType, schema_metaclass=SchemaArray):
    pass


class DeleteArrayExprAlias(DeleteCollectionExprAlias,
                           schema_metaclass=ArrayExprAlias):
    pass


def ensure_schema_collection(
    schema: s_schema.Schema,
    coll_type: Type,
    parent_cmd: sd.Command,
    *,
    src_context: Optional[parsing.ParserContext] = None,
    context: sd.CommandContext,
) -> None:
    if not isinstance(coll_type, Collection):
        raise ValueError(
            f'{coll_type.get_displayname(schema)} is not a collection')

    if coll_type.contains_array_of_tuples(schema):
        raise errors.UnsupportedFeatureError(
            'arrays of tuples are not supported at the schema level',
            context=src_context,
        )

    delta_root = context.top().op
    assert isinstance(delta_root, sd.DeltaRoot)

    if (schema.get_by_id(coll_type.id, None) is None
            and coll_type.id not in delta_root.new_types):
        parent_cmd.add(coll_type.as_create_delta(schema))
        delta_root.new_types.add(coll_type.id)

    if coll_type.id in delta_root.deleted_types:
        # Revert the deletion decision.
        del_cmd = delta_root.deleted_types.pop(coll_type.id)
        delta_root.discard(del_cmd)


def cleanup_schema_collection(
    schema: s_schema.Schema,
    coll_type: Type,
    parent: Type,
    parent_cmd: sd.Command,
    *,
    src_context: Optional[parsing.ParserContext] = None,
    context: sd.CommandContext,
) -> None:
    if not isinstance(coll_type, Collection):
        raise ValueError(
            f'{coll_type.get_displayname(schema)} is not a collection')

    delta_root = context.top().op
    assert isinstance(delta_root, sd.DeltaRoot)

    refs = schema.get_referrers(coll_type)
    if (len(refs) == 1 and list(refs)[0].id == parent.id
            and coll_type.id not in delta_root.deleted_types):
        # The parent is the last user of this collection, drop it.
        del_cmd = coll_type.as_delete_delta(schema)
        delta_root.deleted_types[coll_type.id] = del_cmd
        delta_root.add(del_cmd)
