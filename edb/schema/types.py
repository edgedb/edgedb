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

from edb.common import typed

from edb.edgeql import ast as qlast

from . import abc as s_abc
from . import delta as sd
from . import derivable
from . import expr as s_expr
from . import name as s_name
from . import objects as so
from . import schema as s_schema
from . import utils


TYPE_ID_NAMESPACE = uuid.UUID('00e50276-2502-11e7-97f2-27fe51238dbd')
MAX_TYPE_DISTANCE = 1_000_000_000


class ViewType(enum.IntEnum):
    Select = enum.auto()
    Insert = enum.auto()
    Update = enum.auto()


class Type(so.InheritingObjectBase, derivable.DerivableObjectBase, s_abc.Type):
    """A schema item that is a valid *type*."""

    # For a type representing a view, this would contain the
    # view type.  Non-view types have None here.
    view_type = so.SchemaField(
        ViewType,
        default=None, compcoef=0.909)

    # True for views defined by CREATE VIEW, false for ephemeral
    # views in queries.
    view_is_persistent = so.SchemaField(
        bool,
        default=False, compcoef=None)

    # If this type is a view, expr may contain an expression that
    # defines the view set.
    expr = so.SchemaField(
        s_expr.Expression,
        default=None, coerce=True, allow_ddl_set=True, compcoef=0.909)

    # If this type is a view defined by a nested shape expression,
    # and the nested shape contains references to link properties,
    # rptr will contain the inbound pointer class.
    rptr = so.SchemaField(
        so.Object,
        weak_ref=True,
        default=None, compcoef=0.909)

    def is_blocking_ref(self, schema, reference):
        return reference is not self.get_rptr(schema)

    def derive_subtype(
            self, schema, *, name: str,
            attrs: typing.Optional[typing.Mapping]=None
    ) -> typing.Tuple[s_schema.Schema, Type]:
        raise NotImplementedError

    def is_type(self):
        return True

    def is_object_type(self):
        return False

    def is_polymorphic(self, schema):
        return False

    def is_any(self):
        return False

    def is_anytuple(self):
        return False

    def find_any(self, schema):
        if self.is_any():
            return self
        else:
            return None

    def contains_any(self, schema):
        return self.is_any()

    def is_scalar(self):
        return False

    def is_collection(self):
        return False

    def is_array(self):
        return False

    def is_tuple(self):
        return False

    def is_enum(self, schema):
        return False

    def test_polymorphic(self, schema, poly: 'Type') -> bool:
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

    def resolve_polymorphic(self, schema, other: 'Type') -> 'Type':
        """Resolve the polymorphic type component.

        Examples:

            - `array<anytype>`.resolve_polymorphic(`array<int>`) -> `int`
            - `array<anytype>`.resolve_polymorphic(`tuple<int>`) -> None
        """
        if not self.is_polymorphic(schema):
            return None

        return self._resolve_polymorphic(schema, other)

    def to_nonpolymorphic(self, schema, concrete_type: 'Type') -> 'Type':
        """Produce an non-polymorphic version of self.

        Example:
            `array<anytype>`.to_nonpolymorphic(`int`) -> `array<int>`
            `tuple<int, anytype>`.to_nonpolymorphic(`str`) -> `tuple<int, str>`
        """
        if not self.is_polymorphic(schema):
            raise TypeError('non-polymorphic type')

        return self._to_nonpolymorphic(schema, concrete_type)

    def _test_polymorphic(self, schema, other: 'Type'):
        return False

    def _resolve_polymorphic(self, schema, concrete_type: 'Type'):
        raise NotImplementedError(
            f'{type(self)} does not support resolve_polymorphic()')

    def _to_nonpolymorphic(self, schema, concrete_type: 'Type'):
        raise NotImplementedError(
            f'{type(self)} does not support to_nonpolymorphic()')

    def is_view(self, schema):
        return self.get_view_type(schema) is not None

    def assignment_castable_to(self, other: 'Type', schema) -> bool:
        return self.implicitly_castable_to(other, schema)

    def implicitly_castable_to(self, other: 'Type', schema) -> bool:
        return False

    def get_implicit_cast_distance(self, other: 'Type', schema) -> int:
        return -1

    def find_common_implicitly_castable_type(
            self, other: 'Type', schema) -> typing.Optional['Type']:
        return

    def explicitly_castable_to(self, other: 'Type', schema) -> bool:
        if self.implicitly_castable_to(other, schema):
            return True

        if self.assignment_castable_to(other, schema):
            return True

        return False

    def get_union_of(self, schema):
        return None

    def get_is_opaque_union(self, schema):
        return False

    def material_type(self, schema):
        # When self is a view, this returns the material type
        # under the view.
        t = self
        while t.is_view(schema):
            t = t.get_bases(schema).first(schema)
        return t

    def peel_view(self, schema):
        # When self is a view, this returns the class the view
        # is derived from (which may be another view).  If no
        # parent class is available, returns self.
        if self.is_view(schema):
            return self.get_bases(schema).first(schema)
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

        if ancestor == self:
            return 0
        else:
            ancestors = list(self.get_ancestors(schema).objects(schema))
            return ancestors.index(ancestor) + 1


class Collection(Type, s_abc.Collection):

    def is_polymorphic(self, schema):
        return any(st.is_polymorphic(schema)
                   for st in self.get_subtypes(schema))

    def find_any(self, schema):
        for st in self.get_subtypes(schema):
            any_t = st.find_any(schema)
            if any_t is not None:
                return any_t

    def contains_any(self, schema):
        return any(st.contains_any(schema) for st in self.get_subtypes(schema))

    def contains_object(self, schema):
        return any(
            st.contains_object(schema) if st.is_collection()
            else st.is_object_type()
            for st in self.get_subtypes(schema)
        )

    def contains_array_of_tuples(self, schema):
        raise NotImplementedError

    def is_collection(self):
        return True

    def get_common_parent_type_distance(
            self, other: Type, schema) -> int:
        if other.is_any():
            return 1

        if other.__class__ is not self.__class__:
            return -1

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

    def _issubclass(self, schema, parent):
        if parent.is_any():
            return True

        if parent.__class__ is not self.__class__:
            return False

        parent_types = parent.get_subtypes(schema)
        my_types = self.get_subtypes(schema)

        for pt, my in zip(parent_types, my_types):
            if not pt.is_any() and not my.issubclass(schema, pt):
                return False

        return True

    def issubclass(self, schema, parent):
        if isinstance(parent, tuple):
            return any(self.issubclass(schema, p) for p in parent)
        else:
            if parent.is_type() and parent.is_any():
                return True
            else:
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

    def get_subtypes(self, schema):
        raise NotImplementedError

    def get_typemods(self, schema):
        return ()

    def get_subtype(self, schema, typeref):
        from . import types as s_types

        if isinstance(typeref, so.ObjectRef):
            eltype = typeref._resolve_ref(schema)
        else:
            eltype = typeref

        if isinstance(eltype, s_abc.ScalarType):
            eltype = eltype.get_topmost_concrete_base(schema)
            eltype = s_types.BaseTypeMeta.get_implementation(eltype.name)

        return eltype

    @classmethod
    def get_class(cls, schema_name):
        if schema_name == 'array':
            return Array
        elif schema_name == 'tuple':
            return Tuple
        else:
            raise errors.SchemaError(
                'unknown collection type: {!r}'.format(schema_name))

    @classmethod
    def from_subtypes(cls, schema, subtypes, typemods=None):
        raise NotImplementedError

    def _reduce_to_ref(self, schema):
        strefs = []

        for st in self.get_subtypes(schema):
            st_ref, _ = st._reduce_to_ref(schema)
            strefs.append(st_ref)

        return (
            self.__class__.from_subtypes(
                schema, strefs, typemods=self.get_typemods(schema)),
            (self.__class__, tuple(r.name for r in strefs))
        )

    def _resolve_ref(self, schema):
        if any(hasattr(st, '_resolve_ref')
               for st in self.get_subtypes(schema)):

            subtypes = []
            for st in self.get_subtypes(schema):
                subtypes.append(st._resolve_ref(schema))

            return self.__class__.from_subtypes(
                schema, subtypes, typemods=self.get_typemods(schema))
        else:
            return self

    def __repr__(self):
        return (
            f'<{self.__class__.__name__} '
            f'{self.name} {self.id} '
            f'at 0x{id(self):x}>'
        )

    def dump(self, schema):
        return repr(self)


class EphemeralCollection(Collection):

    name = so.Field(
        s_name.SchemaName,
        inheritable=False, compcoef=0.670)

    view_type = so.Field(
        ViewType,
        default=None, ephemeral=True)

    expr = so.Field(
        s_expr.Expression,
        default=None, ephemeral=True)

    rptr = so.Field(
        so.Object,
        default=None, ephemeral=True)

    def get_name(self, schema):
        return self.name

    def get_shortname(self, schema):
        return self.name

    def get_verbosename(self, schema, *, with_parent=False):
        return self.get_displayname(schema)

    def get_view_type(self, schema):
        return self.view_type

    def get_expr(self, schema):
        return self.expr

    def get_rptr(self, schema):
        return self.rptr


class Dimensions(typed.FrozenTypedList, type=int):
    pass


class BaseArray(Collection, s_abc.Array):
    schema_name = 'array'

    @classmethod
    def create(cls, schema, *, name=None,
               id=so.NoDefault, dimensions=None, element_type, **kwargs):
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

    def contains_array_of_tuples(self, schema):
        return self.get_element_type(schema).is_tuple()

    def get_displayname(self, schema):
        return (
            f'array<{self.get_element_type(schema).get_displayname(schema)}>')

    def is_array(self):
        return True

    def get_container(self):
        return tuple

    def derive_subtype(
            self, schema, *, name: str,
            attrs: typing.Optional[typing.Mapping]=None
    ) -> typing.Tuple[s_schema.Schema, Type]:
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

    def get_typemods(self, schema):
        return (self.get_dimensions(schema),)

    def implicitly_castable_to(self, other: Type, schema) -> bool:
        if not other.is_array():
            return False

        return self.get_element_type(schema).implicitly_castable_to(
            other.get_element_type(schema), schema)

    def get_implicit_cast_distance(self, other: 'Type', schema) -> int:
        if not other.is_array():
            return -1

        return self.get_element_type(schema).get_implicit_cast_distance(
            other.get_element_type(schema), schema)

    def assignment_castable_to(self, other: Type, schema) -> bool:
        if not other.is_array():
            return False

        return self.get_element_type(schema).assignment_castable_to(
            other.get_element_type(schema), schema)

    def find_common_implicitly_castable_type(
            self, other: Type, schema) -> typing.Optional[Type]:

        if not other.is_array():
            return

        if self == other:
            return self

        my_el = self.get_element_type(schema)
        subtype = my_el.find_common_implicitly_castable_type(
            other.get_element_type(schema), schema)

        if subtype is not None:
            return Array.from_subtypes(schema, [subtype])

    def _resolve_polymorphic(self, schema, concrete_type: 'Type'):
        if not concrete_type.is_array():
            return None
        return self.get_element_type(schema).resolve_polymorphic(
            schema, concrete_type.get_element_type(schema))

    def _to_nonpolymorphic(self, schema, concrete_type: 'Type'):
        return Array.from_subtypes(schema, (concrete_type,))

    def _test_polymorphic(self, schema, other: 'Type'):
        if other.is_any():
            return True
        if not other.is_array():
            return False

        return self.get_element_type(schema).test_polymorphic(
            schema, other.get_element_type(schema))

    @classmethod
    def from_subtypes(cls, schema, subtypes, typemods=None, *, name=None):
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
                          dimensions=dimensions, name=name)

    def material_type(self, schema):
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

    def as_create_delta(self, schema, *, view_name=None, attrs=None):
        cmd = sd.CommandGroup()

        if view_name is None:
            name = str(self.id)
            ca = CreateArray(
                classname=name,
            )
        else:
            name = view_name
            ca = CreateArrayView(
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

    def as_delete_delta(self, schema, *, view_name=None):
        if view_name is None:
            name = str(self.id)
            cmd = DeleteArray(
                classname=name,
            )
        else:
            name = view_name
            cmd = DeleteArrayView(
                classname=name,
            )

        el = self.get_element_type(schema)
        if (el.is_collection()
                and list(schema.get_referrers(el))[0].id == self.id):
            cmd.add(el.as_delete_delta(schema))

        return cmd


class Array(EphemeralCollection, BaseArray):

    element_type = so.Field(so.Object)
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

    def get_element_type(self, schema):
        return self.element_type

    def get_subtypes(self, schema):
        return (self.element_type,)

    def get_dimensions(self, schema):
        return self.dimensions


class SchemaCollectionMeta(type(Type)):

    @property
    def is_schema_object(cls):
        return True


class SchemaCollection(so.Object, metaclass=SchemaCollectionMeta):
    pass


class CollectionView(SchemaCollection):

    @classmethod
    def get_schema_class_displayname(cls):
        return 'view'


class SchemaAnonymousCollection(so.UnqualifiedObject, SchemaCollection):
    pass


class BaseSchemaArray(SchemaCollection, BaseArray):

    element_type = so.SchemaField(so.Object)
    dimensions = so.SchemaField(Dimensions, default=None, coerce=True)

    def get_subtypes(self, schema):
        return (self.get_element_type(schema),)


class SchemaArray(SchemaAnonymousCollection, BaseSchemaArray):
    pass


class ArrayView(CollectionView, BaseSchemaArray):
    pass


class BaseTuple(Collection, s_abc.Tuple):
    schema_name = 'tuple'

    named = so.Field(bool)
    element_types = so.Field(dict, coerce=True)

    @classmethod
    def create(cls, schema, *, name=None, id=so.NoDefault,
               element_types: dict, named=False, **kwargs):

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

    def get_displayname(self, schema):
        st_names = ', '.join(st.get_displayname(schema)
                             for st in self.get_subtypes(schema))
        return f'tuple<{st_names}>'

    def is_tuple(self):
        return True

    def get_container(self):
        return dict

    def get_element_names(self, schema):
        raise NotImplementedError

    def iter_subtypes(self, schema):
        raise NotImplementedError

    def is_named(self, schema) -> bool:
        raise NotImplementedError

    def normalize_index(self, schema, field: str) -> str:
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

    def get_subtype(self, schema, field: str) -> Type:
        # index can be a name or a position
        if field.isdecimal():
            idx = int(field)
            subtypes = list(self.get_subtypes(schema))
            if idx >= 0 and idx < len(subtypes):
                return subtypes[idx]

        elif self.is_named(schema):
            subtypes = dict(self.iter_subtypes(schema))
            if field in subtypes:
                return subtypes[field]

        raise errors.InvalidReferenceError(
            f'{field} is not a member of {self.get_displayname(schema)}')

    def get_subtypes(self, schema):
        raise NotImplementedError

    def derive_subtype(
            self, schema, *, name: str,
            attrs: typing.Optional[typing.Mapping]=None
    ) -> typing.Tuple[s_schema.Schema, Type]:
        return schema, Tuple.from_subtypes(
            schema,
            dict(self.iter_subtypes(schema)),
            self.get_typemods(schema),
            name=name,
            **(attrs or {}))

    @classmethod
    def from_subtypes(cls, schema, subtypes, typemods=None, *,
                      name: str=None, **kwargs):
        named = False
        if typemods is not None:
            named = typemods.get('named', False)

        if not isinstance(subtypes, collections.abc.Mapping):
            types = {}
            for i, t in enumerate(subtypes):
                types[str(i)] = t
        else:
            types = subtypes

        return cls.create(schema, element_types=types, named=named,
                          name=name, **kwargs)

    def implicitly_castable_to(self, other: Type, schema) -> bool:
        if not other.is_tuple():
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
        if not other.is_tuple():
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
        if not other.is_tuple():
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
            self, other: Type, schema) -> typing.Optional[Type]:

        if not other.is_tuple():
            return

        if self == other:
            return self

        subs = self.get_subtypes(schema)
        other_subs = other.get_subtypes(schema)

        if len(subs) != len(other_subs):
            return

        new_types = []
        for st, ot in zip(subs, other_subs):
            nt = st.find_common_implicitly_castable_type(ot, schema)
            if nt is None:
                return
            new_types.append(nt)

        if self.is_named(schema) and other.is_named(schema):
            my_names = self.get_element_names(schema)
            other_names = other.get_element_names(schema)

            if my_names == other_names:
                new_types = dict(zip(my_names, new_types))
                typemods = {"named": True}
            else:
                typemods = None
        else:
            typemods = None

        return Tuple.from_subtypes(schema, new_types, typemods)

    def get_typemods(self, schema):
        return {'named': self.is_named(schema)}

    def contains_array_of_tuples(self, schema):
        return any(
            st.contains_array_of_tuples(schema)
            if st.is_collection() else False
            for st in self.get_subtypes(schema)
        )

    def _resolve_polymorphic(self, schema, concrete_type: 'Type'):
        if not concrete_type.is_tuple():
            return None

        self_subtypes = self.get_subtypes(schema)
        other_subtypes = concrete_type.get_subtypes(schema)

        if len(self_subtypes) != len(other_subtypes):
            return None

        for source, target in zip(self_subtypes, other_subtypes):
            if source.is_polymorphic(schema):
                return source.resolve_polymorphic(schema, target)

        return None

    def _to_nonpolymorphic(self, schema, concrete_type: 'Type'):
        new_types = []
        for st in self.get_subtypes(schema):
            if st.is_polymorphic(schema):
                nst = st.to_nonpolymorphic(schema, concrete_type)
            else:
                nst = st
            new_types.append(nst)

        if self.is_named(schema):
            new_types = dict(zip(self.get_element_names(schema), new_types))
            typemods = {"named": True}
        else:
            typemods = None

        return Tuple.from_subtypes(schema, new_types, typemods)

    def _test_polymorphic(self, schema, other: 'Type'):
        if other.is_any() or other.is_anytuple():
            return True
        if not other.is_tuple():
            return False

        self_subtypes = self.get_subtypes(schema)
        other_subtypes = other.get_subtypes(schema)

        if len(self_subtypes) != len(other_subtypes):
            return False

        return all(st.test_polymorphic(schema, ot)
                   for st, ot in zip(self_subtypes, other_subtypes))

    def _reduce_to_ref(self, schema):
        strefs = {}

        for n, st in self.iter_subtypes(schema):
            st_ref, _ = st._reduce_to_ref(schema)
            strefs[n] = st_ref

        return (
            self.__class__.from_subtypes(
                schema, strefs, typemods=self.get_typemods(schema)),
            (self.__class__, tuple(r.name for r in strefs.values()))
        )

    def _resolve_ref(self, schema):
        if any(hasattr(st, '_resolve_ref')
               for st in self.get_subtypes(schema)):
            subtypes = {}
            for st_name, st in self.iter_subtypes(schema):
                subtypes[st_name] = st._resolve_ref(schema)

            return self.__class__.from_subtypes(
                schema, subtypes, typemods=self.get_typemods(schema))
        else:
            return self

    def material_type(self, schema):
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

    def as_create_delta(self, schema, *, view_name=None, attrs=None):
        from . import delta as sd

        cmd = sd.CommandGroup()

        if view_name is None:
            name = str(self.id)

            ct = CreateTuple(
                classname=name,
            )
        else:
            name = view_name
            ct = CreateTupleView(
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

    def as_delete_delta(self, schema, *, view_name=None):
        if view_name is None:
            name = str(self.id)
            cmd = DeleteTuple(
                classname=name,
            )
        else:
            name = view_name
            cmd = DeleteTupleView(
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

    def is_named(self, schema) -> bool:
        return self.named

    def get_element_names(self, schema):
        return list(self.element_types)

    def iter_subtypes(self, schema):
        yield from self.element_types.items()

    def get_subtypes(self, schema) -> typing.Tuple[Type, ...]:
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

    def __getstate__(self):
        state = self.__dict__.copy()
        state['element_types'] = dict(state['element_types'])
        return state

    def __setstate__(self, state):
        state['element_types'] = types.MappingProxyType(state['element_types'])
        self.__dict__.update(state)


class BaseSchemaTuple(SchemaCollection, BaseTuple):

    named = so.SchemaField(
        bool)

    element_types = so.SchemaField(
        so.ObjectDict,
        coerce=True)

    def is_named(self, schema) -> bool:
        return self.get_named(schema)

    def get_element_names(self, schema):
        return tuple(self.get_element_types(schema).keys())

    def iter_subtypes(self, schema):
        yield from self.get_element_types(schema).items(schema)

    def get_subtypes(self, schema) -> typing.Tuple[Type, ...]:
        return self.get_element_types(schema).values(schema)

        if self.element_types:
            return self.element_types.objects(schema)
        else:
            return []


class SchemaTuple(SchemaAnonymousCollection, BaseSchemaTuple):
    pass


class TupleView(CollectionView, BaseSchemaTuple):
    pass


def generate_type_id(id_str: str) -> uuid.UUID:
    return uuid.uuid5(TYPE_ID_NAMESPACE, id_str)


class TypeCommand(sd.ObjectCommand):
    @classmethod
    def _maybe_get_view_expr(cls, astnode):
        for subcmd in astnode.commands:
            if (isinstance(subcmd, qlast.SetField) and
                    subcmd.name.name == 'expr'):
                return subcmd.value

    @classmethod
    def _get_view_expr(cls, astnode):
        expr = cls._maybe_get_view_expr(astnode)
        if expr is None:
            raise errors.InvalidViewDefinitionError(
                f'missing required view expression', context=astnode.context)
        return expr

    @classmethod
    def _compile_view_expr(cls, expr, classname, schema, context):
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
    def _handle_view_op(cls, schema, cmd, astnode, context):
        from . import ordering as s_ordering

        view_expr = cls._maybe_get_view_expr(astnode)
        if view_expr is None:
            return cmd

        classname = cmd.classname
        if not s_name.Name.is_qualified(classname):
            # Collection commands use unqualified names
            # because they use the type id in the general case,
            # but in the case of an explicit named view, we
            # still want a properly qualified name.
            classname = sd.ObjectCommand._classname_from_ast(
                schema, astnode, context)
            cmd.classname = classname

        expr = s_expr.Expression.from_ast(
            view_expr, schema, context.modaliases)

        ir = cls._compile_view_expr(expr.qlast, classname,
                                    schema, context)

        new_schema = ir.schema

        expr = s_expr.Expression.from_ir(expr, ir, schema=schema)

        coll_view_types = []
        prev_coll_view_types = []
        view_types = []
        prev_view_types = []
        prev_ir = None
        old_schema = None

        for vt in ir.views.values():
            if vt.is_collection():
                coll_view_types.append(vt)
            else:
                new_schema = vt.set_field_value(
                    new_schema, 'view_is_persistent', True)

                view_types.append(vt)

        if isinstance(astnode, qlast.AlterObject):
            prev = schema.get(classname)
            prev_ir = cls._compile_view_expr(
                prev.get_expr(schema).qlast, classname, schema, context)
            old_schema = prev_ir.schema
            for vt in prev_ir.views.values():
                if vt.is_collection():
                    prev_coll_view_types.append(vt)
                else:
                    prev_view_types.append(vt)

        derived_delta = sd.DeltaRoot()

        derived_delta.update(so.Object.delta_sets(
            prev_view_types, view_types,
            old_schema=old_schema, new_schema=new_schema))

        for vt in prev_coll_view_types:
            dt = vt.as_delete_delta(prev_ir.schema, view_name=classname)
            derived_delta.prepend(dt)

        for vt in coll_view_types:
            ct = vt.as_create_delta(
                new_schema, view_name=classname,
                attrs={
                    'expr': expr,
                    'view_is_persistent': True,
                    'view_type': ViewType.Select,
                })
            new_schema, _ = ct.apply(new_schema, context)
            derived_delta.add(ct)

        derived_delta = s_ordering.linearize_delta(
            derived_delta, old_schema=old_schema, new_schema=new_schema)

        real_cmd = None
        for op in derived_delta.get_subcommands():
            if op.classname == classname:
                real_cmd = op
                break

        if real_cmd is None:
            raise RuntimeError(
                'view delta does not contain the expected '
                'view Create/Alter command')

        real_cmd.set_attribute_value('expr', expr)

        cmd = sd.CommandGroup()
        cmd.update(derived_delta.get_subcommands())

        cmd.canonical = True

        return cmd


class CollectionTypeCommandContext(sd.ObjectCommandContext):
    pass


class CollectionTypeCommand(sd.UnqualifiedObjectCommand,
                            TypeCommand,
                            context_class=CollectionTypeCommandContext):
    pass


class CollectionViewCommand(TypeCommand,
                            context_class=CollectionTypeCommandContext):
    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        cmd = cls._handle_view_op(schema, cmd, astnode, context)
        return cmd


class CreateCollectionType(CollectionTypeCommand, sd.CreateObject):
    pass


class DeleteCollectionType(CollectionTypeCommand, sd.DeleteObject):
    pass


class CreateCollectionView(CollectionViewCommand, sd.CreateObject):
    pass


class DeleteCollectionView(CollectionViewCommand, sd.DeleteObject):
    pass


class CreateTuple(CreateCollectionType, schema_metaclass=SchemaTuple):
    pass


class CreateTupleView(CreateCollectionView, schema_metaclass=TupleView):
    pass


class CreateArray(CreateCollectionType, schema_metaclass=SchemaArray):
    pass


class CreateArrayView(CreateCollectionView, schema_metaclass=ArrayView):
    pass


class DeleteTuple(DeleteCollectionType, schema_metaclass=SchemaTuple):
    pass


class DeleteTupleView(DeleteCollectionView, schema_metaclass=TupleView):
    pass


class DeleteArray(DeleteCollectionType, schema_metaclass=SchemaArray):
    pass


class DeleteArrayView(DeleteCollectionView, schema_metaclass=ArrayView):
    pass
