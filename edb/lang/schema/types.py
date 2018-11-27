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


import collections.abc
import enum
import types
import typing
import uuid

from edb import errors

from edb.lang.common import typed

from . import abc as s_abc
from . import derivable
from . import expr as s_expr
from . import name as s_name
from . import objects as so


TYPE_ID_NAMESPACE = uuid.UUID('00e50276-2502-11e7-97f2-27fe51238dbd')
MAX_TYPE_DISTANCE = 1_000_000_000


class ViewType(enum.IntEnum):
    Select = enum.auto()
    Insert = enum.auto()
    Update = enum.auto()


class Type(so.Object, derivable.DerivableObjectBase, s_abc.Type):
    """A schema item that is a valid *type*."""

    # For a type representing a view, this would contain the
    # view type.  Non-view types have None here.
    view_type = so.SchemaField(
        ViewType,
        default=None, compcoef=0.909)

    # If this type is a view, expr may contain an expression that
    # defines the view set.
    expr = so.SchemaField(
        s_expr.ExpressionText,
        default=None, coerce=True, allow_ddl_set=True, compcoef=0.909)

    # If this type is a view defined by a nested shape expression,
    # and the nested shape contains references to link properties,
    # rptr will contain the inbound pointer class.
    rptr = so.SchemaField(
        so.Object,
        default=None, compcoef=0.909)

    def derive_subtype(
            self, schema, *, name: str,
            attrs: typing.Optional[typing.Mapping]=None) -> 'Type':
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

    def contains_any(self):
        return self.is_any()

    def is_scalar(self):
        return False

    def is_collection(self):
        return False

    def is_array(self):
        return False

    def is_tuple(self):
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
        if not self.is_polymorphic(schema) or other.is_polymorphic(schema):
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

    def material_type(self, schema):
        # When self is a view, this returns the material type
        # under the view.
        return self

    def peel_view(self, schema):
        # When self is a view, this returns the class the view
        # is derived from (which may be another view).  If no
        # parent class is available, returns self.
        return self


class Collection(Type, s_abc.Collection):

    name = so.Field(
        s_name.SchemaName,
        inheritable=False, compcoef=0.670)

    view_type = so.Field(
        ViewType,
        default=None, ephemeral=True)

    expr = so.Field(
        s_expr.ExpressionText,
        default=None, ephemeral=True)

    rptr = so.Field(
        so.Object,
        default=None, ephemeral=True)

    def get_name(self, schema):
        return self.name

    def get_shortname(self, schema):
        return self.name

    def get_view_type(self, schema):
        return self.view_type

    def get_expr(self, schema):
        return self.expr

    def get_rptr(self, schema):
        return self.rptr

    def is_polymorphic(self, schema):
        return any(st.is_polymorphic(schema)
                   for st in self.get_subtypes())

    def contains_any(self):
        return any(st.contains_any() for st in self.get_subtypes())

    def is_collection(self):
        return True

    @property
    def is_virtual(self):
        # This property in necessary for compatibility with node classes.
        return False

    def get_common_parent_type_distance(
            self, other: Type, schema) -> int:
        if other.is_any():
            return 1

        if other.__class__ is not self.__class__:
            return -1

        other_types = other.get_subtypes()
        my_types = self.get_subtypes()

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

        parent_types = parent.get_subtypes()
        my_types = self.get_subtypes()

        for pt, my in zip(parent_types, my_types):
            if not pt.is_any() and not pt.issubclass(schema, my):
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
            my_subtypes = ours.get_subtypes()
            other_subtypes = theirs.get_subtypes()

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

    def get_subtypes(self):
        raise NotImplementedError

    def get_typemods(self):
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

        for st in self.get_subtypes():
            st_ref, _ = st._reduce_to_ref(schema)
            strefs.append(st_ref)

        return (
            self.__class__.from_subtypes(schema, strefs),
            (self.__class__, tuple(r.name for r in strefs))
        )

    def _resolve_ref(self, schema):
        if any(hasattr(st, '_resolve_ref') for st in self.get_subtypes()):

            subtypes = []
            for st in self.get_subtypes():
                subtypes.append(st._resolve_ref(schema))

            return self.__class__.from_subtypes(
                schema, subtypes, typemods=self.get_typemods())
        else:
            return self

    def __repr__(self):
        return (
            f'<{self.__class__.__name__} '
            f'{self.name} {self.id}'
            f'at 0x{id(self):x}>'
        )

    def dump(self, schema):
        return repr(self)


class Dimensions(typed.FrozenTypedList, type=int):
    pass


class Array(Collection, s_abc.Array):
    schema_name = 'array'
    element_type = so.Field(so.Object)
    dimensions = so.Field(Dimensions, coerce=True)

    @classmethod
    def create(cls, schema, *, name=None,
               id=so.NoDefault, dimensions=None, element_type, **kwargs):
        if id is so.NoDefault:
            id_str = f'array-{element_type.id}'
            id = uuid.uuid5(TYPE_ID_NAMESPACE, id_str)

        if name is None:
            name = s_name.SchemaName(
                module='std',
                name=f'array<{element_type.get_name(schema)}>')

        if dimensions is None:
            dimensions = []

        return super()._create(
            schema, id=id, name=name, element_type=element_type,
            dimensions=dimensions, **kwargs)

    def get_displayname(self, schema):
        return f'array<{self.element_type.get_displayname(schema)}>'

    def is_array(self):
        return True

    def get_container(self):
        return tuple

    def derive_subtype(
            self, schema, *, name: str,
            attrs: typing.Optional[typing.Mapping]=None) -> Type:
        return schema, Array.from_subtypes(
            schema,
            [self.element_type],
            self.get_typemods(),
            name=name,
            **(attrs or {}))

    def get_subtypes(self):
        return (self.element_type,)

    def get_typemods(self):
        return (self.dimensions,)

    def implicitly_castable_to(self, other: Type, schema) -> bool:
        if not other.is_array():
            return False

        return self.element_type.implicitly_castable_to(
            other.element_type, schema)

    def get_implicit_cast_distance(self, other: 'Type', schema) -> int:
        if not other.is_array():
            return -1

        return self.element_type.get_implicit_cast_distance(
            other.element_type, schema)

    def assignment_castable_to(self, other: Type, schema) -> bool:
        if not other.is_array():
            return False

        return self.element_type.assignment_castable_to(
            other.element_type, schema)

    def find_common_implicitly_castable_type(
            self, other: Type, schema) -> typing.Optional[Type]:

        if not other.is_array():
            return

        if self == other:
            return self

        subtype = self.element_type.find_common_implicitly_castable_type(
            other.element_type, schema)

        if subtype is not None:
            return Array.from_subtypes(schema, [subtype])

    def _resolve_polymorphic(self, schema, concrete_type: 'Type'):
        if not concrete_type.is_array():
            return None
        return self.element_type.resolve_polymorphic(
            schema, concrete_type.element_type)

    def _to_nonpolymorphic(self, schema, concrete_type: 'Type'):
        return Array.from_subtypes(schema, (concrete_type,))

    def _test_polymorphic(self, schema, other: 'Type'):
        if other.is_any():
            return True
        if not other.is_array():
            return False

        return self.element_type.test_polymorphic(schema, other.element_type)

    @classmethod
    def from_subtypes(cls, schema, subtypes, typemods=None, *, name=None):
        if len(subtypes) != 1:
            raise errors.SchemaError(
                f'unexpected number of subtypes, expecting 1: {subtypes!r}')
        stype = subtypes[0]

        if isinstance(stype, Array):
            raise errors.UnsupportedFeatureError(
                f'nested arrays are not supported')

        if typemods:
            dimensions = typemods[0]
        else:
            dimensions = []

        if isinstance(stype, cls):
            # There is no array of arrays, only multi-dimensional arrays.
            element_type = stype.element_type
            if not dimensions:
                dimensions.append(-1)
            dimensions += stype.dimensions
        else:
            element_type = stype
            dimensions = []

        return cls.create(schema, element_type=element_type,
                          dimensions=dimensions, name=name)

    def __hash__(self):
        return hash((
            self.__class__,
            self.name,
            self.element_type,
            tuple(self.dimensions),
        ))

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            return False

        return (
            self.name == other.name and
            self.element_type == other.element_type and
            self.dimensions == other.dimensions
        )


class Tuple(Collection, s_abc.Tuple):
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
            id = uuid.uuid5(TYPE_ID_NAMESPACE, id_str)

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
                             for st in self.element_types.values())
        return f'tuple<{st_names}>'

    def __getstate__(self):
        state = self.__dict__.copy()
        state['element_types'] = dict(state['element_types'])
        return state

    def __setstate__(self, state):
        state['element_types'] = types.MappingProxyType(state['element_types'])
        self.__dict__.update(state)

    def is_tuple(self):
        return True

    def get_container(self):
        return dict

    def iter_subtypes(self):
        yield from self.element_types.items()

    def normalize_index(self, schema, field: str) -> str:
        if self.named and field.isdecimal():
            idx = int(field)
            if idx >= 0 and idx < len(self.element_types):
                return list(self.element_types.keys())[idx]
            else:
                raise errors.InvalidReferenceError(
                    f'{field} is not a member of '
                    f'{self.get_displayname(schema)}')

        return field

    def index_of(self, schema, field: str) -> int:
        if field.isdecimal():
            idx = int(field)
            if idx >= 0 and idx < len(self.element_types):
                if self.named:
                    return list(self.element_types.keys()).index(field)
                else:
                    return idx
        elif self.named and field in self.element_types:
            return list(self.element_types.keys()).index(field)

        raise errors.InvalidReferenceError(
            f'{field} is not a member of {self.get_displayname(schema)}')

    def get_subtype(self, schema, field: str) -> Type:
        # index can be a name or a position
        if field.isdecimal():
            idx = int(field)
            if idx >= 0 and idx < len(self.element_types):
                return list(self.element_types.values())[idx]

        elif self.named and field in self.element_types:
            return self.element_types[field]

        raise errors.InvalidReferenceError(
            f'{field} is not a member of {self.get_displayname(schema)}')

    def get_subtypes(self):
        if self.element_types:
            return list(self.element_types.values())
        else:
            return []

    def derive_subtype(
            self, schema, *, name: str,
            attrs: typing.Optional[typing.Mapping]=None) -> Type:
        return schema, Tuple.from_subtypes(
            schema,
            self.element_types,
            self.get_typemods(),
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

        self_subtypes = self.get_subtypes()
        other_subtypes = other.get_subtypes()

        if len(self_subtypes) != len(other_subtypes):
            return False

        for st, ot in zip(self_subtypes, other_subtypes):
            if not st.implicitly_castable_to(ot, schema):
                return False

        return True

    def get_implicit_cast_distance(self, other: Type, schema) -> int:
        if not other.is_tuple():
            return -1

        self_subtypes = self.get_subtypes()
        other_subtypes = other.get_subtypes()

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

        self_subtypes = self.get_subtypes()
        other_subtypes = other.get_subtypes()

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

        subs = self.get_subtypes()
        other_subs = other.get_subtypes()

        if len(subs) != len(other_subs):
            return

        new_types = []
        for st, ot in zip(subs, other_subs):
            nt = st.find_common_implicitly_castable_type(ot, schema)
            if nt is None:
                return
            new_types.append(nt)

        if (self.named and
                other.named and
                self.element_types.keys() == other.element_types.keys()):
            new_types = dict(zip(self.element_types.keys(), new_types))
            typemods = {"named": True}
        else:
            typemods = None

        return Tuple.from_subtypes(schema, new_types, typemods)

    def get_typemods(self):
        return {'named': self.named}

    def _resolve_polymorphic(self, schema, concrete_type: 'Type'):
        if not concrete_type.is_tuple():
            return None

        self_subtypes = self.get_subtypes()
        other_subtypes = concrete_type.get_subtypes()

        if len(self_subtypes) != len(other_subtypes):
            return None

        for source, target in zip(self_subtypes, other_subtypes):
            if source.is_polymorphic(schema):
                return source.resolve_polymorphic(schema, target)

        return None

    def _to_nonpolymorphic(self, schema, concrete_type: 'Type'):
        new_types = []
        for st in self.get_subtypes():
            if st.is_polymorphic(schema):
                nst = st.to_nonpolymorphic(schema, concrete_type)
            else:
                nst = st
            new_types.append(nst)

        if self.named:
            new_types = dict(zip(self.element_types.keys(), new_types))
            typemods = {"named": True}
        else:
            typemods = None

        return Tuple.from_subtypes(schema, new_types, typemods)

    def _test_polymorphic(self, schema, other: 'Type'):
        if other.is_any() or other.is_anytuple():
            return True
        if not other.is_tuple():
            return False

        self_subtypes = self.get_subtypes()
        other_subtypes = other.get_subtypes()

        if len(self_subtypes) != len(other_subtypes):
            return False

        return all(st.test_polymorphic(schema, ot)
                   for st, ot in zip(self_subtypes, other_subtypes))

    def _resolve_ref(self, schema):
        if any(hasattr(st, '_resolve_ref') for st in self.get_subtypes()):
            subtypes = {}
            for st_name, st in self.element_types.items():
                subtypes[st_name] = st._resolve_ref(schema)

            return self.__class__.from_subtypes(
                schema, subtypes, typemods=self.get_typemods())
        else:
            return self

    def __hash__(self):
        return hash((
            self.__class__,
            self.named,
            self.name,
            tuple(self.element_types.items())
        ))

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            return False

        return (
            self.named == other.named and
            self.name == other.name and
            self.element_types == other.element_types
        )
