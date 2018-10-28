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
import typing

from edb.lang.common import typed

from . import derivable
from . import error as s_err
from . import expr as s_expr
from . import name as s_name
from . import objects as so


class ViewType(enum.IntEnum):
    Select = enum.auto()
    Insert = enum.auto()
    Update = enum.auto()


class Type(so.NamedObject, derivable.DerivableObjectBase):
    """A schema item that is a valid *type*."""

    # For a type representing a view, this would contain the
    # view type.  Non-view types have None here.
    view_type = so.Field(ViewType, default=None, compcoef=0.909)

    # If this type is a view, expr may contain an expression that
    # defines the view set.
    expr = so.Field(s_expr.ExpressionText, default=None,
                    coerce=True, compcoef=0.909)

    # If this type is a view defined by a nested shape expression,
    # and the nested shape contains references to link properties,
    # rptr will contain the inbound pointer class.
    rptr = so.Field(so.Object, default=None, compcoef=0.909)

    def is_type(self):
        return True

    def is_polymorphic(self):
        return False

    def is_any(self):
        return False

    def is_scalar(self):
        return False

    def is_array(self):
        return False

    def is_tuple(self):
        return False

    def test_polymorphic(self, poly: 'Type') -> bool:
        """Check if this type can be matched by a polymorphic type.

        Examples:

            - `array<anyscalar>`.test_polymorphic(`array<any>`) -> True
            - `array<str>`.test_polymorphic(`array<any>`) -> True
            - `array<int64>`.test_polymorphic(`anyscalar`) -> False
            - `float32`.test_polymorphic(`anyint`) -> False
            - `int32`.test_polymorphic(`anyint`) -> True
        """

        if not poly.is_polymorphic():
            raise TypeError('expected a polymorphic type as a second argument')

        if poly.is_any():
            return True

        return self._test_polymorphic(poly)

    def resolve_polymorphic(self, other: 'Type') -> 'Type':
        """Resolve the polymorphic type component.

        Examples:

            - `array<any>`.resolve_polymorphic(`array<int>`) -> `int`
            - `array<any>`.resolve_polymorphic(`tuple<int>`) -> None
        """
        if not self.is_polymorphic() or other.is_polymorphic():
            return None

        return self._resolve_polymorphic(other)

    def to_nonpolymorphic(self, concrete_type: 'Type') -> 'Type':
        """Produce an non-polymorphic version of self.

        Example:
            `array<any>`.to_nonpolymorphic(`int`) -> `array<int>`
            `tuple<int, any>`.to_nonpolymorphic(`str`) -> `tuple<int, str>`
        """
        if not self.is_polymorphic():
            raise TypeError('non-polymorphic type')

        return self._to_nonpolymorphic(concrete_type)

    def _test_polymorphic(self, other: 'Type'):
        return False

    def _resolve_polymorphic(self, concrete_type: 'Type'):
        raise NotImplementedError(
            f'{type(self)} does not support resolve_polymorphic()')

    def _to_nonpolymorphic(self, concrete_type: 'Type'):
        raise NotImplementedError(
            f'{type(self)} does not support to_nonpolymorphic()')

    def is_view(self):
        return self.view_type is not None

    def assignment_castable_to(self, other: 'Type', schema) -> bool:
        return self.implicitly_castable_to(other, schema)

    def implicitly_castable_to(self, other: 'Type', schema) -> bool:
        return False

    def find_common_implicitly_castable_type(
            self, other: 'Type', schema) -> typing.Optional['Type']:
        return

    def material_type(self):
        # When self is a view, this returns the material type
        # under the view.
        return self

    def peel_view(self):
        # When self is a view, this returns the class the view
        # is derived from (which may be another view).  If no
        # parent class is available, returns self.
        return self

    def __repr__(self):
        return (
            f'<schema.{self.__class__.__name__} {self.name}'
            f'{" view" if self.is_view() else ""} at 0x{id(self):x}>'
        )

    __str__ = __repr__


class Collection(Type):
    _type = 'collection'

    def __init__(self, *, name=None, **kwargs):
        if name is None:
            name = s_name.SchemaName(module='std', name='collection')
            super().__init__(name=name, **kwargs)
            subtypes = ",".join(st.name for st in self.get_subtypes())
            self.name = s_name.SchemaName(
                module='std', name=f'{self.schema_name}<{subtypes}>')
        else:
            super().__init__(name=name, **kwargs)

    def is_polymorphic(self):
        return any(st.is_polymorphic() for st in self.get_subtypes())

    @property
    def is_virtual(self):
        # This property in necessary for compatibility with node classes.
        return False

    def _issubclass(self, parent):
        if not isinstance(parent, Collection) and parent.is_any():
            return True

        if parent.__class__ is not self.__class__:
            return False

        parent_types = parent.get_subtypes()
        my_types = self.get_subtypes()

        for pt, my in zip(parent_types, my_types):
            if not pt.is_any() and not pt.issubclass(my):
                return False

        return True

    def issubclass(self, parent):
        if isinstance(parent, tuple):
            return any(self.issubclass(p) for p in parent)
        else:
            if parent.is_any():
                return True
            else:
                return self._issubclass(parent)

    @classmethod
    def compare_values(cls, ours, theirs, context, compcoef):
        if (ours is None) != (theirs is None):
            return compcoef
        elif ours is None:
            return 1.0

        if ours.get_canonical_class() != theirs.get_canonical_class():
            basecoef = 0.2
        else:
            my_subtypes = ours.get_subtypes()
            other_subtypes = theirs.get_subtypes()

            similarity = []
            for i, st in enumerate(my_subtypes):
                similarity.append(st.compare(other_subtypes[i], context))

            basecoef = sum(similarity) / len(similarity)

        return basecoef + (1 - basecoef) * compcoef

    def get_container(self):
        raise NotImplementedError

    def get_subtypes(self):
        raise NotImplementedError

    def get_typemods(self):
        return ()

    def get_subtype(self, schema, typeref):
        from . import scalars as s_scalars
        from . import types as s_types

        if isinstance(typeref, so.ObjectRef):
            eltype = schema.get(typeref.classname)
        else:
            eltype = typeref

        if isinstance(eltype, s_scalars.ScalarType):
            eltype = eltype.get_topmost_concrete_base()
            eltype = s_types.BaseTypeMeta.get_implementation(eltype.name)

        return eltype

    def coerce(self, values, schema):
        raise NotImplementedError

    @classmethod
    def get_class(cls, schema_name):
        if schema_name == 'array':
            return Array
        elif schema_name == 'tuple':
            return Tuple
        else:
            raise s_err.SchemaError(
                'unknown collection type: {!r}'.format(schema_name))

    @classmethod
    def from_subtypes(cls, subtypes, typemods=None):
        raise NotImplementedError

    def _reduce_to_ref(self):
        strefs = []

        for st in self.get_subtypes():
            strefs.append(so.ObjectRef(classname=st.name))

        return (
            self.__class__.from_subtypes(strefs),
            (self.__class__, tuple(r.classname for r in strefs))
        )

    def _resolve_ref(self, resolve):
        subtypes = []
        for stref in self.get_subtypes():
            subtypes.append(stref._resolve(resolve))

        return self.__class__.from_subtypes(subtypes)


class Array(Collection):
    schema_name = 'array'
    element_type = so.Field(so.Object)
    dimensions = so.Field(typed.IntList, [], coerce=True)

    def is_array(self):
        return True

    def get_container(self):
        return tuple

    def get_subtypes(self):
        return (self.element_type,)

    def get_typemods(self):
        return (self.dimensions,)

    def coerce(self, items, schema):
        container = self.get_container()

        elements = []

        eltype = self.get_subtype(schema, self.element_type)

        for item in items:
            if not isinstance(item, eltype):
                item = eltype(item)
            elements.append(item)

        return container(elements)

    def implicitly_castable_to(self, other: Type, schema) -> bool:
        if not other.is_array():
            return False

        return self.element_type.implicitly_castable_to(
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
            return Array.from_subtypes([subtype])

    def _resolve_polymorphic(self, concrete_type: 'Type'):
        if not concrete_type.is_array():
            return None
        return self.element_type.resolve_polymorphic(
            concrete_type.element_type)

    def _to_nonpolymorphic(self, concrete_type: 'Type'):
        return Array.from_subtypes((concrete_type,))

    def _test_polymorphic(self, other: 'Type'):
        if other.is_any():
            return True
        if not other.is_array():
            return False

        return self.element_type.test_polymorphic(other.element_type)

    @classmethod
    def from_subtypes(cls, subtypes, typemods=None):
        if len(subtypes) != 1:
            raise s_err.SchemaError(
                f'unexpected number of subtypes, expecting 1: {subtypes!r}')
        stype = subtypes[0]

        if isinstance(stype, Array):
            raise s_err.SchemaError(f'nested arrays are not supported')

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

        return cls(element_type=element_type, dimensions=dimensions)

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


class Tuple(Collection):
    schema_name = 'tuple'

    named = so.Field(bool, False)
    element_types = so.Field(so.ObjectDict, coerce=True)

    def is_tuple(self):
        return True

    def get_container(self):
        return dict

    def get_subtypes(self):
        if self.element_types:
            return list(self.element_types.values())
        else:
            return []

    @classmethod
    def from_subtypes(cls, subtypes, typemods=None):
        named = False
        if typemods is not None:
            named = typemods.get('named', False)

        if not isinstance(subtypes, collections.abc.Mapping):
            types = collections.OrderedDict()
            for i, t in enumerate(subtypes):
                types[str(i)] = t
        else:
            types = subtypes

        return cls(element_types=types, named=named)

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

        return Tuple.from_subtypes(new_types, typemods)

    def get_typemods(self):
        return {'named': self.named}

    def _resolve_polymorphic(self, concrete_type: 'Type'):
        if not concrete_type.is_tuple():
            return None

        self_subtypes = self.get_subtypes()
        other_subtypes = concrete_type.get_subtypes()

        if len(self_subtypes) != len(other_subtypes):
            return None

        for source, target in zip(self_subtypes, other_subtypes):
            if source.is_polymorphic():
                return source.resolve_polymorphic(target)

        return None

    def _to_nonpolymorphic(self, concrete_type: 'Type'):
        new_types = []
        for st in self.get_subtypes():
            if st.is_polymorphic():
                nst = st.to_nonpolymorphic(concrete_type)
            else:
                nst = st
            new_types.append(nst)

        if self.named:
            new_types = dict(zip(self.element_types.keys(), new_types))
            typemods = {"named": True}
        else:
            typemods = None

        return Tuple.from_subtypes(new_types, typemods)

    def _test_polymorphic(self, other: 'Type'):
        if other.is_any():
            return True
        if not other.is_tuple():
            return False

        self_subtypes = self.get_subtypes()
        other_subtypes = other.get_subtypes()

        if len(self_subtypes) != len(other_subtypes):
            return False

        return all(st.test_polymorphic(ot)
                   for st, ot in zip(self_subtypes, other_subtypes))

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
