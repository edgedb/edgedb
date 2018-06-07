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

from edb.lang.common import typed

from . import derivable
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

    def is_view(self):
        return self.view_type is not None

    def assignment_castable_to(self, other: 'Type', schema) -> bool:
        return self.implicitly_castable_to(other, schema)

    def implicitly_castable_to(self, other: 'Type', schema) -> bool:
        return self.issubclass(other)

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

    @property
    def is_virtual(self):
        # This property in necessary for compatibility with node classes.
        return False

    def _issubclass(self, parent):
        if not isinstance(parent, Collection) and parent.name == 'std::any':
            return True

        if parent.__class__ is not self.__class__:
            return False

        parent_types = parent.get_subtypes()
        my_types = self.get_subtypes()

        for pt, my in zip(parent_types, my_types):
            if pt.name != 'std::any' and not pt.issubclass(my):
                return False

        return True

    def issubclass(self, parent):
        if isinstance(parent, tuple):
            return any(self.issubclass(p) for p in parent)
        else:
            if parent.name == 'std::any':
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
        elif schema_name == 'map':
            return Map
        elif schema_name == 'tuple':
            return Tuple
        else:
            raise ValueError(
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

    @classmethod
    def from_subtypes(cls, subtypes, typemods=None):
        if len(subtypes) != 1:
            raise ValueError(
                f'unexpected number of subtypes, expecting 1: {subtypes!r}')

        if typemods:
            dimensions = typemods[0]
        else:
            dimensions = []

        stype = subtypes[0]
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


class Map(Collection):
    schema_name = 'map'

    element_type = so.Field(so.Object)
    key_type = so.Field(so.Object)

    def get_container(self):
        return dict

    def get_subtypes(self):
        return (self.key_type, self.element_type,)

    @classmethod
    def from_subtypes(cls, subtypes, typemods=None):
        if len(subtypes) != 2:
            raise ValueError(
                f'unexpected number of subtypes, expecting 2: {subtypes!r}')
        return cls(key_type=subtypes[0], element_type=subtypes[1])


class Tuple(Collection):
    schema_name = 'tuple'

    named = so.Field(bool, False)
    element_types = so.Field(so.ObjectDict, coerce=True)

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

    def get_typemods(self):
        return {'named': self.named}

    def __hash__(self):
        return hash((
            self.__class__,
            self.named,
            self.name,
            tuple(self.element_types.items())
        ))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return (
            self.named == other.named and self.name == other.name and
            self.element_types == other.element_types
        )
