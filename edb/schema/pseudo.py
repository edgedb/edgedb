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

import typing

from . import inheriting
from . import name as sn
from . import objects as so
from . import types as s_types


if typing.TYPE_CHECKING:
    from . import schema as s_schema


class PseudoType(inheriting.InheritingObject, s_types.Type):

    name = so.Field(sn.UnqualifiedName)

    def get_name(self, schema: s_schema.Schema) -> sn.Name:
        return self.name

    def get_shortname(self, schema: s_schema.Schema) -> str:
        return self.name

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return self.name

    def get_bases(self, schema: s_schema.Schema) -> so.ObjectList:
        return so.ObjectList.create_empty()

    def get_ancestors(self, schema: s_schema.Schema) -> so.ObjectList:
        return so.ObjectList.create_empty()

    def get_is_abstract(self, schema: s_schema.Schema) -> bool:
        return True

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return True

    def material_type(self, schema: s_schema.Schema) -> s_types.Type:
        return self

    def __hash__(self) -> int:
        return hash((
            type(self),
            self.name,
        ))

    def __eq__(self, other: s_types.Type) -> bool:
        return (type(self) is type(other) and
                self.name == other.name)


class AnyMeta(type(PseudoType)):

    @property
    def instance(cls):
        if cls._instance is None:
            cls._instance = cls.create()

        return cls._instance


class Any(PseudoType, metaclass=AnyMeta):

    _instance = None

    @classmethod
    def create(cls):
        return cls._create(None, name=sn.UnqualifiedName('anytype'))

    def is_any(self):
        return True

    def _resolve_polymorphic(self, schema, concrete_type: s_types.Type):
        if concrete_type.is_scalar():
            return concrete_type.get_topmost_concrete_base(schema)
        return concrete_type

    def _to_nonpolymorphic(self, schema, concrete_type: s_types.Type):
        return concrete_type

    def _test_polymorphic(self, schema, other: s_types.Type):
        return other.is_any()

    def implicitly_castable_to(self, other: s_types.Type, schema) -> bool:
        return other.is_any()

    def find_common_implicitly_castable_type(
            self, other: s_types.Type,
            schema) -> typing.Optional[s_types.Type]:

        if self == other:
            return self

    def get_common_parent_type_distance(
            self, other: s_types.Type, schema) -> int:
        if other.is_any():
            return 0
        else:
            return s_types.MAX_TYPE_DISTANCE

    def _reduce_to_ref(self, schema):
        return AnyObjectRef(), sn.UnqualifiedName('anytype')


class AnyObjectRef(so.ObjectRef):

    def __init__(self, *, name=sn.UnqualifiedName('anytype')):
        super().__init__(name=name)

    def _resolve_ref(self, schema):
        return Any.instance


class AnyTuple(PseudoType, metaclass=AnyMeta):

    _instance = None

    @classmethod
    def create(cls):
        return cls._create(None, name=sn.UnqualifiedName('anytuple'))

    def is_anytuple(self):
        return True

    def is_tuple(self):
        return True

    def implicitly_castable_to(self, other: s_types.Type, schema) -> bool:
        return other.is_anytuple()

    def _reduce_to_ref(self, schema):
        return AnyTupleRef(), sn.UnqualifiedName('anytuple')

    def _resolve_polymorphic(self, schema, concrete_type: s_types.Type):
        if (not concrete_type.is_tuple() or
                concrete_type.is_polymorphic(schema)):
            return None
        else:
            return concrete_type

    def _to_nonpolymorphic(self, schema, concrete_type: s_types.Type):
        return concrete_type


class AnyTupleRef(so.ObjectRef):

    def __init__(self, *, name=sn.UnqualifiedName('anytuple')):
        super().__init__(name=name)

    def _resolve_ref(self, schema):
        return AnyTuple.instance
