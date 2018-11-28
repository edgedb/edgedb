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


import typing

from . import inheriting
from . import objects as so
from . import types as s_types


class PseudoType(inheriting.InheritingObject, s_types.Type):
    pass


class Any(PseudoType):

    name = so.Field(str)

    schema_name = 'anytype'
    _instance = None

    @classmethod
    def create(cls):
        if cls._instance is None:
            cls._instance = cls._create(None, name='anytype')
        return cls._instance

    def get_name(self, schema):
        return self.name

    def get__virtual_children(self, schema):
        return None

    def get_bases(self, schema):
        return so.ObjectList.create_empty()

    def get_is_abstract(self, schema):
        return True

    def is_any(self):
        return True

    def is_polymorphic(self, schema):
        return True

    def _resolve_polymorphic(self, schema, concrete_type: s_types.Type):
        if concrete_type.is_scalar():
            return concrete_type.get_topmost_concrete_base(schema)
        return concrete_type

    def _to_nonpolymorphic(self, schema, concrete_type: s_types.Type):
        return concrete_type

    def _test_polymorphic(self, schema, other: s_types.Type):
        return other.is_any()

    def find_common_implicitly_castable_type(
            self, other: s_types.Type,
            schema) -> typing.Optional[s_types.Type]:

        if self == other:
            return self

    def _reduce_to_ref(self, schema):
        return AnyObjectRef(), 'anytype'

    def __hash__(self):
        return hash((
            type(self),
            self.name,
        ))

    def __eq__(self, other):
        return (type(self) is type(other) and
                self.name == other.name)


class AnyObjectRef(so.ObjectRef):

    def __init__(self, *, name='anytype'):
        super().__init__(name=name)

    def _resolve_ref(self, schema):
        return Any.create()
