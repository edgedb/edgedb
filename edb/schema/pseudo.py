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
# Cannot import * from typing because of name conflicts in this file.
from typing import Optional, TypeVar, Tuple, TYPE_CHECKING

from . import name as sn
from . import objects as so
from . import types as s_types

if TYPE_CHECKING:
    from . import schema as s_schema


PseudoType_T = TypeVar("PseudoType_T", bound="PseudoType")


class PseudoType(so.InheritingObject, s_types.Type):

    @classmethod
    def get(cls: PseudoType_T, schema: s_schema.Schema) -> PseudoType_T:
        raise NotImplementedError

    def get_shortname(self, schema: s_schema.Schema) -> str:
        return self.get_name(schema)

    def get_bases(self, schema: s_schema.Schema) -> so.ObjectList:
        return so.ObjectList.create_empty()

    def get_ancestors(self, schema: s_schema.Schema) -> so.ObjectList:
        return so.ObjectList.create_empty()

    def get_is_abstract(self, schema: s_schema.Schema) -> bool:
        return True

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return True

    def material_type(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[s_schema.Schema, s_types.Type]:
        return schema, self


class PseudoTypeShell(s_types.TypeShell):

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return True


class Any(PseudoType):

    @classmethod
    def get(cls, schema: s_schema.Schema) -> Any:
        return schema.get_global(Any, "anytype")

    @classmethod
    def instance(cls) -> Any:
        if cls._instance is None:
            cls._instance = cls.create()
        return cls._instance

    def is_any(self) -> bool:
        return True

    def _resolve_polymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: s_types.Type
    ) -> s_types.Type:
        if concrete_type.is_scalar():
            return concrete_type.get_topmost_concrete_base(schema)
        return concrete_type

    def _to_nonpolymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: s_types.Type
    ) -> Tuple[s_schema.Schema, s_types.Type]:
        return schema, concrete_type

    def _test_polymorphic(
        self,
        schema: s_schema.Schema,
        other: s_types.Type
    ) -> bool:
        return other.is_any()

    def implicitly_castable_to(
        self,
        other: s_types.Type,
        schema: s_schema.Schema
    ) -> bool:
        return other.is_any()

    def find_common_implicitly_castable_type(
        self,
        other: s_types.Type,
        schema: s_schema.Schema,
    ) -> Tuple[s_schema.Schema, Optional[s_types.Type]]:
        if self == other:
            return schema, self
        else:
            return schema, None

    def get_common_parent_type_distance(
        self,
        other: s_types.Type,
        schema: s_schema.Schema
    ) -> int:
        if other.is_any():
            return 0
        else:
            return s_types.MAX_TYPE_DISTANCE

    def as_shell(self, schema):
        return AnyTypeShell()


class AnyTypeShell(PseudoTypeShell):

    def __init__(self, *, name=sn.UnqualifiedName('anytype')):
        super().__init__(name=name, schemaclass=Any)

    def resolve(self, schema: s_schema.Schema) -> Any:
        return Any.get(schema)


class AnyTuple(PseudoType):

    @classmethod
    def get(cls, schema: s_schema.Schema) -> Any:
        return schema.get_global(AnyTuple, "anytuple")

    def is_anytuple(self) -> bool:
        return True

    def is_tuple(self) -> bool:
        return True

    def implicitly_castable_to(
        self,
        other: s_types.Type,
        schema: s_schema.Schema
    ) -> bool:
        return other.is_anytuple()

    def _resolve_polymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: s_types.Type
    ) -> Optional[s_types.Type]:
        if (not concrete_type.is_tuple() or
                concrete_type.is_polymorphic(schema)):
            return None
        else:
            return concrete_type

    def _to_nonpolymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: s_types.Type
    ) -> Tuple[s_schema.Schema, s_types.Type]:
        return schema, concrete_type

    def as_shell(self, schema):
        return AnyTupleShell()


class AnyTupleShell(PseudoTypeShell):

    def __init__(self, *, name=sn.UnqualifiedName('anytuple')):
        super().__init__(name=name, schemaclass=AnyTuple)

    def resolve(self, schema: s_schema.Schema) -> AnyTuple:
        return AnyTuple.get(schema)


def populate_types(schema: s_schema.Schema) -> s_schema.Schema:

    schema, _ = Any.create_in_schema(schema, name='anytype')
    schema, _ = AnyTuple.create_in_schema(schema, name='anytuple')

    return schema
