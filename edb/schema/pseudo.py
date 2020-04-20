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

from edb import errors
from edb.edgeql import ast as qlast

from . import delta as sd
from . import objects as so
from . import scalars as s_scalars
from . import types as s_types

if TYPE_CHECKING:
    from . import schema as s_schema


PseudoType_T = TypeVar("PseudoType_T", bound="PseudoType")


class PseudoType(so.InheritingObject, s_types.Type):

    @classmethod
    def get(cls, schema: s_schema.Schema, name: str) -> PseudoType:
        return schema.get_global(PseudoType, name)

    def as_shell(self, schema: s_schema.Schema) -> PseudoTypeShell:
        return PseudoTypeShell(name=self.get_name(schema))

    def get_shortname(self, schema: s_schema.Schema) -> str:
        return self.get_name(schema)

    def get_bases(
        self,
        schema: s_schema.Schema,
    ) -> so.ObjectList[PseudoType]:
        return so.ObjectList[PseudoType].create_empty()  # type: ignore

    def get_ancestors(
        self,
        schema: s_schema.Schema,
    ) -> so.ObjectList[PseudoType]:
        return so.ObjectList[PseudoType].create_empty()  # type: ignore

    def get_is_abstract(self, schema: s_schema.Schema) -> bool:
        return True

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return True

    def material_type(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[s_schema.Schema, PseudoType]:
        return schema, self

    def is_any(self, schema: s_schema.Schema) -> bool:
        return self.get_name(schema) == 'anytype'

    def is_tuple(self, schema: s_schema.Schema) -> bool:
        return self.get_name(schema) == 'anytuple'

    def is_anytuple(self, schema: s_schema.Schema) -> bool:
        return self.get_name(schema) == 'anytuple'

    def implicitly_castable_to(
        self,
        other: s_types.Type,
        schema: s_schema.Schema
    ) -> bool:
        return self == other

    def find_common_implicitly_castable_type(
        self,
        other: s_types.Type,
        schema: s_schema.Schema,
    ) -> Tuple[s_schema.Schema, Optional[PseudoType]]:
        if self == other:
            return schema, self
        else:
            return schema, None

    def get_common_parent_type_distance(
        self,
        other: s_types.Type,
        schema: s_schema.Schema
    ) -> int:
        if self == other:
            return 0
        else:
            return s_types.MAX_TYPE_DISTANCE

    def _test_polymorphic(
        self,
        schema: s_schema.Schema,
        other: s_types.Type
    ) -> bool:
        return self == other

    def _to_nonpolymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: s_types.Type
    ) -> Tuple[s_schema.Schema, s_types.Type]:
        return schema, concrete_type

    def _resolve_polymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: s_types.Type
    ) -> Optional[s_types.Type]:
        if self.is_any(schema):
            if isinstance(concrete_type, s_scalars.ScalarType):
                return concrete_type.get_topmost_concrete_base(schema)
            return concrete_type
        elif self.is_anytuple(schema):
            if (not concrete_type.is_tuple(schema) or
                    concrete_type.is_polymorphic(schema)):
                return None
            else:
                return concrete_type
        else:
            raise ValueError(
                f'unexpected pseudo type: {self.get_name(schema)}')


class PseudoTypeShell(s_types.TypeShell):

    def __init__(self, *, name: str) -> None:
        super().__init__(name=name, schemaclass=PseudoType)

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return True

    def resolve(self, schema: s_schema.Schema) -> PseudoType:
        return PseudoType.get(schema, self.name)


class PseudoTypeCommandContext(sd.ObjectCommandContext[PseudoType]):
    pass


class PseudoTypeCommand(
    s_types.TypeCommand[PseudoType],
    schema_metaclass=PseudoType,
    context_class=PseudoTypeCommandContext,
):
    pass


class CreatePseudoType(PseudoTypeCommand, sd.CreateObject[PseudoType]):

    astnode = qlast.CreatePseudoType

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        if not context.stdmode and not context.testmode:
            raise errors.UnsupportedFeatureError(
                'user-defined pseudotypes are not supported',
                context=astnode.context
            )

        return super()._cmd_tree_from_ast(schema, astnode, context)
