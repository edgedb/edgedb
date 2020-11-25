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
from typing import *

from edb.common import enum as s_enum


if TYPE_CHECKING:
    T = TypeVar("T", covariant=True)


class ParameterKind(s_enum.StrEnum):
    VariadicParam = 'VariadicParam'
    NamedOnlyParam = 'NamedOnlyParam'
    PositionalParam = 'PositionalParam'

    def to_edgeql(self) -> str:
        if self is ParameterKind.VariadicParam:
            return 'VARIADIC'
        elif self is ParameterKind.NamedOnlyParam:
            return 'NAMED ONLY'
        else:
            return ''


class TypeModifier(s_enum.StrEnum):
    SetOfType = 'SetOfType'
    OptionalType = 'OptionalType'
    SingletonType = 'SingletonType'

    def to_edgeql(self) -> str:
        if self is TypeModifier.SetOfType:
            return 'SET OF'
        elif self is TypeModifier.OptionalType:
            return 'OPTIONAL'
        else:
            return ''


class OperatorKind(s_enum.StrEnum):
    Infix = 'Infix'
    Postfix = 'Postfix'
    Prefix = 'Prefix'
    Ternary = 'Ternary'


class TransactionIsolationLevel(s_enum.StrEnum):
    REPEATABLE_READ = 'REPEATABLE READ'
    SERIALIZABLE = 'SERIALIZABLE'


class TransactionAccessMode(s_enum.StrEnum):
    READ_WRITE = 'READ WRITE'
    READ_ONLY = 'READ ONLY'


class TransactionDeferMode(s_enum.StrEnum):
    DEFERRABLE = 'DEFERRABLE'
    NOT_DEFERRABLE = 'NOT DEFERRABLE'


class SchemaCardinality(s_enum.StrEnum):
    '''This enum is used to store cardinality in the schema.'''
    One = 'One'
    Many = 'Many'
    Unknown = 'Unknown'

    def is_multi(self) -> bool:
        if self is SchemaCardinality.One:
            return False
        elif self is SchemaCardinality.Many:
            return True
        else:
            raise ValueError('cardinality is unknown')

    def is_single(self) -> bool:
        return not self.is_multi()

    def is_known(self) -> bool:
        return self is not SchemaCardinality.Unknown

    def as_ptr_qual(self) -> str:
        if self is SchemaCardinality.One:
            return 'single'
        elif self is SchemaCardinality.Many:
            return 'multi'
        else:
            raise ValueError('cardinality is unknown')

    def to_edgeql(self) -> str:
        return self.as_ptr_qual().upper()


class Cardinality(s_enum.StrEnum):
    '''This enum is used in cardinality inference internally.'''
    # [0, 1]
    AT_MOST_ONE = 'AT_MOST_ONE'
    # [1, 1]
    ONE = 'ONE'
    # [0, inf)
    MANY = 'MANY'
    # [1, inf)
    AT_LEAST_ONE = 'AT_LEAST_ONE'
    # Sentinel
    UNKNOWN = 'UNKNOWN'

    def is_single(self) -> bool:
        return self in {Cardinality.AT_MOST_ONE, Cardinality.ONE}

    def is_multi(self) -> bool:
        return not self.is_single()

    def to_schema_value(self) -> Tuple[bool, SchemaCardinality]:
        return _CARD_TO_TUPLE[self]

    @classmethod
    def from_schema_value(
        cls,
        required: bool,
        card: SchemaCardinality
    ) -> Cardinality:
        return _TUPLE_TO_CARD[(required, card)]


_CARD_TO_TUPLE = {
    Cardinality.AT_MOST_ONE: (False, SchemaCardinality.One),
    Cardinality.ONE: (True, SchemaCardinality.One),
    Cardinality.MANY: (False, SchemaCardinality.Many),
    Cardinality.AT_LEAST_ONE: (True, SchemaCardinality.Many),
}
_TUPLE_TO_CARD = {
    (False, SchemaCardinality.One): Cardinality.AT_MOST_ONE,
    (True, SchemaCardinality.One): Cardinality.ONE,
    (False, SchemaCardinality.Many): Cardinality.MANY,
    (True, SchemaCardinality.Many): Cardinality.AT_LEAST_ONE,
}


class Volatility(s_enum.StrEnum):
    Immutable = 'Immutable'
    Stable = 'Stable'
    Volatile = 'Volatile'

    @classmethod
    def _missing_(cls, name):
        # We want both `volatility := 'immutable'` in SDL and
        # `SET volatility := 'IMMUTABLE`` in DDL to work.
        return cls(name.title())


class Multiplicity(s_enum.StrEnum):
    ZERO = 'ZERO'  # This is valid for empty sets
    ONE = 'ONE'
    MANY = 'MANY'


class DescribeLanguage(s_enum.StrEnum):
    DDL = 'DDL'
    SDL = 'SDL'
    TEXT = 'TEXT'
    JSON = 'JSON'


class SchemaObjectClass(s_enum.StrEnum):

    ALIAS = 'ALIAS'
    ANNOTATION = 'ANNOTATION'
    ARRAY_TYPE = 'ARRAY TYPE'
    CAST = 'CAST'
    CONSTRAINT = 'CONSTRAINT'
    DATABASE = 'DATABASE'
    FUNCTION = 'FUNCTION'
    INDEX = 'INDEX'
    LINK = 'LINK'
    MIGRATION = 'MIGRATION'
    MODULE = 'MODULE'
    OPERATOR = 'OPERATOR'
    PARAMETER = 'PARAMETER'
    PROPERTY = 'PROPERTY'
    PSEUDO_TYPE = 'PSEUDO TYPE'
    ROLE = 'ROLE'
    SCALAR_TYPE = 'SCALAR TYPE'
    TUPLE_TYPE = 'TUPLE TYPE'
    TYPE = 'TYPE'


class LinkTargetDeleteAction(s_enum.StrEnum):
    Restrict = 'Restrict'
    DeleteSource = 'DeleteSource'
    Allow = 'Allow'
    DeferredRestrict = 'DeferredRestrict'

    def to_edgeql(self) -> str:
        if self is LinkTargetDeleteAction.DeleteSource:
            return 'DELETE SOURCE'
        elif self is LinkTargetDeleteAction.DeferredRestrict:
            return 'DEFERRED RESTRICT'
        elif self is LinkTargetDeleteAction.Restrict:
            return 'RESTRICT'
        elif self is LinkTargetDeleteAction.Allow:
            return 'ALLOW'
        else:
            raise ValueError(f'unsupported enum value {self!r}')


class ConfigScope(s_enum.StrEnum):

    SYSTEM = 'SYSTEM'
    DATABASE = 'DATABASE'
    SESSION = 'SESSION'

    def to_edgeql(self) -> str:
        if self is ConfigScope.DATABASE:
            return 'CURRENT DATABASE'
        else:
            return str(self)
