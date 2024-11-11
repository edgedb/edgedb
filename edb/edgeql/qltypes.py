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
from typing import Tuple, TypeVar, TYPE_CHECKING

import enum

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


class SchemaCardinality(s_enum.OrderedEnumMixin, s_enum.StrEnum):
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

    def can_be_zero(self) -> bool:
        return self not in {Cardinality.ONE, Cardinality.AT_LEAST_ONE}

    def to_schema_value(self) -> Tuple[bool, SchemaCardinality]:
        return _CARD_TO_TUPLE[self]

    @classmethod
    def from_schema_value(
        cls, required: bool, card: SchemaCardinality
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


class Volatility(s_enum.OrderedEnumMixin, s_enum.StrEnum):
    # Make sure that the values appear from least volatile to most volatile.
    Immutable = 'Immutable'
    Stable = 'Stable'
    Volatile = 'Volatile'
    Modifying = 'Modifying'

    def is_volatile(self) -> bool:
        return self in (Volatility.Volatile, Volatility.Modifying)

    @classmethod
    def _missing_(cls, name):
        # We want both `volatility := 'immutable'` in SDL and
        # `SET volatility := 'IMMUTABLE`` in DDL to work.
        return cls(name.title())


class Multiplicity(s_enum.OrderedEnumMixin, s_enum.StrEnum):
    # Make sure that the values appear in ascending order.
    EMPTY = 'EMPTY'
    UNIQUE = 'UNIQUE'
    DUPLICATE = 'DUPLICATE'
    UNKNOWN = 'UNKNOWN'

    def is_empty(self) -> bool:
        return self is Multiplicity.EMPTY

    def is_unique(self) -> bool:
        return self is Multiplicity.UNIQUE

    def is_duplicate(self) -> bool:
        return self is Multiplicity.DUPLICATE


class IndexDeferrability(s_enum.OrderedEnumMixin, s_enum.StrEnum):
    Prohibited = 'Prohibited'
    Permitted = 'Permitted'
    Required = 'Required'

    def is_deferrable(self) -> bool:
        return (
            self is IndexDeferrability.Required
            or self is IndexDeferrability.Permitted
        )


class AccessPolicyAction(s_enum.StrEnum):
    Allow = 'Allow'
    Deny = 'Deny'


class AccessKind(s_enum.StrEnum):
    Select = 'Select'
    UpdateRead = 'UpdateRead'
    UpdateWrite = 'UpdateWrite'
    Delete = 'Delete'
    Insert = 'Insert'

    def is_data_check(self) -> bool:
        return self is AccessKind.UpdateWrite or self is AccessKind.Insert


class TriggerTiming(s_enum.StrEnum):
    After = 'After'
    AfterCommitOf = 'After Commit Of'


class TriggerKind(s_enum.StrEnum):
    Update = 'Update'
    Delete = 'Delete'
    Insert = 'Insert'


class TriggerScope(s_enum.StrEnum):
    Each = 'Each'
    All = 'All'


class RewriteKind(s_enum.StrEnum):
    Update = 'Update'
    Insert = 'Insert'


class DescribeLanguage(s_enum.StrEnum):
    DDL = 'DDL'
    SDL = 'SDL'
    TEXT = 'TEXT'
    JSON = 'JSON'


class SchemaObjectClass(s_enum.StrEnum):

    ACCESS_POLICY = 'ACCESS_POLICY'
    ALIAS = 'ALIAS'
    ANNOTATION = 'ANNOTATION'
    ARRAY_TYPE = 'ARRAY TYPE'
    BRANCH = 'BRANCH'
    CAST = 'CAST'
    CONSTRAINT = 'CONSTRAINT'
    DATABASE = 'DATABASE'
    EXTENSION = 'EXTENSION'
    EXTENSION_PACKAGE = 'EXTENSION PACKAGE'
    EXTENSION_PACKAGE_MIGRATION = 'EXTENSION PACKAGE MIGRATION'
    FUTURE = 'FUTURE'
    FUNCTION = 'FUNCTION'
    GLOBAL = 'GLOBAL'
    INDEX = 'INDEX'
    INDEX_MATCH = 'INDEX MATCH'
    LINK = 'LINK'
    MIGRATION = 'MIGRATION'
    MODULE = 'MODULE'
    MULTIRANGE_TYPE = 'MULTIRANGE_TYPE'
    OPERATOR = 'OPERATOR'
    PARAMETER = 'PARAMETER'
    PROPERTY = 'PROPERTY'
    PSEUDO_TYPE = 'PSEUDO TYPE'
    RANGE_TYPE = 'RANGE TYPE'
    REWRITE = 'REWRITE'
    ROLE = 'ROLE'
    SCALAR_TYPE = 'SCALAR TYPE'
    TRIGGER = 'TRIGGER'
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


class LinkSourceDeleteAction(s_enum.StrEnum):
    DeleteTarget = 'DeleteTarget'
    Allow = 'Allow'
    DeleteTargetIfOrphan = 'DeleteTargetIfOrphan'

    def to_edgeql(self) -> str:
        if self is LinkSourceDeleteAction.DeleteTarget:
            return 'DELETE TARGET'
        elif self is LinkSourceDeleteAction.Allow:
            return 'ALLOW'
        elif self is LinkSourceDeleteAction.DeleteTargetIfOrphan:
            return 'DELETE TARGET IF ORPHAN'
        else:
            raise ValueError(f'unsupported enum value {self!r}')


class ConfigScope(s_enum.StrEnum):

    INSTANCE = 'INSTANCE'
    DATABASE = 'DATABASE'
    SESSION = 'SESSION'
    GLOBAL = 'GLOBAL'

    def to_edgeql(self) -> str:
        if self is ConfigScope.DATABASE:
            return 'CURRENT BRANCH'
        else:
            return str(self)


class TypeTag(enum.IntEnum):
    SCALAR = 0
    TUPLE = 1
    ARRAY = 2
