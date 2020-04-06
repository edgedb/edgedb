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


class ParameterKind(s_enum.StrEnum):
    VARIADIC = 'VARIADIC'
    NAMED_ONLY = 'NAMED ONLY'
    POSITIONAL = 'POSITIONAL'

    def to_edgeql(self) -> str:
        if self is ParameterKind.VARIADIC:
            return 'VARIADIC'
        elif self is ParameterKind.NAMED_ONLY:
            return 'NAMED ONLY'
        else:
            return ''


class TypeModifier(s_enum.StrEnum):
    SET_OF = 'SET OF'
    OPTIONAL = 'OPTIONAL'
    SINGLETON = 'SINGLETON'

    def to_edgeql(self) -> str:
        if self is TypeModifier.SET_OF:
            return 'SET OF'
        elif self is TypeModifier.OPTIONAL:
            return 'OPTIONAL'
        else:
            return ''


class OperatorKind(s_enum.StrEnum):
    INFIX = 'INFIX'
    POSTFIX = 'POSTFIX'
    PREFIX = 'PREFIX'
    TERNARY = 'TERNARY'


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
    ONE = 'ONE'
    MANY = 'MANY'

    def as_ptr_qual(self) -> str:
        if self is SchemaCardinality.ONE:
            return 'single'
        else:
            return 'multi'


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
    Cardinality.AT_MOST_ONE: (False, SchemaCardinality.ONE),
    Cardinality.ONE: (True, SchemaCardinality.ONE),
    Cardinality.MANY: (False, SchemaCardinality.MANY),
    Cardinality.AT_LEAST_ONE: (True, SchemaCardinality.MANY),
}
_TUPLE_TO_CARD = {
    (False, SchemaCardinality.ONE): Cardinality.AT_MOST_ONE,
    (True, SchemaCardinality.ONE): Cardinality.ONE,
    (False, SchemaCardinality.MANY): Cardinality.MANY,
    (True, SchemaCardinality.MANY): Cardinality.AT_LEAST_ONE,
}


class Volatility(s_enum.StrEnum):
    IMMUTABLE = 'IMMUTABLE'
    STABLE = 'STABLE'
    VOLATILE = 'VOLATILE'


class DescribeLanguage(s_enum.StrEnum):
    DDL = 'DDL'
    SDL = 'SDL'
    TEXT = 'TEXT'


class SchemaObjectClass(s_enum.StrEnum):

    ANNOTATION = 'ANNOTATION'
    ARRAY_TYPE = 'ARRAY TYPE'
    CAST = 'CAST'
    CONSTRAINT = 'CONSTRAINT'
    DATABASE = 'DATABASE'
    FUNCTION = 'FUNCTION'
    LINK = 'LINK'
    MIGRATION = 'MIGRATION'
    MODULE = 'MODULE'
    OPERATOR = 'OPERATOR'
    PROPERTY = 'PROPERTY'
    ROLE = 'ROLE'
    SCALAR_TYPE = 'SCALAR TYPE'
    TUPLE_TYPE = 'TUPLE TYPE'
    TYPE = 'TYPE'


class LinkTargetDeleteAction(s_enum.StrEnum):
    RESTRICT = 'RESTRICT'
    DELETE_SOURCE = 'DELETE SOURCE'
    ALLOW = 'ALLOW'
    DEFERRED_RESTRICT = 'DEFERRED RESTRICT'
