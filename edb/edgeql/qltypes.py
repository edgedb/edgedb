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

from edb.common import enum as s_enum


class ParameterKind(s_enum.StrEnum):
    VARIADIC = 'VARIADIC'
    NAMED_ONLY = 'NAMED ONLY'
    POSITIONAL = 'POSITIONAL'

    def to_edgeql(self):
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

    def to_edgeql(self):
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


class Cardinality(s_enum.StrEnum):
    ONE = 'ONE'
    MANY = 'MANY'

    def as_ptr_qual(self):
        if self is Cardinality.ONE:
            return 'single'
        else:
            return 'multi'


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
    CAST = 'CAST'
    CONSTRAINT = 'CONSTRAINT'
    FUNCTION = 'FUNCTION'
    LINK = 'LINK'
    MODULE = 'MODULE'
    OPERATOR = 'OPERATOR'
    PROPERTY = 'PROPERTY'
    SCALAR_TYPE = 'SCALAR TYPE'
    TYPE = 'TYPE'


class LinkTargetDeleteAction(s_enum.StrEnum):
    RESTRICT = 'RESTRICT'
    DELETE_SOURCE = 'DELETE SOURCE'
    ALLOW = 'ALLOW'
    DEFERRED_RESTRICT = 'DEFERRED RESTRICT'
