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


import typing

from edb.common import ast, parsing


class Base(ast.AST):
    ns = 'graphql'

    __ast_hidden__ = {'context'}
    context: parsing.ParserContext

    def _extra_repr(self):
        return ''

    def __repr__(self):
        ar = self._extra_repr()
        return f'<{self.__class__.ns}.{self.__class__.__name__} at ' + \
               f'{id(self):#x}{ar}>'


class Value(Base):
    pass


class Name(Base):
    value: str


class StringValue(Value):
    value: object


class IntValue(Value):
    value: object


class FloatValue(Value):
    value: object


class BooleanValue(Value):
    value: bool


class EnumValue(Value):
    value: object


class NullValue(Value):
    value: None


class ListValue(Value):
    values: object


class ObjectField(Base):
    name: Name
    value: object


class ObjectValue(Value):
    fields: typing.List[ObjectField]


class Variable(Base):
    name: Name


class Document(Base):
    definitions: list


class Definition(Base):
    name: typing.Optional[Name]
    selection_set: object


class OperationDefinition(Definition):
    operation: str
    variable_definitions: list
    directives: list


class Type(Base):
    pass


class NamedType(Type):
    name: Name


class NonNullType(Type):
    type: Type


class ListType(Type):
    type: Type


class VariableDefinition(Base):
    variable: Variable
    type: Type
    default_value: object


class Selection(Base):
    pass


class SelectionSet(Base):
    selections: typing.List[Selection]


class Argument(Base):
    name: Name
    value: object


class Field(Selection):
    alias: typing.Optional[Name]
    name: Name
    arguments: typing.List[Argument]
    directives: list
    selection_set: SelectionSet


class FragmentSpread(Selection):
    name: Name
    directives: list


class FragmentDefinition(Definition):
    type_condition: typing.Optional[NamedType]
    directives: list


class InlineFragment(Selection):
    type_condition: typing.Optional[NamedType]
    directives: list
    selection_set: object


class Directive(Base):
    name: Name
    arguments: typing.List[Argument]
