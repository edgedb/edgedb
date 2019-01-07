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
from edb.lang.edgeql import quote as eql_quote


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


class BaseLiteral(Base):
    value: object

    @classmethod
    def from_python(cls, val: object) -> 'BaseLiteral':
        if isinstance(val, str):
            return StringLiteral.from_python(val)
        elif isinstance(val, bool):
            return BooleanLiteral(value='true' if val else 'false')
        elif isinstance(val, int):
            return IntegerLiteral(value=str(val))
        elif isinstance(val, float):
            return FloatLiteral(value=str(val))
        elif isinstance(val, list):
            return ListLiteral(value=[BaseLiteral.from_python(v) for v in val])
        elif isinstance(val, dict):
            return InputObjectLiteral(
                value=[ObjectField(name=n, value=BaseLiteral.from_python(v))
                       for n, v in val.items()])
        else:
            raise ValueError(f'unexpected constant type: {type(val)!r}')


class ScalarLiteral(BaseLiteral):
    value: str


class StringLiteral(ScalarLiteral):
    @classmethod
    def from_python(cls, s: str):
        s = s.replace('\\', '\\\\')
        value = eql_quote.quote_literal(s)
        return cls(value=value[1:-1])


class IntegerLiteral(ScalarLiteral):
    pass


class FloatLiteral(ScalarLiteral):
    pass


class BooleanLiteral(ScalarLiteral):
    pass


class EnumLiteral(ScalarLiteral):
    pass


class NullLiteral(ScalarLiteral):
    value: None


class ListLiteral(BaseLiteral):
    pass


class InputObjectLiteral(BaseLiteral):
    pass


class Variable(Base):
    value: object


class Document(Base):
    definitions: list


class Definition(Base):
    name: str
    selection_set: object


class OperationDefinition(Definition):
    type: str
    variables: list
    directives: list


class FragmentDefinition(Definition):
    on: object
    directives: list


class VariableDefinition(Base):
    name: object
    type: object
    value: object


class VariableType(Base):
    name: object
    nullable: bool = True
    list: bool = False


class Selection(Base):
    pass


class SelectionSet(Base):
    selections: typing.List[Selection]


class Argument(Base):
    name: str
    value: object


class Field(Selection):
    alias: str
    name: object
    arguments: typing.List[Argument]
    directives: list
    selection_set: SelectionSet


class SchemaField(Field):
    pass


class TypeField(Field):
    pass


class TypenameField(Field):
    pass


class FragmentSpread(Selection):
    name: object
    directives: list


class InlineFragment(Selection):
    on: object
    directives: list
    selection_set: object


class Directive(Base):
    name: object
    arguments: typing.List[Argument]


class ObjectField(Base):
    name: object
    value: object
