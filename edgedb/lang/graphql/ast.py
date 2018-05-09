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


import re
import typing

from edgedb.lang.common import ast, parsing


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


class Literal(Base):
    value: object

    def topython(self):
        return self.value


class StringLiteral(Literal):
    def tosource(self):
        value = self.value
        # generic substitutions for '\b' and '\f'
        value = value.replace('\b', '\\b').replace('\f', '\\f')
        value = repr(value)
        # escape \b \f \u1234 and \/ correctly
        value = re.sub(r'\\\\([fb])', r'\\\1', value)
        value = re.sub(r'\\\\(u[0-9a-fA-F]{4})', r'\\\1', value)
        # no need to escape '/' as '\/' since it's legal unescaped

        # need to change quotation style
        if value[0] == "'":
            value = value[1:-1].replace(R"\'", "'").replace('"', R'\"')
            value = '"' + value + '"'

        return value


class IntegerLiteral(Literal):
    pass


class FloatLiteral(Literal):
    pass


class BooleanLiteral(Literal):
    pass


class EnumLiteral(Literal):
    pass


class NullLiteral(Literal):
    value: None


class ListLiteral(Literal):
    def topython(self):
        return [val.topython() for val in self.value]


class ObjectLiteral(Literal):
    def topython(self):
        return {field.name: field.value.topython() for field in self.value}


class Variable(Base):
    value: object


class Document(Base):
    definitions: list  # noqa (pyflakes bug)


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
