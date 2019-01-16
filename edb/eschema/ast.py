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

from edb.common import enum as s_enum
from edb.common import ast, parsing

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes as qlft


class Base(ast.AST):
    __ast_hidden__ = {'context'}
    context: parsing.ParserContext

    def _extra_repr(self):
        return ''

    def __repr__(self):
        ar = self._extra_repr()
        return f'<{self.__class__.__name__} at {id(self):#x}{ar}>'


class Field(Base):
    name: qlast.ObjectRef
    value: qlast.Base


class Spec(Base):
    inherited: bool = False
    fields: typing.List[Field]


class Attribute(Spec):
    name: qlast.ObjectRef
    value: qlast.Base


class Constraint(Spec):
    args: typing.List[qlast.FuncArg]
    attributes: typing.List[Attribute]
    delegated: bool = False
    name: qlast.ObjectRef
    subject: typing.Optional[qlast.Expr]


class Pointer(Spec):
    name: qlast.ObjectRef

    # Computable links don't have a target
    target: typing.Optional[typing.List[qlast.TypeName]]

    attributes: typing.List[Attribute]
    constraints: typing.List[Constraint]

    required: bool = False
    cardinality: qlast.Cardinality

    # Expression of a computable link
    expr: qlast.Base = None


class Index(Spec):
    name: qlast.ObjectRef
    expression: qlast.Base


class OnTargetDelete(Spec):
    cascade: qlast.LinkTargetDeleteAction


class Property(Pointer):
    pass


class Link(Pointer):
    properties: typing.List[Property]
    on_target_delete: OnTargetDelete


class Declaration(Base):
    name: str
    extends: typing.List[qlast.TypeName]
    attributes: typing.List[Attribute]
    fields: typing.List[Field]


class ScalarTypeDeclaration(Declaration):
    abstract: bool = False
    final: bool = False
    constraints: typing.List[Constraint]


class AttributeDeclaration(Declaration):
    inheritable: bool = False


class ObjectTypeDeclaration(Declaration):
    abstract: bool = False
    final: bool = False
    links: typing.List[Link]
    properties: typing.List[Property]
    indexes: typing.List[Index]
    constraints: typing.List[Constraint]


class ConstraintDeclaration(Declaration):
    abstract: bool = False
    params: typing.List[qlast.FuncParam]
    subject: typing.Optional[qlast.Expr]


class ViewDeclaration(Declaration):
    pass


class Language(s_enum.StrEnum):
    SQL = 'SQL'
    EdgeQL = 'EDGEQL'


class FunctionCode(Base):
    language: Language
    code: qlast.Base
    from_name: str


class FunctionDeclaration(Declaration):
    params: list
    returning: qlast.TypeName
    function_code: FunctionCode
    returning_typemod: qlft.TypeModifier


class BasePointerDeclaration(Declaration):
    abstract: bool = False
    indexes: typing.List[Index]
    constraints: typing.List[Constraint]


class PropertyDeclaration(BasePointerDeclaration):
    pass


class LinkDeclaration(BasePointerDeclaration):
    properties: typing.List[Property]


class Import(Base):
    modules: list


class ImportModule(Base):
    module: str
    alias: str = None


class Schema(Base):
    # TODO: Remove union type
    declarations: typing.List[typing.Union[Declaration, Import]]
