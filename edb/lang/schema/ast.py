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

from edb.lang.common import enum as s_enum
from edb.lang.common import ast, parsing

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import functypes as qlft


class Base(ast.AST):
    __ast_hidden__ = {'context'}
    context: parsing.ParserContext

    def _extra_repr(self):
        return ''

    def __repr__(self):
        ar = self._extra_repr()
        return f'<{self.__class__.__name__} at {id(self):#x}{ar}>'


class Spec(Base):
    inherited: bool = False


class Attribute(Spec):
    name: qlast.ObjectRef
    value: qlast.Base


class Policy(Spec):
    __fields = ['event', 'action']  # TODO: type this


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
    policies: typing.List[Policy]

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


# # XXX: to be killed
# class Property(Property):
#     pass


class Declaration(Base):
    name: str
    extends: typing.List[qlast.TypeName]
    attributes: typing.List[Attribute]


class ActionDeclaration(Declaration):
    pass


class ScalarTypeDeclaration(Declaration):
    abstract: bool = False
    final: bool = False
    constraints: typing.List[Constraint]


class AttributeDeclaration(Declaration):
    type: qlast.TypeExpr


class ObjectTypeDeclaration(Declaration):
    abstract: bool = False
    final: bool = False
    links: typing.List[Link]
    properties: typing.List[Property]
    indexes: typing.List[Index]
    constraints: typing.List[Constraint]


class ConstraintDeclaration(Declaration):
    abstract: bool = False
    args: typing.List[qlast.Base]
    subject: typing.Optional[qlast.Expr]


class EventDeclaration(Declaration):
    pass


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
    args: list
    returning: qlast.TypeName
    code: FunctionCode
    returning_typemod: qlft.TypeModifier


class BasePointerDeclaration(Declaration):
    abstract: bool = False
    indexes: typing.List[Index]
    constraints: typing.List[Constraint]
    policies: typing.List[Policy]


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
