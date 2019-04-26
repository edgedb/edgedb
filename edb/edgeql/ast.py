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


import decimal
import typing

from edb.common import enum as s_enum
from edb.common import ast, parsing

from . import qltypes
from . import quote


class SortOrder(s_enum.StrEnum):
    Asc = 'ASC'
    Desc = 'DESC'


SortAsc = SortOrder.Asc
SortDesc = SortOrder.Desc
SortDefault = SortAsc


class NonesOrder(s_enum.StrEnum):
    First = 'first'
    Last = 'last'


NonesFirst = NonesOrder.First
NonesLast = NonesOrder.Last


class LinkTargetDeleteAction(s_enum.StrEnum):
    RESTRICT = 'RESTRICT'
    DELETE_SOURCE = 'DELETE SOURCE'
    ALLOW = 'ALLOW'
    DEFERRED_RESTRICT = 'DEFERRED RESTRICT'


class SchemaItemClass(s_enum.StrEnum):

    OPERATOR = 'OPERATOR'


class Base(ast.AST):
    __ast_hidden__ = {'context'}
    context: parsing.ParserContext


class Expr(Base):
    """Abstract parent for all query expressions."""
    pass


class SubExpr(Base):
    """A subexpression (used for anchors)."""

    expr: typing.Union[Expr, object]
    anchors: typing.Dict[typing.Union[str, ast.MetaAST],
                         typing.Union[Expr, object]]


class Clause(Base):
    """Abstract parent for all query clauses."""
    pass


class SortExpr(Clause):
    path: Expr
    direction: SortOrder
    nones_order: str


class BaseAlias(Clause):
    alias: str


class AliasedExpr(BaseAlias):
    expr: Expr


class ModuleAliasDecl(BaseAlias):
    module: str


class BaseSessionCommand(Base):
    pass


class BaseSessionSet(BaseSessionCommand):
    pass


class BaseSessionConfigSet(BaseSessionSet):
    system: bool = False


class SessionSetAliasDecl(ModuleAliasDecl, BaseSessionSet):
    pass


class BaseSessionReset(BaseSessionCommand):
    pass


class SessionResetAliasDecl(BaseAlias, BaseSessionReset):
    pass


class SessionResetModule(BaseSessionReset):
    pass


class SessionResetAllAliases(BaseSessionReset):
    pass


class BaseObjectRef(Expr):
    pass


class ObjectRef(BaseObjectRef):
    name: str
    module: str
    itemclass: SchemaItemClass


class PseudoObjectRef(BaseObjectRef):
    pass


class AnyType(PseudoObjectRef):
    pass


class AnyTuple(PseudoObjectRef):
    pass


class Source(Expr):
    pass


class Subject(Expr):  # __subject__
    pass


class DetachedExpr(Expr):  # DETACHED Expr
    expr: Expr


class Index(Base):
    index: Expr


class Slice(Base):
    start: Expr
    stop: Expr


class Indirection(Expr):
    arg: Expr
    indirection: typing.List[typing.Union[Index, Slice]]


class BinOp(Expr):
    left: Expr
    op: str
    right: Expr


class WindowSpec(Clause):
    orderby: typing.List[SortExpr]
    partition: typing.List[Expr]


class FunctionCall(Expr):
    func: typing.Union[tuple, str]
    args: typing.List[Expr]
    kwargs: typing.Dict[str, Expr]
    window: WindowSpec


class BaseConstant(Expr):
    value: str

    @classmethod
    def from_python(cls, val: object) -> 'BaseConstant':
        if isinstance(val, str):
            return StringConstant.from_python(val)
        elif isinstance(val, bool):
            return BooleanConstant(value='true' if val else 'false')
        elif isinstance(val, int):
            return IntegerConstant(value=str(val))
        elif isinstance(val, decimal.Decimal):
            if val.to_integral_value() == val:
                return IntegerConstant(value=str(val.to_integral_value()))
            else:
                return FloatConstant(value=str(val))
        elif isinstance(val, float):
            return FloatConstant(value=str(val))
        elif isinstance(val, bytes):
            return BytesConstant.from_python(value=val)
        else:
            raise ValueError(f'unexpected constant type: {type(val)!r}')


class StringConstant(BaseConstant):
    quote: str

    @classmethod
    def from_python(cls, s: str):
        s = s.replace('\\', '\\\\')
        value = quote.quote_literal(s)
        return cls(value=value[1:-1], quote="'")


class RawStringConstant(BaseConstant):
    quote: str

    @classmethod
    def from_python(cls, s: str):
        value = quote.quote_literal(s)
        return cls(value=value[1:-1], quote="'")


class BaseRealConstant(BaseConstant):
    is_negative: bool = False


class IntegerConstant(BaseRealConstant):
    pass


class DecimalConstant(BaseRealConstant):
    pass


class FloatConstant(BaseRealConstant):
    pass


class BooleanConstant(BaseConstant):
    pass


class BytesConstant(BaseConstant):
    quote: str

    @classmethod
    def from_python(cls, s: bytes):
        rs = repr(s)
        return cls(value=rs[2:-1], quote=rs[-1])


class Parameter(Expr):
    name: str


class UnaryOp(Expr):
    op: str
    operand: Expr


class TypeExpr(Base):
    pass


class TypeOf(TypeExpr):
    expr: Expr


class TypeExprLiteral(TypeExpr):
    # Literal type exprs are used in enum declarations.
    val: StringConstant


class TypeName(TypeExpr):
    name: str  # name is used for types in named tuples
    maintype: BaseObjectRef
    subtypes: typing.Optional[typing.List[TypeExpr]]
    dimensions: typing.Optional[typing.List[int]]


class TypeOp(TypeExpr):
    name: str
    left: TypeExpr
    op: str
    right: TypeExpr


class FuncParam(Base):
    name: str
    type: TypeExpr
    typemod: qltypes.TypeModifier = qltypes.TypeModifier.SINGLETON
    kind: qltypes.ParameterKind
    default: Expr


class IsOp(Expr):
    left: Expr
    op: str
    right: TypeExpr


class TypeIndirection(Base):
    type: TypeExpr


class Ptr(Base):
    ptr: ObjectRef
    direction: str
    type: str


class Path(Expr):
    steps: typing.List[typing.Union[Expr, Ptr, TypeIndirection]]
    quantifier: Expr
    partial: bool = False


class TypeCast(Expr):
    expr: Expr
    type: TypeExpr


class Introspect(Expr):
    type: TypeExpr


class IfElse(Expr):
    condition: Expr
    if_expr: Expr
    else_expr: Expr


class Coalesce(Expr):
    args: typing.List[Expr]


class RequiredExpr(Expr):
    expr: Expr


class TupleElement(Base):
    name: ObjectRef
    val: Expr


class NamedTuple(Expr):
    elements: typing.List[TupleElement]


class Tuple(Expr):
    elements: typing.List[Expr]


class Array(Expr):
    elements: typing.List[Expr]


class Set(Expr):
    elements: typing.List[Expr]


# Expressions used only in statements
#

class ByExprBase(Base):
    '''Abstract parent of all grouping sets.'''
    pass


class ByExpr(ByExprBase):
    each: bool
    expr: Expr


class GroupBuiltin(ByExprBase):
    name: str
    elements: typing.List[ByExpr]


class GroupExpr(Expr):
    subject: Expr
    subject_alias: str
    by: typing.List[ByExprBase]


# Statements
#

class Statement(Expr):
    aliases: typing.List[typing.Union[AliasedExpr, ModuleAliasDecl]]


class SubjStatement(Statement):
    subject: Expr
    subject_alias: str


class ReturningStatement(Statement):
    result: Expr
    result_alias: str


class SelectClauseStatement(Statement):
    where: Expr
    orderby: typing.List[SortExpr]
    offset: Expr
    limit: Expr
    implicit: bool = False


class SelectQuery(ReturningStatement, SelectClauseStatement):
    pass


class GroupQuery(SelectQuery, SubjStatement):
    using: typing.List[AliasedExpr]
    by: typing.List[Expr]
    into: str


class InsertQuery(SubjStatement):
    shape: typing.List[Expr]


class UpdateQuery(SubjStatement):
    shape: typing.List[Expr]
    where: Expr


class DeleteQuery(SubjStatement, SelectClauseStatement):
    pass


class ForQuery(SelectQuery):
    iterator: Expr
    iterator_alias: str


class ShapeElement(Expr):
    expr: Expr
    elements: typing.List[Expr]
    where: Expr
    orderby: typing.List[SortExpr]
    offset: Expr
    limit: Expr
    compexpr: Expr
    cardinality: qltypes.Cardinality
    required: bool = False


class Shape(Expr):
    expr: Expr
    elements: typing.List[ShapeElement]


# Transactions
#

class Transaction(Base):
    '''Abstract parent for all transaction operations.'''
    pass


class StartTransaction(Transaction):
    isolation: typing.Optional[qltypes.TransactionIsolationLevel] = None
    access: typing.Optional[qltypes.TransactionAccessMode] = None
    deferrable: typing.Optional[qltypes.TransactionDeferMode] = None


class CommitTransaction(Transaction):
    pass


class RollbackTransaction(Transaction):
    pass


class DeclareSavepoint(Transaction):

    name: str


class RollbackToSavepoint(Transaction):

    name: str


class ReleaseSavepoint(Transaction):

    name: str


# DDL
#

class DDL(Base):
    '''Abstract parent for all DDL statements.'''
    pass


class CompositeDDL(Statement, DDL):
    pass


class Position(DDL):
    ref: str
    position: str


class AlterAddInherit(DDL):
    bases: typing.Union[typing.List[TypeName], typing.List[ObjectRef]]
    position: Position


class AlterDropInherit(DDL):
    bases: typing.List[TypeName]


class AlterTarget(DDL):
    target: TypeExpr


class OnTargetDelete(DDL):
    cascade: LinkTargetDeleteAction


class ObjectDDL(CompositeDDL):
    name: ObjectRef
    commands: typing.List[DDL]


class CreateObject(ObjectDDL):
    is_abstract: bool = False


class AlterObject(ObjectDDL):
    pass


class DropObject(ObjectDDL):
    pass


class CreateExtendingObject(CreateObject):
    bases: typing.List[TypeName]
    is_final: bool = False


class Rename(DDL):
    new_name: ObjectRef

    @property
    def name(self) -> ObjectRef:
        return self.new_name


class Delta:
    pass


class CreateDelta(CreateObject, Delta):
    parents: typing.List[ObjectRef]
    language: str
    target: object


class GetDelta(ObjectDDL, Delta):
    pass


class AlterDelta(AlterObject, Delta):
    pass


class DropDelta(DropObject, Delta):
    pass


class CommitDelta(ObjectDDL, Delta):
    pass


class Database:
    pass


class CreateDatabase(CreateObject, Database):
    pass


class AlterDatabase(AlterObject, Database):
    pass


class DropDatabase(DropObject, Database):
    pass


class CreateModule(CreateObject):
    pass


class AlterModule(AlterObject):
    pass


class DropModule(DropObject):
    pass


class CreateRole(CreateObject):
    bases: typing.List[TypeName]


class AlterRole(AlterObject):
    pass


class DropRole(DropObject):
    pass


class CreateAnnotation(CreateExtendingObject):
    type: typing.Optional[TypeExpr]
    inheritable: bool


class DropAnnotation(DropObject):
    pass


class CreateScalarType(CreateExtendingObject):
    pass


class AlterScalarType(AlterObject):
    pass


class DropScalarType(DropObject):
    pass


class CreateProperty(CreateExtendingObject):
    pass


class AlterProperty(AlterObject):
    pass


class DropProperty(DropObject):
    pass


class CreateConcretePointer(CreateObject):

    bases: typing.List[TypeName]
    is_required: bool = False
    target: typing.Union[Expr, TypeExpr]
    cardinality: qltypes.Cardinality


class CreateConcreteProperty(CreateConcretePointer):
    pass


class AlterConcreteProperty(AlterObject):
    pass


class DropConcreteProperty(AlterObject):
    pass


class SetSpecialField(DDL):
    name: str
    value: object


class CreateObjectType(CreateExtendingObject):
    pass


class AlterObjectType(AlterObject):
    pass


class DropObjectType(DropObject):
    pass


class CreateView(CreateObject):
    pass


class AlterView(AlterObject):
    pass


class DropView(DropObject):
    pass


class CreateLink(CreateExtendingObject):
    pass


class AlterLink(AlterObject):
    pass


class DropLink(DropObject):
    pass


class CreateConcreteLink(CreateExtendingObject, CreateConcretePointer):
    pass


class AlterConcreteLink(AlterObject):
    pass


class DropConcreteLink(DropObject):
    pass


class CallableObject(ObjectDDL):
    params: typing.List[FuncParam]


class CreateConstraint(CreateExtendingObject, CallableObject):
    subjectexpr: typing.Optional[Expr]


class AlterConstraint(AlterObject):
    pass


class DropConstraint(DropObject):
    pass


class CreateConcreteConstraint(CreateObject):
    args: typing.List[Expr]
    is_abstract: bool = False
    subjectexpr: typing.Optional[Expr]


class AlterConcreteConstraint(AlterObject):
    pass


class DropConcreteConstraint(DropObject):
    pass


class CreateIndex(CreateObject):
    expr: Expr


class DropIndex(DropObject):
    pass


class BaseSetField(ObjectDDL):
    value: Expr


class SetField(BaseSetField):
    pass


class CreateAnnotationValue(CreateObject):
    value: Expr


class DropAnnotationValue(DropObject):
    pass


class Language(s_enum.StrEnum):
    SQL = 'SQL'
    EdgeQL = 'EDGEQL'


class FunctionCode(Clause):
    language: Language
    code: str
    from_function: str
    from_expr: bool


class CreateFunction(CreateObject, CallableObject):
    returning: TypeExpr
    code: FunctionCode
    returning_typemod: qltypes.TypeModifier = qltypes.TypeModifier.SINGLETON


class AlterFunction(AlterObject):
    value: Base


class DropFunction(DropObject, CallableObject):
    pass


class OperatorCode(Clause):
    language: Language
    from_operator: typing.Optional[typing.Tuple[str, ...]]
    from_function: str
    from_expr: bool
    code: str


class OperatorCommand(CallableObject):
    kind: qltypes.OperatorKind


class CreateOperator(CreateObject, OperatorCommand):
    returning: TypeExpr
    returning_typemod: qltypes.TypeModifier = qltypes.TypeModifier.SINGLETON
    code: OperatorCode


class AlterOperator(AlterObject, OperatorCommand):
    pass


class DropOperator(DropObject, OperatorCommand):
    pass


class CastCode(Clause):
    language: Language
    from_function: str
    from_expr: bool
    from_cast: bool
    code: str


class CastCommand(ObjectDDL):
    from_type: TypeName
    to_type: TypeName


class CreateCast(CreateObject, CastCommand):
    code: CastCode
    allow_implicit: bool
    allow_assignment: bool


class AlterCast(AlterObject, CastCommand):
    pass


class DropCast(DropObject, CastCommand):
    pass


class _Optional(Expr):
    expr: Expr


#
# Config
#


class ConfigOp(Expr):

    name: ObjectRef
    system: bool
    backend_setting: str


class ConfigSet(ConfigOp):

    expr: Expr


class ConfigInsert(ConfigOp):

    shape: typing.List[ShapeElement]


class ConfigReset(ConfigOp):

    where: typing.Optional[Expr]


#
# SDL
#

class SDL(Base):
    '''Abstract parent for all SDL statements.'''
    pass


class Field(SDL):
    name: ObjectRef
    value: Base


class Spec(SDL):
    inherited: bool = False
    fields: typing.List[Field]


class Annotation(Spec):
    name: ObjectRef
    value: Base


class Constraint(Spec):
    annotations: typing.List[Annotation]
    delegated: bool = False
    name: ObjectRef
    args: typing.List[Expr]
    subject: typing.Optional[Expr]


class Pointer(Spec):
    name: str

    extends: typing.List[TypeName]

    # Computable links don't have a target
    target: typing.Optional[typing.List[TypeName]]

    annotations: typing.List[Annotation]
    constraints: typing.List[Constraint]

    required: bool = False
    cardinality: qltypes.Cardinality

    # Expression of a computable link
    expr: Base = None


class IndexDeclaration(Spec):
    name: ObjectRef
    expression: Base


class SDLOnTargetDelete(Spec):
    cascade: LinkTargetDeleteAction


class Property(Pointer):
    pass


class Link(Pointer):
    properties: typing.List[Property]
    on_target_delete: SDLOnTargetDelete


class Declaration(SDL):
    name: str
    extends: typing.List[TypeName]
    annotations: typing.List[Annotation]
    fields: typing.List[Field]


class ScalarTypeDeclaration(Declaration):
    abstract: bool = False
    final: bool = False
    constraints: typing.List[Constraint]


class AnnotationDeclaration(Declaration):
    abstract: bool = False
    inheritable: bool = False


class ObjectTypeDeclaration(Declaration):
    abstract: bool = False
    final: bool = False
    links: typing.List[Link]
    properties: typing.List[Property]
    indexes: typing.List[IndexDeclaration]
    constraints: typing.List[Constraint]


class ConstraintDeclaration(Declaration):
    abstract: bool = False
    params: typing.List[FuncParam]
    subject: typing.Optional[Expr]


class ViewDeclaration(Declaration):
    pass


class SDLFunctionCode(SDL):
    language: Language
    code: Base
    from_function: str
    from_expr: bool


class FunctionDeclaration(Declaration):
    params: list
    returning: TypeName
    function_code: SDLFunctionCode
    returning_typemod: qltypes.TypeModifier


class BasePointerDeclaration(Declaration):
    abstract: bool = False
    indexes: typing.List[IndexDeclaration]
    constraints: typing.List[Constraint]


class PropertyDeclaration(BasePointerDeclaration):
    pass


class LinkDeclaration(BasePointerDeclaration):
    properties: typing.List[Property]


class Import(SDL):
    modules: list


class ImportModule(SDL):
    module: str
    alias: str = None


class Schema(SDL):
    # TODO: Remove union type
    declarations: typing.List[typing.Union[Declaration, Import]]


#
# These utility functions work on EdgeQL AST nodes
#


def get_targets(target: TypeExpr):
    if target is None:
        return []
    elif isinstance(target, TypeOp):
        return get_targets(target.left) + get_targets(target.right)
    else:
        return [target]


def union_targets(names):
    target = TypeName(
        maintype=ObjectRef(name=names[0].name,
                           module=names[0].module)
    )

    for tname in names[1:]:
        target = TypeOp(
            left=target,
            op='|',
            right=TypeName(
                maintype=ObjectRef(name=tname.name,
                                   module=tname.module)
            )
        )

    return target
