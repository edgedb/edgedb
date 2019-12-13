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


from __future__ import annotations
# Do not import "from typing *"; this module contains
# AST classes that name-clash with classes from the typing module.

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


class Base(ast.AST):
    __abstract_node__ = True
    __ast_hidden__ = {'context'}
    context: parsing.ParserContext
    # System-generated comment.
    system_comment: str

    # parent: typing.Optional[Base]


class OffsetLimitMixin(Base):
    __abstract_node__ = True
    offset: Expr
    limit: Expr


class OrderByMixin(Base):
    __abstract_node__ = True
    orderby: typing.List[SortExpr]


class FilterMixin(Base):
    __abstract_node__ = True
    where: Expr


class OptionValue(Base):
    """An option value resulting from a syntax."""

    name: str
    val: typing.Any


class Flag(OptionValue):

    val: bool


class Options(Base):

    options: typing.Dict[str, OptionValue]

    def get_flag(self, k: str) -> Flag:
        try:
            flag = self[k]
        except KeyError:
            return Flag(name=k, val=False)
        else:
            assert isinstance(flag, Flag)
            return flag

    def __getitem__(self, k: str) -> OptionValue:
        return self.options[k]

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self.options)

    def __len__(self) -> int:
        return len(self.options)


class Expr(Base):
    """Abstract parent for all query expressions."""
    __abstract_node__ = True


class SubExpr(Base):
    """A subexpression (used for anchors)."""

    expr: typing.Union[Expr, object]
    anchors: typing.Dict[typing.Union[str, ast.MetaAST],
                         typing.Union[Expr, object]]


class Clause(Base):
    """Abstract parent for all query clauses."""
    __abstract_node__ = True


class SortExpr(Clause):
    path: Expr
    direction: SortOrder
    nones_order: str


class BaseAlias(Clause):
    __abstract_node__ = True
    alias: str


class AliasedExpr(BaseAlias):
    expr: Expr


class ModuleAliasDecl(BaseAlias):
    module: str


class BaseSessionCommand(Base):
    __abstract_node__ = True


class BaseSessionSet(BaseSessionCommand):
    __abstract_node__ = True


class BaseSessionConfigSet(BaseSessionSet):
    __abstract_node__ = True
    system: bool = False


class SessionSetAliasDecl(ModuleAliasDecl, BaseSessionSet):
    pass


class BaseSessionReset(BaseSessionCommand):
    __abstract_node__ = True


class SessionResetAliasDecl(BaseAlias, BaseSessionReset):
    pass


class SessionResetModule(BaseSessionReset):
    pass


class SessionResetAllAliases(BaseSessionReset):
    pass


class BaseObjectRef(Base):
    __abstract_node__ = True


class ObjectRef(BaseObjectRef):
    name: str
    module: str
    itemclass: typing.Optional[qltypes.SchemaObjectClass]


class PseudoObjectRef(BaseObjectRef):
    __abstract_node__ = True


class AnyType(PseudoObjectRef):
    pass


class AnyTuple(PseudoObjectRef):
    pass


class SpecialAnchor(Expr):
    __abstract_node__ = True


SpecialAnchorT = typing.Type[SpecialAnchor]


class Source(SpecialAnchor):  # __source__
    pass


class Subject(SpecialAnchor):  # __subject__
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


class WindowSpec(Clause, OrderByMixin):
    orderby: typing.List[SortExpr]
    partition: typing.List[Expr]


class FunctionCall(Expr):
    func: typing.Union[tuple, str]
    args: typing.List[Expr]
    kwargs: typing.Dict[str, Expr]
    window: WindowSpec


class BaseConstant(Expr):
    __abstract_node__ = True
    value: str

    @classmethod
    def from_python(cls, val: typing.Any) -> BaseConstant:
        if isinstance(val, str):
            return StringConstant.from_python(val)
        elif isinstance(val, bool):
            return BooleanConstant(value='true' if val else 'false')
        elif isinstance(val, int):
            return IntegerConstant(value=str(val))
        elif isinstance(val, decimal.Decimal):
            return DecimalConstant(value=str(val))
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
    __abstract_node__ = True
    is_negative: bool = False


class IntegerConstant(BaseRealConstant):
    pass


class FloatConstant(BaseRealConstant):
    pass


class BigintConstant(BaseRealConstant):
    pass


class DecimalConstant(BaseRealConstant):
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
    name: str  # name is used for types in named tuples


class TypeOf(TypeExpr):
    expr: Expr


class TypeExprLiteral(TypeExpr):
    # Literal type exprs are used in enum declarations.
    val: StringConstant


class TypeName(TypeExpr):
    maintype: BaseObjectRef
    subtypes: typing.Optional[typing.List[TypeExpr]]
    dimensions: typing.Optional[typing.List[int]]


class TypeOp(TypeExpr):
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
    steps: typing.List[typing.Union[Expr, Ptr, TypeIndirection, ObjectRef]]
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
    __abstract_node__ = True


class ByExpr(ByExprBase):
    each: bool
    expr: Expr


class GroupBuiltin(ByExprBase):
    name: str
    elements: typing.List[ByExpr]


# Statements
#

class Command(Base):
    __abstract_node__ = True
    aliases: typing.List[typing.Union[AliasedExpr, ModuleAliasDecl]]


class Statement(Command, Expr):
    __abstract_node__ = True


class SubjectMixin(Base):
    __abstract_node__ = True
    subject: Expr
    subject_alias: str


class ReturningMixin(Base):
    __abstract_node__ = True
    result: Expr
    result_alias: str


class SelectClauseMixin(OrderByMixin, OffsetLimitMixin, FilterMixin):
    __abstract_node__ = True
    implicit: bool = False


class ShapeElement(OffsetLimitMixin, OrderByMixin, FilterMixin, Expr):
    expr: Path
    elements: typing.List[ShapeElement]
    compexpr: Expr
    cardinality: qltypes.Cardinality
    required: bool = False


class Shape(Expr):
    expr: Expr
    elements: typing.List[ShapeElement]


class SelectQuery(Statement, ReturningMixin, SelectClauseMixin):
    pass


class GroupQuery(SelectQuery, SubjectMixin):
    using: typing.List[AliasedExpr]
    by: typing.List[Expr]
    into: str


class InsertQuery(Statement, SubjectMixin):
    subject: Path
    shape: typing.List[ShapeElement]


class UpdateQuery(Statement, SubjectMixin, FilterMixin):
    shape: typing.List[ShapeElement]


class DeleteQuery(Statement, SubjectMixin, SelectClauseMixin):
    pass


class ForQuery(Statement, ReturningMixin):
    iterator: Expr
    iterator_alias: str


# Transactions
#

class Transaction(Base):
    '''Abstract parent for all transaction operations.'''
    __abstract_node__ = True


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
    __abstract_node__ = True


class CompositeDDL(Command, DDL):
    __abstract_node__ = True


class BasesMixin(DDL):
    __abstract_node__ = True
    bases: typing.Union[typing.List[TypeName], typing.List[ObjectRef], None]


class Position(DDL):
    ref: ObjectRef
    position: str


class DDLCommand(DDL):
    # `name` will either be `str` if node is BaseSetField, or
    # `ObjectRef` if node is ObjectDDL.
    name: typing.Any


class SetPointerType(DDLCommand):
    type: TypeExpr


class SetLinkType(SetPointerType):
    pass


class SetPropertyType(SetPointerType):
    pass


class AlterAddInherit(DDLCommand, BasesMixin):
    position: Position


class AlterDropInherit(DDLCommand, BasesMixin):
    pass


class OnTargetDelete(DDLCommand):
    cascade: LinkTargetDeleteAction


class BaseSetField(DDLCommand):
    __abstract_node__ = True
    name: str
    value: Expr


class SetField(BaseSetField):
    pass


class SetSpecialField(BaseSetField):
    value: typing.Any


class ObjectDDL(DDLCommand, CompositeDDL):
    __abstract_node__ = True
    name: ObjectRef
    commands: typing.Sequence[DDLCommand]


class CreateObject(ObjectDDL):
    is_abstract: bool = False
    sdl_alter_if_exists: bool = False
    create_if_not_exists: bool = False


class AlterObject(ObjectDDL):
    pass


class DropObject(ObjectDDL):
    pass


class CreateExtendingObject(CreateObject, BasesMixin):
    is_final: bool = False


class Rename(DDLCommand):
    new_name: ObjectRef

    @property
    def name(self) -> ObjectRef:
        return self.new_name


class Delta:
    __abstract_node__ = True


class CreateMigration(CreateObject, Delta):
    parents: typing.List[ObjectRef]
    target: typing.Any


class GetMigration(ObjectDDL, Delta):
    pass


class AlterMigration(AlterObject, Delta):
    pass


class DropMigration(DropObject, Delta):
    pass


class CommitMigration(ObjectDDL, Delta):
    pass


class Database:
    __abstract_node__ = True


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


class CreateRole(CreateObject, BasesMixin):
    pass


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


class CreateConcretePointer(CreateObject, BasesMixin):
    is_required: bool = False
    declared_overloaded: bool = False
    target: typing.Optional[typing.Union[Expr, TypeExpr]]
    cardinality: qltypes.Cardinality


class CreateConcreteProperty(CreateConcretePointer):
    pass


class AlterConcreteProperty(AlterObject):
    pass


class DropConcreteProperty(DropObject):
    pass


class CreateObjectType(CreateExtendingObject):
    pass


class AlterObjectType(AlterObject):
    pass


class DropObjectType(DropObject):
    pass


class CreateAlias(CreateObject):
    pass


class AlterAlias(AlterObject):
    pass


class DropAlias(DropObject):
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
    __abstract_node__ = True
    params: typing.List[FuncParam]


class CreateConstraint(CreateExtendingObject, CallableObject):
    subjectexpr: typing.Optional[Expr]
    is_abstract: bool = True


class AlterConstraint(AlterObject):
    pass


class DropConstraint(DropObject):
    pass


class ConstraintOp(ObjectDDL):
    __abstract_node__ = True
    args: typing.List[Expr]
    subjectexpr: typing.Optional[Expr]


class CreateConcreteConstraint(CreateObject, ConstraintOp):
    delegated: bool = False


class AlterConcreteConstraint(AlterObject, ConstraintOp):
    pass


class DropConcreteConstraint(DropObject, ConstraintOp):
    pass


class IndexOp(ObjectDDL):
    __abstract_node__ = True
    expr: Expr


class CreateIndex(CreateObject, IndexOp):
    pass


class AlterIndex(AlterObject, IndexOp):
    pass


class DropIndex(DropObject, IndexOp):
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
    code: typing.Optional[str]
    from_function: typing.Optional[str]
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
    __abstract_node__ = True
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
    __abstract_node__ = True
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
    __abstract_node__ = True
    name: ObjectRef
    system: bool
    backend_setting: str


class ConfigSet(ConfigOp):

    expr: Expr


class ConfigInsert(ConfigOp):

    shape: typing.List[ShapeElement]


class ConfigReset(ConfigOp, FilterMixin):
    pass


#
# Describe
#

class DescribeStmt(Statement):

    language: qltypes.DescribeLanguage
    object: ObjectRef
    options: Options


#
# SDL
#


class SDL(Base):
    '''Abstract parent for all SDL statements.'''
    __abstract_node__ = True


class ModuleDeclaration(SDL):
    # The 'name' is treated same as in CreateModule, for consistency,
    # since this declaration also implies creating a module.
    name: ObjectRef
    declarations: typing.List[DDL]


class Schema(SDL):
    declarations: typing.List[typing.Union[DDL, ModuleDeclaration]]


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
