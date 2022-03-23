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

import typing

from edb.common import enum as s_enum
from edb.common import ast, parsing

from . import qltypes


DDLCommand_T = typing.TypeVar(
    'DDLCommand_T',
    bound='DDLCommand',
    covariant=True,
)

ObjectDDL_T = typing.TypeVar(
    'ObjectDDL_T',
    bound='ObjectDDL',
    covariant=True,
)


Base_T = typing.TypeVar(
    'Base_T',
    bound='Base',
)


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


class CardinalityModifier(s_enum.StrEnum):
    Optional = 'OPTIONAL'
    Required = 'REQUIRED'


class DescribeGlobal(s_enum.StrEnum):
    Schema = 'SCHEMA'
    DatabaseConfig = 'DATABASE CONFIG'
    InstanceConfig = 'INSTANCE CONFIG'
    Roles = 'ROLES'

    def to_edgeql(self) -> str:
        return self.value


class Base(ast.AST):
    __abstract_node__ = True
    __ast_hidden__ = {'context'}
    context: typing.Optional[parsing.ParserContext] = None
    # System-generated comment.
    system_comment: typing.Optional[str] = None

    # parent: typing.Optional[Base]

    def dump_edgeql(self) -> None:
        from edb.common.debug import dump_edgeql
        dump_edgeql(self)


class OffsetLimitMixin(Base):
    __abstract_node__ = True
    offset: typing.Optional[Expr] = None
    limit: typing.Optional[Expr] = None


class OrderByMixin(Base):
    __abstract_node__ = True
    orderby: typing.Optional[typing.List[SortExpr]] = None


class FilterMixin(Base):
    __abstract_node__ = True
    where: typing.Optional[Expr] = None


class OptionValue(Base):
    """An option value resulting from a syntax."""

    name: str
    val: typing.Any


class Flag(OptionValue):

    val: bool


class Options(Base):

    options: typing.Dict[str, OptionValue] = ast.field(factory=dict)

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


class Placeholder(Expr):
    """An interpolation placeholder used in expression templates."""
    name: str


class SubExpr(Base):
    """A subexpression (used for anchors)."""

    expr: Expr
    anchors: typing.Dict[str, typing.Any]


class Clause(Base):
    """Abstract parent for all query clauses."""
    __abstract_node__ = True


class SortExpr(Clause):
    path: Expr
    direction: typing.Optional[SortOrder] = None
    nones_order: typing.Optional[NonesOrder] = None


class BaseAlias(Clause):
    __abstract_node__ = True
    alias: typing.Optional[str]


class OptionallyAliasedExpr(BaseAlias):
    expr: Expr


class AliasedExpr(OptionallyAliasedExpr):
    # alias isn't optional
    alias: str


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
    alias: str


class SessionResetModule(BaseSessionReset):
    pass


class SessionResetAllAliases(BaseSessionReset):
    pass


class BaseObjectRef(Base):
    __abstract_node__ = True


class ObjectRef(BaseObjectRef):
    name: str
    module: typing.Optional[str] = None
    itemclass: typing.Optional[qltypes.SchemaObjectClass] = None


class PseudoObjectRef(BaseObjectRef):
    __abstract_node__ = True


class AnyType(PseudoObjectRef):
    pass


class AnyTuple(PseudoObjectRef):
    pass


class Anchor(Expr):
    __abstract_node__ = True
    name: str


class SpecialAnchor(Anchor):
    __abstract_node__ = True


class Source(SpecialAnchor):  # __source__
    name: str = '__source__'


class Subject(SpecialAnchor):  # __subject__
    name: str = '__subject__'


class DetachedExpr(Expr):  # DETACHED Expr
    expr: Expr


class Index(Base):
    index: Expr


class Slice(Base):
    start: typing.Optional[Expr]
    stop: typing.Optional[Expr]


class Indirection(Expr):
    arg: Expr
    indirection: typing.List[typing.Union[Index, Slice]]


class BinOp(Expr):
    left: Expr
    op: str
    right: Expr


class SetConstructorOp(BinOp):
    op: str = 'UNION'


class WindowSpec(Clause, OrderByMixin):
    orderby: typing.List[SortExpr]
    partition: typing.List[Expr]


class FunctionCall(Expr):
    func: typing.Union[tuple, str]
    args: typing.List[Expr] = ast.field(factory=list)
    kwargs: typing.Dict[str, Expr] = ast.field(factory=dict)
    window: typing.Optional[WindowSpec] = None


class BaseConstant(Expr):
    __abstract_node__ = True
    value: str

    @classmethod
    def from_python(cls, val: typing.Any) -> BaseConstant:
        raise NotImplementedError


class StringConstant(BaseConstant):

    @classmethod
    def from_python(cls, s: str) -> StringConstant:
        return cls(value=s)


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

    @classmethod
    def from_python(cls, b: bool) -> BooleanConstant:
        return cls(value=str(b).lower())


class BytesConstant(BaseConstant):
    # This should really just be str to match, though
    value: bytes  # type: ignore[assignment]

    @classmethod
    def from_python(cls, s: bytes) -> BytesConstant:
        return cls(value=s)


class Parameter(Expr):
    name: str
    optional: bool


class UnaryOp(Expr):
    op: str
    operand: Expr


class TypeExpr(Base):
    name: typing.Optional[str] = None  # name is used for types in named tuples


class TypeOf(TypeExpr):
    expr: Expr


class TypeExprLiteral(TypeExpr):
    # Literal type exprs are used in enum declarations.
    val: StringConstant


class TypeName(TypeExpr):
    maintype: BaseObjectRef
    subtypes: typing.Optional[typing.List[TypeExpr]] = None
    dimensions: typing.Optional[typing.List[int]] = None


class TypeOp(TypeExpr):
    left: TypeExpr
    op: str
    right: TypeExpr


class FuncParam(Base):
    name: str
    type: TypeExpr
    typemod: qltypes.TypeModifier = qltypes.TypeModifier.SingletonType
    kind: qltypes.ParameterKind
    default: typing.Optional[Expr] = None


class IsOp(Expr):
    left: Expr
    op: str
    right: TypeExpr


class TypeIntersection(Base):
    type: TypeExpr


class Ptr(Base):
    ptr: ObjectRef
    direction: typing.Optional[str] = None
    type: typing.Optional[str] = None


PathElement = typing.Union[Expr, Ptr, TypeIntersection, ObjectRef]


class Path(Expr):
    steps: typing.List[PathElement]
    partial: bool = False


class TypeCast(Expr):
    expr: Expr
    type: TypeExpr
    cardinality_mod: typing.Optional[CardinalityModifier] = None


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


# Statements
#

class Command(Base):
    __abstract_node__ = True
    aliases: typing.Optional[
        typing.List[typing.Union[AliasedExpr, ModuleAliasDecl]]] = None


class Statement(Command, Expr):
    __abstract_node__ = True


class SubjectMixin(Base):
    __abstract_node__ = True
    subject: Expr


class ReturningMixin(Base):
    __abstract_node__ = True
    result_alias: typing.Optional[str] = None
    result: Expr


class SelectClauseMixin(OrderByMixin, OffsetLimitMixin, FilterMixin):
    __abstract_node__ = True
    implicit: bool = False
    # This is a hack, indicating that rptr should be forwarded through
    # this select. Used when we generate implicit selects that need to
    # not interfere with linkprops.
    rptr_passthrough: bool = False


class ShapeOp(s_enum.StrEnum):

    APPEND = 'APPEND'
    SUBTRACT = 'SUBTRACT'
    ASSIGN = 'ASSIGN'
    MATERIALIZE = 'MATERIALIZE'  # This is an internal implementation artifact


# Need indirection over ShapeOp to preserve the source context.
class ShapeOperation(Base):

    op: ShapeOp


class ShapeElement(OffsetLimitMixin, OrderByMixin, FilterMixin, Expr):
    expr: Path
    elements: typing.Optional[typing.List[ShapeElement]] = None
    compexpr: typing.Optional[Expr] = None
    cardinality: typing.Optional[qltypes.SchemaCardinality] = None
    required: typing.Optional[bool] = None
    operation: ShapeOperation = ShapeOperation(op=ShapeOp.ASSIGN)


class Shape(Expr):
    expr: typing.Optional[Expr]
    elements: typing.List[ShapeElement]


class Query(Statement):
    __abstract_node__ = True


class SelectQuery(Query, ReturningMixin, SelectClauseMixin):
    pass


class GroupingIdentList(Base):
    elements: typing.Tuple[GroupingAtom, ...]


GroupingAtom = typing.Union[ObjectRef, Path, GroupingIdentList]


class GroupingElement(Base):
    __abstract_node__ = True


class GroupingSimple(GroupingElement):
    element: GroupingAtom


class GroupingSets(GroupingElement):
    sets: typing.List[GroupingElement]


class GroupingOperation(GroupingElement):
    oper: str
    elements: typing.List[GroupingAtom]


class GroupQuery(Query, SubjectMixin):
    subject_alias: typing.Optional[str] = None
    using: typing.Optional[typing.List[AliasedExpr]]
    by: typing.List[GroupingElement]


class InternalGroupQuery(
        GroupQuery, ReturningMixin, FilterMixin, OrderByMixin):
    group_alias: str
    grouping_alias: typing.Optional[str]
    from_desugaring: bool = False


class InsertQuery(Query, SubjectMixin):
    subject: Path
    shape: typing.List[ShapeElement]
    unless_conflict: typing.Optional[
        typing.Tuple[typing.Optional[Expr], typing.Optional[Expr]]] = None


class UpdateQuery(Query, SubjectMixin, FilterMixin):
    shape: typing.List[ShapeElement]


class DeleteQuery(Query, SubjectMixin, SelectClauseMixin):
    pass


class ForQuery(Query, ReturningMixin):
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


class BasesMixin(DDL):
    __abstract_node__ = True
    bases: typing.List[TypeName]


class Position(DDL):
    ref: typing.Optional[ObjectRef] = None
    position: str


class DDLOperation(DDL):
    __abstract_node__ = True
    commands: typing.List[DDLOperation] = ast.field(factory=list)


class DDLCommand(Command, DDLOperation):
    __abstract_node__ = True


class AlterAddInherit(DDLOperation, BasesMixin):
    position: typing.Optional[Position] = None


class AlterDropInherit(DDLOperation, BasesMixin):
    pass


class OnTargetDelete(DDLOperation):
    cascade: typing.Optional[qltypes.LinkTargetDeleteAction]


class SetField(DDLOperation):
    name: str
    value: typing.Union[Expr, TypeExpr, None]
    #: Indicates that this AST originated from a special DDL syntax
    #: rather than from a generic `SET field := value` statement, and
    #: so must not be subject to the "allow_ddl_set" constraint.
    #: This attribute is also considered by the codegen to emit appropriate
    #: syntax.
    special_syntax: bool = False


class SetPointerType(SetField):
    name: str = 'target'
    special_syntax: bool = True
    value: typing.Optional[TypeExpr]
    cast_expr: typing.Optional[Expr] = None


class SetPointerCardinality(SetField):
    name: str = 'cardinality'
    special_syntax: bool = True
    conv_expr: typing.Optional[Expr] = None


class SetPointerOptionality(SetField):
    name: str = 'required'
    special_syntax: bool = True
    fill_expr: typing.Optional[Expr] = None


class NamedDDL(DDLCommand):
    __abstract_node__ = True
    name: ObjectRef


class ObjectDDL(NamedDDL):

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass


class CreateObject(ObjectDDL):
    abstract: bool = False
    sdl_alter_if_exists: bool = False
    create_if_not_exists: bool = False


class AlterObject(ObjectDDL):
    pass


class DropObject(ObjectDDL):
    pass


class CreateExtendingObject(CreateObject, BasesMixin):
    # final is not currently implemented, and the syntax is not
    # supported except in old dumps. We track it only to allow us to
    # error on it.
    final: bool = False


class Rename(NamedDDL):
    new_name: ObjectRef

    @property
    def name(self) -> ObjectRef:  # type: ignore[override]  # mypy bug?
        return self.new_name


class NestedQLBlock(DDL):

    commands: typing.List[DDLOperation]
    text: typing.Optional[str] = None


class MigrationCommand:

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.MIGRATION)


class CreateMigration(CreateObject, MigrationCommand):

    body: NestedQLBlock
    parent: typing.Optional[ObjectRef] = None
    message: typing.Optional[str] = None
    metadata_only: bool = False


class StartMigration(DDLCommand, MigrationCommand):

    target: Schema


class AbortMigration(DDLCommand, MigrationCommand):
    pass


class PopulateMigration(DDLCommand, MigrationCommand):
    pass


class AlterCurrentMigrationRejectProposed(DDLCommand, MigrationCommand):
    pass


class DescribeCurrentMigration(DDLCommand, MigrationCommand):

    language: qltypes.DescribeLanguage


class CommitMigration(DDLCommand, MigrationCommand):
    pass


class AlterMigration(AlterObject, MigrationCommand):
    pass


class DropMigration(DropObject, MigrationCommand):
    pass


class UnqualifiedObjectCommand(ObjectDDL):

    __abstract_node__ = True


class GlobalObjectCommand(UnqualifiedObjectCommand):

    __abstract_node__ = True


class ExternalObjectCommand(GlobalObjectCommand):

    __abstract_node__ = True


class DatabaseCommand(ExternalObjectCommand):
    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.DATABASE)


class CreateDatabase(CreateObject, DatabaseCommand):

    template: typing.Optional[ObjectRef] = None


class AlterDatabase(AlterObject, DatabaseCommand):
    pass


class DropDatabase(DropObject, DatabaseCommand):
    pass


class ExtensionPackageCommand(GlobalObjectCommand):
    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.EXTENSION_PACKAGE)
    version: StringConstant


class CreateExtensionPackage(CreateObject, ExtensionPackageCommand):

    body: NestedQLBlock


class DropExtensionPackage(DropObject, ExtensionPackageCommand):
    pass


class ExtensionCommand(UnqualifiedObjectCommand):

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.EXTENSION)
    version: typing.Optional[StringConstant] = None


class CreateExtension(CreateObject, ExtensionCommand):
    pass


class DropExtension(DropObject, ExtensionCommand):
    pass


class ModuleCommand(UnqualifiedObjectCommand):

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.MODULE)


class CreateModule(ModuleCommand, CreateObject):
    pass


class AlterModule(ModuleCommand, AlterObject):
    pass


class DropModule(ModuleCommand, DropObject):
    pass


class RoleCommand(GlobalObjectCommand):
    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.ROLE)


class CreateRole(CreateObject, BasesMixin, RoleCommand):
    superuser: bool = False


class AlterRole(AlterObject, RoleCommand):
    pass


class DropRole(DropObject, RoleCommand):
    pass


class AnnotationCommand(ObjectDDL):

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.ANNOTATION)


class CreateAnnotation(CreateExtendingObject, AnnotationCommand):
    type: typing.Optional[TypeExpr]
    inheritable: bool


class AlterAnnotation(AlterObject, AnnotationCommand):
    pass


class DropAnnotation(DropObject, AnnotationCommand):
    pass


class PseudoTypeCommand(ObjectDDL):

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.PSEUDO_TYPE)


class CreatePseudoType(CreateObject, PseudoTypeCommand):
    pass


class ScalarTypeCommand(ObjectDDL):

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.SCALAR_TYPE)


class CreateScalarType(CreateExtendingObject, ScalarTypeCommand):
    pass


class AlterScalarType(AlterObject, ScalarTypeCommand):
    pass


class DropScalarType(DropObject, ScalarTypeCommand):
    pass


class PropertyCommand(ObjectDDL):

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.PROPERTY)


class CreateProperty(CreateExtendingObject, PropertyCommand):
    pass


class AlterProperty(AlterObject, PropertyCommand):
    pass


class DropProperty(DropObject, PropertyCommand):
    pass


class CreateConcretePointer(CreateObject, BasesMixin):
    is_required: typing.Optional[bool] = None
    declared_overloaded: bool = False
    target: typing.Optional[typing.Union[Expr, TypeExpr]]
    cardinality: qltypes.SchemaCardinality


class CreateConcreteProperty(CreateConcretePointer, PropertyCommand):
    pass


class AlterConcreteProperty(AlterObject, PropertyCommand):
    pass


class DropConcreteProperty(DropObject, PropertyCommand):
    pass


class ObjectTypeCommand(ObjectDDL):

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.TYPE)


class CreateObjectType(CreateExtendingObject, ObjectTypeCommand):
    pass


class AlterObjectType(AlterObject, ObjectTypeCommand):
    pass


class DropObjectType(DropObject, ObjectTypeCommand):
    pass


class AliasCommand(ObjectDDL):

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.ALIAS)


class CreateAlias(CreateObject, AliasCommand):
    pass


class AlterAlias(AlterObject, AliasCommand):
    pass


class DropAlias(DropObject, AliasCommand):
    pass


class LinkCommand(ObjectDDL):

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.LINK)


class CreateLink(CreateExtendingObject, LinkCommand):
    pass


class AlterLink(AlterObject, LinkCommand):
    pass


class DropLink(DropObject, LinkCommand):
    pass


class CreateConcreteLink(
    CreateExtendingObject,
    CreateConcretePointer,
    LinkCommand,
):
    pass


class AlterConcreteLink(AlterObject, LinkCommand):
    pass


class DropConcreteLink(DropObject, LinkCommand):
    pass


class CallableObjectCommand(ObjectDDL):

    __abstract_node__ = True
    params: typing.List[FuncParam] = ast.field(factory=list)


class ConstraintCommand(ObjectDDL):

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.CONSTRAINT)


class CreateConstraint(
    CreateExtendingObject,
    CallableObjectCommand,
    ConstraintCommand,
):
    subjectexpr: typing.Optional[Expr]
    abstract: bool = True


class AlterConstraint(AlterObject, ConstraintCommand):
    pass


class DropConstraint(DropObject, ConstraintCommand):
    pass


class ConcreteConstraintOp(ConstraintCommand):

    __abstract_node__ = True
    args: typing.List[Expr]
    subjectexpr: typing.Optional[Expr]


class CreateConcreteConstraint(ConcreteConstraintOp, CreateObject):
    delegated: bool = False


class AlterConcreteConstraint(ConcreteConstraintOp, AlterObject):
    pass


class DropConcreteConstraint(ConcreteConstraintOp, DropObject):
    pass


class IndexCommand(ObjectDDL):

    __abstract_node__ = True
    expr: Expr
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.INDEX)


class CreateIndex(IndexCommand, CreateObject):
    pass


class AlterIndex(IndexCommand, AlterObject):
    pass


class DropIndex(IndexCommand, DropObject):
    pass


class CreateAnnotationValue(AnnotationCommand, CreateObject):
    value: Expr


class AlterAnnotationValue(AnnotationCommand, AlterObject):
    value: typing.Optional[Expr]


class DropAnnotationValue(AnnotationCommand, DropObject):
    pass


class Language(s_enum.StrEnum):
    SQL = 'SQL'
    EdgeQL = 'EDGEQL'


class FunctionCode(Clause):
    language: Language = Language.EdgeQL
    code: typing.Optional[str] = None
    nativecode: typing.Optional[Expr] = None
    from_function: typing.Optional[str] = None
    from_expr: bool = False


class FunctionCommand(CallableObjectCommand):

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.FUNCTION)


class CreateFunction(CreateObject, FunctionCommand):

    returning: TypeExpr
    code: FunctionCode
    nativecode: typing.Optional[Expr]
    returning_typemod: qltypes.TypeModifier = \
        qltypes.TypeModifier.SingletonType


class AlterFunction(AlterObject, FunctionCommand):

    code: FunctionCode = FunctionCode  # type: ignore
    nativecode: typing.Optional[Expr]


class DropFunction(DropObject, FunctionCommand):
    pass


class OperatorCode(Clause):
    language: Language
    from_operator: typing.Optional[typing.Tuple[str, ...]]
    from_function: typing.Optional[typing.Tuple[str, ...]]
    from_expr: bool
    code: typing.Optional[str]


class OperatorCommand(CallableObjectCommand):

    __abstract_node__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.OPERATOR)
    kind: qltypes.OperatorKind


class CreateOperator(CreateObject, OperatorCommand):
    returning: TypeExpr
    returning_typemod: qltypes.TypeModifier = \
        qltypes.TypeModifier.SingletonType
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
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.CAST)
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
    scope: qltypes.ConfigScope


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
    object: typing.Union[ObjectRef, DescribeGlobal]
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
    declarations: typing.List[typing.Union[NamedDDL, ModuleDeclaration]]


#
# These utility functions work on EdgeQL AST nodes
#


def get_targets(target: typing.Union[None, TypeExpr, Expr]):
    if target is None:
        return []
    elif isinstance(target, TypeOp):
        return get_targets(target.left) + get_targets(target.right)
    else:
        return [target]


def get_ddl_field_command(
    ddlcmd: DDLOperation,
    name: str,
) -> typing.Optional[SetField]:
    for cmd in ddlcmd.commands:
        if isinstance(cmd, SetField) and cmd.name == name:
            return cmd

    return None


def get_ddl_field_value(
    ddlcmd: DDLOperation,
    name: str,
) -> typing.Union[Expr, TypeExpr, None]:
    cmd = get_ddl_field_command(ddlcmd, name)
    return cmd.value if cmd is not None else None


def get_ddl_subcommand(
    ddlcmd: DDLOperation,
    cmdtype: typing.Type[DDLOperation],
) -> typing.Optional[DDLOperation]:
    for cmd in ddlcmd.commands:
        if isinstance(cmd, cmdtype):
            return cmd
    else:
        return None


def has_ddl_subcommand(
    ddlcmd: DDLOperation,
    cmdtype: typing.Type[DDLOperation],
) -> bool:
    return bool(get_ddl_subcommand(ddlcmd, cmdtype))
