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
import enum
from dataclasses import dataclass, field

from edb.common import enum as s_enum
from edb.common import ast, span

from . import qltypes

Span = span.Span

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


def ast_dataclass(cls):
    return dataclass(cls, kw_only=True, repr=False, eq=False, slots=True)


@ast_dataclass
class Base(ast.AST):
    __abstract_node__ = True
    __ast_hidden__ = {'span', 'system_comment'}
    __rust_ignore__ = True

    span: typing.Optional[Span] = None

    # System-generated comment.
    system_comment: typing.Optional[str] = None

    def dump_edgeql(self) -> None:
        from edb.common.debug import dump_edgeql

        dump_edgeql(self)


@ast_dataclass
class OptionValue(Base):
    """An option value resulting from a syntax."""

    name: str


@ast_dataclass
class OptionFlag(OptionValue):

    val: bool


@ast_dataclass
class Options(Base):

    options: typing.Dict[str, OptionValue] = field(default_factory=dict)

    def get_flag(self, k: str) -> OptionFlag:
        try:
            flag = self[k]
        except KeyError:
            return OptionFlag(name=k, val=False)
        else:
            assert isinstance(flag, OptionFlag)
            return flag

    def __getitem__(self, k: str) -> OptionValue:
        return self.options[k]

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self.options)

    def __len__(self) -> int:
        return len(self.options)


@ast_dataclass
class Expr(Base):
    """Abstract parent for all query expressions."""

    __abstract_node__ = True


@ast_dataclass
class Placeholder(Expr):
    """An interpolation placeholder used in expression templates."""

    name: str


@ast_dataclass
class SortExpr(Base):
    path: Expr
    direction: typing.Optional[SortOrder] = None
    nones_order: typing.Optional[NonesOrder] = None


@ast_dataclass
class AliasedExpr(Base):
    alias: str
    expr: Expr


@ast_dataclass
class ModuleAliasDecl(Base):
    module: str
    alias: typing.Optional[str]


@ast_dataclass
class BaseObjectRef(Base):
    __abstract_node__ = True


@ast_dataclass
class ObjectRef(BaseObjectRef):
    name: str
    module: typing.Optional[str] = None
    itemclass: typing.Optional[qltypes.SchemaObjectClass] = None


@ast_dataclass
class PseudoObjectRef(BaseObjectRef):
    '''anytype, anytuple or anyobject'''
    name: str


@ast_dataclass
class Anchor(Expr):
    '''Identifier that resolves to some pre-compiled expression.
       For example in shapes, the anchor __subject__ refers to object that the
       shape is defined on.
    '''
    __abstract_node__ = True
    name: str


@ast_dataclass
class IRAnchor(Anchor):
    has_dml: bool = False


@ast_dataclass
class SpecialAnchor(Anchor):
    pass


@ast_dataclass
class DetachedExpr(Expr):  # DETACHED Expr
    expr: Expr
    preserve_path_prefix: bool = False


@ast_dataclass
class GlobalExpr(Expr):  # GLOBAL Name
    name: ObjectRef


@ast_dataclass
class Index(Base):
    index: Expr


@ast_dataclass
class Slice(Base):
    start: typing.Optional[Expr]
    stop: typing.Optional[Expr]


@ast_dataclass
class Indirection(Expr):
    arg: Expr
    indirection: typing.List[typing.Union[Index, Slice]]


@ast_dataclass
class BinOp(Expr):
    left: Expr
    op: str
    right: Expr

    rebalanced: bool = False
    set_constructor: bool = False


@ast_dataclass
class WindowSpec(Base):
    orderby: typing.List[SortExpr]
    partition: typing.List[Expr]


@ast_dataclass
class FunctionCall(Expr):
    func: typing.Union[typing.Tuple[str, str], str]
    args: typing.List[Expr] = field(default_factory=list)
    kwargs: typing.Dict[str, Expr] = field(default_factory=dict)
    window: typing.Optional[WindowSpec] = None


@ast_dataclass
class BaseConstant(Expr):
    """Constant (a literal value)."""
    __abstract_node__ = True


@ast_dataclass
class Constant(BaseConstant):
    """Constant whose value we can store in a string."""
    kind: ConstantKind
    value: str

    @classmethod
    def string(cls, value: str) -> Constant:
        return Constant(kind=ConstantKind.STRING, value=value)

    @classmethod
    def boolean(cls, b: bool) -> Constant:
        return Constant(kind=ConstantKind.BOOLEAN, value=str(b).lower())

    @classmethod
    def integer(cls, i: int) -> Constant:
        return Constant(kind=ConstantKind.INTEGER, value=str(i))


class ConstantKind(enum.IntEnum):
    STRING = 0
    BOOLEAN = 1
    INTEGER = 2
    FLOAT = 3
    BIGINT = 4
    DECIMAL = 5


@ast_dataclass
class BytesConstant(BaseConstant):
    value: bytes

    @classmethod
    def from_python(cls, s: bytes) -> BytesConstant:
        return BytesConstant(value=s)


@ast_dataclass
class Parameter(Expr):
    name: str


@ast_dataclass
class UnaryOp(Expr):
    op: str
    operand: Expr


@ast_dataclass
class TypeExpr(Base):
    __abstract_node__ = True

    name: typing.Optional[str] = None  # name is used for types in named tuples


@ast_dataclass
class TypeOf(TypeExpr):
    expr: Expr


@ast_dataclass
class TypeExprLiteral(TypeExpr):
    # Literal type exprs are used in enum declarations.
    val: Constant


@ast_dataclass
class TypeName(TypeExpr):
    maintype: BaseObjectRef
    subtypes: typing.Optional[typing.List[TypeExpr]] = None
    dimensions: typing.Optional[typing.List[int]] = None


@ast_dataclass
class TypeOp(TypeExpr):
    __rust_box__ = {'left', 'right'}

    left: TypeExpr
    op: str
    right: TypeExpr


@ast_dataclass
class FuncParam(Base):
    name: str
    type: TypeExpr
    typemod: qltypes.TypeModifier = qltypes.TypeModifier.SingletonType
    kind: qltypes.ParameterKind
    default: typing.Optional[Expr] = None


@ast_dataclass
class IsOp(Expr):
    left: Expr
    op: str
    right: TypeExpr


@ast_dataclass
class TypeIntersection(Base):
    type: TypeExpr


@ast_dataclass
class Ptr(Base):
    name: str
    direction: typing.Optional[str] = None
    type: typing.Optional[str] = None


@ast_dataclass
class Splat(Base):
    """Represents a splat operation (expansion to all props/links) in shapes"""

    #: Expansion depth
    depth: int
    #: Source type expression, e.g in Type.**
    type: typing.Optional[TypeExpr] = None
    #: Type intersection on the source which would result
    #: in polymorphic expansion, e.g. [is Type].**
    intersection: typing.Optional[TypeIntersection] = None


PathElement = typing.Union[Expr, Ptr, TypeIntersection, ObjectRef, Splat]


@ast_dataclass
class Path(Expr):
    steps: typing.List[PathElement]
    partial: bool = False


@ast_dataclass
class TypeCast(Expr):
    expr: Expr
    type: TypeExpr
    cardinality_mod: typing.Optional[CardinalityModifier] = None


@ast_dataclass
class Introspect(Expr):
    type: TypeExpr


@ast_dataclass
class IfElse(Expr):
    condition: Expr
    if_expr: Expr
    else_expr: Expr
    # Just affects pretty-printing
    python_style: bool = False


@ast_dataclass
class TupleElement(Base):
    # This stores the name in another node instead of as a str just so
    # that the name can have a separate source context.
    name: Ptr
    val: Expr


@ast_dataclass
class NamedTuple(Expr):
    elements: typing.List[TupleElement]


@ast_dataclass
class Tuple(Expr):
    elements: typing.List[Expr]


@ast_dataclass
class Array(Expr):
    elements: typing.List[Expr]


@ast_dataclass
class Set(Expr):
    elements: typing.List[Expr]


# Statements
#


@ast_dataclass
class Command(Base):
    """
    A top-level node that is evaluated by our server and
    cannot be a part of a sub expression.
    """

    __abstract_node__ = True
    aliases: typing.Optional[
        typing.List[typing.Union[AliasedExpr, ModuleAliasDecl]]
    ] = None


@ast_dataclass
class SessionSetAliasDecl(Command):
    decl: ModuleAliasDecl


@ast_dataclass
class SessionResetAliasDecl(Command):
    alias: str


@ast_dataclass
class SessionResetModule(Command):
    pass


@ast_dataclass
class SessionResetAllAliases(Command):
    pass


SessionCommand = (
    SessionSetAliasDecl
    | SessionResetAliasDecl
    | SessionResetModule
    | SessionResetAllAliases
)
SessionCommand_tuple = (
    SessionSetAliasDecl,
    SessionResetAliasDecl,
    SessionResetModule,
    SessionResetAllAliases
)


class ShapeOp(s_enum.StrEnum):
    APPEND = 'APPEND'
    SUBTRACT = 'SUBTRACT'
    ASSIGN = 'ASSIGN'
    MATERIALIZE = 'MATERIALIZE'  # This is an internal implementation artifact


# Need indirection over ShapeOp to preserve the source context.
@ast_dataclass
class ShapeOperation(Base):
    op: ShapeOp


class ShapeOrigin(s_enum.StrEnum):
    EXPLICIT = 'EXPLICIT'
    DEFAULT = 'DEFAULT'
    SPLAT_EXPANSION = 'SPLAT_EXPANSION'
    MATERIALIZATION = 'MATERIALIZATION'


@ast_dataclass
class ShapeElement(Expr):
    expr: Path
    elements: typing.Optional[typing.List[ShapeElement]] = None
    compexpr: typing.Optional[Expr] = None
    cardinality: typing.Optional[qltypes.SchemaCardinality] = None
    required: typing.Optional[bool] = None
    operation: ShapeOperation = ShapeOperation(op=ShapeOp.ASSIGN)
    origin: ShapeOrigin = ShapeOrigin.EXPLICIT

    where: typing.Optional[Expr] = None

    orderby: typing.Optional[typing.List[SortExpr]] = None

    offset: typing.Optional[Expr] = None
    limit: typing.Optional[Expr] = None


@ast_dataclass
class Shape(Expr):
    expr: typing.Optional[Expr]
    elements: typing.List[ShapeElement]


@ast_dataclass
class Query(Expr):
    __abstract_node__ = True

    aliases: typing.Optional[
        typing.List[typing.Union[AliasedExpr, ModuleAliasDecl]]
    ] = None


"""A node that can have a WITH block"""
Statement = Query | Command


@ast_dataclass
class SelectQuery(Query):
    result_alias: typing.Optional[str] = None
    result: Expr

    where: typing.Optional[Expr] = None

    orderby: typing.Optional[typing.List[SortExpr]] = None

    offset: typing.Optional[Expr] = None
    limit: typing.Optional[Expr] = None

    # This is a hack, indicating that rptr should be forwarded through
    # this select. Used when we generate implicit selects that need to
    # not interfere with linkprops.
    rptr_passthrough: bool = False

    implicit: bool = False


@ast_dataclass
class GroupingIdentList(Base):
    elements: typing.Tuple[GroupingAtom, ...]


GroupingAtom = typing.Union[ObjectRef, Path, GroupingIdentList]


@ast_dataclass
class GroupingElement(Base):
    __abstract_node__ = True


@ast_dataclass
class GroupingSimple(GroupingElement):
    element: GroupingAtom


@ast_dataclass
class GroupingSets(GroupingElement):
    sets: typing.List[GroupingElement]


@ast_dataclass
class GroupingOperation(GroupingElement):
    oper: str
    elements: typing.List[GroupingAtom]


@ast_dataclass
class GroupQuery(Query):
    subject_alias: typing.Optional[str] = None
    using: typing.Optional[typing.List[AliasedExpr]]
    by: typing.List[GroupingElement]

    subject: Expr


@ast_dataclass
class InternalGroupQuery(GroupQuery):
    group_alias: str
    grouping_alias: typing.Optional[str]
    from_desugaring: bool = False

    result_alias: typing.Optional[str] = None
    result: Expr

    where: typing.Optional[Expr] = None

    orderby: typing.Optional[typing.List[SortExpr]] = None


@ast_dataclass
class InsertQuery(Query):
    subject: ObjectRef
    shape: typing.List[ShapeElement]
    unless_conflict: typing.Optional[
        typing.Tuple[typing.Optional[Expr], typing.Optional[Expr]]
    ] = None


@ast_dataclass
class UpdateQuery(Query):
    shape: typing.List[ShapeElement]

    subject: Expr

    where: typing.Optional[Expr] = None


@ast_dataclass
class DeleteQuery(Query):
    subject: Expr

    where: typing.Optional[Expr] = None

    orderby: typing.Optional[typing.List[SortExpr]] = None

    offset: typing.Optional[Expr] = None
    limit: typing.Optional[Expr] = None


@ast_dataclass
class ForQuery(Query):
    from_desugaring: bool = False
    has_union: bool = True  # whether UNION was used in the syntax

    optional: bool = False
    iterator: Expr
    iterator_alias: str

    result_alias: typing.Optional[str] = None
    result: Expr


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
    __rust_ignore__ = True


class Position(DDL):
    ref: typing.Optional[ObjectRef] = None
    position: str


class DDLOperation(DDL):
    '''A change to schema'''

    __abstract_node__ = True
    commands: typing.List[DDLOperation] = field(default_factory=list)


class DDLCommand(Command, DDLOperation):
    __abstract_node__ = True


class AlterAddInherit(DDLOperation):
    position: typing.Optional[Position] = None
    bases: typing.List[TypeName]


class AlterDropInherit(DDLOperation):
    bases: typing.List[TypeName]


class OnTargetDelete(DDLOperation):
    cascade: typing.Optional[qltypes.LinkTargetDeleteAction]


class OnSourceDelete(DDLOperation):
    cascade: typing.Optional[qltypes.LinkSourceDeleteAction]


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
    __ast_hidden__ = {'object_class'}
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


class CreateExtendingObject(CreateObject):
    # final is not currently implemented, and the syntax is not
    # supported except in old dumps. We track it only to allow us to
    # error on it.
    final: bool = False
    bases: typing.List[TypeName]


class Rename(NamedDDL):
    new_name: ObjectRef

    @property
    def name(self) -> ObjectRef:  # type: ignore[override]  # mypy bug?
        return self.new_name


class NestedQLBlock(DDL):

    commands: typing.List[DDLOperation]
    text: typing.Optional[str] = None


class MigrationCommand(DDLCommand):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.MIGRATION
    )


class CreateMigration(CreateObject, MigrationCommand):

    body: NestedQLBlock
    parent: typing.Optional[ObjectRef] = None
    metadata_only: bool = False


class CommittedSchema(DDL):
    pass


class StartMigration(MigrationCommand):

    target: Schema | CommittedSchema


class AbortMigration(MigrationCommand):
    pass


class PopulateMigration(MigrationCommand):
    pass


class AlterCurrentMigrationRejectProposed(MigrationCommand):
    pass


class DescribeCurrentMigration(MigrationCommand):

    language: qltypes.DescribeLanguage


class CommitMigration(MigrationCommand):
    pass


class AlterMigration(AlterObject, MigrationCommand):
    pass


class DropMigration(DropObject, MigrationCommand):
    pass


class ResetSchema(MigrationCommand):

    target: ObjectRef


class StartMigrationRewrite(MigrationCommand):
    pass


class AbortMigrationRewrite(MigrationCommand):
    pass


class CommitMigrationRewrite(MigrationCommand):
    pass


class UnqualifiedObjectCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True


class GlobalObjectCommand(UnqualifiedObjectCommand):

    __abstract_node__ = True
    __rust_ignore__ = True


class ExternalObjectCommand(GlobalObjectCommand):

    __abstract_node__ = True
    __rust_ignore__ = True


class BranchType(s_enum.StrEnum):
    EMPTY = 'EMPTY'
    SCHEMA = 'SCHEMA'
    DATA = 'DATA'


class DatabaseCommand(ExternalObjectCommand):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.BRANCH
    flavor: str = 'BRANCH'


class CreateDatabase(CreateObject, DatabaseCommand):

    template: typing.Optional[ObjectRef] = None
    branch_type: BranchType


class AlterDatabase(AlterObject, DatabaseCommand):
    force: bool = False


class DropDatabase(DropObject, DatabaseCommand):
    force: bool = False


class ExtensionPackageCommand(GlobalObjectCommand):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.EXTENSION_PACKAGE
    )
    version: Constant


class CreateExtensionPackage(CreateObject, ExtensionPackageCommand):

    body: NestedQLBlock


class DropExtensionPackage(DropObject, ExtensionPackageCommand):
    pass


class ExtensionCommand(UnqualifiedObjectCommand):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.EXTENSION
    )
    version: typing.Optional[Constant] = None


class CreateExtension(CreateObject, ExtensionCommand):
    # HACK: I think there is a bug in our plugin that made us not
    # understand that this was overridden in ExtensionCommand.
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.EXTENSION
    )


class DropExtension(DropObject, ExtensionCommand):
    pass


class FutureCommand(UnqualifiedObjectCommand):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.FUTURE


class CreateFuture(CreateObject, FutureCommand):
    pass


class DropFuture(DropObject, ExtensionCommand):
    pass


class ModuleCommand(UnqualifiedObjectCommand):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.MODULE


class CreateModule(ModuleCommand, CreateObject):
    pass


class AlterModule(ModuleCommand, AlterObject):
    pass


class DropModule(ModuleCommand, DropObject):
    pass


class RoleCommand(GlobalObjectCommand):
    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.ROLE


class CreateRole(CreateObject, RoleCommand):
    superuser: bool = False
    bases: typing.List[TypeName]


class AlterRole(AlterObject, RoleCommand):
    pass


class DropRole(DropObject, RoleCommand):
    pass


class AnnotationCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.ANNOTATION
    )


class CreateAnnotation(CreateExtendingObject, AnnotationCommand):
    type: typing.Optional[TypeExpr]
    inheritable: bool


class AlterAnnotation(AlterObject, AnnotationCommand):
    pass


class DropAnnotation(DropObject, AnnotationCommand):
    pass


class PseudoTypeCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.PSEUDO_TYPE
    )


class CreatePseudoType(CreateObject, PseudoTypeCommand):
    pass


class ScalarTypeCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.SCALAR_TYPE
    )


class CreateScalarType(CreateExtendingObject, ScalarTypeCommand):
    pass


class AlterScalarType(AlterObject, ScalarTypeCommand):
    pass


class DropScalarType(DropObject, ScalarTypeCommand):
    pass


class PropertyCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.PROPERTY


class CreateProperty(CreateExtendingObject, PropertyCommand):
    pass


class AlterProperty(AlterObject, PropertyCommand):
    pass


class DropProperty(DropObject, PropertyCommand):
    pass


class CreateConcretePointer(CreateObject):
    is_required: typing.Optional[bool] = None
    declared_overloaded: bool = False
    target: typing.Optional[typing.Union[Expr, TypeExpr]]
    cardinality: qltypes.SchemaCardinality
    bases: typing.List[TypeName]


class CreateConcreteUnknownPointer(CreateConcretePointer):
    pass


class AlterConcreteUnknownPointer(AlterObject, PropertyCommand):
    pass


class CreateConcreteProperty(CreateConcretePointer, PropertyCommand):
    pass


class AlterConcreteProperty(AlterObject, PropertyCommand):
    pass


class DropConcreteProperty(DropObject, PropertyCommand):
    pass


class ObjectTypeCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.TYPE


class CreateObjectType(CreateExtendingObject, ObjectTypeCommand):
    pass


class AlterObjectType(AlterObject, ObjectTypeCommand):
    pass


class DropObjectType(DropObject, ObjectTypeCommand):
    pass


class AliasCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.ALIAS


class CreateAlias(CreateObject, AliasCommand):
    pass


class AlterAlias(AlterObject, AliasCommand):
    pass


class DropAlias(DropObject, AliasCommand):
    pass


class GlobalCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.GLOBAL


class CreateGlobal(CreateObject, GlobalCommand):
    is_required: typing.Optional[bool] = None
    target: typing.Optional[typing.Union[Expr, TypeExpr]]
    cardinality: typing.Optional[qltypes.SchemaCardinality]


class AlterGlobal(AlterObject, GlobalCommand):
    pass


class DropGlobal(DropObject, GlobalCommand):
    pass


class SetGlobalType(SetField):
    name: str = 'target'
    special_syntax: bool = True
    value: typing.Optional[TypeExpr]
    cast_expr: typing.Optional[Expr] = None
    reset_value: bool = False


class LinkCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.LINK


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


class ConstraintCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.CONSTRAINT
    )


class CreateConstraint(
    CreateExtendingObject,
    ConstraintCommand,
):
    subjectexpr: typing.Optional[Expr]
    abstract: bool = True
    params: typing.List[FuncParam] = field(default_factory=list)


class AlterConstraint(AlterObject, ConstraintCommand):
    pass


class DropConstraint(DropObject, ConstraintCommand):
    pass


class ConcreteConstraintOp(ConstraintCommand):

    __abstract_node__ = True
    __rust_ignore__ = True
    args: typing.List[Expr]
    subjectexpr: typing.Optional[Expr]
    except_expr: typing.Optional[Expr] = None


class CreateConcreteConstraint(ConcreteConstraintOp, CreateObject):
    delegated: bool = False


class AlterConcreteConstraint(ConcreteConstraintOp, AlterObject):
    pass


class DropConcreteConstraint(ConcreteConstraintOp, DropObject):
    pass


class IndexType(DDL):
    name: ObjectRef
    args: typing.List[Expr] = field(default_factory=list)
    kwargs: typing.Dict[str, Expr] = field(default_factory=dict)


class IndexCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.INDEX


class IndexCode(DDL):
    language: Language
    code: str


class CreateIndex(
    CreateExtendingObject,
    IndexCommand,
):
    kwargs: typing.Dict[str, Expr] = field(default_factory=dict)
    index_types: typing.List[IndexType]
    code: typing.Optional[IndexCode] = None
    params: typing.List[FuncParam] = field(default_factory=list)


class AlterIndex(AlterObject, IndexCommand):
    pass


class DropIndex(DropObject, IndexCommand):
    pass


class ConcreteIndexCommand(IndexCommand):

    __abstract_node__ = True
    __rust_ignore__ = True
    kwargs: typing.Dict[str, Expr] = field(default_factory=dict)
    expr: Expr
    except_expr: typing.Optional[Expr] = None
    deferred: typing.Optional[bool] = None


class CreateConcreteIndex(ConcreteIndexCommand, CreateObject):
    pass


class AlterConcreteIndex(ConcreteIndexCommand, AlterObject):
    pass


class DropConcreteIndex(ConcreteIndexCommand, DropObject):
    pass


class CreateAnnotationValue(AnnotationCommand, CreateObject):
    value: Expr


class AlterAnnotationValue(AnnotationCommand, AlterObject):
    value: typing.Optional[Expr]


class DropAnnotationValue(AnnotationCommand, DropObject):
    pass


class AccessPolicyCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = (
        qltypes.SchemaObjectClass.ACCESS_POLICY
    )


class CreateAccessPolicy(CreateObject, AccessPolicyCommand):
    condition: typing.Optional[Expr]
    action: qltypes.AccessPolicyAction
    access_kinds: typing.List[qltypes.AccessKind]
    expr: typing.Optional[Expr]


class SetAccessPerms(DDLOperation):
    access_kinds: typing.List[qltypes.AccessKind]
    action: qltypes.AccessPolicyAction


class AlterAccessPolicy(AlterObject, AccessPolicyCommand):
    pass


class DropAccessPolicy(DropObject, AccessPolicyCommand):
    pass


class TriggerCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.TRIGGER


class CreateTrigger(CreateObject, TriggerCommand):
    timing: qltypes.TriggerTiming
    kinds: typing.List[qltypes.TriggerKind]
    scope: qltypes.TriggerScope
    expr: Expr
    condition: typing.Optional[Expr]


class AlterTrigger(AlterObject, TriggerCommand):
    pass


class DropTrigger(DropObject, TriggerCommand):
    pass


class RewriteCommand(ObjectDDL):
    """
    Mutation rewrite command.

    Note that kinds are basically identifiers of the command, so they need to
    be present for all commands.

    List of kinds is converted into multiple commands when creating delta
    commands in `_cmd_tree_from_ast`.
    """

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.REWRITE

    kinds: typing.List[qltypes.RewriteKind]


class CreateRewrite(CreateObject, RewriteCommand):
    expr: Expr


class AlterRewrite(AlterObject, RewriteCommand):
    pass


class DropRewrite(DropObject, RewriteCommand):
    pass


class Language(s_enum.StrEnum):
    SQL = 'SQL'
    EdgeQL = 'EDGEQL'


class FunctionCode(DDL):
    language: Language = Language.EdgeQL
    code: typing.Optional[str] = None
    nativecode: typing.Optional[Expr] = None
    from_function: typing.Optional[str] = None
    from_expr: bool = False


class FunctionCommand(DDLCommand):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.FUNCTION
    params: typing.List[FuncParam] = field(default_factory=list)


class CreateFunction(CreateObject, FunctionCommand):

    returning: TypeExpr
    code: FunctionCode
    nativecode: typing.Optional[Expr]
    returning_typemod: qltypes.TypeModifier = qltypes.TypeModifier.SingletonType


class AlterFunction(AlterObject, FunctionCommand):

    code: FunctionCode = FunctionCode  # type: ignore
    nativecode: typing.Optional[Expr]


class DropFunction(DropObject, FunctionCommand):
    pass


class OperatorCode(DDL):
    language: Language
    from_operator: typing.Optional[typing.Tuple[str, ...]]
    from_function: typing.Optional[typing.Tuple[str, ...]]
    from_expr: bool
    code: typing.Optional[str]


class OperatorCommand(DDLCommand):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.OPERATOR
    kind: qltypes.OperatorKind
    params: typing.List[FuncParam] = field(default_factory=list)


class CreateOperator(CreateObject, OperatorCommand):
    returning: TypeExpr
    returning_typemod: qltypes.TypeModifier = qltypes.TypeModifier.SingletonType
    code: OperatorCode


class AlterOperator(AlterObject, OperatorCommand):
    pass


class DropOperator(DropObject, OperatorCommand):
    pass


class CastCode(DDL):
    language: Language
    from_function: str
    from_expr: bool
    from_cast: bool
    code: str


class CastCommand(ObjectDDL):

    __abstract_node__ = True
    __rust_ignore__ = True
    object_class: qltypes.SchemaObjectClass = qltypes.SchemaObjectClass.CAST
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
    __rust_ignore__ = True

    expr: Expr


#
# Config
#


class ConfigOp(Base):
    __abstract_node__ = True
    name: ObjectRef
    scope: qltypes.ConfigScope


class ConfigSet(ConfigOp):

    expr: Expr


class ConfigInsert(ConfigOp):

    shape: typing.List[ShapeElement]


class ConfigReset(ConfigOp):
    where: typing.Optional[Expr] = None


#
# Describe
#


class DescribeStmt(Command):

    language: qltypes.DescribeLanguage
    object: typing.Union[ObjectRef, DescribeGlobal]
    options: Options


#
# Explain
#


class ExplainStmt(Command):

    args: typing.Optional[NamedTuple]
    query: Query


#
# Administer
#


class AdministerStmt(Command):

    expr: FunctionCall


#
# SDL
#


class SDL(Base):
    '''Abstract parent for all SDL statements.'''

    __abstract_node__ = True
    __rust_ignore__ = True


class ModuleDeclaration(SDL):
    # The 'name' is treated same as in CreateModule, for consistency,
    # since this declaration also implies creating a module.
    name: ObjectRef
    declarations: typing.List[typing.Union[NamedDDL, ModuleDeclaration]]


class Schema(SDL):
    declarations: typing.List[typing.Union[NamedDDL, ModuleDeclaration]]


#
# These utility functions work on EdgeQL AST nodes
#


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


ReturningQuery = SelectQuery | ForQuery | InternalGroupQuery


FilteringQuery = (
    SelectQuery | DeleteQuery | ShapeElement | UpdateQuery | ConfigReset
)


SubjectQuery = DeleteQuery | UpdateQuery | GroupQuery


OffsetLimitQuery = SelectQuery | DeleteQuery | ShapeElement


BasedOn = (
    AlterAddInherit
    | AlterDropInherit
    | CreateExtendingObject
    | CreateRole
    | CreateConcretePointer
)
# TODO: this is required because mypy does support `instanceof(x, A | B)`
BasedOnTuple = (
    AlterAddInherit,
    AlterDropInherit,
    CreateExtendingObject,
    CreateRole,
    CreateConcretePointer,
)

CallableObjectCommand = (
    CreateConstraint | CreateIndex | FunctionCommand | OperatorCommand
)
# TODO: this is required because mypy does support `instanceof(x, A | B)`
CallableObjectCommandTuple = (
    CreateConstraint,
    CreateIndex,
    FunctionCommand,
    OperatorCommand,
)
