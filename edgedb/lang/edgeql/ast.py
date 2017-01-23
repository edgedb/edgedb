##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import typing

from edgedb.lang.common import enum as s_enum
from edgedb.lang.common import ast, parsing


class Base(ast.AST):
    __ast_hidden__ = {'context'}
    context: parsing.ParserContext


class Root(Base):
    children: list


class ClassRef(Base):
    name: str
    module: str


class Indirection(Base):
    arg: Base
    indirection: list


class Index(Base):
    index: Base


class Slice(Base):
    start: Base
    stop: Base


class ArgList(Base):
    name: str
    args: list


class BinOp(Base):
    left: object
    op: str
    right: object


class WindowSpec(Base):
    orderby: list
    partition: list


class NamedArg(Base):
    name: str
    arg: object


class FunctionCall(Base):
    func: object  # tuple or str
    args: typing.List[Base]
    agg_sort: list
    agg_filter: object
    window: object


class Var(Base):
    name: str


class PathVar(Var):
    pass


class Constant(Base):
    value: object


class EmptySet(Base):
    pass


class Parameter(Base):
    name: str


class DefaultValue(Base):
    pass


class UnaryOp(Base):
    op: str
    operand: Base


class PostfixOp(Base):
    op: str
    operand: Base


class Path(Base):
    steps: list
    quantifier: Base
    pathspec: list
    partial: bool = False


class PathDisjunction(Base):
    left: Base
    right: Base


class Ptr(Base):
    ptr: ClassRef
    direction: str
    target: Base
    type: str


class Statement(Base):
    namespaces: list
    aliases: list


class Position(Base):
    ref: str
    position: str


class ExpressionText(Base):
    expr: Base


class TypeName(Base):
    maintype: Base
    subtypes: list


class TypeCast(Base):
    expr: Base
    type: TypeName


class TypeFilter(Base):
    expr: Base
    type: TypeName


class IfElse(Base):
    condition: Base
    if_expr: Base
    else_expr: Base


class Coalesce(Base):
    args: typing.List[Base]


class Transaction(Base):
    pass


class StartTransaction(Transaction):
    pass


class CommitTransaction(Transaction):
    pass


class RollbackTransaction(Transaction):
    pass


class DDL(Base):
    pass


class CompositeDDL(Statement, DDL):
    pass


class AlterSchema(Base):
    commands: list


class AlterAddInherit(DDL):
    bases: list
    position: object


class AlterDropInherit(DDL):
    bases: list


class AlterTarget(DDL):
    targets: list


class ObjectDDL(CompositeDDL):
    namespaces: list
    name: ClassRef
    commands: list


class CreateObject(ObjectDDL):
    pass


class AlterObject(ObjectDDL):
    pass


class DropObject(ObjectDDL):
    pass


class CreateInheritingObject(CreateObject):
    bases: list
    is_abstract: bool = False
    is_final: bool = False


class Rename(DDL):
    new_name: ClassRef


class Delta:
    pass


class CreateDelta(CreateObject, Delta):
    parents: list
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


class DropDatabase(DropObject, Database):
    pass


class CreateModule(CreateObject):
    pass


class AlterModule(AlterObject):
    pass


class DropModule(DropObject):
    pass


class CreateAction(CreateObject):
    pass


class AlterAction(AlterObject):
    pass


class DropAction(DropObject):
    pass


class CreateEvent(CreateInheritingObject):
    pass


class AlterEvent(AlterObject):
    pass


class DropEvent(DropObject):
    pass


class CreateAttribute(CreateObject):
    type: TypeName


class DropAttribute(DropObject):
    pass


class CreateAtom(CreateInheritingObject):
    pass


class AlterAtom(AlterObject):
    pass


class DropAtom(DropObject):
    pass


class CreateLinkProperty(CreateInheritingObject):
    pass


class AlterLinkProperty(AlterObject):
    pass


class DropLinkProperty(DropObject):
    pass


class CreateConcreteLinkProperty(CreateObject):
    is_required: bool = False
    target: Base


class AlterConcreteLinkProperty(AlterObject):
    pass


class DropConcreteLinkProperty(AlterObject):
    pass


class SetSpecialField(Base):
    name: str
    value: object
    as_expr: bool = False


class CreateConcept(CreateInheritingObject):
    pass


class AlterConcept(AlterObject):
    pass


class DropConcept(DropObject):
    pass


class CreateLink(CreateInheritingObject):
    pass


class AlterLink(AlterObject):
    pass


class DropLink(DropObject):
    pass


class CreateConcreteLink(CreateInheritingObject):
    is_required: bool = False
    targets: list


class AlterConcreteLink(AlterObject):
    pass


class DropConcreteLink(DropObject):
    pass


class CreateConstraint(CreateInheritingObject):
    pass


class AlterConstraint(AlterObject):
    pass


class DropConstraint(DropObject):
    pass


class CreateConcreteConstraint(CreateObject):
    args: list
    is_abstract: bool = False


class AlterConcreteConstraint(AlterObject):
    pass


class DropConcreteConstraint(DropObject):
    pass


class CreateLocalPolicy(CompositeDDL):
    event: ClassRef
    actions: list


class AlterLocalPolicy(CompositeDDL):
    event: ClassRef
    actions: list


class DropLocalPolicy(CompositeDDL):
    event: ClassRef


class CreateIndex(CreateObject):
    expr: Base


class DropIndex(DropObject):
    pass


class CreateAttributeValue(CreateObject):
    value: Base
    as_expr: bool = False


class AlterAttributeValue(AlterObject):
    value: Base


class DropAttributeValue(DropObject):
    pass


class FuncArg(Base):
    name: str
    type: TypeName
    variadic: bool = False
    default: Base


class Language(s_enum.StrEnum):
    SQL = 'SQL'
    EdgeQL = 'EDGEQL'


class FunctionCode(Base):
    language: Language
    code: str
    from_name: str


class CreateFunction(CreateObject):
    args: list
    returning: Base
    single: bool = False
    aggregate: bool = False
    code: FunctionCode


class AlterFunction(AlterObject):
    value: Base


class DropFunction(DropObject):
    pass


class SelectQuery(Statement):
    single: bool = False
    distinct: bool = False
    result: Base
    where: Base
    groupby: list
    having: Base
    orderby: list
    offset: Base
    limit: Base
    cges: list
    op: str
    op_larg: Base
    op_rarg: Base


class InsertQuery(Statement):
    subject: Base
    pathspec: list
    result: Base
    cges: list
    single: bool = False
    source: Statement


class UpdateQuery(Statement):
    subject: Base
    pathspec: list
    where: Base
    result: Base
    cges: list
    single: bool = False


class UpdateExpr(Base):
    expr: Base
    value: Base


class DeleteQuery(Statement):
    subject: Base
    where: Base
    result: Base
    cges: list
    single: bool = False


class ValuesQuery(Statement):
    result: list
    orderby: list
    offset: Base
    limit: Base
    cges: list


class CGE(Base):
    expr: Base
    alias: str


class NamespaceAliasDecl(Base):
    namespace: str
    alias: object


class ExpressionAliasDecl(Base):
    expr: Base
    alias: object


class SortExpr(Base):
    path: Base
    direction: str
    nones_order: object


class Predicate(Base):
    expr: Base


class ExistsPredicate(Predicate):
    pass


class SelectPathSpec(Base):
    expr: Base
    pathspec: list
    where: Base
    orderby: list
    offset: Base
    limit: Base
    compexpr: Base
    recurse: bool = False
    recurse_limit: typing.Union[Constant, Parameter]


class PointerGlob(Base):
    filters: list
    type: ClassRef


class PointerGlobFilter(Base):
    property: Base
    value: object
    any: bool = False


class FromExpr(Base):
    expr: Base
    alias: Base


class StructElement(Base):
    name: ClassRef
    val: Base


class Struct(Base):
    elements: typing.List[StructElement]


class EmptyCollection(Base):
    pass


class Tuple(Base):
    elements: list


class Array(Base):
    elements: list


class Mapping(Base):
    keys: typing.List[Base]
    values: typing.List[Base]


class NoneTest(Base):
    expr: Base


class EdgeQLOperator(ast.ops.Operator):
    pass


class TextSearchOperator(EdgeQLOperator):
    pass

SEARCH = TextSearchOperator('@@')


class EdgeQLComparisonOperator(EdgeQLOperator, ast.ops.ComparisonOperator):
    pass


class EdgeQLMatchOperator(EdgeQLComparisonOperator):
    def __new__(cls, val, *, strname=None, **kwargs):
        return super().__new__(cls, val, **kwargs)

    def __init__(self, val, *, strname=None, **kwargs):
        super().__init__(val, **kwargs)
        self._strname = strname

    def __str__(self):
        if self._strname:
            return self._strname
        else:
            return super().__str__()


class SetOperator(EdgeQLOperator):
    pass

UNION = SetOperator('UNION')
INTERSECT = SetOperator('INTERSECT')
EXCEPT = SetOperator('EXCEPT')


AND = ast.ops.AND
OR = ast.ops.OR
NOT = ast.ops.NOT
IN = ast.ops.IN
NOT_IN = ast.ops.NOT_IN
LIKE = EdgeQLMatchOperator('~~', strname='LIKE')
NOT_LIKE = EdgeQLMatchOperator('!~~', strname='NOT LIKE')
ILIKE = EdgeQLMatchOperator('~~*', strname='ILIKE')
NOT_ILIKE = EdgeQLMatchOperator('!~~*', strname='NOT ILIKE')

REMATCH = EdgeQLMatchOperator('~')
REIMATCH = EdgeQLMatchOperator('~*')

RENOMATCH = EdgeQLMatchOperator('!~')
RENOIMATCH = EdgeQLMatchOperator('!~*')

IS_OF = EdgeQLOperator('IS OF')
IS_NOT_OF = EdgeQLOperator('IS NOT OF')


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


class SetOperator(EdgeQLOperator):
    pass


UNION = SetOperator('UNION')
INTERSECT = SetOperator('INTERSECT')
EXCEPT = SetOperator('EXCEPT')


class Position(s_enum.StrEnum):
    AFTER = 'AFTER'
    BEFORE = 'BEFORE'
    FIRST = 'FIRST'
    LAST = 'LAST'


AFTER = Position.AFTER
BEFORE = Position.BEFORE
FIRST = Position.FIRST
LAST = Position.LAST
