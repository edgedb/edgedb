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


class Expr(Base):
    '''Abstract parent for all query expressions.'''
    pass


class Clause(Base):
    '''Abstract parent for all query clauses.'''
    pass


class SortExpr(Clause):
    path: Expr
    direction: str
    nones_order: str


class AliasedExpr(Clause):
    expr: Expr
    alias: str


class NamespaceAliasDecl(Clause):
    namespace: str
    alias: str


class ClassRef(Expr):
    name: str
    module: str


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


class NamedArg(Base):
    name: str
    arg: Expr


class FunctionCall(Expr):
    func: typing.Union[tuple, str]
    args: typing.List[Base]
    agg_sort: typing.List[SortExpr]
    agg_filter: Expr
    window: WindowSpec
    agg_set_modifier: str = ''


class Constant(Expr):
    value: typing.Union[int, str, float, bool]


class EmptySet(Expr):
    pass


class Parameter(Expr):
    name: str


class UnaryOp(Expr):
    op: str
    operand: Expr


class Ptr(Base):
    ptr: ClassRef
    direction: str
    target: Expr
    type: str


class TypeName(Expr):
    maintype: Expr
    subtypes: typing.List[ClassRef]


class TypeFilter(Expr):
    expr: Expr
    type: TypeName


class Path(Expr):
    steps: typing.List[typing.Union[Expr, Ptr]]
    quantifier: Expr
    partial: bool = False


class TypeCast(Expr):
    expr: Expr
    type: TypeName


class IfElse(Expr):
    condition: Expr
    if_expr: Expr
    else_expr: Expr


class Coalesce(Expr):
    args: typing.List[Expr]


class ExistsPredicate(Expr):
    expr: Expr


class StructElement(Base):
    name: ClassRef
    val: Expr


class Struct(Expr):
    elements: typing.List[StructElement]


class EmptyCollection(Expr):
    pass


class Tuple(Expr):
    elements: typing.List[Expr]


class Array(Expr):
    elements: typing.List[Expr]


class Mapping(Expr):
    keys: typing.List[Expr]
    values: typing.List[Expr]


# Statements
#

class Statement(Expr):
    aliases: typing.List[typing.Union[AliasedExpr, NamespaceAliasDecl]]
    result: Expr
    result_alias: str
    single: bool = False


class SubjStatement(Statement):
    subject: Expr
    subject_alias: str


class SelectQuery(Statement):
    where: Expr
    orderby: typing.List[SortExpr]
    offset: Expr
    limit: Expr


class GroupQuery(SubjStatement):
    where: Expr
    groupby: typing.List[Expr]
    orderby: typing.List[SortExpr]
    offset: Expr
    limit: Expr


class InsertQuery(SubjStatement):
    shape: typing.List[Expr]
    source_el: str
    source: Expr


class UpdateQuery(SubjStatement):
    shape: typing.List[Expr]
    where: Expr


class DeleteQuery(SubjStatement):
    where: Expr


class ShapeElement(Expr):
    expr: Expr
    elements: typing.List[Expr]
    where: Expr
    orderby: typing.List[SortExpr]
    offset: Expr
    limit: Expr
    compexpr: Expr
    recurse: bool = False
    recurse_limit: typing.Union[Constant, Parameter]


class Shape(Expr):
    expr: Expr
    elements: typing.List[ShapeElement]


# Transactions
#

class Transaction(Base):
    '''Abstract parent for all transaction operations.'''
    pass


class StartTransaction(Transaction):
    pass


class CommitTransaction(Transaction):
    pass


class RollbackTransaction(Transaction):
    pass


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


class ExpressionText(DDL):
    expr: Expr


class AlterAddInherit(DDL):
    bases: typing.List[ClassRef]
    position: Position


class AlterDropInherit(DDL):
    bases: typing.List[ClassRef]


class AlterTarget(DDL):
    targets: typing.List[ClassRef]


class ObjectDDL(CompositeDDL):
    namespaces: list  # XXX: is it even used?
    name: ClassRef
    commands: typing.List[DDL]


class CreateObject(ObjectDDL):
    pass


class AlterObject(ObjectDDL):
    pass


class DropObject(ObjectDDL):
    pass


class CreateInheritingObject(CreateObject):
    bases: typing.List[ClassRef]
    is_abstract: bool = False
    is_final: bool = False


class Rename(DDL):
    new_name: ClassRef


class Delta:
    pass


class CreateDelta(CreateObject, Delta):
    parents: typing.List[ClassRef]
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
    target: Expr


class AlterConcreteLinkProperty(AlterObject):
    pass


class DropConcreteLinkProperty(AlterObject):
    pass


class SetSpecialField(Base):
    name: str
    value: bool
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
    targets: typing.List[Expr]


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
    # args: list
    is_abstract: bool = False


class AlterConcreteConstraint(AlterObject):
    pass


class DropConcreteConstraint(DropObject):
    pass


class CreateLocalPolicy(CompositeDDL):
    event: ClassRef
    actions: typing.List[ClassRef]


class AlterLocalPolicy(CompositeDDL):
    event: ClassRef
    actions: typing.List[ClassRef]


class DropLocalPolicy(CompositeDDL):
    event: ClassRef


class CreateIndex(CreateObject):
    expr: Expr


class DropIndex(DropObject):
    pass


class CreateAttributeValue(CreateObject):
    value: Expr
    as_expr: bool = False


class AlterAttributeValue(AlterObject):
    value: Expr


class DropAttributeValue(DropObject):
    pass


class FuncArg(Base):
    name: str
    type: TypeName
    variadic: bool = False
    default: Expr


class Language(s_enum.StrEnum):
    SQL = 'SQL'
    EdgeQL = 'EDGEQL'


class FunctionCode(Clause):
    language: Language
    code: str
    from_name: str


class CreateFunction(CreateObject):
    args: typing.List[FuncArg]
    returning: typing.Union[TypeName, Shape]
    single: bool = False
    aggregate: bool = False
    code: FunctionCode


class AlterFunction(AlterObject):  # XXX: is this needed in the future?
    value: Base


class DropFunction(DropObject):  # XXX: is this needed in the future?
    pass


# Operators
#

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


class SetModifier(s_enum.StrEnum):
    ALL = 'ALL'
    DISTINCT = 'DISTINCT'
    NONE = ''


AggALL = SetModifier.ALL
AggDISTINCT = SetModifier.DISTINCT
AggNONE = SetModifier.NONE
