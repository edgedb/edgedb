##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import typing

from edgedb.lang.common import enum as s_enum
from edgedb.lang.common import ast, parsing


# Operators
#

class EdgeQLOperator(ast.ops.Operator):
    pass


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
UNION_ALL = SetOperator('UNION ALL')
DISTINCT = SetOperator('DISTINCT')

AND = ast.ops.AND
OR = ast.ops.OR
NOT = ast.ops.NOT
IN = ast.ops.IN
NOT_IN = ast.ops.NOT_IN

LIKE = EdgeQLMatchOperator('LIKE')
NOT_LIKE = EdgeQLMatchOperator('NOT LIKE')
ILIKE = EdgeQLMatchOperator('ILIKE')
NOT_ILIKE = EdgeQLMatchOperator('NOT ILIKE')

IS_OF = EdgeQLOperator('IS OF')
IS_NOT_OF = EdgeQLOperator('IS NOT OF')


class EquivalenceOperator(EdgeQLOperator):
    pass


EQUIVALENT = EquivalenceOperator('?=')
NEQIUVALENT = EquivalenceOperator('?!=')


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


class SetModifier(s_enum.StrEnum):
    ALL = 'ALL'
    DISTINCT = 'DISTINCT'
    NONE = ''


AggALL = SetModifier.ALL
AggDISTINCT = SetModifier.DISTINCT
AggNONE = SetModifier.NONE


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
    direction: SortOrder
    nones_order: str


# TODO: this needs clean-up and refactoring to account for different
# uses of aliases
class AliasedExpr(Clause):
    expr: Expr
    alias: str


class NamespaceAliasDecl(Clause):
    namespace: str
    alias: str


class ClassRef(Expr):
    name: str
    module: str


class Self(Expr):
    pass


class Subject(Expr):  # __subject__
    pass


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
    # FIXME: drop this completely
    agg_set_modifier: typing.Optional[SetModifier]


class Constant(Expr):
    value: typing.Union[int, str, float, bool]


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


class _TypeName(Expr):
    pass


class TypeName(_TypeName):
    name: str  # name is used for types in named tuples
    maintype: ClassRef
    subtypes: typing.List[_TypeName]
    dimensions: typing.Union[typing.List[int], None]


class FuncArg(Base):
    name: str
    type: TypeName
    is_set: bool = False
    variadic: bool = False
    default: Expr  # noqa (pyflakes bug)


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
    if_expr: Expr  # noqa (pyflakes bug)
    else_expr: Expr  # noqa (pyflakes bug)


class Coalesce(Expr):
    args: typing.List[Expr]


class ExistsPredicate(Expr):
    expr: Expr


class TupleElement(Base):
    name: ClassRef
    val: Expr


class NamedTuple(Expr):
    elements: typing.List[TupleElement]


class EmptyCollection(Expr):
    pass


class Tuple(Expr):
    elements: typing.List[Expr]


class Array(Expr):
    elements: typing.List[Expr]


class Mapping(Expr):
    keys: typing.List[Expr]
    values: typing.List[Expr]


class Set(Expr):
    elements: typing.List[Expr]


# Statements
#

class GroupExpr(Expr):
    subject: Expr
    by: typing.List[Expr]


class Statement(Expr):
    aliases: typing.List[typing.Union[AliasedExpr, NamespaceAliasDecl]]


class SubjStatement(Statement):
    subject: Expr
    subject_alias: str


class ReturningStatement(Statement):
    result: Expr
    result_alias: str
    single: bool = False


class SelectQuery(ReturningStatement):
    where: Expr
    orderby: typing.List[SortExpr]
    offset: Expr
    limit: Expr


class InsertQuery(SubjStatement):
    shape: typing.List[Expr]


class UpdateQuery(SubjStatement):
    shape: typing.List[Expr]
    where: Expr


class DeleteQuery(SubjStatement):
    where: Expr


class ForQuery(SelectQuery):
    iterator: Expr
    iterator_aliases: typing.List[str]


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


class CreateExtendingObject(CreateObject):
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


class CreateEvent(CreateExtendingObject):
    pass


class AlterEvent(AlterObject):
    pass


class DropEvent(DropObject):
    pass


class CreateAttribute(CreateExtendingObject):
    type: typing.Optional[TypeName]


class DropAttribute(DropObject):
    pass


class CreateAtom(CreateExtendingObject):
    pass


class AlterAtom(AlterObject):
    pass


class DropAtom(DropObject):
    pass


class CreateLinkProperty(CreateExtendingObject):
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


class CreateConcept(CreateExtendingObject):
    pass


class AlterConcept(AlterObject):
    pass


class DropConcept(DropObject):
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


class CreateConcreteLink(CreateExtendingObject):
    is_required: bool = False
    targets: typing.List[Expr]


class AlterConcreteLink(AlterObject):
    pass


class DropConcreteLink(DropObject):
    pass


class CreateConstraint(CreateExtendingObject):
    args: typing.List[FuncArg]
    subject: typing.Optional[Expr]


class AlterConstraint(AlterObject):
    pass


class DropConstraint(DropObject):
    pass


class CreateConcreteConstraint(CreateObject):
    args: typing.List[Base]
    is_abstract: bool = False
    subject: typing.Optional[Expr]


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


class Language(s_enum.StrEnum):
    SQL = 'SQL'
    EdgeQL = 'EDGEQL'


class FunctionCode(Clause):
    language: Language
    code: str
    from_name: str


class CreateFunction(CreateObject):
    args: typing.List[FuncArg]
    returning: TypeName
    aggregate: bool = False
    initial_value: Expr
    code: FunctionCode
    set_returning: bool = False


class AlterFunction(AlterObject):
    value: Base


class DropFunction(DropObject):
    args: typing.List[FuncArg]
    aggregate: bool = False
