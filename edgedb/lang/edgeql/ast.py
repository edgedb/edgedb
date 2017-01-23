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


class RootNode(Base):
    children: list


class ClassRefNode(Base):
    name: str
    module: str


class IndirectionNode(Base):
    arg: Base
    indirection: list


class IndexNode(Base):
    index: Base


class SliceNode(Base):
    start: Base
    stop: Base


class ArgListNode(Base):
    name: str
    args: list


class BinOpNode(Base):
    left: object
    op: str
    right: object


class WindowSpecNode(Base):
    orderby: list
    partition: list


class NamedArgNode(Base):
    name: str
    arg: object


class FunctionCallNode(Base):
    func: object  # tuple or str
    args: typing.List[Base]
    agg_sort: list
    agg_filter: object
    window: object


class VarNode(Base):
    name: str


class PathVarNode(VarNode):
    pass


class ConstantNode(Base):
    value: object


class EmptySetNode(Base):
    pass


class ParameterNode(Base):
    name: str


class DefaultValueNode(Base):
    pass


class UnaryOpNode(Base):
    op: str
    operand: Base


class PostfixOpNode(Base):
    op: str
    operand: Base


class PathNode(Base):
    steps: list
    quantifier: Base
    pathspec: list
    partial: bool = False


class PathDisjunctionNode(Base):
    left: Base
    right: Base


class PtrNode(Base):
    ptr: ClassRefNode
    direction: str
    target: Base
    type: str


class StatementNode(Base):
    namespaces: list
    aliases: list


class PositionNode(Base):
    ref: str
    position: str


class ExpressionTextNode(Base):
    expr: Base


class TypeNameNode(Base):
    maintype: Base
    subtypes: list


class TypeCastNode(Base):
    expr: Base
    type: TypeNameNode


class TypeFilterNode(Base):
    expr: Base
    type: TypeNameNode


class IfElseNode(Base):
    condition: Base
    if_expr: Base
    else_expr: Base


class CoalesceNode(Base):
    args: typing.List[Base]


class TransactionNode(Base):
    pass


class StartTransactionNode(TransactionNode):
    pass


class CommitTransactionNode(TransactionNode):
    pass


class RollbackTransactionNode(TransactionNode):
    pass


class DDLNode(Base):
    pass


class CompositeDDLNode(StatementNode, DDLNode):
    pass


class AlterSchemaNode(Base):
    commands: list


class AlterAddInheritNode(DDLNode):
    bases: list
    position: object


class AlterDropInheritNode(DDLNode):
    bases: list


class AlterTargetNode(DDLNode):
    targets: list


class ObjectDDLNode(CompositeDDLNode):
    namespaces: list
    name: ClassRefNode
    commands: list


class CreateObjectNode(ObjectDDLNode):
    pass


class AlterObjectNode(ObjectDDLNode):
    pass


class DropObjectNode(ObjectDDLNode):
    pass


class CreateInheritingObjectNode(CreateObjectNode):
    bases: list
    is_abstract: bool = False
    is_final: bool = False


class RenameNode(DDLNode):
    new_name: ClassRefNode


class DeltaNode:
    pass


class CreateDeltaNode(CreateObjectNode, DeltaNode):
    parents: list
    language: str
    target: object


class GetDeltaNode(ObjectDDLNode, DeltaNode):
    pass


class AlterDeltaNode(AlterObjectNode, DeltaNode):
    pass


class DropDeltaNode(DropObjectNode, DeltaNode):
    pass


class CommitDeltaNode(ObjectDDLNode, DeltaNode):
    pass


class DatabaseNode:
    pass


class CreateDatabaseNode(CreateObjectNode, DatabaseNode):
    pass


class DropDatabaseNode(DropObjectNode, DatabaseNode):
    pass


class CreateModuleNode(CreateObjectNode):
    pass


class AlterModuleNode(AlterObjectNode):
    pass


class DropModuleNode(DropObjectNode):
    pass


class CreateActionNode(CreateObjectNode):
    pass


class AlterActionNode(AlterObjectNode):
    pass


class DropActionNode(DropObjectNode):
    pass


class CreateEventNode(CreateInheritingObjectNode):
    pass


class AlterEventNode(AlterObjectNode):
    pass


class DropEventNode(DropObjectNode):
    pass


class CreateAttributeNode(CreateObjectNode):
    type: TypeNameNode


class DropAttributeNode(DropObjectNode):
    pass


class CreateAtomNode(CreateInheritingObjectNode):
    pass


class AlterAtomNode(AlterObjectNode):
    pass


class DropAtomNode(DropObjectNode):
    pass


class CreateLinkPropertyNode(CreateInheritingObjectNode):
    pass


class AlterLinkPropertyNode(AlterObjectNode):
    pass


class DropLinkPropertyNode(DropObjectNode):
    pass


class CreateConcreteLinkPropertyNode(CreateObjectNode):
    is_required: bool = False
    target: Base


class AlterConcreteLinkPropertyNode(AlterObjectNode):
    pass


class DropConcreteLinkPropertyNode(AlterObjectNode):
    pass


class SetSpecialFieldNode(Base):
    name: str
    value: object
    as_expr: bool = False


class CreateConceptNode(CreateInheritingObjectNode):
    pass


class AlterConceptNode(AlterObjectNode):
    pass


class DropConceptNode(DropObjectNode):
    pass


class CreateLinkNode(CreateInheritingObjectNode):
    pass


class AlterLinkNode(AlterObjectNode):
    pass


class DropLinkNode(DropObjectNode):
    pass


class CreateConcreteLinkNode(CreateInheritingObjectNode):
    is_required: bool = False
    targets: list


class AlterConcreteLinkNode(AlterObjectNode):
    pass


class DropConcreteLinkNode(DropObjectNode):
    pass


class CreateConstraintNode(CreateInheritingObjectNode):
    pass


class AlterConstraintNode(AlterObjectNode):
    pass


class DropConstraintNode(DropObjectNode):
    pass


class CreateConcreteConstraintNode(CreateObjectNode):
    args: list
    is_abstract: bool = False


class AlterConcreteConstraintNode(AlterObjectNode):
    pass


class DropConcreteConstraintNode(DropObjectNode):
    pass


class CreateLocalPolicyNode(CompositeDDLNode):
    event: ClassRefNode
    actions: list


class AlterLocalPolicyNode(CompositeDDLNode):
    event: ClassRefNode
    actions: list


class DropLocalPolicyNode(CompositeDDLNode):
    event: ClassRefNode


class CreateIndexNode(CreateObjectNode):
    expr: Base


class DropIndexNode(DropObjectNode):
    pass


class CreateAttributeValueNode(CreateObjectNode):
    value: Base
    as_expr: bool = False


class AlterAttributeValueNode(AlterObjectNode):
    value: Base


class DropAttributeValueNode(DropObjectNode):
    pass


class FuncArgNode(Base):
    name: str
    type: TypeNameNode
    variadic: bool = False
    default: Base


class Language(s_enum.StrEnum):
    SQL = 'SQL'
    EdgeQL = 'EDGEQL'


class FunctionCode(Base):
    language: Language
    code: str
    from_name: str


class CreateFunctionNode(CreateObjectNode):
    args: list
    returning: Base
    single: bool = False
    aggregate: bool = False
    code: FunctionCode


class AlterFunctionNode(AlterObjectNode):
    value: Base


class DropFunctionNode(DropObjectNode):
    pass


class SelectQueryNode(StatementNode):
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


class InsertQueryNode(StatementNode):
    subject: Base
    pathspec: list
    result: Base
    cges: list
    single: bool = False
    source: StatementNode


class UpdateQueryNode(StatementNode):
    subject: Base
    pathspec: list
    where: Base
    result: Base
    cges: list
    single: bool = False


class UpdateExprNode(Base):
    expr: Base
    value: Base


class DeleteQueryNode(StatementNode):
    subject: Base
    where: Base
    result: Base
    cges: list
    single: bool = False


class ValuesQueryNode(StatementNode):
    result: list
    orderby: list
    offset: Base
    limit: Base
    cges: list


class CGENode(Base):
    expr: Base
    alias: str


class NamespaceAliasDeclNode(Base):
    namespace: str
    alias: object


class ExpressionAliasDeclNode(Base):
    expr: Base
    alias: object


class SortExprNode(Base):
    path: Base
    direction: str
    nones_order: object


class PredicateNode(Base):
    expr: Base


class ExistsPredicateNode(PredicateNode):
    pass


class SelectPathSpecNode(Base):
    expr: Base
    pathspec: list
    where: Base
    orderby: list
    offset: Base
    limit: Base
    compexpr: Base
    recurse: bool = False
    recurse_limit: typing.Union[ConstantNode, ParameterNode]


class PointerGlobNode(Base):
    filters: list
    type: ClassRefNode


class PointerGlobFilter(Base):
    property: Base
    value: object
    any: bool = False


class FromExprNode(Base):
    expr: Base
    alias: Base


class StructElementNode(Base):
    name: ClassRefNode
    val: Base


class StructNode(Base):
    elements: typing.List[StructElementNode]


class EmptyCollectionNode(Base):
    pass


class SequenceNode(Base):
    elements: list


class ArrayNode(Base):
    elements: list


class MappingNode(Base):
    keys: typing.List[Base]
    values: typing.List[Base]


class NoneTestNode(Base):
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
