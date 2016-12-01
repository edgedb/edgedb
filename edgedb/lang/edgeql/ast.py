##
# Copyright (c) 2008-2012, 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import enum as s_enum
from edgedb.lang.common import ast, parsing


class Base(ast.AST):
    __ast_hidden__ = {'context'}
    context: parsing.ParserContext = None


class RootNode(Base):
    children: list


class IndirectionNode(Base):
    arg: Base = None
    indirection: list


class IndexNode(Base):
    index: Base = None


class SliceNode(Base):
    start: Base = None
    stop: Base = None


class ArgListNode(Base):
    name: str = None
    args: list


class BinOpNode(Base):
    left: object
    op: str = None
    right: object


class WindowSpecNode(Base):
    orderby: list
    partition: list


class NamedArgNode(Base):
    name: str
    arg: object


class FunctionCallNode(Base):
    func: object  # tuple or str
    args: list
    agg_sort: list
    agg_filter: object
    window: object


class VarNode(Base):
    name: str = None


class PathVarNode(VarNode):
    pass


class ConstantNode(Base):
    value: object
    index: object


class DefaultValueNode(Base):
    pass


class UnaryOpNode(Base):
    op: str = None
    operand: Base = None


class PostfixOpNode(Base):
    op: str = None
    operand: Base = None


class PathNode(Base):
    steps: list
    quantifier: Base = None
    pathspec: list


class PathDisjunctionNode(Base):
    left: Base = None
    right: Base = None


class PathStepNode(Base):
    namespace: str = None
    expr: object  # str or LinkNode
    link_expr: object


class LinkNode(Base):
    name: str = None
    namespace: str = None
    direction: str = None
    target: Base = None
    type: str = None


class LinkExprNode(Base):
    expr: Base = None


class LinkPropExprNode(Base):
    expr: Base = None


class StatementNode(Base):
    namespaces: list
    aliases: list


class ClassRefNode(Base):
    name: str = None
    module: str = None


class PositionNode(Base):
    ref: str = None
    position: str = None


class ExpressionTextNode(Base):
    expr: Base = None


class TypeNameNode(Base):
    maintype: Base = None
    subtypes: list


class TypeCastNode(Base):
    expr: Base = None
    type: TypeNameNode = None


class TypeInterpretationNode(Base):
    expr: Base = None
    type: TypeNameNode = None


class IfElseNode(Base):
    condition: Base = None
    ifexpr: Base = None
    elseexpr: Base = None


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
    name: ClassRefNode = None
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
    new_name: ClassRefNode = None


class DeltaNode:
    pass


class CreateDeltaNode(CreateObjectNode, DeltaNode):
    parents: list
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
    type: TypeNameNode = None


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
    target: Base = None


class AlterConcreteLinkPropertyNode(AlterObjectNode):
    pass


class DropConcreteLinkPropertyNode(AlterObjectNode):
    pass


class SetSpecialFieldNode(Base):
    name: str = None
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
    event: ClassRefNode = None
    actions: list


class AlterLocalPolicyNode(CompositeDDLNode):
    event: ClassRefNode = None
    actions: list


class DropLocalPolicyNode(CompositeDDLNode):
    event: ClassRefNode = None


class CreateIndexNode(CreateObjectNode):
    expr: Base = None


class DropIndexNode(DropObjectNode):
    pass


class CreateAttributeValueNode(CreateObjectNode):
    value: Base = None
    as_expr: bool = False


class AlterAttributeValueNode(AlterObjectNode):
    value: Base


class DropAttributeValueNode(DropObjectNode):
    pass


class FuncArgNode(Base):
    name: str = None
    type: TypeNameNode = None
    mode: Base = None
    default: Base = None


class CreateFunctionNode(CreateObjectNode):
    args: list
    returning: Base = None
    single: bool = False
    aggregate: bool = False


class AlterFunctionNode(AlterObjectNode):
    value: Base = None


class DropFunctionNode(DropObjectNode):
    pass


class SelectQueryNode(StatementNode):
    single: bool = False
    distinct: bool = False
    targets: list
    where: Base = None
    groupby: list
    having: Base = None
    orderby: list
    offset: ConstantNode = None
    limit: ConstantNode = None
    _hash: tuple = None
    cges: list
    op: str = None
    op_larg: Base = None
    op_rarg: Base = None


class InsertQueryNode(StatementNode):
    subject: Base = None
    pathspec: list
    targets: list
    cges: list
    single: bool = False


class UpdateQueryNode(StatementNode):
    subject: Base = None
    pathspec: list
    where: Base = None
    targets: list
    cges: list
    single: bool = False


class UpdateExprNode(Base):
    expr: Base = None
    value: Base = None


class DeleteQueryNode(StatementNode):
    subject: Base = None
    where: Base = None
    targets: list
    cges: list
    single: bool = False


class CGENode(Base):
    expr: Base = None
    alias: str = None


class NamespaceAliasDeclNode(Base):
    namespace: str = None
    alias: object


class ExpressionAliasDeclNode(Base):
    expr: Base = None
    alias: object


class DetachedPathDeclNode(Base):
    expr: Base = None
    alias: str = None


class SortExprNode(Base):
    path: Base = None
    direction: str = None
    nones_order: object


class PredicateNode(Base):
    expr: Base = None


class ExistsPredicateNode(PredicateNode):
    pass


class SelectExprNode(Base):
    expr: Base = None


class SelectPathSpecNode(Base):
    expr: Base = None
    pathspec: list
    where: Base = None
    orderby: list
    offset: ConstantNode = None
    limit: ConstantNode = None
    compexpr: Base = None
    recurse: bool = False
    recurse_limit: ConstantNode = None


class PointerGlobNode(Base):
    filters: list
    type: ClassRefNode


class PointerGlobFilter(Base):
    property: Base = None
    value: object
    any: bool = False


class FromExprNode(Base):
    expr: Base = None
    alias: Base = None


class SequenceNode(Base):
    elements: list


class ArrayNode(Base):
    elements: list


class MappingNode(Base):
    items: list


class NoneTestNode(Base):
    expr: Base = None


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
