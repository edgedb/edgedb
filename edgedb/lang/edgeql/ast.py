##
# Copyright (c) 2008-2012, 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import enum as s_enum

from edgedb.lang.common import ast, parsing


class Base(ast.AST):
    __fields = [('context', parsing.ParserContext, None,
                 True, None, True  # this last True is "hidden" attribute
                 )]


class RootNode(Base):
    __fields = ['children']


class IndirectionNode(Base):
    __fields = ['arg', 'indirection']


class IndexNode(Base):
    __fields = ['index']


class SliceNode(Base):
    __fields = ['start', 'stop']


class ArgListNode(Base):
    __fields = ['name', ('args', list)]


class BinOpNode(Base):
    __fields = ['left', 'op', 'right']


class WindowSpecNode(Base):
    __fields = [('orderby', list), ('partition', list)]


class NamedArgNode(Base):
    __fields = [('name', str), 'arg']


class FunctionCallNode(Base):
    __fields = ['func', ('args', list), ('agg_sort', list),
                'agg_filter', 'window']


class VarNode(Base):
    __fields = ['name']


class PathVarNode(VarNode):
    pass


class ConstantNode(Base):
    __fields = ['value', 'index']


class DefaultValueNode(Base):
    pass


class UnaryOpNode(Base):
    __fields = ['op', 'operand']


class PostfixOpNode(Base):
    __fields = ['op', 'operand']


class PathNode(Base):
    __fields = [('steps', list), 'quantifier', 'pathspec']


class PathDisjunctionNode(Base):
    __fields = ['left', 'right']


class PathStepNode(Base):
    __fields = ['namespace', 'expr', 'link_expr']


class TypeIndirection(Base):
    pass


class LinkNode(Base):
    __fields = ['name', 'namespace', 'direction', 'target', 'type']


class LinkExprNode(Base):
    __fields = ['expr']


class LinkPropExprNode(Base):
    __fields = ['expr']


class StatementNode(Base):
    __fields = [('namespaces', list), ('aliases', list)]


class PrototypeRefNode(Base):
    __fields = ['name', 'module']


class PositionNode(Base):
    __fields = ['ref', 'position']


class ExpressionTextNode(Base):
    __fields = ['expr']


class DDLNode(Base):
    pass


class CompositeDDLNode(StatementNode, DDLNode):
    pass


class AlterSchemaNode(Base):
    __fields = ['commands']


class AlterAddInheritNode(DDLNode):
    __fields = ['bases', 'position']


class AlterDropInheritNode(DDLNode):
    __fields = ['bases']


class AlterTargetNode(DDLNode):
    __fields = ['targets']


class ObjectDDLNode(CompositeDDLNode):
    __fields = ['namespaces', ('name', PrototypeRefNode), ('commands', list)]


class CreateObjectNode(ObjectDDLNode):
    pass


class AlterObjectNode(ObjectDDLNode):
    pass


class DropObjectNode(ObjectDDLNode):
    pass


class CreateInheritingObjectNode(CreateObjectNode):
    __fields = ['bases', 'is_abstract', 'is_final']


class RenameNode(DDLNode):
    __fields = [('new_name', PrototypeRefNode)]


class DeltaNode:
    pass


class CreateDeltaNode(CreateObjectNode, DeltaNode):
    __fields = [('parents', list), 'target']


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
    __fields = ['type']


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
    __fields = ['is_required', 'target']


class AlterConcreteLinkPropertyNode(AlterObjectNode):
    pass


class DropConcreteLinkPropertyNode(AlterObjectNode):
    pass


class SetSpecialFieldNode(Base):
    __fields = ['name', 'value', ('as_expr', bool)]


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
    __fields = ['is_required', 'targets']


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
    __fields = ['args', 'is_abstract']


class AlterConcreteConstraintNode(AlterObjectNode):
    pass


class DropConcreteConstraintNode(DropObjectNode):
    pass


class CreateLocalPolicyNode(CompositeDDLNode):
    __fields = ['event', 'actions']


class AlterLocalPolicyNode(CompositeDDLNode):
    __fields = ['event', 'actions']


class DropLocalPolicyNode(CompositeDDLNode):
    __fields = ['event']


class CreateIndexNode(CreateObjectNode):
    __fields = ['expr']


class DropIndexNode(DropObjectNode):
    pass


class CreateAttributeValueNode(CreateObjectNode):
    __fields = ['value', ('as_expr', bool)]


class AlterAttributeValueNode(AlterObjectNode):
    __fields = ['value']


class DropAttributeValueNode(DropObjectNode):
    pass


class FuncArgNode(Base):
    __fields = ['name', 'mode', 'default']


class CreateFunctionNode(CreateObjectNode):
    __fields = ['args', 'returning']


class AlterFunctionNode(AlterObjectNode):
    __fields = ['value']


class DropFunctionNode(DropObjectNode):
    pass


class SelectQueryNode(StatementNode):
    __fields = ['distinct', ('targets', list), 'where',
                ('groupby', list), ('orderby', list), 'offset', 'limit',
                '_hash', ('cges', list), 'op', 'op_larg', 'op_rarg']


class InsertQueryNode(StatementNode):
    __fields = ['subject', ('pathspec', list),
                ('targets', list), ('cges', list)]


class UpdateQueryNode(StatementNode):
    __fields = ['subject', ('pathspec', list), 'where',
                ('targets', list), ('cges', list)]


class UpdateExprNode(Base):
    __fields = ['expr', 'value']


class DeleteQueryNode(StatementNode):
    __fields = ['subject', 'where',
                ('targets', list), ('cges', list)]


class SubqueryNode(Base):
    __fields = ['expr']


class CGENode(Base):
    __fields = ['expr', 'alias']


class NamespaceAliasDeclNode(Base):
    __fields = ['namespace', 'alias']


class ExpressionAliasDeclNode(Base):
    __fields = ['expr', 'alias']


class SortExprNode(Base):
    __fields = ['path', 'direction', 'nones_order']


class PredicateNode(Base):
    __fields = ['expr']


class ExistsPredicateNode(PredicateNode):
    pass


class SelectExprNode(Base):
    __fields = ['expr', 'alias']


class SelectPathSpecNode(Base):
    __fields = ['expr', 'pathspec', 'recurse', 'where', 'orderby', 'offset',
                'limit', 'compexpr']


class SelectTypeRefNode(Base):
    __fields = ['attrs']


class PointerGlobNode(Base):
    __fields = ['filters', 'type']


class PointerGlobFilter(Base):
    __fields = ['property', 'value', 'any']


class FromExprNode(Base):
    __fields = ['expr', 'alias']


class SequenceNode(Base):
    __fields = [('elements', list)]


class MappingNode(Base):
    __fields = [('items', list)]


class TypeNameNode(Base):
    __fields = [('maintype', str), ('subtype', Base, None)]


class TypeCastNode(Base):
    __fields = ['expr', ('type', TypeNameNode)]


class TypeRefNode(Base):
    __fields = ['expr']


class NoneTestNode(Base):
    __fields = ['expr']


class EdgeQLOperator(ast.ops.Operator):
    pass


class TextSearchOperator(EdgeQLOperator):
    pass

SEARCH = TextSearchOperator('@@')
SEARCHEX = TextSearchOperator('@@!')


class EdgeQLComparisonOperator(EdgeQLOperator, ast.ops.ComparisonOperator):
    pass


class EdgeQLMatchOperator(EdgeQLComparisonOperator):
    pass


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
LIKE = EdgeQLMatchOperator('~~')
NOT_LIKE = EdgeQLMatchOperator('!~~')
ILIKE = EdgeQLMatchOperator('~~*')
NOT_ILIKE = EdgeQLMatchOperator('!~~*')

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
