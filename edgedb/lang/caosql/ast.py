##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import enum as s_enum

from edgedb.lang.common import ast


class RootNode(ast.AST): __fields = ['children']

class IndirectionNode(ast.AST):
    __fields = ['arg', 'indirection']

class IndexNode(ast.AST):
    __fields = ['index']

class SliceNode(ast.AST):
    __fields = ['start', 'stop']

class ArgListNode(ast.AST): __fields = ['name', ('args', list)]
class BinOpNode(ast.AST):  __fields = ['left', 'op', 'right']

class WindowSpecNode(ast.AST):
    __fields = [('orderby', list), ('partition', list)]

class NamedArgNode(ast.AST):
    __fields = [('name', str), 'arg']

class FunctionCallNode(ast.AST):
    __fields = ['func', ('args', list), ('agg_sort', list),
                'agg_filter', 'window']

class VarNode(ast.AST): __fields = ['name']
class PathVarNode(VarNode): pass
class ConstantNode(ast.AST): __fields = ['value', 'index']
class DefaultValueNode(ast.AST): pass

class UnaryOpNode(ast.AST): __fields = ['op', 'operand']

class PostfixOpNode(ast.AST): __fields = ['op', 'operand']

class PathNode(ast.AST): __fields = [('steps', list), 'quantifier', 'pathspec']

class PathDisjunctionNode(ast.AST): __fields = ['left', 'right']

class PathStepNode(ast.AST): __fields = ['namespace', 'expr', 'link_expr']

class TypeIndirection(ast.AST): pass

class LinkNode(ast.AST): __fields = ['name', 'namespace', 'direction', 'target', 'type']

class LinkExprNode(ast.AST): __fields = ['expr']

class LinkPropExprNode(ast.AST): __fields = ['expr']

class StatementNode(ast.AST):
    __fields = [('namespaces', list), ('aliases', list)]

class PrototypeRefNode(ast.AST):
    __fields = ['name', 'module']


class PositionNode(ast.AST):
    __fields = ['ref', 'position']


class ExpressionTextNode(ast.AST):
    __fields = ['expr']


class DDLNode(ast.AST):
    pass

class CompositeDDLNode(StatementNode, DDLNode):
    pass


class AlterSchemaNode(ast.AST):
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


class CreateDatabaseNode(CreateObjectNode):
    pass


class DropDatabaseNode(DropObjectNode):
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
    __fields = ['type', 'constraint']


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


class SetSpecialFieldNode(ast.AST):
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

class UpdateExprNode(ast.AST):
    __fields = ['expr', 'value']

class DeleteQueryNode(StatementNode):
    __fields = ['subject', 'where',
                ('targets', list), ('cges', list)]

class SubqueryNode(ast.AST):
    __fields = ['expr']

class CGENode(ast.AST):
    __fields = ['expr', 'alias']

class NamespaceAliasDeclNode(ast.AST):
    __fields = ['namespace', 'alias']

class ExpressionAliasDeclNode(ast.AST):
    __fields = ['expr', 'alias']

class SortExprNode(ast.AST): __fields = ['path', 'direction', 'nones_order']

class PredicateNode(ast.AST): __fields = ['expr']

class ExistsPredicateNode(PredicateNode): pass

class SelectExprNode(ast.AST): __fields = ['expr', 'alias']

class SelectPathSpecNode(ast.AST):
    __fields = ['expr', 'pathspec', 'recurse', 'where', 'orderby', 'offset',
                'limit', 'compexpr']

class SelectTypeRefNode(ast.AST):
    __fields = ['attrs']

class PointerGlobNode(ast.AST): __fields = ['filters', 'type']

class PointerGlobFilter(ast.AST): __fields = ['property', 'value', 'any']

class FromExprNode(ast.AST): __fields = ['expr', 'alias']

class SequenceNode(ast.AST): __fields = [('elements', list)]

class MappingNode(ast.AST):
    __fields = [('items', list)]

class TypeNameNode(ast.AST):
    __fields = [('maintype', str), ('subtype', ast.AST, None)]

class TypeCastNode(ast.AST): __fields = ['expr', ('type', TypeNameNode)]

class TypeRefNode(ast.AST): __fields = ['expr']

class NoneTestNode(ast.AST): __fields = ['expr']

class CaosQLOperator(ast.ops.Operator):
    pass

class TextSearchOperator(CaosQLOperator):
    pass

SEARCH = TextSearchOperator('@@')
SEARCHEX = TextSearchOperator('@@!')


class CaosQLComparisonOperator(CaosQLOperator, ast.ops.ComparisonOperator):
    pass


class CaosQLMatchOperator(CaosQLComparisonOperator):
    pass


class SetOperator(CaosQLOperator):
    pass

UNION = SetOperator('UNION')
INTERSECT = SetOperator('INTERSECT')
EXCEPT = SetOperator('EXCEPT')


AND = ast.ops.AND
OR = ast.ops.OR
NOT = ast.ops.NOT
IN = ast.ops.IN
NOT_IN = ast.ops.NOT_IN
LIKE = CaosQLMatchOperator('~~')
NOT_LIKE = CaosQLMatchOperator('!~~')
ILIKE = CaosQLMatchOperator('~~*')
NOT_ILIKE = CaosQLMatchOperator('!~~*')

REMATCH = CaosQLMatchOperator('~')
REIMATCH = CaosQLMatchOperator('~*')

RENOMATCH = CaosQLMatchOperator('!~')
RENOIMATCH = CaosQLMatchOperator('!~*')

IS_OF = CaosQLOperator('IS OF')
IS_NOT_OF = CaosQLOperator('IS NOT OF')


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


class SetOperator(CaosQLOperator):
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
