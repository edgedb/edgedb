##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos.ir import ast as irast
from metamagic.utils import ast


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
                'window']

class VarNode(ast.AST): __fields = ['name']
class PathVarNode(VarNode): pass
class ConstantNode(ast.AST): __fields = ['value', 'index']

class UnaryOpNode(ast.AST): __fields = ['op', 'operand']

class PostfixOpNode(ast.AST): __fields = ['op', 'operand']

class PathNode(ast.AST): __fields = [('steps', list), 'quantifier', 'var', 'lvar', 'pathspec']

class PathDisjunctionNode(ast.AST): __fields = ['left', 'right']

class PathStepNode(ast.AST): __fields = ['namespace', 'expr', 'link_expr']

class TypeIndirection(ast.AST): pass

class LinkNode(ast.AST): __fields = ['name', 'namespace', 'direction', 'target', 'type']

class LinkExprNode(ast.AST): __fields = ['expr']

class LinkPropExprNode(ast.AST): __fields = ['expr']

class StatementNode(ast.AST):
    pass

class SelectQueryNode(StatementNode):
    __fields = ['namespaces', 'distinct', ('targets', list), 'where',
                ('groupby', list), ('orderby', list), 'offset', 'limit',
                '_hash', ('cges', list), 'op', 'op_larg', 'op_rarg']

class UpdateQueryNode(StatementNode):
    __fields = ['namespaces', 'subject', 'where', ('values', list),
                ('targets', list), ('cges', list)]

class UpdateExprNode(ast.AST):
    __fields = ['expr', 'value']

class DeleteQueryNode(StatementNode):
    __fields = ['namespaces', 'subject', 'where',
                ('targets', list), ('cges', list)]

class SubqueryNode(ast.AST):
    __fields = ['expr']

class CGENode(ast.AST):
    __fields = ['expr', 'alias']

class NamespaceDeclarationNode(ast.AST): __fields = ['namespace', 'alias']

class SortExprNode(ast.AST): __fields = ['path', 'direction', 'nones_order']

class PredicateNode(ast.AST): __fields = ['expr']

class ExistsPredicateNode(PredicateNode): pass

class SelectExprNode(ast.AST): __fields = ['expr', 'alias']

class SelectPathSpecNode(ast.AST):
    __fields = ['expr', 'pathspec', 'recurse', 'where', 'orderby', 'offset',
                'limit']

class SelectTypeRefNode(ast.AST):
    __fields = ['attrs']

class PointerGlobNode(ast.AST): __fields = ['filters', 'type']

class PointerGlobFilter(ast.AST): __fields = ['property', 'value', 'any']

class FromExprNode(ast.AST): __fields = ['expr', 'alias']

class SequenceNode(ast.AST): __fields = [('elements', list)]

class PrototypeRefNode(ast.AST): __fields = ['name', 'module']

class TypeCastNode(ast.AST): __fields = ['expr', 'type']

class TypeRefNode(ast.AST): __fields = ['expr']

class NoneTestNode(ast.AST): __fields = ['expr']

class CaosQLOperator(ast.ops.Operator):
    pass

class CaosQLMatchOperator(CaosQLOperator, irast.CaosMatchOperator):
    pass

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


class SortOrder(irast.SortOrder):
    _map = {
        irast.SortAsc: 'SortAsc',
        irast.SortDesc: 'SortDesc',
        irast.SortDefault: 'SortDefault'
    }

SortAsc = SortOrder(irast.SortAsc)
SortDesc = SortOrder(irast.SortDesc)
SortDefault = SortOrder(irast.SortDefault)


class NonesOrder(irast.NonesOrder):
    _map = {
        irast.NonesFirst: 'NonesFirst',
        irast.NonesLast: 'NonesLast'
    }

NonesFirst = NonesOrder(irast.NonesFirst)
NonesLast = NonesOrder(irast.NonesLast)


class SetOperator(CaosQLOperator):
    pass

UNION = SetOperator('UNION')
INTERSECT = SetOperator('INTERSECT')
EXCEPT = SetOperator('EXCEPT')
