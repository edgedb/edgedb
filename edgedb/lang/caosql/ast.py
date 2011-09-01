##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import ast
from semantix.utils.datastructures import StrSingleton


class RootNode(ast.AST): __fields = ['children']

class ArgListNode(ast.AST): __fields = ['name', ('args', list)]
class BinOpNode(ast.AST):  __fields = ['left', 'op', 'right']
class FunctionCallNode(ast.AST): __fields = ['func', ('args', list)]

class VarNode(ast.AST): __fields = ['name']
class PathVarNode(VarNode): pass
class ConstantNode(ast.AST): __fields = ['value', 'index']

class UnaryOpNode(ast.AST): __fields = ['op', 'operand']

class PostfixOpNode(ast.AST): __fields = ['op', 'operand']

class PathNode(ast.AST): __fields = [('steps', list), 'quantifier', 'var', 'lvar']

class PathDisjunctionNode(ast.AST): __fields = ['left', 'right']

class PathStepNode(ast.AST): __fields = ['namespace', 'expr', 'link_expr']

class LinkNode(ast.AST): __fields = ['name', 'namespace', 'direction']

class LinkExprNode(ast.AST): __fields = ['expr']

class LinkPropExprNode(ast.AST): __fields = ['expr']

class SelectQueryNode(ast.AST):
    __fields = ['namespaces', 'distinct', ('targets', list), 'where', ('groupby', list),
                ('orderby', list), 'offset', 'limit', '_hash']

class NamespaceDeclarationNode(ast.AST): __fields = ['namespace', 'alias']

class SortExprNode(ast.AST): __fields = ['path', 'direction', 'nones_order']

class PredicateNode(ast.AST): __fields = ['expr']

class ExistsPredicateNode(PredicateNode): pass

class SelectExprNode(ast.AST): __fields = ['expr', 'alias']

class FromExprNode(ast.AST): __fields = ['expr', 'alias']

class SequenceNode(ast.AST): __fields = [('elements', list)]

class PrototypeRefNode(ast.AST): __fields = ['name', 'module']

class TypeCastNode(ast.AST): __fields = ['expr', 'type']


class SortOrder(StrSingleton):
    _map = {
        'ASC': 'SortAsc',
        'DESC': 'SortDesc',
        'SORT_DEFAULT': 'SortDefault'
    }

SortAsc = SortOrder('ASC')
SortDesc = SortOrder('DESC')
SortDefault = SortAsc


class NonesOrder(StrSingleton):
    _map = {
        'NONES_FIRST': 'NonesFirst',
        'NONES_LAST': 'NonesLast',
        'NONES_DEFAULT': 'NonesDefault'
    }

NonesFirst = NonesOrder('NONES_FIRST')
NonesLast = NonesOrder('NONES_LAST')
NonesDefault = NonesOrder('NONES_DEFAULT')


class CaosQLOperator(ast.ops.Operator):
    pass


LIKE = CaosQLOperator('~~')
NOT_LIKE = CaosQLOperator('!~~')
ILIKE = CaosQLOperator('~~*')
NOT_ILIKE = CaosQLOperator('!~~*')
IS_OF = CaosQLOperator('IS OF')
IS_NOT_OF = CaosQLOperator('IS NOT OF')
