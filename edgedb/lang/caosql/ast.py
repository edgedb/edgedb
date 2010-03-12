##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix import ast

class RootNode(ast.AST): __fields = ['children']

class ArgListNode(ast.AST): __fields = ['name', ('args', list)]
class BinOpNode(ast.AST):  __fields = ['left', 'op', 'right']
class FunctionCallNode(ast.AST): __fields = ['func', ('args', list)]

class VarNode(ast.AST): __fields = ['name']
class PathVarNode(VarNode): pass
class ConstantNode(ast.AST): __fields = ['value']

class UnaryOpNode(ast.AST): __fields = ['op', 'operand']

class PathNode(ast.AST): __fields = [('steps', list), 'quantifier', 'var']

class PathDisjunctionNode(ast.AST): __fields = ['left', 'right']

class PathStepNode(ast.AST): __fields = ['namespace', 'expr', 'link_expr']

class LinkNode(ast.AST): __fields = ['name', 'namespace', 'direction']

class LinkExprNode(ast.AST): __fields = ['expr']

class SelectQueryNode(ast.AST): __fields = ['namespaces', 'distinct', ('targets', list), 'where',
                                        ('orderby', list)]

class NamespaceDeclarationNode(ast.AST): __fields = ['namespace', 'alias']

class SortExprNode(ast.AST): __fields = ['path', 'direction']

class PredicateNode(ast.AST): __fields = ['expr']

class ExistsPredicateNode(PredicateNode): pass

class SelectExprNode(ast.AST): __fields = ['expr', 'alias']

class FromExprNode(ast.AST): __fields = ['expr', 'alias']

class SequenceNode(ast.AST): __fields = [('elements', list)]

class GraphObjectRefNode(ast.AST): __fields = ['name', 'module']
