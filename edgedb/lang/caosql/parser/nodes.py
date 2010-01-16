from semantix.ast import *

class RootNode(AST): __fields = ['children']

class ArgListNode(AST): __fields = ['name', '*args']
class BinOpNode(AST):  __fields = ['left', 'op', 'right']
class CallFunctionNode(AST): __fields = ['func', '*args']

class VarNode(AST): __fields = ['name']
class PathVarNode(VarNode): pass
class ConstantNode(AST): __fields = ['value']

class UnaryOpNode(AST): __fields = ['op', 'operand']

class PathNode(AST): __fields = ['*steps', 'quantifier', 'var']

class PathDisjunctionNode(AST): __fields = ['left', 'right']

class PathStepNode(AST): __fields = ['namespace', 'expr', 'link_expr']

class LinkNode(AST): __fields = ['name', 'namespace', 'direction']

class LinkExprNode(AST): __fields = ['expr']

class SelectQueryNode(AST): __fields = ['namespaces', 'distinct', '*targets', 'where', '*orderby']

class NamespaceDeclarationNode(AST): __fields = ['namespace', 'alias']

class SortExprNode(AST): __fields = ['path', 'direction']

class PredicateNode(AST): __fields = ['*expr']

class ExistsPredicateNode(PredicateNode): pass

class SelectExprNode(AST): __fields = ['expr', 'alias']

class FromExprNode(AST): __fields = ['expr', 'alias']

class SequenceNode(AST): __fields = ['*elements']
