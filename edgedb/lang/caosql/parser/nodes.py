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

class PathStepNode(AST): __fields = ['expr', 'link_expr']

class LinkNode(AST): __fields = ['name', 'direction']

<<<<<<< HEAD
class SelectQueryNode(AST): __fields = ['distinct', '*fromlist', '*targets', 'where']
=======
class SelectQueryNode(AST): _fields = ['distinct', '*targets', 'where']
>>>>>>> 7b31f38... caosql: Work-in-progress

class PredicateNode(AST): __fields = ['*expr']

class ExistsPredicateNode(PredicateNode): pass

class SelectExprNode(AST): __fields = ['expr', 'alias']

class FromExprNode(AST): __fields = ['expr', 'alias']
