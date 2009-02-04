from semantix.ast import *

class ArgListNode(AST): __fields = ['name', '*args']
class BinOpNode(AST):  __fields = ['left', 'op', 'right']
class CallFunctionNode(AST):  __fields = ['func', '*args']

class VarNode(AST): __fields = ['name']
class PathVarNode(VarNode): pass
class ConstantNode(AST): __fields = ['value']

class UnaryOpNode(AST): __fields = ['op', 'operand']

class PredicateNode(AST): __fields = ['*expr']

class SelectExprNode(AST): __fields = ['expr', 'alias']

class FromExprNode(AST): __fields = ['expr', 'alias']

class TableNode(AST):
    __fields = ['name', 'concept', 'alias', '#_bonds']

class SelectQueryNode(TableNode):
    __fields = ['distinct', '*fromlist', '*targets', 'where', '*ctes', '_source_graph']

class CTENode(SelectQueryNode):
    __fields = ['*_referrers']

class CTEAttrRefNode(AST): __fields = ['cte', 'attr']

class JoinNode(TableNode):
    __fields = ['left', 'right', 'condition', 'type']

class ExistsNode(AST): __fields = ['expr']

class FieldRefNode(AST): __fields = ['table', 'field']
