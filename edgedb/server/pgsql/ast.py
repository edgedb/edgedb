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
    __fields = ['name', 'schema', 'concept', 'alias', '#_bonds']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        if not self._bonds:
            self._bonds = {}

    def bonds(self, key):
        return self._bonds[key]

    def addbond(self, key, bond):
        if key not in self._bonds:
            self._bonds[key] = [bond]
        else:
            self._bonds[key].append(bond)

        return bond

    def updatebonds(self, node):
        for key, values in node._bonds.items():
            if key not in self._bonds:
                self._bonds[key] = values
            else:
                self._bonds[key].extend(values)


class SelectQueryNode(TableNode):
    __fields = ['distinct', '*fromlist', '*targets', 'where', '*orderby', '*ctes',
                '_source_graph', '#concept_node_map']

class CTENode(SelectQueryNode):
    __fields = ['*_referrers']

class CTEAttrRefNode(AST): __fields = ['cte', 'attr']

class JoinNode(TableNode):
    __fields = ['left', 'right', 'condition', 'type']

class ExistsNode(AST): __fields = ['expr']

class FieldRefNode(AST): __fields = ['table', 'field']

class SequenceNode(AST): __fields = ['*elements']

class SortExprNode(AST): __fields = ['expr', 'direction']

class FunctionCallNode(AST): __fields = ['name', '*args']
