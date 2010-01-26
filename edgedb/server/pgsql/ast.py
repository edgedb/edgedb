from semantix import ast

class ArgListNode(ast.AST): __fields = ['name', '*args']
class BinOpNode(ast.AST):  __fields = ['left', 'op', 'right']
class CallFunctionNode(ast.AST):  __fields = ['func', '*args']

class VarNode(ast.AST): __fields = ['name']
class PathVarNode(VarNode): pass
class ConstantNode(ast.AST): __fields = ['value']

class UnaryOpNode(ast.AST): __fields = ['op', 'operand']

class PredicateNode(ast.AST): __fields = ['*expr']

class SelectExprNode(ast.AST): __fields = ['expr', 'alias']

class FromExprNode(ast.AST): __fields = ['expr', 'alias']

class TableNode(ast.AST):
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

class CTEAttrRefNode(ast.AST): __fields = ['cte', 'attr']

class JoinNode(TableNode):
    __fields = ['left', 'right', 'condition', 'type']

class ExistsNode(ast.AST): __fields = ['expr']

class FieldRefNode(ast.AST): __fields = ['table', 'field']

class SequenceNode(ast.AST): __fields = ['*elements']

class SortExprNode(ast.AST): __fields = ['expr', 'direction']

class FunctionCallNode(ast.AST): __fields = ['name', '*args']
