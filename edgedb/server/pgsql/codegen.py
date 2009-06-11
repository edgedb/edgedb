from semantix.ast import codegen
from psycopg2.extensions import adapt as dbadapt

class SQLSourceGeneratorError(Exception): pass

class SQLSourceGenerator(codegen.SourceGenerator):
    def generic_visit(self, node):
        raise SQLSourceGeneratorError('No method to generate code for %s' % node.__class__.__name__)

    def visit_SelectQueryNode(self, node):
        self.new_lines = 1

        self.write('(SELECT')
        if node.distinct is not None:
            self.write(' DISTINCT')

        self.new_lines = 1
        self.indentation += 2

        count = len(node.targets)
        for i, target in enumerate(node.targets):
            self.new_lines = 1
            self.visit(target)
            if i != count -1:
                self.write(',')

        self.indentation -= 2

        if node.fromlist:
            self.indentation += 1
            self.new_lines = 1
            self.write('FROM')
            self.new_lines = 1
            self.indentation += 1
            count = len(node.fromlist)
            for i, source in enumerate(node.fromlist):
                self.new_lines = 1
                self.visit(source)
                if i != count - 1:
                    self.write(',')

            self.indentation -= 2

        if node.where:
            self.indentation += 1
            self.new_lines = 1
            self.write('WHERE')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.where)
            self.indentation -= 2

        if node.orderby:
            self.indentation += 1
            self.new_lines = 1
            self.write('ORDER BY')
            self.new_lines = 1
            self.indentation += 1
            count = len(node.orderby)
            for i, sortexpr in enumerate(node.orderby):
                self.new_lines = 1
                self.visit(sortexpr)
                if i != count - 1:
                    self.write(',')
            self.indentation -= 2

        self.new_lines = 1
        self.write(')')

        if node.alias:
            self.write(' AS ' + node.alias)

    def visit_SelectExprNode(self, node):
        self.visit(node.expr)
        if node.alias:
            self.write(' AS ' + node.alias)

    def visit_FieldRefNode(self, node):
        self.write(node.table.alias + '.' + node.field)

    def visit_FromExprNode(self, node):
        self.visit(node.expr)
        if node.alias:
            self.write(' AS ' + node.alias)

    def visit_JoinNode(self, node):
        self.visit(node.left)
        self.new_lines = 1
        self.write(node.type.upper() + ' JOIN ')
        self.visit(node.right)
        self.write(' ON ')
        self.visit(node.condition)

    def visit_TableNode(self, node):
        self.write('caos.' + node.name)
        if node.alias:
            self.write(' AS ' + node.alias)

    def visit_BinOpNode(self, node):
        self.write('(')
        self.visit(node.left)
        self.write(' ' + node.op.upper() + ' ')
        self.visit(node.right)
        self.write(')')

    def visit_PredicateNode(self, node):
        self.visit(node.expr)

    def visit_ConstantNode(self, node):
        self.write(str(dbadapt(node.value)))

    def visit_SequenceNode(self, node):
        self.write('(')
        count = len(node.elements)
        for i, e in enumerate(node.elements):
            self.visit(e)
            if i != count - 1:
                self.write(', ')

        self.write(')')

    def visit_FunctionCallNode(self, node):
        self.write(node.name)
        self.write('(')
        count = len(node.args)
        for i, e in enumerate(node.args):
            self.visit(e)
            if i != count - 1:
                self.write(', ')

        self.write(')')

    def visit_SortExprNode(self, node):
        self.visit(node.expr)
        if node.direction:
            self.write(' ' + node.direction)
