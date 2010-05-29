##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix import SemantixError
from semantix.utils.ast import codegen

from . import ast as caosql_ast


class CaosQLSourceGeneratorError(SemantixError):
    pass


class CaosQLSourceGenerator(codegen.SourceGenerator):
    def generic_visit(self, node):
        raise CaosQLSourceGeneratorError('No method to generate code for %s' % node.__class__.__name__)

    def visit_BinOpNode(self, node):
        self.write('(')
        self.visit(node.left)
        self.write(' ' + str(node.op).upper() + ' ')
        self.visit(node.right)
        self.write(')')

    def visit_SequenceNode(self, node):
        self.write('(')
        count = len(node.elements)
        for i, e in enumerate(node.elements):
            self.visit(e)
            if i != count - 1:
                self.write(', ')

        self.write(')')

    def visit_PathNode(self, node):
        for i, e in enumerate(node.steps):
            if i > 0:
                if isinstance(e, caosql_ast.LinkPropExprNode):
                    self.write('@')
                else:
                    self.write('.')
            self.visit(e)

    def visit_PathStepNode(self, node):
        if node.namespace:
            self.write('[%s.%s]' % (node.namespace, node.expr))
        else:
            self.write(node.expr)

    def visit_LinkExprNode(self, node):
        self.visit(node.expr)

    def visit_LinkPropExprNode(self, node):
        self.visit(node.expr)

    def visit_LinkNode(self, node):
        if node.namespace:
            self.write('[%s.%s]' % (node.namespace, node.name))
        else:
            self.write(node.expr)
