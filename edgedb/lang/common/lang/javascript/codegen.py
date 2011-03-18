##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.ast import SourceGenerator
from semantix.utils.lang.javascript import ast


class JavascriptSourceGenerator(SourceGenerator):
    def visit_ProgramNode(self, node):
        for el in node.code:
            self.visit(el)

    def visit_StatementNode(self, node):
        self.newline()
        self.visit(node.statement)
        self.write(';')

    def visit_VarDeclarationNode(self, node):
        self.write('var ')
        for i, var in enumerate(node.vars):
            self.visit(var)
            if i != (len(node.vars) - 1):
                self.write(', ')

    def visit_VarInitNode(self, node):
        self.write(node.name)
        if node.value is not None:
            self.write(' = ')
            self.visit(node.value)

    def visit_LiteralNode(self, node):
        #XXX
        self.write(str(node.value))

    def visit_IDNode(self, node):
        self.write(node.name)

    def visit_BinExpressionNode(self, node):
        #XXX
        self.visit(node.left)
        self.write(node.op)
        self.visit(node.right)

    def visit_CallNode(self, node):
        #XXX
        self.visit(node.call)
        self.write('(')
        for i, var in enumerate(node.arguments):
            self.visit(var)
            if i != (len(node.arguments) - 1):
                self.write(', ')
        self.write(')')

