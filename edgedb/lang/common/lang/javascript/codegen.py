##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.ast import SourceGenerator
from semantix.utils.lang.javascript import ast

class JavascriptSourceGenerator(SourceGenerator):

    def _visit_list(self, list):
        # goes through a list and visits each member printing ','
        for i, var in enumerate(list):
            if var:
                self.visit(var)
            if i != (len(list) - 1):
                self.write(', ')

    def visit_ProgramNode(self, node):
        for el in node.code:
            self.visit(el)

    def visit_StatementNode(self, node):
        self.newline()
        if node.statement:
            self.visit(node.statement)
        self.write(';')

    def visit_VarDeclarationNode(self, node):
        self.write('var ')
        self._visit_list(node.vars)

    def visit_VarInitNode(self, node):
        self.write(node.name)
        if node.value:
            self.write(' = ')
            self.visit(node.value)

    def visit_StringLiteralNode(self, node):
        self.write('"')
        self.write(node.value)
        self.write('"')

    def visit_NumericLiteralNode(self, node):
        self.write(str(node.value))

    def visit_BooleanLiteralNode(self, node):
        self.write(node.value and 'true' or 'false')

    def visit_ArrayLiteralNode(self, node):
        self.write('[')
        self._visit_list(node.array)
        self.write(']')

    def visit_ObjectLiteralNode(self, node):
        self.write('{')
        self._visit_list(node.properties)
        self.write('}')

    def visit_IDNode(self, node):
        self.write(node.name)

    def visit_ThisNode(self, node):
        self.write("this")

    def visit_NullNode(self, node):
        self.write("null")

    def visit_ParenthesisNode(self, node):
        self.write('(')
        self._visit_list(node.expression)
        self.write(')')

    def visit_ExpressionListNode(self, node):
        self._visit_list(node.expressions)

    def visit_PrefixExpressionNode(self, node):
        self.write(node.op)
        self.visit(node.expression)

    def visit_PostfixExpressionNode(self, node):
        self.visit(node.expression)
        self.write(node.op)

    def visit_BinExpressionNode(self, node):
        self.visit(node.left)
        self.write(node.op)
        self.visit(node.right)

    def visit_CallNode(self, node):
        self.visit(node.call)
        self.write('(')
        self._visit_list(node.arguments)
        self.write(')')

    def visit_NewNode(self, node):
        self.write('new ')
        self.visit(node.expression)
        if node.arguments:
            self.visit(node.arguments)

    def visit_SBracketExpresionNode(self, node):
        self.visit(node.list)
        self.write('[')
        self._visit_list(node.element)
        self.write(']')

    def visit_DeleteNode(self, node):
        self.write('delete ')
        self.visit(node.expression)

    def visit_VoidNode(self, node):
        self.write('void ')
        self.visit(node.expression)

    def visit_TypeOfNode(self, node):
        self.write('typeof ')
        self.visit(node.expression)

    def visit_SimplePropertyNode(self, node):
        self.visit(node.name)
        self.write(" : ")
        self.visit(node.value)

    def visit_GetPropertyNode(self, node):
        self.write("get ")
        self.visit(node.name)
        self.write("() ")
        self.visit(node.function)

    def visit_SetPropertyNode(self, node):
        self.write("set ")
        self.visit(node.name)
        self.write("(")
        self.visit(node.param)
        self.write(") ")
        self.visit(node.function)

    def visit_FunctionBodyNode(self, node):
        self.write("{\n")
        if node.body:
            self.visit(node.body)
        self.write("}\n")
