##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.ast import SourceGenerator
from semantix.utils.lang.javascript import ast


def string_escape(string):
    # walk through the string and escape ', ", \, <newline>, tabs
    replace = {'"' : r'\"',
               "'" : r'\'',
               '\\': r'\\',
               '\n': r'\n',
               '\r': r'\r',
               '\t': r'\t',
               '\f': r'\f',
               '\b': r'\b',
               }

    return "".join([replace.get(char, char) for char in string])


class JavascriptSourceGenerator(SourceGenerator):
    def _visit_list(self, list):
        "goes through a list and visits each member printing ','"

        for i, var in enumerate(list):
            if var:
                self.visit(var)
            if i != (len(list) - 1):
                self.write(', ')

    def visit_StatementNode(self, node):
        if node.statement:
            self.visit(node.statement)
        self.write(';')
        self.newline()

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
        self.write(string_escape(node.value))
        self.write('"')

    def visit_NumericLiteralNode(self, node):
        self.write(str(node.value))

    def visit_BooleanLiteralNode(self, node):
        self.write(node.value and 'true' or 'false')

    def visit_ArrayLiteralNode(self, node):
        self.write('[')
        self._visit_list(node.array)

        # this is needed to properly render [,,,,] and similar array literals
        #
        if node.array and node.array[-1] == None:
            self.write(',')
        self.write(']')

    def visit_ObjectLiteralNode(self, node):
        if isinstance(node.parent, ast.StatementNode):
            self.write('(')
        self.write('{')
        self.indentation += 1
        self._visit_list(node.properties)
        self.indentation -= 1
        self.write('}')
        if isinstance(node.parent, ast.StatementNode):
            self.write(')')

    def visit_RegExpNode(self, node):
        self.write(node.regexp)

    def visit_IDNode(self, node):
        self.write(node.name)

    def visit_ThisNode(self, node):
        self.write("this")

    def visit_NullNode(self, node):
        self.write("null")

    def visit_ExpressionListNode(self, node):
        self.write('(')
        self._visit_list(node.expressions)
        self.write(')')

    def visit_PrefixExpressionNode(self, node):
        self.write(node.op)
        self.visit(node.expression)

    def visit_PostfixExpressionNode(self, node):
        self.visit(node.expression)
        self.write(node.op)

    def visit_BinExpressionNode(self, node):
        if isinstance(node.parent, ast.Expression):
            self.write('(')
        self.visit(node.left)
        self.write(' ' + node.op + ' ')
        self.visit(node.right)
        if isinstance(node.parent, ast.Expression):
            self.write(')')

    def visit_AssignmentExpressionNode(self, node):
        if isinstance(node.parent, ast.Expression):
            self.write('(')
        self.visit(node.left)
        self.write(' ' + node.op + ' ')
        if isinstance(node.parent, ast.Expression):
            self.write(')')
        self.visit(node.right)

    def visit_DotExpressionNode(self, node):
        self.visit(node.left)
        self.write('.')
        self.visit(node.right)

    def visit_ConditionalExpressionNode(self, node):
        if isinstance(node.parent, ast.Expression):
            self.write('(')
        self.visit(node.condition)
        self.write(' ? ')
        self.visit(node.true)
        self.write(' : ')
        self.visit(node.false)
        if isinstance(node.parent, ast.Expression):
            self.write(')')

    def visit_CallNode(self, node):
        self.visit(node.call)
        self.write('(')
        self._visit_list(node.arguments)
        self.write(')')

    def visit_NewNode(self, node):
        self.write('new ')
        self.visit(node.expression)
        if node.arguments:
            self.write('(')
            self._visit_list(node.arguments)
            self.write(')')

    def visit_SBracketExpressionNode(self, node):
        self.visit(node.list)
        self.write('[')
        self.visit(node.element)
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

    def visit_InstanceOfNode(self, node):
        if isinstance(node.parent, ast.Expression):
            self.write('(')
        self.visit(node.expression)
        self.write(' instanceof ')
        self.visit(node.type)
        if isinstance(node.parent, ast.Expression):
            self.write(')')

    def visit_InNode(self, node):
        if isinstance(node.parent, ast.Expression):
            self.write('(')
        self.visit(node.expression)
        self.write(' in ')
        self.visit(node.container)
        if isinstance(node.parent, ast.Expression):
            self.write(')')

    def visit_SimplePropertyNode(self, node):
        if len(node.parent.properties) > 1:
            self.newline()
        self.visit(node.name)
        self.write(" : ")
        self.visit(node.value)

    def visit_GetPropertyNode(self, node):
        if len(node.parent.properties) > 1:
            self.newline()
        self.write("get ")
        self.visit(node.name)
        self.write("() ")

        # We only want a newline at the end of function declaration
        #
        self.visit_StatementBlockNode(node.functionbody, endnewline=False)

    def visit_SetPropertyNode(self, node):
        if len(node.parent.properties) > 1:
            self.newline()
        self.write("set ")
        self.visit(node.name)
        self.write("(")
        self.visit(node.param)
        self.write(") ")

        # We only want a newline at the end of function declaration
        #
        self.visit_StatementBlockNode(node.functionbody, endnewline=False)

    def visit_FunctionNode(self, node):
        if isinstance(node.parent, ast.Expression):
            self.write("(")
        self.write("function ")
        if node.name:
            self.write(node.name)
        self.write("(")
        if node.param:
            self._visit_list(node.param)
        self.write(") ")

        # We only want a newline at the end of function declaration
        #
        make_newline = node.isdeclaration

        self.visit_StatementBlockNode(node.body, endnewline=make_newline)

        if isinstance(node.parent, ast.Expression):
            self.write(")")

        if make_newline:
            self.newline()

    def visit_SourceElementsNode(self, node):
        if node.code:
            for el in node.code:
                if el:
                    self.visit(el)

    def visit_StatementBlockNode(self, node, endnewline=True):
        self.write("{")
        self.indentation += 1
        self.newline()
        for statement in node.statements:
            if statement:
                self.visit(statement)
        self.indentation -= 1
        self.write("}")
        if endnewline:
            self.newline()

    def visit_IfNode(self, node):
        self.write("if (")
        self.visit(node.ifclause)
        self.write(") ")
        if node.thenclause:
            if isinstance(node.thenclause, ast.StatementBlockNode):
                self.visit_StatementBlockNode(node.thenclause,
                                              endnewline=not node.elseclause)
            else:
                self.visit(node.thenclause)
        else:
            self.write(";")
            self.newline()
        if node.elseclause:
            self.write(" else ")
            self.visit(node.elseclause)

    def visit_DoNode(self, node):
        self.write("do ")
        self.newline()
        if node.statement:
            self.visit(node.statement)
        else:
            self.write(";")
            self.newline()
        self.write("while (")
        self.visit(node.expression)
        self.write(");")
        self.newline()

    def visit_WhileNode(self, node):
        self.write("while (")
        self.visit(node.expression)
        self.write(") ")
        self.newline()
        if node.statement:
            self.visit(node.statement)
        else:
            self.write(";")
            self.newline()

    def visit_ForNode(self, node):
        self.write("for (")
        if node.part1:
            self.visit(node.part1)
        self.write('; ')
        if node.part2:
            self.visit(node.part2)
        self.write('; ')
        if node.part3:
            self.visit(node.part3)
        self.write(') ')
        if node.statement:
            self.visit(node.statement)
        else:
            self.write(";")
            self.newline()

    def visit_ForInNode(self, node):
        self.write("for (")
        self.visit(node.init)
        self.write(" in ")
        self.visit(node.array)
        self.write(") ")
        if node.statement:
            self.visit(node.statement)
        else:
            self.write(";")
            self.newline()

    def visit_WithNode(self, node):
        self.write("with (")
        self.visit(node.expression)
        self.write(") ")
        if node.statement:
            self.visit(node.statement)
        else:
            self.write(";")
            self.newline()

    def visit_ContinueNode(self, node):
        self.write("continue")
        if node.id:
            self.write(" " + node.id)
        self.write(";")
        self.newline()

    def visit_BreakNode(self, node):
        self.write("break")
        if node.id:
            self.write(" " + node.id)
        self.write(";")
        self.newline()

    def visit_ReturnNode(self, node):
        self.write("return")
        if node.expression:
            self.write(" ")
            self.visit(node.expression)
        self.write(";")
        self.newline()

    def visit_LabelNode(self, node):
        self.write(node.id)
        self.write(" : ")
        if node.statement:
            self.visit(node.statement)
        else:
            self.write(";")
            self.newline()

    def visit_SwitchNode(self, node):
        self.write("switch (")
        self.visit(node.expression)
        self.write(") ")
        self.visit(node.cases)

    def visit_CaseNode(self, node):
        self.write("case ")
        self.visit(node.case)
        self.write(":")
        self.indentation += 1
        self.newline()
        self.visit(node.statements)
        self.indentation -= 1

    def visit_DefaultNode(self, node):
        self.write("default:")
        self.indentation += 1
        self.newline()
        self.visit(node.statements)
        self.indentation -= 1

    def visit_ThrowNode(self, node):
        self.write("throw")
        if node.expression:
            self.write(" ")
            self.visit(node.expression)
        self.write(";")
        self.newline()

    def visit_TryNode(self, node):
        self.write("try ")
        self.visit(node.tryblock)
        if node.catchblock:
            self.write("catch (" + node.catchid + ") ")
            self.visit(node.catchblock)
        if node.finallyblock:
            self.write("finally ")
            self.visit(node.finallyblock)

    def visit_DebuggerNode(self, node):
        self.write("debugger;")
        self.newline()
