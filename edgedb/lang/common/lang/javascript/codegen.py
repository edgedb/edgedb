##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.lang.javascript import ast as jsast
from semantix.utils.lang.preprocessor import codegen


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


class JavascriptSourceGenerator(codegen.PP_SourceGenerator):
    def visit_StatementNode(self, node):
        if node.statement:
            self.visit(node.statement)
        self.write(';')
        self.newline()

    def visit_VarDeclarationNode(self, node):
        self.write('var ')
        self.visit_list_helper(node.vars)

    def visit_VarInitNode(self, node):
        self.visit(node.name)
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
        self.visit_list_helper(node.array)

        # this is needed to properly render [,,,,] and similar array literals
        #
        if node.array and node.array[-1] == None:
            self.write(',')
        self.write(']')

    def visit_ObjectLiteralNode(self, node):
        if isinstance(node.parent, jsast.StatementNode):
            self.write('(')
        self.write('{')
        self.indentation += 1
        self.visit_list_helper(node.properties)
        self.indentation -= 1
        self.write('}')
        if isinstance(node.parent, jsast.StatementNode):
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
        self.visit_list_helper(node.expressions)
        self.write(')')

    def visit_PrefixExpressionNode(self, node):
        self.write(node.op)
        self.visit(node.expression)

    def visit_PostfixExpressionNode(self, node):
        self.visit(node.expression)
        self.write(node.op)

    def visit_BinExpressionNode(self, node):
        if isinstance(node.parent, jsast.Expression):
            self.write('(')
        self.visit(node.left)
        self.write(' ' + node.op + ' ')
        self.visit(node.right)
        if isinstance(node.parent, jsast.Expression):
            self.write(')')

    def visit_AssignmentExpressionNode(self, node):
        if isinstance(node.parent, jsast.Expression):
            self.write('(')
        self.visit(node.left)
        self.write(' ' + node.op + ' ')
        self.visit(node.right)
        if isinstance(node.parent, jsast.Expression):
            self.write(')')

    def visit_DotExpressionNode(self, node):
        self.visit(node.left)
        self.write('.')
        self.visit(node.right)

    def visit_ConditionalExpressionNode(self, node):
        if isinstance(node.parent, jsast.Expression):
            self.write('(')
        self.visit(node.condition)
        self.write(' ? ')
        self.visit(node.true)
        self.write(' : ')
        self.visit(node.false)
        if isinstance(node.parent, jsast.Expression):
            self.write(')')

    def visit_CallNode(self, node):
        self.visit(node.call)
        self.write('(')
        self.visit_list_helper(node.arguments)
        self.write(')')

    def visit_NewNode(self, node):
        self.write('new ')
        self.visit(node.expression)
        if node.arguments:
            self.write('(')
            self.visit_list_helper(node.arguments)
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
        if isinstance(node.parent, jsast.Expression):
            self.write('(')
        self.visit(node.expression)
        self.write(' instanceof ')
        self.visit(node.type)
        if isinstance(node.parent, jsast.Expression):
            self.write(')')

    def visit_InNode(self, node):
        if isinstance(node.parent, jsast.Expression):
            self.write('(')
        self.visit(node.expression)
        self.write(' in ')
        self.visit(node.container)
        if isinstance(node.parent, jsast.Expression):
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
        if isinstance(node.parent, jsast.Expression):
            self.write("(")
        self.write("function ")
        if node.name:
            self.write(node.name)
        self.write("(")
        if node.param:
            self.visit_list_helper(node.param)
        self.write(") ")

        # We only want a newline at the end of function declaration
        #
        make_newline = node.isdeclaration

        self.visit_StatementBlockNode(node.body, endnewline=make_newline)

        if isinstance(node.parent, jsast.Expression):
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
            if isinstance(node.thenclause, jsast.StatementBlockNode):
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
        self.visit(node.container)
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

    def visit_YieldNode(self, node):
        self.write("yield")
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
        if node.catch:
            if type(node.catch) == list:
                self.visit_list_helper(node.catch, separator='')
            else:
                self.visit(node.catch)
        if node.finallyblock:
            self.write("finally ")
            self.visit(node.finallyblock)

    def visit_CatchNode(self, node):
        self.write("catch (" + node.catchid + ") ")
        self.visit(node.catchblock)

    def visit_DebuggerNode(self, node):
        self.write("debugger;")
        self.newline()

    #
    # Features
    #

    def visit_LetDeclarationNode(self, node):
        self.write('let ')
        self.visit_list_helper(node.vars)

    def visit_LetExpressionNode(self, node):
        if isinstance(node.parent, jsast.Expression):
            self.write('(')
        self.write('let (')
        self.visit_list_helper(node.vars)
        self.write(') ')
        self.visit(node.expression)
        if isinstance(node.parent, jsast.Expression):
            self.write(')')

    def visit_LetStatementNode(self, node):
        self.write('let (')
        self.visit_list_helper(node.vars)
        self.write(') ')
        self.visit(node.statement)

    def visit_ForEachNode(self, node):
        self.write("for each (")
        self.visit(node.var)
        self.write(" in ")
        self.visit(node.container)
        self.write(") ")
        if node.statement:
            self.visit(node.statement)
        else:
            self.write(";")
            self.newline()

#    def visit_TryCatchIfNode(self, node):
#        self.write("try ")
#        self.visit(node.tryblock)
#        if node.catch:
#            self.visit_list_helper(node.catch, separator='')
#        if node.finallyblock:
#            self.write("finally ")
#            self.visit(node.finallyblock)

    def visit_CatchIfNode(self, node):
        self.write("catch (" + node.catchid)
        if node.condition:
            self.write(" if ")
            self.visit(node.condition)
        self.write(") ")
        self.visit(node.catchblock)

    def visit_ArrayComprehensionNode(self, node):
        self.write('[')
#        self.visit_comprehension_helepr(node, forstring='for each')
        self.visit(node.generator)
        self.write(']')

    def visit_ComprehensionNode(self, node):
        self.write('(')
        self.visit(node.var)
        self.write(" in ")
        self.visit(node.container)
        self.write(')')
        if node.condition:
            self.write(' if (')
            self.visit(node.condition)
            self.write(')')

    def visit_GeneratorExprNode(self, node):
        is_array_compr = isinstance(node.parent, jsast.ArrayComprehensionNode)

        if (isinstance(node.parent, jsast.VarInitNode) or
            isinstance(node.parent, jsast.Expression) and not is_array_compr):
            self.write("(")
        self.visit_comprehension_helepr(node, forstring=node.forstring)
        if (isinstance(node.parent, jsast.VarInitNode) or
            isinstance(node.parent, jsast.Expression) and not is_array_compr):
            self.write(")")

    def visit_comprehension_helepr(self, node, forstring='for'):
        self.visit(node.expr)
        if len(node.comprehensions) > 1:
            self.indentation+=1
            self.newline()
        else:
            self.write(' ')

        for i, compr in enumerate(node.comprehensions):
            self.write(forstring + ' ')
            self.visit(compr)
            if i != (len(node.comprehensions) - 1):
                self.indentation+=1
                self.newline()

        if len(node.comprehensions) > 1:
            self.indentation-=len(node.comprehensions)

