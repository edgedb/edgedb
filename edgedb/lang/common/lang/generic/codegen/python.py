##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.ast import SourceGenerator
from semantix.utils.lang.generic import ast

BOOLOP_SYMBOLS = {
    ast.And:        'and',
    ast.Or:         'or'
}

BINOP_SYMBOLS = {
    ast.Add:        '+',
    ast.Sub:        '-',
    ast.Mult:       '*',
    ast.Div:        '/',
    ast.FloorDiv:   '//',
    ast.Mod:        '%',
    ast.LShift:     '<<',
    ast.RShift:     '>>',
    ast.BitOr:      '|',
    ast.BitAnd:     '&',
    ast.BitXor:     '^',
    ast.Pow:        '**',
    ast.And:        'and',
    ast.Or:        'or'
}

CMPOP_SYMBOLS = {
    ast.Eq:         '==',
    ast.Gt:         '>',
    ast.GtE:        '>=',
    ast.In:         'in',
    ast.Is:         'is',
    ast.IsNot:      'is not',
    ast.Lt:         '<',
    ast.LtE:        '<=',
    ast.NotEq:      '!=',
    ast.NotIn:      'not in'
}

UNARYOP_SYMBOLS = {
    ast.Invert:     '~',
    ast.Not:        'not',
    ast.UAdd:       '+',
    ast.USub:       '-'
}

class GenericPythonSourceGenerator(SourceGenerator):
    def __body(self, statements):
        self.new_lines = 1
        self.indentation += 1
        for stmt in statements:
            self.visit(stmt)
            self.newline()
        self.indentation -= 1

    def __sequence(left, right):
        def visit(self, node):
            self.write(left)
            for idx, item in enumerate(node.elements):
                if idx:
                    self.write(', ')
                self.visit(item)
            self.write(right)
        return visit

    visit_List = __sequence('[', ']')
    visit_Tuple = __sequence('(', ')')
    visit_Set = __sequence('{', '}')

    def visit_Dict(self, node):
        self.write('{')
        for idx, (key, value) in enumerate(zip(node.keys, node.values)):
            if idx:
                self.write(', ')
            self.visit(key)
            self.write(': ')
            self.visit(value)
        self.write('}')

    def visit_NoneConst(self, node):
        self.write('None')

    def visit_TrueConst(self, node):
        self.write('True')

    def visit_FalseConst(self, node):
        self.write('False')

    def visit_EllipsisConst(self, node):
        self.write('...')

    def visit_Number(self, node):
        self.write(repr(node.value))

    def visit_String(self, node):
        self.write(repr(node.value))

    def visit_Bytes(self, node):
        self.write(repr(node.value))

    def visit_Name(self, node):
        self.write(node.id)

    def visit_Return(self, node):
        self.newline(node)
        self.write('return ')
        self.visit(node.value)

    def visit_Assign(self, node):
        self.visit(node.target)
        self.write(' = ')
        self.visit(node.value)

    def visit_GetItem(self, node):
        self.visit(node.value)
        self.write('[')
        self.visit(node.slice)
        self.write(']')

    def visit_Index(self, node):
        self.visit(node.value)

    def visit_Slice(self, node):
        if node.lower:
            self.visit(node.lower)
        self.write(':')

        if node.upper:
            self.visit(node.upper)

        if node.step:
            self.write(':')
            #if not (isinstance(node.step, ast.Name) and node.step.id == 'None'):
            self.visit(node.step)

    def visit_BinaryOperation(self, node):
        self.write('(')
        self.visit(node.left)
        self.write(' %s ' % BINOP_SYMBOLS[type(node.op)])
        self.visit(node.right)
        self.write(')')

    def visit_BooleanOperation(self, node):
        self.write('(')
        self.visit(node.left)
        self.write(' %s ' % BOOLOP_SYMBOLS[type(node.op)])
        self.visit(node.right)
        self.write(')')

    def visit_UnaryOperation(self, node):
        self.write('(')
        self.write('%s ' % UNARYOP_SYMBOLS[type(node.op)])
        self.visit(node.operand)
        self.write(')')

    def visit_Compare(self, node):
        self.write('(')
        self.visit(node.left)
        self.write(' %s ' % CMPOP_SYMBOLS[type(node.op)])
        self.visit(node.right)
        self.write(')')

    def visit_Function(self, node):
        self.newline(extra=1)
        self.write('def %s(' % node.name)

        kwonly_flag = False
        first = True
        for arg in node.args:
            if first:
                first = False
            else:
                self.write(', ')

            if arg.default:
                self.write('%s=' % arg.id)
                self.visit(arg.default)
            else:
                self.write(arg.id)

        self.write('):')
        self.__body(node.body)

    def visit_Call(self, node):
        self.visit(node.target)
        self.write('(')

        first = True
        for arg in node.args:
            if first:
                first = False
            else:
                self.write(', ')

            self.visit(arg)

        self.write(')')
