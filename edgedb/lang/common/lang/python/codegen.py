##
# Portions Copyright (c) 2008-2010 Sprymix Inc.
# Portions Copyright (c) 2008 Armin Ronacher.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of Sprymix Inc. nor the names of its contributors
#      may be used to endorse or promote products derived from this software
#      without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL SPRYMIX INC. BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
##


import itertools
from .ast import *


BOOLOP_SYMBOLS = {
    PyAnd:        'and',
    PyOr:         'or'
}

BINOP_SYMBOLS = {
    PyAdd:        '+',
    PySub:        '-',
    PyMult:       '*',
    PyDiv:        '/',
    PyFloorDiv:   '//',
    PyMod:        '%',
    PyLShift:     '<<',
    PyRShift:     '>>',
    PyBitOr:      '|',
    PyBitAnd:     '&',
    PyBitXor:     '^',
    PyPow:        '**',
    PyAnd:        'and',
    PyOr:         'or',
    PyIs:         'is',
    PyIsNot:      'is not'
}

CMPOP_SYMBOLS = {
    PyEq:         '==',
    PyGt:         '>',
    PyGtE:        '>=',
    PyIn:         'in',
    PyIs:         'is',
    PyIsNot:      'is not',
    PyLt:         '<',
    PyLtE:        '<=',
    PyNotEq:      '!=',
    PyNotIn:      'not in'
}

UNARYOP_SYMBOLS = {
    PyInvert:     '~',
    PyNot:        'not',
    PyUAdd:       '+',
    PyUSub:       '-'
}

ALL_SYMBOLS = {}
ALL_SYMBOLS.update(BOOLOP_SYMBOLS)
ALL_SYMBOLS.update(BINOP_SYMBOLS)
ALL_SYMBOLS.update(CMPOP_SYMBOLS)
ALL_SYMBOLS.update(UNARYOP_SYMBOLS)


class BasePythonSourceGenerator(SourceGenerator):
    def _body(self, statements):
        self.new_lines = 1
        self.indentation += 1
        for stmt in statements:
            self.visit(stmt)
        self.indentation -= 1

    def _body_or_else(self, node):
        self._body(node.body)
        if node.orelse:
            self.newline()
            self.write('else:')
            self._body(node.orelse)

    def _signature(self, node):
        want_comma = []
        def write_comma():
            if want_comma:
                self.write(', ')
            else:
                want_comma.append(True)

        padding = [None] * (len(node.args) - len(node.defaults))
        for arg, default in zip(node.args, padding + node.defaults):
            write_comma()
            self.visit(arg)
            if default is not None:
                self.write('=')
                self.visit(default)
        if node.vararg is not None:
            write_comma()
            self.write('*' + node.vararg)

        if node.kwonlyargs:
            if not node.vararg:
                write_comma()
                self.write('*')

            for arg, default in itertools.zip_longest(node.kwonlyargs, node.kw_defaults):
                write_comma()
                self.visit(arg)

                if default is not None:
                    self.write('=')
                    self.visit(default)

        if node.kwarg is not None:
            write_comma()
            self.write('**' + node.kwarg)

    def _decorators(self, node):
        for decorator in node.decorator_list:
            self.newline(decorator)
            self.write('@')
            self.visit(decorator)

    # Statements

    def visit_PyAssign(self, node):
        self.newline(node)
        for idx, target in enumerate(node.targets):
            if idx:
                self.write(', ')
            self.visit(target)
        self.write(' = ')
        self.visit(node.value)

    def visit_PyAugAssign(self, node):
        self.newline(node)
        self.visit(node.target)
        self.write(BINOP_SYMBOLS[type(node.op)] + '=')
        self.visit(node.value)

    def visit_PyImportFrom(self, node):
        self.newline(node)
        self.write('from %s%s import ' % ('.' * node.level, node.module))
        for idx, item in enumerate(node.names):
            if idx:
                self.write(', ')
            self.visit(item)

    def visit_PyImport(self, node):
        self.newline(node)
        for item in node.names:
            self.write('import ')
            self.visit(item)

    def visit_PyExpr(self, node):
        self.newline(node)
        self.generic_visit(node)

    def visit_PyFunctionDef(self, node):
        self.newline(extra=1)
        self._decorators(node)
        self.newline(node)
        self.write('def %s(' % node.name)
        self._signature(node.args)
        self.write('):')
        self._body(node.body)

    def visit_PyClassDef(self, node):
        have_args = []
        def paren_or_comma():
            if have_args:
                self.write(', ')
            else:
                have_args.append(True)
                self.write('(')

        self.newline(extra=2)
        self._decorators(node)
        self.newline(node)
        self.write('class %s' % node.name)
        for base in node.bases:
            paren_or_comma()
            self.visit(base)
        # XXX: the if here is used to keep this module compatible
        #      with python 2.6.
        if hasattr(node, 'keywords'):
            for keyword in node.keywords:
                paren_or_comma()
                self.write(keyword.arg + '=')
                self.visit(keyword.value)
            if node.starargs is not None:
                paren_or_comma()
                self.write('*')
                self.visit(node.starargs)
            if node.kwargs is not None:
                paren_or_comma()
                self.write('**')
                self.visit(node.kwargs)
        self.write(have_args and '):' or ':')
        self._body(node.body)

    def visit_PyIf(self, node):
        self.newline(node)
        self.write('if ')
        self.visit(node.test)
        self.write(':')
        self._body(node.body)
        while True:
            else_ = node.orelse

            if len(else_) == 0:
                break

            if len(else_) == 1 and isinstance(else_[0], PyIf):
                node = else_[0]
                self.newline()
                self.write('elif ')
                self.visit(node.test)
                self.write(':')
                self._body(node.body)
            else:
                self.newline()
                self.write('else:')
                self._body(else_)
                break

    def visit_PyFor(self, node):
        self.newline(node)
        self.write('for ')
        self.visit(node.target)
        self.write(' in ')
        self.visit(node.iter)
        self.write(':')
        self._body_or_else(node)

    def visit_PyWhile(self, node):
        self.newline(node)
        self.write('while ')
        self.visit(node.test)
        self.write(':')
        self._body_or_else(node)

    def visit_PyWith(self, node):
        self.newline(node)
        self.write('with ')
        self.visit(node.context_expr)
        if node.optional_vars is not None:
            self.write(' as ')
            self.visit(node.optional_vars)
        self.write(':')
        self._body(node.body)

    def visit_PyPass(self, node):
        self.newline(node)
        self.write('pass')

    def visit_PyDelete(self, node):
        self.newline(node)
        self.write('del ')
        for idx, target in enumerate(node.targets):
            if idx:
                self.write(', ')
            self.visit(target)

    def visit_PyTryExcept(self, node):
        self.newline(node)
        self.write('try:')
        self._body(node.body)
        for handler in node.handlers:
            self.visit(handler)

    def visit_PyTryFinally(self, node):
        self.newline(node)
        self.write('try:')
        self._body(node.body)
        self.newline(node)
        self.write('finally:')
        self._body(node.finalbody)

    def visit_PyGlobal(self, node):
        self.newline(node)
        self.write('global ' + ', '.join(node.names))

    def visit_PyNonlocal(self, node):
        self.newline(node)
        self.write('nonlocal ' + ', '.join(node.names))

    def visit_PyReturn(self, node):
        self.newline(node)
        self.write('return ')
        if node.value:
            self.visit(node.value)

    def visit_PyBreak(self, node):
        self.newline(node)
        self.write('break')

    def visit_PyContinue(self, node):
        self.newline(node)
        self.write('continue')

    def visit_PyRaise(self, node):
        # XXX: Python 2.6 / 3.0 compatibility
        self.newline(node)
        self.write('raise')
        if hasattr(node, 'exc') and node.exc is not None:
            self.write(' ')
            self.visit(node.exc)
            if node.cause is not None:
                self.write(' from ')
                self.visit(node.cause)
        elif hasattr(node, 'type') and node.type is not None:
            self.visit(node.type)
            if node.inst is not None:
                self.write(', ')
                self.visit(node.inst)
            if node.tback is not None:
                self.write(', ')
                self.visit(node.tback)

    # Expressions

    def visit_PyAttribute(self, node):
        self.visit(node.value)
        self.write('.' + node.attr)

    def visit_PyCall(self, node):
        want_comma = []
        def write_comma():
            if want_comma:
                self.write(', ')
            else:
                want_comma.append(True)

        self.visit(node.func)
        self.write('(')
        for arg in node.args:
            write_comma()
            self.visit(arg)
        for keyword in node.keywords:
            write_comma()
            self.write(keyword.arg + '=')
            self.visit(keyword.value)
        if node.starargs is not None:
            write_comma()
            self.write('*')
            self.visit(node.starargs)
        if node.kwargs is not None:
            write_comma()
            self.write('**')
            self.visit(node.kwargs)
        self.write(')')

    def visit_PyName(self, node):
        self.write(node.id)

    def visit_PyStr(self, node):
        self.write(repr(node.s))

    def visit_PyBytes(self, node):
        self.write(repr(node.s))

    def visit_PyNum(self, node):
        self.write(repr(node.n))

    def visit_PyTuple(self, node):
        self.write('(')
        idx = -1
        for idx, item in enumerate(node.elts):
            if idx:
                self.write(', ')
            self.visit(item)
        self.write(idx and ')' or ',)')

    def sequence_visit(left, right):
        def visit(self, node):
            self.write(left)
            for idx, item in enumerate(node.elts):
                if idx:
                    self.write(', ')
                self.visit(item)
            self.write(right)
        return visit

    visit_List = sequence_visit('[', ']')
    visit_Set = sequence_visit('{', '}')
    del sequence_visit

    def visit_PyDict(self, node):
        self.write('{')
        for idx, (key, value) in enumerate(zip(node.keys, node.values)):
            if idx:
                self.write(', ')
            self.visit(key)
            self.write(': ')
            self.visit(value)
        self.write('}')

    def visit_PyList(self, node):
        self.write('[')
        for idx, value in enumerate(node.elts):
            if idx:
                self.write(', ')
            self.visit(value)
        self.write(']')

    def visit_PyBinOp(self, node):
        self.write('(')
        self.visit(node.left)
        self.write(' %s ' % BINOP_SYMBOLS[type(node.op)])
        self.visit(node.right)
        self.write(')')

    def visit_PyBoolOp(self, node):
        self.write('(')
        for idx, value in enumerate(node.values):
            if idx:
                self.write(' %s ' % BOOLOP_SYMBOLS[type(node.op)])
            self.visit(value)
        self.write(')')

    def visit_PyCompare(self, node):
        self.write('(')
        self.visit(node.left)
        for op, right in zip(node.ops, node.comparators):
            self.write(' %s ' % CMPOP_SYMBOLS[type(op)])
            self.visit(right)
        self.write(')')

    def visit_PyUnaryOp(self, node):
        self.write('(')
        op = UNARYOP_SYMBOLS[type(node.op)]
        self.write(op)
        if op == 'not':
            self.write(' ')
        self.visit(node.operand)
        self.write(')')

    def visit_PySubscript(self, node):
        self.visit(node.value)
        self.write('[')
        self.visit(node.slice)
        self.write(']')

    def visit_PySlice(self, node):
        if node.lower is not None:
            self.visit(node.lower)
        self.write(':')
        if node.upper is not None:
            self.visit(node.upper)
        if node.step is not None:
            self.write(':')
            if not (isinstance(node.step, PyName) and node.step.id == 'None'):
                self.visit(node.step)

    def visit_PyExtSlice(self, node):
        for idx, item in node.dims:
            if idx:
                self.write(', ')
            self.visit(item)

    def visit_PyYield(self, node):
        self.write('yield ')
        self.visit(node.value)

    def visit_PyLambda(self, node):
        self.write('lambda ')
        self._signature(node.args)
        self.write(': ')
        self.visit(node.body)

    def visit_PyEllipsis(self, node):
        self.write('Ellipsis')

    def generator_visit(left, right):
        def visit(self, node):
            self.write(left)
            self.visit(node.elt)
            for comprehension in node.generators:
                self.visit(comprehension)
            self.write(right)
        return visit

    visit_PyListComp = generator_visit('[', ']')
    visit_PyGeneratorExp = generator_visit('(', ')')
    visit_PySetComp = generator_visit('{', '}')
    del generator_visit

    def visit_PyDictComp(self, node):
        self.write('{')
        self.visit(node.key)
        self.write(': ')
        self.visit(node.value)
        for comprehension in node.generators:
            self.visit(comprehension)
        self.write('}')

    def visit_PyIfExp(self, node):
        self.write('(')
        self.write('(')
        self.visit(node.body)
        self.write(')')
        self.write(' if ')
        self.visit(node.test)
        self.write(' else ')
        self.write('(')
        self.visit(node.orelse)
        self.write(')')
        self.write(')')

    def visit_PyStarred(self, node):
        self.write('*')
        self.visit(node.value)

    def visit_PyRepr(self, node):
        # XXX: python 2.6 only
        self.write('`')
        self.visit(node.value)
        self.write('`')

    # Helper Nodes

    def visit_Pyalias(self, node):
        self.write(node.name)
        if node.asname is not None:
            self.write(' as ' + node.asname)

    def visit_Pyarg(self, node):
        self.write(node.arg)

    def visit_Pycomprehension(self, node):
        self.write(' for ')
        self.visit(node.target)
        self.write(' in ')
        self.visit(node.iter)
        if node.ifs:
            for if_ in node.ifs:
                self.write(' if ')
                self.visit(if_)

    def visit_PyExceptHandler(self, node):
        self.newline(node)
        self.write('except')
        if node.type is not None:
            self.write(' ')
            self.visit(node.type)
            if node.name is not None:
                self.write(' as ')
                if isinstance(node.name, PyAST):
                    self.visit(node.name)
                else:
                    self.write(node.name)
        self.write(':')
        self._body(node.body)
