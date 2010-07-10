##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import numbers

from semantix.caos import types as caos_types

from semantix.utils import ast

from semantix.utils.lang.python import ast as py_ast
from semantix.utils.lang.python import codegen as py_codegen

from semantix.utils import datastructures

from . import ast as caos_ast


_operator_map = {
    ast.ops.AND:    py_ast.PyAnd,
    ast.ops.OR:     py_ast.PyOr,

    ast.ops.ADD:    py_ast.PyAdd,
    ast.ops.SUB:    py_ast.PySub,
    ast.ops.MUL:    py_ast.PyMult,
    ast.ops.DIV:    py_ast.PyDiv,
    ast.ops.MOD:    py_ast.PyMod,
    ast.ops.POW:    py_ast.PyPow,

    ast.ops.EQ:     py_ast.PyEq,
    ast.ops.GT:     py_ast.PyGt,
    ast.ops.GE:     py_ast.PyGtE,
    ast.ops.IN:     py_ast.PyIn,
    ast.ops.IS:     py_ast.PyIs,
    ast.ops.IS_NOT: py_ast.PyIsNot,
    ast.ops.LT:     py_ast.PyLt,
    ast.ops.LE:     py_ast.PyLtE,
    ast.ops.NE:     py_ast.PyNotEq,
    ast.ops.NOT_IN: py_ast.PyNotIn,

    ast.ops.NOT:    py_ast.PyNot,
    ast.ops.UPLUS:  py_ast.PyUAdd,
    ast.ops.UMINUS: py_ast.PyUSub
}


class PythonQuery:
    def __init__(self, text, result_types, argument_types, context=None):
        self.text = text
        self.statement = compile(text, '<string>', 'eval')
        self.context = context
        self.result_types = result_types
        self.argument_types = argument_types
        self.record = datastructures.Record('pyquery_result', self.result_types, None)

    def __call__(self, **kwargs):
        return [self.record(**dict(eval(self.statement, {}, kwargs)))]

    def first(self, **kwargs):
        return eval(self.statement, {}, kwargs)[0][1]

    def rows(self, **kwargs):
        return [collections.OrderedDict(eval(self.statement, {}, kwargs))]

    def describe_output(self):
        return collections.OrderedDict(self.result_types)

    def describe_arguments(self):
        return dict(self.argument_types)

    def prepare(self, session):
        return self

    __iter__ = rows


class Adapter:
    def __init__(self):
        self.transformer = CaosToPythonTransformer()

    def transform(self, tree):
        pytree = self.transformer.transform(tree)

        text = py_codegen.BasePythonSourceGenerator.to_source(pytree)

        restypes = collections.OrderedDict()
        for k, v in tree.result_types.items():
            if isinstance(v[0], caos_types.ProtoNode):
                restypes[k] = (self.sessions.schema.get(v[0].name), v[1])
            else:
                restypes[k] = v

        return PythonQuery(text, result_types=restypes, argument_types=tree.argument_types)


class CaosToPythonTransformer(ast.visitor.NodeVisitor):
    def transform(self, tree):
        result = py_ast.PyTuple()
        for i, selexpr in enumerate(tree.selector):
            if selexpr.name:
                name = py_ast.PyStr(s=selexpr.name)
            else:
                name = py_ast.PyStr(s=str(i))

            pyexpr = self._process_expr(selexpr.expr)
            result.elts.append(py_ast.PyTuple(elts=[name, pyexpr]))

        return result

    def _process_expr(self, expr):
        if isinstance(expr, caos_ast.BinOp):
            left = self._process_expr(expr.left)
            right = self._process_expr(expr.right)
            op = _operator_map[expr.op]()

            if isinstance(expr.op, ast.ops.ComparisonOperator):
                result = py_ast.PyCompare(left=left, ops=[op], comparators=[right])
            else:
                result = py_ast.PyBinOp(left=left, right=right, op=op)

        elif isinstance(expr, caos_ast.Constant):
            if expr.expr:
                result = self._process_expr(expr.expr)
            elif expr.index:
                result = py_ast.PyName(id=expr.index)
            else:
                if isinstance(expr.value, numbers.Number):
                    result = py_ast.PyNum(n=expr.value)
                else:
                    result = py_ast.PyStr(s=expr.value)

        elif isinstance(expr, caos_ast.FunctionCall):
            args = [self._process_expr(a) for a in expr.args]

            func = py_ast.PyName(id=expr.name)
            result = py_ast.PyCall(func=func, args=args)

        else:
            assert False, "unexpected expression: %r" % expr

        return result
