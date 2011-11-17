##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import numbers

from semantix.caos import types as caos_types
from semantix.caos.tree.transformer import TreeTransformer
from semantix.caos.expr.record import QuerySelectorRow
from semantix.caos.backends import query as backend_query

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


class PythonQuery(backend_query.Query):
    def __init__(self, text, result_types, argument_types):
        self.text = text
        self.result_types = result_types
        self.argument_types = argument_types

    def prepare(self, session):
        return PreparedPythonQuery(self, session)


class PreparedPythonQuery:
    def __init__(self, query, session):
        self.query = query

        import semantix
        from semantix import caos
        self.globals = {'caos': caos, 'semantix': semantix}

        self.statement = compile(query.text, '<string>', 'eval')

    def first(self, **kwargs):
        return eval(self.statement, self.globals, kwargs)[0][1]

    def rows(self, **kwargs):
        return [QuerySelectorRow(eval(self.statement, self.globals, kwargs))]

    def describe_output(self, session):
        return self.query.describe_output(session)

    def describe_arguments(self, session):
        return self.query.describe_arguments(session)

    def convert_arguments(self, **kwargs):
        return dict(kwargs)

    __iter__ = rows
    __call__ = rows


class Adapter:
    def __init__(self):
        self.transformer = CaosToPythonTransformer()

    def transform(self, tree, scrolling_cursor=False, context=None, *, proto_schema):
        pytree = self.transformer.transform(tree, context=context, proto_schema=proto_schema)

        text = py_codegen.BasePythonSourceGenerator.to_source(pytree)

        restypes = collections.OrderedDict()
        for k, v in tree.result_types.items():
            if v[0] is not None: # XXX get_expr_type
                restypes[k] = (v[0].name, v[1])
            else:
                restypes[k] = v

        argtypes = {}

        for k, v in tree.argument_types.items():
            if v is not None: # XXX get_expr_type
                if isinstance(v, tuple):
                    argtypes[k] = (v[0], v[1].name)
            else:
                argtypes[k] = v

        return PythonQuery(text, result_types=restypes, argument_types=argtypes)


class CaosToPythonTransformerContext:
    def __init__(self, proto_schema, context=None):
        self.proto_schema = proto_schema
        self.name_context = context


class CaosToPythonTransformer(TreeTransformer):
    def transform(self, tree, proto_schema, context=None):
        context = CaosToPythonTransformerContext(proto_schema=proto_schema, context=context)
        result = py_ast.PyTuple()
        for i, selexpr in enumerate(tree.selector):
            if selexpr.name:
                name = py_ast.PyStr(s=selexpr.name)
            else:
                name = py_ast.PyStr(s=str(i))

            pyexpr = self._process_expr(selexpr.expr, context)
            result.elts.append(py_ast.PyTuple(elts=[name, pyexpr]))

        return result

    def _process_expr(self, expr, context):
        if isinstance(expr, caos_ast.BinOp):
            left = self._process_expr(expr.left, context)
            right = self._process_expr(expr.right, context)

            if expr.op == caos_ast.LIKE:
                f = caos_ast.FunctionCall(name=('str', 'like'), args=[expr.left, expr.right])
                result = self._process_expr(f, context)
            elif expr.op == caos_ast.ILIKE:
                f = caos_ast.FunctionCall(name=('str', 'ilike'), args=[expr.left, expr.right])
                result = self._process_expr(f, context)
            else:
                op = _operator_map[expr.op]()

                if isinstance(expr.op, ast.ops.ComparisonOperator):
                    result = py_ast.PyCompare(left=left, ops=[op], comparators=[right])
                else:
                    result = py_ast.PyBinOp(left=left, right=right, op=op)

        elif isinstance(expr, caos_ast.UnaryOp):
            operand = self._process_expr(expr.expr, context)
            op = _operator_map[expr.op]()

            result = py_ast.PyUnaryOp(op=op, operand=operand)

        elif isinstance(expr, caos_ast.BaseRefExpr):
            result = self._process_expr(expr.expr, context)

        elif isinstance(expr, (caos_ast.AtomicRefSimple, caos_ast.LinkPropRefSimple)):
            path = [expr.name]
            node = expr.ref

            source = node

            if isinstance(expr, caos_ast.LinkPropRefSimple):
                node = node.source

            while node:
                if node.rlink:
                    path.append(node.rlink.link_proto.normal_name())
                    node = node.rlink.source
                else:
                    source = node
                    node = None

            if isinstance(expr, caos_ast.LinkPropRefSimple):
                # XXX
                source = expr.ref

            assert source.anchor and source.anchor in context.name_context

            result = py_ast.PyName(id='__context_%s' % source.anchor)

            for attr in reversed(path):
                result = py_ast.PyCall(func=py_ast.PyName(id='getattr'),
                                       args=[result, py_ast.PyStr(s=str(attr))])


        elif isinstance(expr, caos_ast.Disjunction):
            if len(expr.paths) == 1:
                result = self._process_expr(next(iter(expr.paths)), context)
            else:
                assert False, 'unsupported path combination: "%r"' % expr

        elif isinstance(expr, caos_ast.Constant):
            if expr.expr:
                result = self._process_expr(expr.expr, context)
            elif expr.index:
                result = py_ast.PyName(id=expr.index)
            else:
                if isinstance(expr.value, numbers.Number):
                    result = py_ast.PyNum(n=expr.value)
                else:
                    result = py_ast.PyStr(s=expr.value)

        elif isinstance(expr, caos_ast.FunctionCall):
            args = [self._process_expr(a, context) for a in expr.args]

            fcls = caos_types.FunctionMeta.get_function_class(expr.name)

            if fcls and hasattr(fcls, 'call'):
                funcpath = fcls.__module__.split('.') + [fcls.__name__, 'call']
            elif expr.name == ('datetime', 'current_datetime'):
                funcpath = ('caos', 'objects', 'datetime', 'DateTime', 'now')
            elif not isinstance(expr.name, tuple):
                funcpath = [expr.name]
            else:
                assert False, 'unsupported function: "%r"' % (expr.name,)

            func = py_ast.PyName(id=funcpath[0])
            for step in funcpath[1:]:
                func = py_ast.PyAttribute(value=func, attr=step)
            result = py_ast.PyCall(func=func, args=args)

        else:
            assert False, "unexpected expression: %r" % expr

        return result
