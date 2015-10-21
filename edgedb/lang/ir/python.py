##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import itertools
import numbers

from metamagic.caos import types as caos_types
from metamagic.caos.tree.transformer import TreeTransformer
from metamagic.caos.tree import utils as ir_utils
from metamagic.caos.backends import query as backend_query

from metamagic.utils import ast

from metamagic.utils.lang.python import ast as py_ast
from metamagic.utils.lang.python import codegen as py_codegen

from metamagic.utils import datastructures
from metamagic.utils.algos.persistent_hash import persistent_hash

from . import ast as caos_ast
from metamagic.caos.caosql import ast as caosql_ast


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
    def __init__(self, text, result_types, argument_types, context_vars):
        self.text = text
        self.result_types = result_types
        self.argument_types = argument_types
        self.context_vars = context_vars
        self.keymap = collections.OrderedDict(zip(self.result_types, itertools.count()))

    def prepare(self, session):
        return PreparedPythonQuery(self, session)


class Row(tuple):
    @classmethod
    def from_sequence(cls, keymap, seq):
        result = cls(seq)
        result.keymap = keymap
        return result

    def keys(self):
        return self.keymap.keys()

    def values(self):
        return iter(self)

    def items(self):
        return zip(self.keymap, self)


class PreparedPythonQuery:
    def __init__(self, query, session):
        self.query = query

        import metamagic
        from metamagic import caos
        self.globals = {'caos': caos, 'metamagic': metamagic}

        self.statement = compile(query.text, '<string>', 'eval')

    def first(self, **kwargs):
        return self.rows(**kwargs)[0][0]

    def rows(self, **kwargs):
        var_vals = {k.replace('.', '__'): v for k, v in kwargs.items()}
        row = eval(self.statement, self.globals, var_vals)
        return (Row.from_sequence(seq=row, keymap=self.query.keymap),)

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

    def transform(self, tree, scrolling_cursor=False, context=None, *, proto_schema,
                                                                       output_format=None):
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
                    argtypes[k] = v.name
            else:
                argtypes[k] = v

        return PythonQuery(text, result_types=restypes, argument_types=argtypes,
                                 context_vars=tree.context_vars)


class CaosToPythonTransformerContext:
    def __init__(self, proto_schema, context=None):
        self.proto_schema = proto_schema
        self.name_context = context


class CaosToPythonTransformer(TreeTransformer):
    def transform(self, tree, proto_schema, context=None):
        if (tree.grouper or tree.sorter or tree.set_op
                or tree.op in {'update', 'delete'}):
            raise NotImplementedError('unsupported query tree')

        context = CaosToPythonTransformerContext(proto_schema=proto_schema, context=context)
        result = py_ast.PyTuple()
        for i, selexpr in enumerate(tree.selector):
            pyexpr = self._process_expr(selexpr.expr, context)
            result.elts.append(pyexpr)

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

            elif expr.op == caosql_ast.REMATCH:
                f = caos_ast.FunctionCall(name=('re', 'match'), args=[expr.right, expr.left])
                result = self._process_expr(f, context)

            elif expr.op == caosql_ast.REIMATCH:
                flags = caos_ast.Sequence(elements=[caos_ast.Constant(value='i')])
                f = caos_ast.FunctionCall(name=('re', 'match'), args=[expr.right, expr.left, flags])
                result = self._process_expr(f, context)

            else:
                op = _operator_map[expr.op]()

                if isinstance(expr.op, (ast.ops.ComparisonOperator, ast.ops.MembershipOperator)):
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
            if expr.anchor:
                result = py_ast.PyName(id=expr.anchor)

            else:
                node = expr.ref

                if expr.name == 'metamagic.caos.builtins.target':
                    path = [node.link_proto.normal_name()]
                else:
                    path = [expr.name]

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

                if (isinstance(expr, caos_ast.LinkPropRefSimple)
                            and expr.name != 'metamagic.caos.builtins.target'):
                    # XXX
                    source = expr.ref

                if not source.anchor:
                    msg = 'reference to unachored node: {!r}'.format(source)
                    raise NotImplementedError(msg)

                result = py_ast.PyName(id='__context_%s' % source.anchor)

                for attr in reversed(path):
                    result = py_ast.PyCall(func=py_ast.PyName(id='getattr'),
                                           args=[result, py_ast.PyStr(s=str(attr))])

        elif isinstance(expr, caos_ast.EntitySet):
            if expr.anchor:
                name = expr.anchor
            else:
                name = 'n{:x}'.format(persistent_hash(str(expr.concept.name)))
            result = py_ast.PyName(id=name)

        elif isinstance(expr, caos_ast.Disjunction):
            if len(expr.paths) == 1:
                result = self._process_expr(next(iter(expr.paths)), context)
            else:
                raise NotImplementedError('multipaths are not supported by this backend')

        elif isinstance(expr, caos_ast.Constant):
            if expr.expr:
                result = self._process_expr(expr.expr, context)
            elif expr.index:
                result = py_ast.PyName(id=expr.index.replace('.', '__'))
            else:
                if isinstance(expr.value, numbers.Number):
                    result = py_ast.PyNum(n=expr.value)
                else:
                    result = py_ast.PyStr(s=expr.value)

        elif isinstance(expr, caos_ast.Sequence):
            elements = [self._process_expr(el, context) for el in expr.elements]
            result = py_ast.PyTuple(elts=elements)

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
                raise NotImplementedError('function {!r} is not implemented by this backend'
                                            .format(expr.name))

            func = py_ast.PyName(id=funcpath[0])
            for step in funcpath[1:]:
                func = py_ast.PyAttribute(value=func, attr=step)
            result = py_ast.PyCall(func=func, args=args)

        elif isinstance(expr, caos_ast.TypeCast):
            expr_type = ir_utils.infer_type(expr.expr, context.proto_schema)
            result = self._cast(context, self._process_expr(expr.expr, context),
                                expr_type, expr.type)

        else:
            raise NotImplementedError('unsupported query tree node: {!r}'.format(expr))

        return result

    def _cast(self, context, expr, from_type, to_type):
        result = None

        if (isinstance(from_type, caos_types.ProtoNode)
                    and from_type.name == "metamagic.caos.builtins.str"
                    and to_type.name == "metamagic.caos.builtins.bytes"):
            result = py_ast.PyCall(func=py_ast.PyAttribute(value=expr, attr="encode"),
                                   args=[py_ast.PyStr(s="utf8")])
        elif isinstance(to_type, caos_types.ProtoNode):
            top_type = to_type.get_topmost_base(context.proto_schema)
            top_type_n = '{}.{}'.format(top_type.__module__, top_type.__name__)

            type_path = top_type_n.split('.')

            if (top_type_n.startswith('metamagic.caos.objects')
                            or top_type_n.startswith('builtins.')):
                type_path = type_path[1:]

            type_ref = py_ast.PyName(id=type_path[0])
            for step in type_path[1:]:
                type_ref = py_ast.PyAttribute(value=type_ref, attr=step)

            result = py_ast.PyCall(func=type_ref, args=[expr])

        if result is None:
            # XXX: this should really be an error
            result = expr

        return result
