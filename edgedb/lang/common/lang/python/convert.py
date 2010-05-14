##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import ast
import copy
import itertools
import functools

from . import ast as py_ast
from semantix.exceptions import SemantixError
from semantix.utils.lang.generic import ast as generic_ast


_Token = object()


class AstToPyAstConverter:
    @classmethod
    def convert(cls, node:ast.AST):
        name = node.__class__.__name__

        new_cls = getattr(py_ast, 'Py' + name, None)

        if new_cls is None:
            raise SemantixError('unknown python ast class "%s"' % name)

        new_node = new_cls()

        for field, value in py_ast.iter_fields(node):
            if isinstance(value, list):
                new_value = []
                for item in value:
                    if isinstance(item, ast.AST):
                        new_item = cls.convert(item)
                        new_item.parent = new_node
                        new_value.append(new_item)
                    else:
                        new_value.append(item)
                setattr(new_node, field, new_value)
            elif isinstance(value, ast.AST):
                new_item = cls.convert(value)
                new_item.parent = new_node
                setattr(new_node, field, new_item)
            else:
                setattr(new_node, field, value)

        return new_node


class PyAstToGeneric:
    CTX_MAP = {
        py_ast.PyLoad:  generic_ast.NAME_CONTEXT_LOAD,
        py_ast.PyStore: generic_ast.NAME_CONTEXT_STORE,
        py_ast.PyDel:   generic_ast.NAME_CONTEXT_DELETE
    }

    BOOLOP_MAP = {
        py_ast.PyAnd:       generic_ast.And,
        py_ast.PyOr:        generic_ast.Or
    }

    BINOP_MAP = {
        py_ast.PyAdd:       generic_ast.Add,
        py_ast.PySub:       generic_ast.Sub,
        py_ast.PyMult:      generic_ast.Mult,
        py_ast.PyDiv:       generic_ast.Div,
        py_ast.PyMod:       generic_ast.Mod,
        py_ast.PyLShift:    generic_ast.LShift,
        py_ast.PyRShift:    generic_ast.RShift,
        py_ast.PyBitOr:     generic_ast.BitOr,
        py_ast.PyBitXor:    generic_ast.BitXor,
        py_ast.PyBitAnd:    generic_ast.BitAnd,
        py_ast.PyFloorDiv:  generic_ast.FloorDiv
    }

    UNARYOP_MAP = {
        py_ast.PyInvert:    generic_ast.Invert,
        py_ast.PyNot:       generic_ast.Not,
        py_ast.PyUAdd:      generic_ast.UAdd,
        py_ast.PyUSub:      generic_ast.USub,
    }

    COMPOP_MAP = {
        py_ast.PyEq:        generic_ast.Eq,
        py_ast.PyNotEq:     generic_ast.NotEq,
        py_ast.PyLt:        generic_ast.Lt,
        py_ast.PyLtE:       generic_ast.LtE,
        py_ast.PyGt:        generic_ast.Gt,
        py_ast.PyGtE:       generic_ast.GtE,
        py_ast.PyIs:        generic_ast.Is,
        py_ast.PyIsNot:     generic_ast.IsNot,
        py_ast.PyIn:        generic_ast.In,
        py_ast.PyNotIn:     generic_ast.NotIn
    }

    def __init__(self):
        self.var_cnt = 0

    def introduce_variable(self):
        self.var_cnt += 1
        return '_tmp_%d' % self.var_cnt

    def convert(self, node):
        name = node.__class__.__name__

        method = getattr(self, 'visit_%s' % name, None)

        if method:
            return method(node)

        raise SemantixError('unable to convert python ast to generic: unknown ast node %s' % name)

    def visit_PyModule(self, old):
        return generic_ast.Module(body=[self.convert(el) for el in old.body])

    def visit_PyCompare(self, old):
        if len(old.ops) == 1:
            return generic_ast.Compare(left=self.convert(old.left),
                                       op=self.COMPOP_MAP[type(old.ops[0])](),
                                       right=self.convert(old.comparators[0]))

        ops = []
        comparators = [old.left] + old.comparators
        for i in range(len(comparators)-1):
            left, right = comparators[i], comparators[i+1]
            op = old.ops[i]

            ops.append(generic_ast.Compare(left=self.convert(left),
                                           op=self.COMPOP_MAP[type(op)](),
                                           right=self.convert(right)))

        return functools.reduce(lambda prev, next: \
                                       generic_ast.BooleanOperation(left=prev,
                                                                   op=generic_ast.And(),
                                                                   right=next),
                                ops)

    def visit_PySubscript(self, old):
        new = generic_ast.GetItem(value=self.convert(old.value),
                                  slice=self.convert(old.slice))

        if old.ctx:
            new.context = self.CTX_MAP[old.ctx.__class__]

        return new

    def visit_PyIndex(self, old):
        return generic_ast.Index(value=self.convert(old.value))

    def visit_PySlice(self, old):
        return generic_ast.Slice(lower=self.convert(old.lower) if old.lower else None,
                                 upper=self.convert(old.upper) if old.upper else None,
                                 step=self.convert(old.step) if old.step else None)

    def visit_PyBoolOp(self, old):
        return functools.reduce(lambda prev, next: \
                                       generic_ast.BooleanOperation(
                                            left=prev if isinstance(prev, generic_ast.Base) \
                                                                            else self.convert(prev),
                                            op=self.BOOLOP_MAP[type(old.op)](),
                                            right=self.convert(next)
                                       ),
                                old.values)
        body = []

        for i in range(len(old.values)-1):
            left, right = old.values[i:i+1]
            body.append(
                generic_ast.BooleanOperation(
                    left=self.convert(left),
                    op=self.BOOLOP_MAP[type(old.op)](),
                    right=self.convert(right)
                )
            )

        return body

    def visit_PyAssign(self, old):
        assert len(old.targets) == 1
        target = old.targets[0]

        if not isinstance(target, py_ast.PyTuple):
            return generic_ast.Assign(target=self.convert(target),
                                      value=self.convert(old.value))


        def flatten(left:py_ast.PyTuple, source:generic_ast.Name, body:list):
            i = 0
            for el in left.elts:
                if isinstance(el, py_ast.PyName):
                    body.append(
                        generic_ast.Assign(
                            target=self.convert(el),
                            value=generic_ast.GetItem(
                                value=copy.copy(source),
                                slice=generic_ast.Index(value=generic_ast.Number(value=i)),
                                context=generic_ast.NAME_CONTEXT_LOAD
                            )
                        )
                    )

                    i += 1

                elif isinstance(el, py_ast.PyTuple):
                    var = self.introduce_variable()
                    body.append(
                        generic_ast.Assign(
                            target=generic_ast.Name(id=var, context=generic_ast.NAME_CONTEXT_STORE),
                            value=generic_ast.GetItem(
                                value=copy.copy(source),
                                slice=generic_ast.Index(value=generic_ast.Number(value=i)),
                                context=generic_ast.NAME_CONTEXT_LOAD
                            )
                        )
                    )

                    flatten(el, generic_ast.Name(id=var, context=generic_ast.NAME_CONTEXT_LOAD),
                            body)

                    i += 1

                elif isinstance(el, py_ast.PyStarred):
                    body.append(
                        generic_ast.Assign(
                            target=self.convert(el.value),
                            value=generic_ast.Call(
                                target=generic_ast.Name(
                                    id='list',
                                    context=generic_ast.NAME_CONTEXT_LOAD
                                ),

                                args=[
                                    generic_ast.GetItem(
                                        value=copy.copy(source),
                                        slice=generic_ast.Slice(
                                            lower=generic_ast.Number(value=i)
                                        ),
                                        context=generic_ast.NAME_CONTEXT_LOAD
                                    )
                                ]
                            )
                        )
                    )

                else:
                    assert False

        body = []

        if isinstance(old.value, py_ast.PyName):
            var = self.convert(old.value)
        else:
            var = self.introduce_variable()
            body.append(
                generic_ast.Assign(
                    target=generic_ast.Name(id=var, context=generic_ast.NAME_CONTEXT_STORE),
                    value=generic_ast.Call(
                        target=generic_ast.Name(
                            id='list',
                            context=generic_ast.NAME_CONTEXT_LOAD
                        ),
                        args=[self.convert(old.value)]
                    )
                )
            )
            var = generic_ast.Name(id=var, context=generic_ast.NAME_CONTEXT_LOAD)

        flatten(target, var, body)
        return body

    def visit_PyList(self, old):
        new = generic_ast.List(elements=[self.convert(el) for el in old.elts])

        if old.ctx:
            new.context = self.CTX_MAP[old.ctx.__class__]
        return new

    def visit_PyTuple(self, old):
        new = generic_ast.Tuple(elements=[self.convert(el) for el in old.elts])

        if old.ctx:
            new.context = self.CTX_MAP[old.ctx.__class__]
        return new

    def visit_PyDict(self, old):
        new = generic_ast.Dict(keys=[self.convert(key) for key in old.keys],
                               values=[self.convert(value) for value in old.values])
        return new

    def visit_PyBinOp(self, old):
        return generic_ast.BinaryOperation(left=self.convert(old.left),
                                           op=self.BINOP_MAP[type(old.op)](),
                                           right=self.convert(old.right))

    def visit_PyUnaryOp(self, old):
        return generic_ast.UnaryOperation(op=self.UNARYOP_MAP[type(old.op)](),
                                          operand=self.convert(old.operand))

    def visit_PyName(self, old):
        if old.ctx and isinstance(old.ctx, py_ast.PyLoad) \
            and old.id in ('None', 'True', 'False'):

            return getattr(generic_ast, '%sConst' % old.id)()

        new = generic_ast.Name(id=old.id)

        if old.ctx:
            new.context = self.CTX_MAP[old.ctx.__class__]

        return new

    def visit_PyAssert(self, old):
        return generic_ast.Assert(test=old.test, message=old.msg)

    def visit_PyStr(self, old):
        return generic_ast.String(value=old.s)

    def visit_PyNum(self, old):
        return generic_ast.Number(value=old.n)

    def visit_PyBytes(self, old):
        return generic_ast.Bytes(value=old.s)

    def visit_PyReturn(self, old):
        return generic_ast.Return(value=self.convert(old.value))

    def visit_PyCall(self, old):
        new = generic_ast.Call(target=self.convert(old.func),
                               args=[self.convert(arg) for arg in old.args])

        assert not old.keywords
        assert old.starargs is None
        assert old.kwargs is None

        return new

    def visit_PyFunctionDef(self, old):
        new = generic_ast.Function(name=old.name)

        assert isinstance(old.args, py_ast.Pyarguments)
        assert not old.decorator_list, 'function decorators are not currently supported'
        assert old.args.vararg is None, '*args are not currently supported'
        assert old.args.kwarg is None, '**kwargs are not currently supported'

        args = []
        for arg, default in reversed(tuple(itertools.zip_longest(reversed(old.args.args),
                                                                 reversed(old.args.defaults),
                                                                 fillvalue=_Token))):
            assert isinstance(arg, py_ast.Pyarg)

            new_arg = generic_ast.FunctionArgument()
            new_arg.id = arg.arg
            if default is not _Token:
                new_arg.default = self.convert(default)
            if arg.annotation:
                new_arg.annotation = self.convert(arg.annotation)
            args.append(new_arg)

        for arg, default in zip(old.args.kwonlyargs, old.args.kw_defaults):
            new_arg = generic_ast.FunctionArgument(kwonly=True)
            new_arg.id = arg.arg
            new_arg.default = self.convert(default)
            if arg.annotation:
                new_arg.annotation = self.convert(arg.annotation)
            args.append(new_arg)

        new.args = args

        if old.returns:
            new.returns_annotation = self.convert(old.returns)

        for node in old.body:
            new.append_node(self.convert(node))

        return new
