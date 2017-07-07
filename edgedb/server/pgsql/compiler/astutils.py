##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import typing

from edgedb.lang.common import ast
from edgedb.lang.ir import ast as irast

from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import pointers as s_pointers

from edgedb.server.pgsql import ast as pgast


class TupleElement:
    def __init__(self, path_id: irast.PathId,
                 name: typing.Optional[str]=None,
                 val: typing.Optional[pgast.Base]=None):
        self.path_id = path_id
        self.name = name
        self.val = val

    def __repr__(self):
        return f'<{self.__class__.__name__} ' \
               f'name={self.name} val={self.val} path_id={self.path_id}>'


class TupleVar:
    def __init__(self, elements: typing.List[TupleElement], named: bool=False):
        self.elements = elements
        self.named = named

    def __repr__(self):
        return f'<{self.__class__.__name__} [{self.elements!r}]'


class ColumnList:
    def __init__(self, elements: typing.List[str], named: bool=False):
        self.elements = elements
        self.named = named


def tuple_element_for_shape_el(shape_el, value):
    rptr = shape_el.rptr
    ptrcls = rptr.ptrcls
    ptrdir = rptr.direction or s_pointers.PointerDirection.Outbound
    ptrname = ptrcls.shortname

    attr_name = s_pointers.PointerVector(
        name=ptrname.name, module=ptrname.module,
        direction=ptrdir, target=ptrcls.get_far_endpoint(ptrdir),
        is_linkprop=isinstance(ptrcls, s_lprops.LinkProperty))

    return TupleElement(
        path_id=shape_el.path_id,
        name=attr_name,
        val=value
    )


def is_null_const(expr):
    if isinstance(expr, pgast.TypeCast):
        expr = expr.arg
    return isinstance(expr, pgast.Constant) and expr.val is None


def is_set_op_query(query):
    return getattr(query, 'op', None) is not None


def for_each_query_in_set(qry, cb):
    if qry.op:
        result = for_each_query_in_set(qry.larg, cb)
        result.extend(for_each_query_in_set(qry.rarg, cb))
    else:
        result = [cb(qry)]

    return result


def new_binop(lexpr, rexpr, op):
    return pgast.Expr(
        kind=pgast.ExprKind.OP,
        name=op,
        lexpr=lexpr,
        rexpr=rexpr
    )


def extend_binop(binop, *exprs, op=ast.ops.AND, reversed=False):
    exprs = list(exprs)
    binop = binop or exprs.pop(0)

    for expr in exprs:
        if expr is not binop:
            if reversed:  # XXX: dead
                binop = new_binop(rexpr=binop, op=op, lexpr=expr)
            else:
                binop = new_binop(lexpr=binop, op=op, rexpr=expr)

    return binop


def new_unop(op, expr):
    return pgast.Expr(
        kind=pgast.ExprKind.OP,
        name=op,
        rexpr=expr
    )


def set_as_exists_op(pg_expr, negated=False):
    if isinstance(pg_expr, pgast.Query):
        result = pgast.SubLink(
            type=pgast.SubLinkType.EXISTS, expr=pg_expr)

    elif isinstance(pg_expr, (pgast.Constant, pgast.ParamRef)):
        result = pgast.NullTest(arg=pg_expr, negated=True)

    else:
        raise RuntimeError(  # pragma: no cover
            f'unexpected argument to _set_as_exists_op: {pg_expr!r}')

    if negated:
        result = new_unop(ast.ops.NOT, result)

    return result


def is_nullable(expr):
    if isinstance(expr, TupleVar):
        return False
    else:
        return True
