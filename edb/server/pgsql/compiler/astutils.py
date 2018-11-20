#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import typing

from edb.lang.common import ast

from edb.lang.schema import pointers as s_pointers

from edb.server.pgsql import ast as pgast


def tuple_element_for_shape_el(shape_el, value, *, ctx):
    if shape_el.path_id.is_type_indirection_path(ctx.env.schema):
        rptr = shape_el.rptr.source.rptr
    else:
        rptr = shape_el.rptr
    ptrcls = rptr.ptrcls
    ptrdir = rptr.direction or s_pointers.PointerDirection.Outbound
    ptrname = ptrcls.get_shortname(ctx.env.schema)

    attr_name = s_pointers.PointerVector(
        name=ptrname.name,
        module=ptrname.module,
        direction=ptrdir,
        target=ptrcls.get_far_endpoint(ctx.env.schema, ptrdir),
        is_linkprop=ptrcls.is_link_property(ctx.env.schema))

    return pgast.TupleElement(
        path_id=shape_el.path_id,
        name=attr_name,
        val=value,
    )


def is_null_const(expr):
    if isinstance(expr, pgast.TypeCast):
        expr = expr.arg
    return isinstance(expr, pgast.NullConstant)


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


def extend_binop(binop, *exprs, op=ast.ops.AND):
    exprs = list(exprs)
    binop = binop or exprs.pop(0)

    for expr in exprs:
        if expr is not None and expr is not binop:
            binop = new_binop(lexpr=binop, op=op, rexpr=expr)

    return binop


def new_unop(op, expr):
    return pgast.Expr(
        kind=pgast.ExprKind.OP,
        name=op,
        rexpr=expr
    )


def join_condition(lref: pgast.ColumnRef, rref: pgast.ColumnRef) -> pgast.Base:
    if lref.nullable or rref.nullable:
        op = 'IS NOT DISTINCT FROM'
    else:
        op = '='

    path_cond = new_binop(lref, rref, op=op)

    if lref.optional:
        opt_cond = pgast.NullTest(arg=lref)
        path_cond = extend_binop(
            path_cond, opt_cond, op=ast.ops.OR)

    if rref.optional:
        opt_cond = pgast.NullTest(arg=rref)
        path_cond = extend_binop(
            path_cond, opt_cond, op=ast.ops.OR)

    return path_cond


def safe_array_expr(elements: typing.List[pgast.Base]) -> pgast.Base:
    result = pgast.ArrayExpr(elements=elements)
    if any(el.nullable for el in elements):
        result = pgast.FuncCall(
            name=('edgedb', '_nullif_array_nulls'),
            args=[result]
        )
    return result
