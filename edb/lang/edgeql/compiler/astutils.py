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


"""EdgeQL compiler helpers for AST classification and basic transforms."""


import typing

from edb.lang.edgeql import ast as qlast

from edb.lang.ir import ast as irast
from edb.lang.ir import typeutils as irtyputils

from . import context
from . import inference


def extend_qlbinop(binop, *exprs, op='AND'):
    exprs = list(exprs)
    binop = binop or exprs.pop(0)

    for expr in exprs:
        if expr is not binop:
            binop = qlast.BinOp(
                left=binop,
                right=expr,
                op=op
            )

    return binop


def ensure_qlstmt(expr):
    if not isinstance(expr, qlast.Statement):
        expr = qlast.SelectQuery(
            result=expr,
            implicit=True,
        )
    return expr


def is_ql_empty_set(expr):
    return isinstance(expr, qlast.Set) and len(expr.elements) == 0


def is_set_op_set(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        isinstance(ir_expr.expr, irast.SetOp)
    )


def is_ql_path(qlexpr):
    if isinstance(qlexpr, qlast.Shape):
        qlexpr = qlexpr.expr

    if not isinstance(qlexpr, qlast.Path):
        return False

    start = qlexpr.steps[0]

    return isinstance(start, (qlast.Source, qlast.ObjectRef, qlast.Ptr))


def is_degenerate_select(qlstmt):
    if not isinstance(qlstmt, qlast.SelectQuery) or not qlstmt.implicit:
        return False

    qlexpr = qlstmt.result

    # This is a normal path
    if not is_ql_path(qlexpr):
        return False

    if isinstance(qlexpr, qlast.Shape):
        qlexpr = qlexpr.expr

    start = qlexpr.steps[0]

    views = [
        e.alias for e in qlstmt.aliases
        if isinstance(e, qlast.AliasedExpr)
    ]

    return (
        # Not a reference to a view defined in this statement
        (not isinstance(start, qlast.ObjectRef) or
            start.module is not None or start.name not in views) and
        # No FILTER, ORDER BY, OFFSET or LIMIT
        qlstmt.where is None and
        qlstmt.orderby is None and
        qlstmt.offset is None and
        qlstmt.limit is None
    )


def make_tuple(
        elements: typing.List[irast.TupleElement], *,
        named: bool,
        ctx: context.ContextLevel) -> irast.Tuple:

    tup = irast.Tuple(elements=elements, named=named)
    tup.typeref = irtyputils.type_to_typeref(
        ctx.env.schema,
        inference.infer_type(tup, env=ctx.env))
    return tup


def make_array(
        elements: typing.List[irast.Base], *,
        ctx: context.ContextLevel) -> irast.Array:

    arr = irast.Array(elements=elements)
    arr.typeref = irtyputils.type_to_typeref(
        ctx.env.schema,
        inference.infer_type(arr, env=ctx.env))
    return arr
