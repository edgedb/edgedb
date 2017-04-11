##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL compiler helpers for AST classification and basic transforms."""


from edgedb.lang.common import ast
from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.ir import ast as irast


def extend_qlbinop(binop, *exprs, op=ast.ops.AND):
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
        )
    return expr


def is_type_filter(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        isinstance(ir_expr.expr, irast.TypeFilter)
    )


def is_exists_expr_set(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        isinstance(ir_expr.expr, irast.ExistPred)
    )


def is_set_op_set(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        isinstance(ir_expr.expr, irast.SetOp)
    )
