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


def extend_irbinop(binop, *exprs, op=ast.ops.AND):
    exprs = list(exprs)
    binop = binop or exprs.pop(0)

    for expr in exprs:
        if expr is not binop:
            binop = irast.BinOp(
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


def is_ql_path(qlexpr):
    if isinstance(qlexpr, qlast.Shape):
        qlexpr = qlexpr.expr

    if not isinstance(qlexpr, qlast.Path):
        return False

    start = qlexpr.steps[0]

    return isinstance(start, (qlast.Self, qlast.ClassRef, qlast.Ptr))


def is_degenerate_select(qlstmt):
    if not isinstance(qlstmt, qlast.SelectQuery):
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
        (not isinstance(start, qlast.ClassRef) or
            start.module is not None or start.name not in views) and
        # No FILTER, ORDER BY, OFFSET or LIMIT
        qlstmt.where is None and
        qlstmt.orderby is None and
        qlstmt.offset is None and
        qlstmt.limit is None
    )
