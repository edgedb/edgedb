##
# Copyright (c) 2015-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast

from edgedb.lang.schema import pointers as s_pointers

from . import ast as irast
from .inference import infer_arg_types, infer_type  # NOQA


def get_source_references(ir):
    result = set()

    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    for ir_set in ir_sets:
        result.add(ir_set.scls)

    return result


def get_terminal_references(ir):
    result = set()
    parents = set()

    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    for ir_set in ir_sets:
        result.add(ir_set)
        if ir_set.rptr:
            parents.add(ir_set.rptr.source)

    return result - parents


def get_variables(ir):
    result = set()
    flt = lambda n: isinstance(n, irast.Parameter)
    result.update(ast.find_children(ir, flt))
    return result


def is_const(ir):
    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    variables = get_variables(ir)
    return not ir_sets and not variables


def is_aggregated_expr(ir):
    def flt(n):
        if isinstance(n, irast.FunctionCall):
            return n.func.aggregate
        elif isinstance(n, irast.Stmt):
            # Make sure we don't dip into subqueries
            raise ast.SkipNode()

    return bool(set(ast.find_children(ir, flt)))


def extend_path(schema, source_set, ptr):
    scls = source_set.scls

    if isinstance(ptr, str):
        ptrcls = scls.resolve_pointer(schema, ptr)
    else:
        ptrcls = ptr

    path_id = source_set.path_id.extend(
        ptrcls, s_pointers.PointerDirection.Outbound, ptrcls.target)

    target_set = irast.Set()
    target_set.scls = ptrcls.target
    target_set.path_id = path_id

    ptr = irast.Pointer(
        source=source_set,
        target=target_set,
        ptrcls=ptrcls,
        direction=s_pointers.PointerDirection.Outbound
    )

    target_set.rptr = ptr

    return target_set


def get_subquery_shape(ir_expr):
    if (isinstance(ir_expr, irast.Set) and
            isinstance(ir_expr.expr, irast.Stmt) and
            isinstance(ir_expr.expr.result, irast.Shape)):
        return ir_expr.expr.result
    elif (isinstance(ir_expr, irast.Set) and
            isinstance(ir_expr.expr, irast.Stmt) and
            isinstance(ir_expr.expr.result, irast.Set)):
        return get_subquery_shape(ir_expr.expr.result)
    else:
        return None


def is_view_set(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        ir_expr.path_id and ir_expr.path_id[0].name.module == '__view__'
    )


def ensure_stmt(ir_expr):
    if not isinstance(ir_expr, irast.Stmt):
        ir_expr = irast.SelectStmt(result=ir_expr)
    return ir_expr
