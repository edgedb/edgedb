"""
This file inserts
conditional deduplicaiton statements into the EdgeQL query language.

A projection M.l is not deduplicated if
there are eventually a link property projection M.l.....@l
, otherwise it is deduplicated.

"""

from . import data_ops as e
from . import expr_ops as eops
from typing import Optional


def insert_conditional_dedup(expr: e.Expr):

    def do_recursive_insert(sub: e.Expr) -> e.Expr:
        match sub:
            case e.ObjectProjExpr(subject=subject, label=label):
                return e.ConditionalDedupExpr(
                    e.ObjectProjExpr(
                        subject=do_recursive_insert(subject), label=label
                    )
                )
            case _:
                return insert_conditional_dedup(sub)

    def do_not_recursive_insert(sub: e.Expr) -> e.Expr:
        match sub:
            case e.LinkPropProjExpr(subject=subject, linkprop=linkprop):
                return e.LinkPropProjExpr(
                    subject=do_not_recursive_insert(subject), linkprop=linkprop
                )
            case e.TpIntersectExpr(subject=subject, tp=tp):
                return e.TpIntersectExpr(
                    subject=do_not_recursive_insert(subject), tp=tp
                )
            case e.ObjectProjExpr(subject=subject, label=label):
                return e.ObjectProjExpr(
                    subject=insert_conditional_dedup(subject), label=label
                )
            case e.BackLinkExpr(subject=subject, label=label):
                return e.BackLinkExpr(
                    subject=insert_conditional_dedup(subject), label=label
                )
            case _:
                return insert_conditional_dedup(sub)

    def insert_function_top(sub: e.Expr) -> Optional[e.Expr]:
        if isinstance(sub, e.LinkPropProjExpr):
            return do_not_recursive_insert(sub)
        elif isinstance(sub, e.ObjectProjExpr):
            if sub.label == "__edgedb_reserved_subject__":
                return do_not_recursive_insert(sub)
            else:
                return do_recursive_insert(sub)
        else:
            return None

    result = eops.map_expr(insert_function_top, expr)
    return result
