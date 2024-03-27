
from typing import Optional
from ..data import data_ops as e
from ..data import expr_ops as eops

def try_collect_constraints_from_filter(expr: e.Expr) -> Optional[e.EdgeDatabaseSelectFilter]:
    if not isinstance(expr, e.BindingExpr):
        return None
    result = []
    bnd_name = expr.var
    to_process_body = [expr.body]
    
    while to_process_body:
        next_expr = to_process_body.pop()
        match next_expr:
            case e.FunAppExpr(
                fun=e.QualifiedName(["std", "="]),
                overloading_index=_,
                args=[arg1, arg2],
                kwargs={}
            ):
                match (arg1, arg2):
                    case (e.ConditionalDedupExpr(
                            e.ObjectProjExpr(
                                subject=e.BoundVarExpr(subject_name),
                                label=label)),
                        e.ScalarVal(_)):
                        if subject_name == bnd_name:
                            result.append(e.EdgeDatabaseEqFilter(label, arg2))
                        else:
                            return None
                    case (e.ScalarVal(_),
                         e.ConditionalDedupExpr(
                            e.ObjectProjExpr(
                                subject=e.BoundVarExpr(subject_name),
                                label=label))):
                        if subject_name == bnd_name:
                            result.append(e.EdgeDatabaseEqFilter(label, arg1))
                        else:
                            return None
                    case _:
                        return None

    return result

    
def refine_subject_with_filter(subject: e.Expr, filter: e.EdgeDatabaseSelectFilter) -> Optional[e.Expr]:
    all_labels = [f.propname for f in filter]
    match subject:
        case e.QualifiedName(name):
            return e.QualifiedNameWithFilter(subject, filter)
        case e.MultiSetExpr(expr=[e.QualifiedName(name)]):
            return e.QualifiedNameWithFilter(e.QualifiedName(name), filter)
        case e.ShapedExprExpr(expr=main, shape=shape):
            if any(l.label in all_labels for l in shape.shape.keys() if isinstance(l, e.StrLabel)):
                return None
            else:
                expr_refined = refine_subject_with_filter(main, filter)
                if expr_refined:
                    return e.ShapedExprExpr(
                        expr=expr_refined,
                        shape=shape
                    )
                else:
                    return None
    




def select_optimize(expr: e.Expr) -> e.Expr:
    def sub_f(sub: e.Expr) -> e.Expr:
        match sub:
            case e.FilterOrderExpr(
                subject=subject,
                filter=filter,
                order=order
            ):
                if order:
                    return None
                else:
                    subject_new = select_optimize(subject)
                    filter_new = select_optimize(filter)

                    default_return_expr = e.FilterOrderExpr(
                        subject=subject_new,
                        filter=filter_new,
                        order=order
                    )
                    possible_filter = try_collect_constraints_from_filter(filter_new)
                    if possible_filter:
                        possible_subject = refine_subject_with_filter(subject_new, possible_filter)
                        if possible_subject:
                            return e.FilterOrderExpr(
                                subject=possible_subject,
                                filter=filter_new,
                                order=order
                                )
                        else:
                            return default_return_expr
                    else:
                        return default_return_expr
                
    return eops.map_expr(sub_f, expr)
