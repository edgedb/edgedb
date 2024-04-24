from typing import Optional
from ..data import data_ops as e
from ..data import expr_ops as eops


def try_collect_constraints_from_filter(
    expr: e.Expr,
) -> Optional[e.EdgeDatabaseSelectFilter]:
    if not isinstance(expr, e.BindingExpr):
        return None
    bnd_name = expr.var

    def try_iterative_collection(
        expr_body: e.Expr,
    ) -> Optional[e.EdgeDatabaseSelectFilter]:
        match expr_body:
            case e.FunAppExpr(
                fun=e.QualifiedName(["std", "="]),
                overloading_index=_,
                args=[arg1, arg2],
                kwargs={},
            ):
                match (arg1, arg2):
                    case (
                        e.ConditionalDedupExpr(
                            e.ObjectProjExpr(
                                subject=e.BoundVarExpr(subject_name),
                                label=label,
                            )
                        ),
                        _,
                    ):
                        if (
                            subject_name == bnd_name
                            and not eops.appears_in_expr(
                                e.FreeVarExpr(bnd_name), arg2
                            )
                        ):
                            return e.EdgeDatabaseEqFilter(label, arg2)
                        else:
                            return None
                    case (
                        _,
                        e.ConditionalDedupExpr(
                            e.ObjectProjExpr(
                                subject=e.BoundVarExpr(subject_name),
                                label=label,
                            )
                        ),
                    ):
                        if (
                            subject_name == bnd_name
                            and not eops.appears_in_expr(
                                e.FreeVarExpr(bnd_name), arg1
                            )
                        ):
                            return e.EdgeDatabaseEqFilter(label, arg1)
                        else:
                            return None
                    case _:
                        return None
            case e.FunAppExpr(
                fun=e.QualifiedName(["std", "IN"]),
                overloading_index=_,
                args=[
                    e.ConditionalDedupExpr(
                        e.ObjectProjExpr(
                            subject=e.BoundVarExpr(subject_name), label=label
                        )
                    ),
                    arg2,
                ],
                kwargs={},
            ):
                if subject_name == bnd_name and not eops.appears_in_expr(
                    e.FreeVarExpr(bnd_name), arg2
                ):
                    return e.EdgeDatabaseEqFilter(label, arg2)
                else:
                    return None
            case _:
                return None

    return try_iterative_collection(expr.body)


def is_trivial_shape_element(shape: e.ShapeExpr, label: str) -> bool:
    shape_elem = shape.shape[e.StrLabel(label)]
    bnd_name = shape_elem.var
    match shape_elem.body:
        case e.ConditionalDedupExpr(
            e.ObjectProjExpr(
                subject=e.BoundVarExpr(subject_name), label=proj_label
            )
        ):
            if subject_name == bnd_name and proj_label == label:
                return True
            else:
                return False
        case _:
            return False


def refine_subject_with_filter(
    subject: e.Expr, filter: e.EdgeDatabaseSelectFilter
) -> Optional[e.Expr]:
    all_labels = eops.collect_names_in_select_filter(filter)
    match subject:
        case e.QualifiedName(name):
            bindings = {}
            binding_extraction_failed = False

            def extract_expression_bindings(expr: e.Expr) -> Optional[e.Expr]:
                nonlocal binding_extraction_failed
                if isinstance(expr, e.EdgeDatabaseSelectFilter):  # type: ignore
                    return None
                elif isinstance(expr, e.FreeVarExpr | e.ScalarVal):
                    return expr
                elif eops.is_effect_free(expr):
                    binder_name = e.next_name("filter_bnd")
                    bindings[binder_name] = expr
                    return e.FreeVarExpr(binder_name)
                else:
                    binding_extraction_failed = True
                    return None

            after_filter = eops.map_edge_select_filter(
                extract_expression_bindings, filter
            )
            if binding_extraction_failed:
                return None
            assert isinstance(after_filter, e.EdgeDatabaseSelectFilter)  # type: ignore
            return_expr: e.Expr = e.QualifiedNameWithFilter(
                subject, after_filter  # type: ignore
            )
            for bnd_name, bnd_expr in bindings.items():
                return_expr = e.WithExpr(
                    bnd_expr, eops.abstract_over_expr(return_expr, bnd_name)
                )
            return return_expr
        case e.MultiSetExpr(expr=[e.QualifiedName(name)]):
            return refine_subject_with_filter(e.QualifiedName(name), filter)
        case e.ShapedExprExpr(expr=main, shape=shape):
            if any(
                l.label in all_labels
                for l in shape.shape.keys()
                if isinstance(l, e.StrLabel)
                and not is_trivial_shape_element(shape, l.label)
            ):
                return None
            else:
                expr_refined = refine_subject_with_filter(main, filter)
                if expr_refined:
                    return e.ShapedExprExpr(expr=expr_refined, shape=shape)
                else:
                    return None
        case _:
            return None


def select_optimize(expr: e.Expr) -> e.Expr:
    def sub_f(sub: e.Expr) -> Optional[e.Expr]:
        match sub:
            case e.FilterOrderExpr(
                subject=subject, filter=filter, order=order
            ):
                if order:
                    return None
                else:
                    subject_new = select_optimize(subject)
                    filter_new = select_optimize(filter)
                    assert isinstance(filter_new, e.BindingExpr)

                    default_return_expr = e.FilterOrderExpr(
                        subject=subject_new, filter=filter_new, order=order
                    )
                    possible_filter = try_collect_constraints_from_filter(
                        filter_new
                    )
                    if possible_filter:
                        possible_subject = refine_subject_with_filter(
                            subject_new, possible_filter
                        )
                        if possible_subject:
                            return e.FilterOrderExpr(
                                subject=possible_subject,
                                filter=filter_new,
                                order=order,
                            )
                        else:
                            return default_return_expr
                    else:
                        return default_return_expr
            case _:
                return None

    return eops.map_expr(sub_f, expr)
