from functools import reduce
import operator
from typing import Tuple, Dict, Sequence, List

from edb import errors
from ..data import data_ops as e
from ..data import expr_ops as eops
from ..data import type_ops as tops
from ..data import module_ops as mops
from ..data import path_factor as path_factor
from .dml_checking import insert_checking, update_checking
from ..data import expr_to_str as pp
from .function_checking import func_call_checking
from .cast_checking import check_castable
from ..schema import subtyping_resolution as subtp_resol


def synthesize_type_for_val(val: e.Val) -> e.Tp:
    match val:
        case e.ScalarVal(tp, _):
            return tp
        case _:
            raise ValueError("Not implemented", val)


def check_shape_transform(
    ctx: e.TcCtx, s: e.ShapeExpr, tp: e.Tp
) -> Tuple[e.Tp, e.ShapeExpr]:

    result_s_tp = e.ObjectTp({})
    result_l_tp = e.ObjectTp({})
    result_expr = e.ShapeExpr({})

    for lbl, comp in s.shape.items():
        match lbl:
            case e.StrLabel(s_lbl):
                new_ctx, body, bnd_var = eops.tcctx_add_binding(
                    ctx, comp, e.ResultTp(tp, e.CardOne)
                )
                result_tp, checked_body = synthesize_type(new_ctx, body)
                result_s_tp = e.ObjectTp({**result_s_tp.val, s_lbl: result_tp})
                result_expr = e.ShapeExpr(
                    {
                        **result_expr.shape,
                        lbl: eops.abstract_over_expr(checked_body, bnd_var),
                    }
                )
            case e.LinkPropLabel(l_lbl):
                new_ctx, body, bnd_var = eops.tcctx_add_binding(
                    ctx, comp, e.ResultTp(tp, e.CardOne)
                )
                result_tp, checked_body = synthesize_type(new_ctx, body)
                result_l_tp = e.ObjectTp({**result_l_tp.val, l_lbl: result_tp})
                result_expr = e.ShapeExpr(
                    {
                        **result_expr.shape,
                        lbl: eops.abstract_over_expr(checked_body, bnd_var),
                    }
                )

    ret_tp = tops.combine_tp_with_subject_tp(ctx, tp, result_s_tp)
    ret_tp = tops.combine_tp_with_linkprop_tp(ctx, ret_tp, result_l_tp)
    return ret_tp, result_expr


def type_cast_tp(ctx: e.TcCtx, from_tp: e.ResultTp, to_tp: e.Tp) -> e.ResultTp:
    if (from_tp.tp, to_tp) in ctx.schema.casts:
        return e.ResultTp(to_tp, from_tp.mode)
    else:
        raise ValueError("Not Implemented", from_tp, to_tp)


def check_filter_body_is_exclusive(ctx: e.TcCtx, filter_ck: e.Expr) -> bool:
    match filter_ck:
        case e.FunAppExpr(fun=e.QualifiedName(["std", "="]), args=args):
            if len(args) != 2:
                return False

            if (
                isinstance(args[0], e.ObjectProjExpr)
                and isinstance(args[1], e.ScalarVal)
            ) or (
                isinstance(args[1], e.ObjectProjExpr)
                and isinstance(args[0], e.ScalarVal)
            ):
                proj = (
                    args[0]
                    if isinstance(args[0], e.ObjectProjExpr)
                    else args[1]
                )
                match proj:
                    case e.ObjectProjExpr(
                        subject=e.FreeVarExpr(varname), label=label
                    ):
                        result_tp, _ = ctx.varctx[varname]
                        match result_tp:
                            case e.NominalLinkTp(
                                subject=_, name=name, linkprop=_
                            ):
                                type_def = mops.resolve_type_def(
                                    ctx.schema, name
                                )
                            case e.NamedNominalLinkTp(name=name, linkprop=_):
                                assert isinstance(
                                    name, e.QualifiedName
                                ), "should have been resolved"
                                type_def = mops.resolve_type_def(
                                    ctx.schema, name
                                )
                            case _:
                                return False
                        assert isinstance(type_def.typedef, e.ObjectTp)
                        if label in type_def.typedef.val:
                            if [
                                c
                                for c in type_def.constraints
                                if isinstance(c, e.ExclusiveConstraint)
                                and c.name == label
                            ]:
                                return True
                            else:
                                return False
                        else:
                            return False
                    case _:
                        return False

            else:
                return False

        case _:
            return False


def synthesize_type(ctx: e.TcCtx, expr: e.Expr) -> Tuple[e.ResultTp, e.Expr]:
    result_tp: e.Tp
    result_card: e.CMMode
    result_expr: e.Expr = expr  # by default we don't change expr

    match expr:
        case e.ScalarVal(_):
            result_tp = synthesize_type_for_val(expr)
            result_card = e.CardOne
        case e.FreeVarExpr(var=var):
            if var in ctx.varctx.keys():
                result_tp, result_card = ctx.varctx[var]
            else:
                possible_resolved_name = mops.try_resolve_simple_name(
                    ctx, e.UnqualifiedName(var)
                )
                if possible_resolved_name is not None:
                    return synthesize_type(ctx, possible_resolved_name)
                else:
                    raise ValueError(
                        "Unknown variable",
                        var,
                        "list of known vars",
                        list(ctx.varctx.keys()),
                    )
        case e.QualifiedName(_):
            module_entity = mops.try_resolve_module_entity(ctx, expr)
            match module_entity:
                case e.ModuleEntityTypeDef(typedef=typedef):
                    assert isinstance(
                        typedef, e.ObjectTp
                    ), "Cannot select Scalar type"
                    result_tp = e.NominalLinkTp(
                        subject=typedef, name=expr, linkprop=e.ObjectTp({})
                    )
                    result_expr = e.MultiSetExpr(
                        expr=[
                            name
                            for name in subtp_resol.find_all_subtypes_of_tp_in_schema(  # NoQA
                                ctx.schema, expr
                            )
                            if not mops.tp_name_is_abstract(name, ctx.schema)
                        ]
                    )
                    result_card = e.CardAny
                case _:
                    raise ValueError(
                        "Unsupported Module Entity", module_entity
                    )

        case e.TypeCastExpr(tp=tp, arg=arg):
            tp_ck = check_type_valid(ctx, tp)
            if expr_tp_is_not_synthesizable(arg):
                result_card, result_expr = check_type_no_card(ctx, arg, tp_ck)
                result_tp = tp_ck
            else:
                (arg_tp, arg_v) = synthesize_type(ctx, arg)
                candidate_cast = check_castable(ctx, arg_tp.tp, tp_ck)
                if candidate_cast is not None:
                    (result_tp, result_card) = (tp_ck, arg_tp.mode)
                    result_expr = e.CheckedTypeCastExpr(
                        cast_tp=(arg_tp.tp, tp_ck),
                        cast_spec=candidate_cast,
                        arg=arg_v,
                    )
                else:
                    raise ValueError("Cannot cast", arg_tp, tp_ck)
        case e.ParameterExpr(name=name, tp=tp, is_required=is_required):
            result_tp = check_type_valid(ctx, tp)
            result_card = e.CardOne if is_required else e.CardAtMostOne
            result_expr = e.ParameterExpr(
                name=name, tp=result_tp, is_required=is_required
            )

        case e.ShapedExprExpr(expr=subject, shape=shape):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            result_tp, shape_ck = check_shape_transform(
                ctx, shape, subject_tp.tp
            )
            if not eops.is_effect_free(shape):
                raise ValueError("Shape should be effect free", shape)
            result_card = subject_tp.mode
            result_expr = e.ShapedExprExpr(subject_ck, shape_ck)
        case e.UnionExpr(left=l, right=r):
            (l_tp, l_ck) = synthesize_type(ctx, l)
            (r_tp, r_ck) = synthesize_type(ctx, r)
            result_tp = tops.construct_tp_union(l_tp.tp, r_tp.tp)
            result_card = l_tp.mode + r_tp.mode
            result_expr = e.UnionExpr(l_ck, r_ck)
        case e.FunAppExpr(fun=_, args=_, overloading_index=_):
            (e_result_tp, e_ck) = func_call_checking(ctx, expr)
            result_tp = e_result_tp.tp
            result_card = e_result_tp.mode
            result_expr = e_ck
        case e.FreeObjectExpr():
            result_tp = e.NominalLinkTp(
                subject=e.ObjectTp({}),
                name=e.QualifiedName(["std", "FreeObject"]),
                linkprop=e.ObjectTp({}),
            )
            result_card = e.CardOne
            result_expr = expr
        case e.ConditionalDedupExpr(expr=inner):
            (inner_tp, inner_ck) = synthesize_type(ctx, inner)
            result_tp = inner_tp.tp
            result_card = inner_tp.mode
            result_expr = e.ConditionalDedupExpr(inner_ck)
        case e.ObjectProjExpr(subject=subject, label=label):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            result_tp, result_card = tops.tp_project(
                ctx, subject_tp, e.StrLabel(label)
            )
            # If the projection is a computable expression,
            # project from the subject
            if isinstance(result_tp, e.ComputableTp):
                comp_expr = e.WithExpr(subject_ck, result_tp.expr)
                result_expr = check_type(
                    ctx, comp_expr, e.ResultTp(result_tp.tp, result_card)
                )
                result_tp = result_tp.tp
            else:
                if tops.is_tp_projection_tuple_proj(subject_tp.tp):
                    result_expr = e.TupleProjExpr(subject_ck, label)
                else:
                    result_expr = e.ObjectProjExpr(subject_ck, label)
        case e.LinkPropProjExpr(subject=subject, linkprop=lp):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            result_tp, result_card = tops.tp_project(
                ctx, subject_tp, e.LinkPropLabel(lp)
            )
            if isinstance(result_tp, e.ComputableTp):
                comp_expr = e.WithExpr(subject_ck, result_tp.expr)
                result_expr = check_type(
                    ctx, comp_expr, e.ResultTp(result_tp.tp, result_card)
                )
                result_tp = result_tp.tp
            else:
                result_expr = e.LinkPropProjExpr(subject_ck, lp)
        case e.BackLinkExpr(subject=subject, label=label):
            (_, subject_ck) = synthesize_type(ctx, subject)
            candidates: List[e.NamedNominalLinkTp] = []
            for t_name, name_def in mops.enumerate_all_object_type_defs(ctx):
                for name_label, comp_tp in name_def.val.items():
                    if name_label == label:
                        match comp_tp.tp:
                            case e.NamedNominalLinkTp(_):
                                candidates = [
                                    *candidates,
                                    e.NamedNominalLinkTp(
                                        t_name, comp_tp.tp.linkprop
                                    ),
                                ]
                            case _:
                                candidates = [
                                    *candidates,
                                    e.NamedNominalLinkTp(
                                        t_name, e.ObjectTp({})
                                    ),
                                ]
            result_expr = e.BackLinkExpr(subject_ck, label)
            if len(candidates) == 0:
                result_tp = e.AnyTp()
            else:
                result_tp = reduce(
                    tops.construct_tp_union,  # type: ignore[arg-type]
                    candidates,
                )
            result_card = e.CardAny
        case e.IsTpExpr(subject=subject, tp=intersect_tp):
            # intersect_tp = check_type_valid(ctx, intersect_tp)
            if isinstance(intersect_tp, e.UncheckedTypeName):
                intersect_tp_name, _ = mops.resolve_raw_name_and_type_def(
                    ctx, intersect_tp.name
                )
            if isinstance(intersect_tp, e.AnyTp):
                intersect_tp_name = e.QualifiedName(
                    ["std", "any" + (intersect_tp.specifier or "")]
                )
            else:
                assert isinstance(intersect_tp, e.RawName)  # type: ignore
                intersect_tp_name, _ = mops.resolve_raw_name_and_type_def(
                    ctx, intersect_tp  # type: ignore
                )
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            result_expr = e.IsTpExpr(subject_ck, intersect_tp_name)
            result_card = subject_tp.mode
            result_tp = e.BoolTp()
        case e.TpIntersectExpr(subject=subject, tp=intersect_tp):
            if isinstance(intersect_tp, e.UncheckedTypeName):
                intersect_tp = intersect_tp.name
            assert isinstance(intersect_tp, e.RawName)  # type: ignore
            intersect_tp_name, _ = mops.resolve_raw_name_and_type_def(
                ctx, intersect_tp  # type: ignore
            )
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            result_expr = e.TpIntersectExpr(subject_ck, intersect_tp_name)
            if all(
                isinstance(t, e.NamedNominalLinkTp)
                for t in tops.collect_tp_union(subject_tp.tp)
            ):
                candidates = []
                for t in tops.collect_tp_union(subject_tp.tp):
                    assert isinstance(t, e.NamedNominalLinkTp)
                    if t.name == intersect_tp_name:
                        candidates = [*candidates, t]
                if len(candidates) == 0:
                    result_tp = tops.construct_tp_intersection(
                        subject_tp.tp,
                        e.NamedNominalLinkTp(
                            name=intersect_tp_name, linkprop=e.ObjectTp({})
                        ),
                    )
                else:
                    result_tp = reduce(
                        tops.construct_tp_union,  # type: ignore[arg-type]
                        candidates,
                    )
            else:
                result_tp = tops.construct_tp_intersection(
                    subject_tp.tp,
                    e.NamedNominalLinkTp(
                        name=intersect_tp_name, linkprop=e.ObjectTp({})
                    ),
                )  # TODO: get linkprop
            result_card = e.CMMode(
                e.CardNumZero,
                subject_tp.mode.upper,
            )
        case e.SubqueryExpr(expr=sub_expr):
            (sub_expr_tp, sub_expr_ck) = synthesize_type(ctx, sub_expr)
            result_expr = e.SubqueryExpr(sub_expr_ck)
            result_tp = sub_expr_tp.tp
            result_card = sub_expr_tp.mode
        case e.DetachedExpr(expr=sub_expr):
            (sub_expr_tp, sub_expr_ck) = synthesize_type(ctx, sub_expr)
            result_expr = e.SubqueryExpr(sub_expr_ck)
            result_tp = sub_expr_tp.tp
            result_card = sub_expr_tp.mode
        case e.WithExpr(bound=bound_expr, next=next_expr):
            (bound_tp, bound_ck) = synthesize_type(ctx, bound_expr)
            new_ctx, body, bound_var = eops.tcctx_add_binding(
                ctx, next_expr, bound_tp
            )
            (next_tp, next_ck) = synthesize_type(new_ctx, body)
            result_expr = e.WithExpr(
                bound_ck, eops.abstract_over_expr(next_ck, bound_var)
            )
            result_tp, result_card = next_tp
        case e.FilterOrderExpr(subject=subject, filter=filter, order=order):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            filter_ctx, filter_body, filter_bound_var = eops.tcctx_add_binding(
                ctx, filter, e.ResultTp(subject_tp.tp, e.CardOne)
            )

            order_ck: Dict[str, e.BindingExpr] = {}
            for order_label, o in order.items():
                order_ctx, order_body, order_bound_var = (
                    eops.tcctx_add_binding(
                        ctx, o, e.ResultTp(subject_tp.tp, e.CardOne)
                    )
                )
                (_, o_ck) = synthesize_type(order_ctx, order_body)
                order_ck = {
                    **order_ck,
                    order_label: eops.abstract_over_expr(
                        o_ck, order_bound_var
                    ),
                }

            assert eops.is_effect_free(filter), "Expecting effect-free filter"
            assert all(
                eops.is_effect_free(o) for o in order.values()
            ), "Expecting effect-free order"

            (_, filter_ck) = check_type_no_card(
                filter_ctx, filter_body, e.BoolTp()
            )

            result_expr = e.FilterOrderExpr(
                subject_ck,
                eops.abstract_over_expr(filter_ck, filter_bound_var),
                order_ck,
            )
            result_tp = subject_tp.tp
            # pass cardinality if filter body can be determined to be true
            if filter_body == e.BoolVal(True):
                result_card = subject_tp.mode
            elif check_filter_body_is_exclusive(filter_ctx, filter_ck):
                result_card = e.CardAtMostOne
            else:
                result_card = e.CMMode(e.CardNumZero, subject_tp.mode.upper)
        case e.OffsetLimitExpr(subject=subject, offset=offset, limit=limit):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            offset_mode, offset_ck = check_type_no_card(ctx, offset, e.IntTp())
            limit_mode, limit_ck = check_type_no_card(ctx, limit, e.IntTp())
            if offset_mode.upper == e.CardNumInf:
                raise errors.QueryError("Offset must have cardinality (<=1)")
            if limit_mode.upper == e.CardNumInf:
                raise errors.QueryError("Limit must have cardinality (<=1)")
            result_expr = e.OffsetLimitExpr(subject_ck, offset_ck, limit_ck)
            result_tp = subject_tp.tp
            if isinstance(limit_ck, e.ScalarVal):
                v = limit_ck.val
                assert isinstance(limit_ck.val, int), "Expecting int"
                if v > 1:
                    upper_card_bound = subject_tp.mode.upper
                else:
                    upper_card_bound = e.CardNumOne
            else:
                upper_card_bound = subject_tp.mode.upper

            result_card = e.CMMode(
                e.CardNumZero,
                upper_card_bound,
            )

        case e.InsertExpr(name=_, new=arg):
            result_expr = insert_checking(ctx, expr)
            assert isinstance(result_expr, e.InsertExpr) and isinstance(
                result_expr.name, e.QualifiedName
            )
            result_tp = e.NamedNominalLinkTp(
                name=result_expr.name, linkprop=e.ObjectTp({})
            )
            result_card = e.CardOne
        case e.UpdateExpr(subject=subject, shape=shape_expr):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            (shape_ck) = update_checking(ctx, shape_expr, subject_tp.tp)
            result_expr = e.UpdateExpr(subject_ck, shape_ck)
            result_tp, result_card = subject_tp
        case e.DeleteExpr(subject=subject):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            assert eops.is_effect_free(
                subject
            ), "Expecting subject expr to be effect-free"
            result_expr = e.DeleteExpr(subject_ck)
            result_tp, result_card = subject_tp
        case e.IfElseExpr(
            then_branch=then_branch,
            condition=condition,
            else_branch=else_branch,
        ):
            (_, condition_ck) = check_type_no_card(
                ctx, condition, e.ScalarTp(e.QualifiedName(["std", "bool"]))
            )
            then_tp, then_ck = synthesize_type(ctx, then_branch)
            else_tp, else_ck = synthesize_type(ctx, else_branch)
            result_tp = tops.construct_tp_union(then_tp.tp, else_tp.tp)
            result_card = e.CMMode(
                e.min_cardinal(then_tp.mode.lower, else_tp.mode.lower),
                e.max_cardinal(then_tp.mode.upper, else_tp.mode.upper),
            )
            result_expr = e.IfElseExpr(
                then_branch=then_ck,
                condition=condition_ck,
                else_branch=else_ck,
            )
        case e.ForExpr(bound=bound, next=next):
            (bound_tp, bound_ck) = synthesize_type(ctx, bound)
            new_ctx, next_body, bound_var = eops.tcctx_add_binding(
                ctx, next, e.ResultTp(bound_tp.tp, e.CardOne)
            )
            (next_tp, next_ck) = synthesize_type(new_ctx, next_body)
            result_expr = e.ForExpr(
                bound=bound_ck,
                next=eops.abstract_over_expr(next_ck, bound_var),
            )
            result_tp = next_tp.tp
            result_card = next_tp.mode * bound_tp.mode
        case e.OptionalForExpr(bound=bound, next=next):
            (bound_tp, bound_ck) = synthesize_type(ctx, bound)
            if bound_tp.mode.lower == e.CardNumZero:
                bound_card = e.CardAtMostOne
            elif bound_tp.mode.lower == e.CardNumOne:
                bound_card = e.CardOne
            else:
                raise ValueError("Cannot have inf as lower bound")
            new_ctx, next_body, bound_var = eops.tcctx_add_binding(
                ctx, next, e.ResultTp(bound_tp.tp, bound_card)
            )
            (next_tp, next_ck) = synthesize_type(new_ctx, next_body)
            result_expr = e.OptionalForExpr(
                bound=bound_ck,
                next=eops.abstract_over_expr(next_ck, bound_var),
            )
            result_tp = next_tp.tp
            result_card = next_tp.mode * e.CMMode(
                e.CardNumOne, bound_tp.mode.upper
            )
        case e.UnnamedTupleExpr(val=arr):
            [res_tps, cks] = zip(*[synthesize_type(ctx, v) for v in arr])
            result_expr = e.UnnamedTupleExpr(list(cks))
            [tps, cards] = zip(*res_tps)
            result_tp = e.UnnamedTupleTp(list(tps))
            result_card = reduce(operator.mul, cards, e.CardOne)
        case e.NamedTupleExpr(val=arr):
            [res_tps, cks] = zip(
                *[synthesize_type(ctx, v) for _, v in arr.items()]
            )
            result_expr = e.NamedTupleExpr(
                {k: c for k, c in zip(arr.keys(), cks)}
            )
            [tps, cards] = zip(*res_tps)
            result_tp = e.NamedTupleTp({k: t for k, t in zip(arr.keys(), tps)})
            result_card = reduce(operator.mul, cards, e.CardOne)
        case e.ArrExpr(elems=arr):
            if len(arr) == 0:
                raise ValueError("Empty array does not support type synthesis")
            (first_tp, first_ck) = synthesize_type(ctx, arr[0])
            if len(arr[1:]) > 0:
                rest_card: Sequence[e.CMMode]
                (rest_card, rest_cks) = zip(
                    *[
                        check_type_no_card(ctx, arr_elem, first_tp.tp)
                        for arr_elem in arr[1:]
                    ]
                )
            else:
                rest_card = []
                rest_cks = ()
            # TODO: change to use unions
            result_expr = e.ArrExpr([first_ck] + list(rest_cks))
            result_tp = e.ArrTp(first_tp.tp)
            result_card = reduce(
                operator.mul, rest_card, first_tp.mode
            )  # type: ignore[arg-type]
        case e.MultiSetExpr(expr=arr):
            if len(arr) == 0:
                raise ValueError(
                    "Empty multiset does not support type synthesis"
                )
            (first_tp, first_ck) = synthesize_type(ctx, arr[0])
            if len(arr[1:]) == 0:
                result_expr = e.MultiSetExpr([first_ck])
                result_tp = first_tp.tp
                result_card = first_tp.mode
            else:
                (rest_res_tps, rest_cks) = zip(
                    *[synthesize_type(ctx, arr_elem) for arr_elem in arr[1:]]
                )
                rest_tps, rest_cards = zip(*rest_res_tps)
                result_expr = e.MultiSetExpr([first_ck] + list(rest_cks))
                result_tp = reduce(
                    tops.construct_tp_union, rest_tps, first_tp.tp
                )
                result_card = reduce(
                    operator.add, rest_cards, first_tp.mode
                )  # type: ignore[arg-type]
        case _:
            raise ValueError("Not Implemented", expr)

    if isinstance(result_tp, e.ObjectTp):
        raise ValueError(
            "Must return NominalLinkTp instead of object tp", expr, result_tp
        )
    if isinstance(result_tp, e.UncheckedTypeName):
        raise ValueError("Must not return UncheckedTypeName", expr, result_tp)

    return (e.ResultTp(result_tp, result_card), result_expr)


def expr_tp_is_not_synthesizable(expr: e.Expr) -> bool:
    match expr:
        case e.MultiSetExpr(expr=[]):
            return True
        case e.ArrExpr(elems=[]):
            return True
        case _:
            return False


def check_type_no_card(
    ctx: e.TcCtx, expr: e.Expr, tp: e.Tp, with_assignment_cast: bool = False
) -> Tuple[e.CMMode, e.Expr]:
    match expr:
        case e.MultiSetExpr(expr=[]):
            return (e.CardAtMostOne, expr)
        case e.ArrExpr(elems=[]):
            return (e.CardAtMostOne, expr)
        case _:
            expr_tp, expr_ck = synthesize_type(ctx, expr)
            default_return = (expr_tp.mode, expr_ck)
            if tops.check_is_subtype(ctx, expr_tp.tp, tp):
                return default_return
            else:
                if (expr_tp.tp, tp) in ctx.schema.casts:
                    cast_fun = ctx.schema.casts[(expr_tp.tp, tp)]
                    match cast_fun.kind:
                        case e.TpCastKind.Explicit:
                            raise ValueError("Not a sub type", expr_tp.tp, tp)
                        case e.TpCastKind.Implicit:
                            return (
                                expr_tp.mode,
                                e.CheckedTypeCastExpr(
                                    (expr_tp.tp, tp), cast_fun, expr_ck
                                ),
                            )
                        case e.TpCastKind.Assignment:
                            if with_assignment_cast:
                                return (
                                    expr_tp.mode,
                                    e.CheckedTypeCastExpr(
                                        (expr_tp.tp, tp), cast_fun, expr_ck
                                    ),
                                )
                            else:
                                raise ValueError(
                                    "Not a sub type", expr_tp.tp, tp
                                )
                        case _:
                            raise ValueError("Not Implemented", cast_fun.kind)
                else:
                    raise ValueError(
                        "Not a sub type", pp.show(expr_tp.tp), pp.show(tp)
                    )


def check_type(
    ctx: e.TcCtx,
    expr: e.Expr,
    tp: e.ResultTp,
    with_assignment_cast: bool = False,
) -> e.Expr:
    synth_mode, expr_ck = check_type_no_card(
        ctx, expr, tp.tp, with_assignment_cast
    )
    tops.assert_cardinal_subtype(synth_mode, tp.mode)
    return expr_ck


def check_type_valid(ctx: e.TcCtx | e.DBSchema, tp: e.Tp) -> e.Tp:
    """
    Check that a raw schema type is a valid type.
    Returns the checked valid type.
    """
    match tp:
        case e.UncheckedTypeName(name=name):
            resolved_name, resolved_tp = mops.resolve_raw_name_and_type_def(
                ctx, name
            )
            match resolved_tp:
                case e.ScalarTp(_):
                    return resolved_tp
                case e.ObjectTp(_):
                    return e.NamedNominalLinkTp(
                        name=resolved_name, linkprop=e.ObjectTp({})
                    )
                case _:
                    raise ValueError("Not Implemented", resolved_tp)
        case e.AnyTp(_):
            return tp
        case e.CompositeTp(kind=kind, tps=tps, labels=labels):
            return e.CompositeTp(
                kind=kind,
                tps=[check_type_valid(ctx, t) for t in tps],
                labels=labels,
            )
        case e.QualifiedName(_):
            return tp
        case _:
            raise ValueError("Not Implemented", tp)
