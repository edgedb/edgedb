
from functools import reduce
import operator
from typing import Tuple, Dict, Sequence, Optional, List

from .data import data_ops as e
from .data import expr_ops as eops
from .data import type_ops as tops
from edb.common import debug
from .data import path_factor as path_factor

# def enforce_singular(expr: e.Expr, card: e.CMMode) -> e.Expr:
#     """ returns the singular expression of the upper bound
#     of the cardinality is one"""
#     if (isinstance(card.upper, e.FiniteCardinal)
#             and card.upper.value == 1
#             and not isinstance(expr, e.SingularExpr)):
#         return e.SingularExpr(expr=expr)
#     else:
#         return expr


# def synthesize_type_for_val_seq(
#                                 val_seq: Sequence[e.Val]) -> e.Tp:
#     first_type = synthesize_type_for_val(val_seq[0])
#     [check_type(e.TcCtx(ctx, {}), v,
#                 e.ResultTp(first_type, e.CardOne))
#         for v in val_seq[1:]]
#     return first_type


# def synthesize_type_for_multiset_val(
#         ctx: e.RTData,
#         val: e.MultiSetVal) -> e.ResultTp:
#     if len(val.vals) == 0:
#         raise ValueError("Cannot synthesize type for empty list")
#     else:
#         card = len(val.vals)
#         return e.ResultTp(
#             synthesize_type_for_val_seq(ctx, val.vals),
#             e.CMMode(e.FiniteCardinal(card), e.FiniteCardinal(card)))


# def synthesize_type_for_object_val(
#         ctx: e.RTData,
#         val: e.ObjectVal) -> e.Tp:
#     obj_tp: Dict[str, e.ResultTp] = {}
#     linkprop_tp: Dict[str, e.ResultTp] = {}

#     for lbl, v in val.val.items():
#         match lbl:
#             case e.StrLabel(label=s_lbl):
#                 if s_lbl in obj_tp.keys():
#                     raise ValueError("duplicate keys in object val")
#                 else:
#                     obj_tp = {
#                         **obj_tp,
#                         s_lbl: synthesize_type_for_multiset_val(ctx, v[1])}
#             case e.LinkPropLabel(label=l_lbl):
#                 if l_lbl in linkprop_tp.keys():
#                     raise ValueError("duplicate keys in object val")
#                 else:
#                     linkprop_tp = {
#                         **linkprop_tp,
#                         l_lbl: synthesize_type_for_multiset_val(ctx, v[1])}

#     if len(linkprop_tp.keys()) == 0:
#         return e.ObjectTp(obj_tp)
#     else:
#         return e.LinkPropTp(
#             e.ObjectTp(obj_tp),
#             e.ObjectTp(linkprop_tp))


def synthesize_type_for_val(val: e.Val) -> e.Tp:
    match val:
        case e.StrVal(_):
            return e.StrTp()
        case e.IntVal(_):
            return e.IntTp()
        case e.IntInfVal():
            return e.IntInfTp()
        case e.BoolVal(_):
            return e.BoolTp()
        # case e.RefVal(refid=id, val=obj):
        #     # ref_tp = ctx.schema.val[ctx.cur_db.dbdata[id].tp]
        #     ref_obj = ctx.cur_db.dbdata[id].data
        #     combined = eops.combine_object_val(ref_obj, obj)
        #     return synthesize_type_for_object_val(ctx, combined)
        # case e.FreeVal(val=obj):
        #     return synthesize_type_for_object_val(ctx, obj)
        # case e.ArrVal(val=arr):
        #     return e.ArrTp(
        #         synthesize_type_for_val_seq(ctx, arr))
        # case e.UnnamedTupleVal(val=arr):
        #     return e.UnnamedTupleTp(
        #         [synthesize_type_for_val(ctx, e) for e in arr])
        # case e.NamedTupleVal(val=obj):
        #     return e.NamedTupleTp(
        #        {n: synthesize_type_for_val(ctx, e) for n, e in obj.items()})
        # case e.LinkPropVal(refid=id, linkprop=linkprop):
        #     obj_tp: e.Tp = ctx.schema.val[ctx.cur_db.dbdata[id].tp.name]
        #     obj_tp = tops.get_runtime_tp(obj_tp)
        #     linkprop_tp = synthesize_type_for_object_val(ctx, linkprop)
        #     assert isinstance(linkprop_tp, e.ObjectTp)
        #     return e.LinkPropTp(obj_tp, linkprop_tp)
        case _:
            raise ValueError("Not implemented", val)


def check_shape_transform(ctx: e.TcCtx, s: e.ShapeExpr,
                          tp: e.Tp,
                          is_insert_shape: bool = False
                          ) -> Tuple[e.Tp, e.ShapeExpr]:
    s_tp: e.ObjectTp
    l_tp: e.ObjectTp

    # populate result skeleton
    match tp:
        case e.LinkPropTp(subject=subject_tp, linkprop=linkprop_tp):
            l_tp = linkprop_tp
            if isinstance(subject_tp, e.ObjectTp):
                s_tp = subject_tp
            elif isinstance(subject_tp, e.VarTp):
                s_tp = tops.dereference_var_tp(ctx.schema, subject_tp)
            else:
                raise ValueError("NI", subject_tp)
        case e.ObjectTp(_):
            s_tp = tp
            l_tp = e.ObjectTp({})
        case _:
            raise ValueError("NI")

    result_s_tp = e.ObjectTp({})
    result_l_tp = e.ObjectTp({})
    result_expr = e.ShapeExpr({})

    for lbl, comp in s.shape.items():
        match lbl:
            case e.StrLabel(s_lbl):
                if s_lbl in s_tp.val.keys():
                    new_ctx, body, bnd_var = eops.tcctx_add_binding(
                        ctx, comp, e.ResultTp(tp, e.CardOne))
                    result_tp = s_tp.val[s_lbl]
                    body_tp_synth, body_ck = synthesize_type(new_ctx, body)
                    if is_insert_shape:
                        tops.assert_insert_subtype(
                            ctx, body_tp_synth.tp, result_tp.tp)
                    else:
                        tops.assert_shape_subtype(
                            ctx, body_tp_synth.tp, result_tp.tp)
                    tops.assert_cardinal_subtype(
                        body_tp_synth.mode, result_tp.mode)
                    result_s_tp = e.ObjectTp({**result_s_tp.val,
                                              s_lbl: body_tp_synth})
                    result_expr = e.ShapeExpr(
                        {**result_expr.shape,
                         lbl: eops.abstract_over_expr(body_ck, bnd_var)})
                else:
                    new_ctx, body, bnd_var = eops.tcctx_add_binding(
                        ctx, comp, e.ResultTp(tp, e.CardOne))
                    result_tp, checked_body = synthesize_type(
                        new_ctx, body)
                    result_s_tp = e.ObjectTp({**result_s_tp.val,
                                              s_lbl: result_tp})
                    result_expr = e.ShapeExpr(
                        {**result_expr.shape,
                         lbl: eops.abstract_over_expr(checked_body, bnd_var)})
            case e.LinkPropLabel(l_lbl):
                if l_lbl in l_tp.val.keys():
                    new_ctx, body, bnd_var = eops.tcctx_add_binding(
                        ctx, comp, e.ResultTp(tp, e.CardOne))
                    result_tp = l_tp.val[l_lbl]
                    body_synth_tp, body_ck = synthesize_type(new_ctx, body)
                    if is_insert_shape:
                        tops.assert_insert_subtype(
                            ctx, body_synth_tp.tp, result_tp.tp)
                    else:
                        tops.assert_shape_subtype(
                            ctx, body_synth_tp.tp, result_tp.tp)
                    tops.assert_cardinal_subtype(
                        body_synth_tp.mode, result_tp.mode)
                    result_l_tp = e.ObjectTp({**result_l_tp.val,
                                              l_lbl: body_synth_tp})
                    result_expr = e.ShapeExpr(
                        {**result_expr.shape,
                         lbl: eops.abstract_over_expr(body_ck, bnd_var)})
                else:
                    new_ctx, body, bnd_var = eops.tcctx_add_binding(
                        ctx, comp, e.ResultTp(tp, e.CardOne))
                    result_tp, checked_body = synthesize_type(
                        new_ctx, body)
                    result_l_tp = e.ObjectTp({**result_l_tp.val,
                                              l_lbl: result_tp})
                    result_expr = e.ShapeExpr(
                        {**result_expr.shape,
                         lbl: eops.abstract_over_expr(checked_body, bnd_var)})

    for t_lbl, s_comp_tp in s_tp.val.items():
        if e.StrLabel(t_lbl) not in s.shape.keys():
            result_s_tp = e.ObjectTp({**result_s_tp.val,
                                      t_lbl: s_comp_tp})

    for t_lbl, l_comp_tp in l_tp.val.items():
        if e.LinkPropLabel(t_lbl) not in s.shape.keys():
            result_l_tp = e.ObjectTp({**result_l_tp.val,
                                      t_lbl: l_comp_tp})

    return e.LinkPropTp(result_s_tp, result_l_tp), result_expr


def type_cast_tp(from_tp: e.ResultTp, to_tp: e.Tp) -> e.ResultTp:
    match from_tp.tp, from_tp.mode, to_tp:
        case e.UnifiableTp(id=_, resolution=None), card, _:
            assert isinstance(from_tp.tp, e.UnifiableTp)  # for mypy
            from_tp.tp.resolution = to_tp
            return e.ResultTp(to_tp, card)

        case e.IntTp(), card, e.IntTp():
            return e.ResultTp(e.IntTp(), card)
        case e.UuidTp(), card, e.StrTp():
            return e.ResultTp(e.StrTp(), card)
        case e.IntTp(), card, e.StrTp():
            return e.ResultTp(e.StrTp(), card)
        case _:
            raise ValueError("Not Implemented", from_tp, to_tp)


def synthesize_type(ctx: e.TcCtx, expr: e.Expr) -> Tuple[e.ResultTp, e.Expr]:
    result_tp: e.Tp
    result_card: e.CMMode
    result_expr: e.Expr = expr  # by default we don't change expr

    match expr:
        case (e.StrVal(_)
              | e.IntVal(_)
              | e.BoolVal(_)
              ):
            result_tp = synthesize_type_for_val(expr)
            result_card = e.CardOne
        case e.FreeVarExpr(var=var):
            if var in ctx.varctx.keys():
                result_tp, result_card = ctx.varctx[var]
            elif var in ctx.schema.val.keys():
                result_tp = ctx.schema.val[var]
                result_card = e.CardAny
            else:
                raise ValueError("Unknown variable", var,
                                 "list of known vars",
                                 list(ctx.varctx.keys())
                                 + list(ctx.schema.val.keys()))
        case e.TypeCastExpr(tp=tp, arg=arg):
            (arg_tp, arg_v) = synthesize_type(ctx, arg)
            (result_tp, result_card) = type_cast_tp(arg_tp, tp)
            result_expr = e.TypeCastExpr(tp, arg_v)
        case e.ShapedExprExpr(expr=subject, shape=shape):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            result_tp, shape_ck = check_shape_transform(
                ctx, shape, subject_tp.tp)
            if not eops.is_effect_free(shape):
                raise ValueError("Shape should be effect free", shape)
            result_card = subject_tp.mode
            result_expr = e.ShapedExprExpr(subject_ck, shape_ck)
        case e.UnionExpr(left=l, right=r):
            (l_tp, l_ck) = synthesize_type(ctx, l)
            (r_tp, r_ck) = synthesize_type(ctx, r)
            # assert l_tp.tp == r_tp.tp, "Union types must match"
            result_tp = tops.construct_tp_union(l_tp.tp, r_tp.tp)
            result_card = l_tp.mode + r_tp.mode
            result_expr = e.UnionExpr(l_ck, r_ck)
        case e.FunAppExpr(fun=fname, args=args, overloading_index=idx):
            assert idx is None, ("Overloading should be empty "
                                 "before type checking")
            fun_tp = ctx.schema.fun_defs[fname].tp
            if fun_tp.effect_free:
                assert all(eops.is_effect_free(arg) for arg in args), (
                    "Expect effectful arguments to effect-free functions")
            
            assert len(args) == len(fun_tp.args_mod), "argument count mismatch"

            # if len(fun_tp.args_ret_types) > 1:
            #     raise ValueError("Overloading not implemented")
            # assert len(fun_tp.args_ret_types) == 1, "TODO: overloading"


            def try_args_ret_type(args_ret_type: e.FunArgRetType) -> Tuple[
                    e.Tp, e.CMMode, e.Expr]:

                some_tp_mapping: Dict[int, e.UnifiableTp] = {}

                def instantiate_some_tp(tp: e.Tp) -> Optional[e.Tp]:
                    nonlocal some_tp_mapping
                    if isinstance(tp, e.SomeTp):
                        if tp.index in some_tp_mapping.keys():
                            return some_tp_mapping[tp.index]
                        else:
                            new_tp = e.UnifiableTp(id=e.next_id(),
                                                   resolution=None)
                            some_tp_mapping[tp.index] = new_tp
                            return new_tp
                    else:
                        return None

                args_ret_type = e.FunArgRetType(
                    args_tp=[eops.map_tp(instantiate_some_tp, t)
                             for t in args_ret_type.args_tp],
                    ret_tp=e.ResultTp(eops.map_tp(instantiate_some_tp,
                                                  args_ret_type.ret_tp.tp),
                                      args_ret_type.ret_tp.mode))

                arg_cards, arg_cks = zip(*[check_type_no_card(
                        ctx, arg, args_ret_type.args_tp[i])
                        for i, arg in enumerate(args)])

                # special processing of cardinality inference for certain functions
                match fname:
                    case "??":
                        assert len(arg_cards) == 2
                        result_card = e.CMMode(
                            e.min_cardinal(arg_cards[0].lower,
                                           arg_cards[1].lower),
                            e.max_cardinal(arg_cards[0].upper,
                                           arg_cards[1].upper),
                            e.max_cardinal(arg_cards[0].multiplicity,
                                           arg_cards[1].multiplicity)
                        )
                    case _:
                        # take the product of argument cardinalities
                        arg_card_product = reduce(
                            operator.mul,
                            (tops.match_param_modifier(param_mod, arg_card)
                             for param_mod, arg_card
                             in zip(fun_tp.args_mod, arg_cards, strict=True)))
                        result_card = (arg_card_product
                                       * args_ret_type.ret_tp.mode)
                
                result_tp = args_ret_type.ret_tp.tp
                result_expr = e.FunAppExpr(fun=fname, args=arg_cks,
                                           overloading_index=idx)
                return result_tp, result_card, result_expr

            if idx is not None:
                args_ret_type = fun_tp.args_ret_types[idx]
                result_tp, result_card, result_expr = try_args_ret_type(
                    args_ret_type)
            else:
                for i, args_ret_type in enumerate(fun_tp.args_ret_types):
                    try:
                        result_tp, result_card, result_expr = \
                            try_args_ret_type(args_ret_type)
                        break
                    except ValueError:
                        if i == len(fun_tp.args_ret_types) - 1:
                            raise
                        else:
                            continue
        case e.ObjectExpr(val=dic):
            s_tp = e.ObjectTp({})
            link_tp = e.ObjectTp({})
            dic_ck: Dict[e.Label, e.Expr] = {}
            for k, v in dic.items():
                (v_tp, v_ck) = synthesize_type(ctx, v)
                dic_ck = {**dic_ck, k: v_ck}
                match k:
                    case e.StrLabel(s_lbl):
                        s_tp = e.ObjectTp({**s_tp.val,
                                           s_lbl: v_tp})
                    case e.LinkPropLabel(s_lbl):
                        link_tp = e.ObjectTp({**link_tp.val,
                                              s_lbl: v_tp})
            if len(link_tp.val.keys()) > 0:
                result_tp = e.LinkPropTp(s_tp, link_tp)
            else:
                result_tp = s_tp
            result_card = e.CardOne
            result_expr = e.ObjectExpr(dic_ck)
        case e.ObjectProjExpr(subject=subject, label=label):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            result_expr = e.ObjectProjExpr(subject_ck, label)
            result_tp, result_card = tops.tp_project(
                ctx, subject_tp, e.StrLabel(label))
        case e.LinkPropProjExpr(subject=subject, linkprop=lp):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            result_expr = e.LinkPropProjExpr(subject_ck, lp)
            result_tp, result_card = tops.tp_project(
                ctx, subject_tp, e.LinkPropLabel(lp))
        case e.BackLinkExpr(subject=subject, label=label):
            (_, subject_ck) = synthesize_type(ctx, subject)
            candidates: List[e.LinkPropTp] = []
            for (name, name_def) in ctx.schema.val.items():
                for (name_label, comp_tp) in name_def.val.items():
                    if name_label == label:
                        match comp_tp.tp:
                            case e.LinkPropTp(_):
                                candidates = [
                                    *candidates,
                                    e.LinkPropTp(e.VarTp(name),
                                                 comp_tp.tp.linkprop)
                                ] 
                            case _:
                                candidates = [
                                    *candidates,
                                    e.LinkPropTp(e.VarTp(name),
                                                 e.ObjectTp({}))
                                ]
            result_expr = e.BackLinkExpr(subject_ck, label)
            if len(candidates) == 0:
                result_tp = e.AnyTp()
            else:
                result_tp = reduce(
                    tops.construct_tp_union,  # type: ignore[arg-type]
                    candidates)
            result_card = e.CardAny
        case e.TpIntersectExpr(subject=subject, tp=intersect_tp):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            result_expr = e.TpIntersectExpr(subject_ck, intersect_tp)
            if all(isinstance(t, e.LinkPropTp)
                   for t in tops.collect_tp_union(subject_tp.tp)):
                candidates = []
                for t in tops.collect_tp_union(subject_tp.tp):
                    assert isinstance(t, e.LinkPropTp)
                    # TODO: use real subtp
                    if t.subject == e.VarTp(intersect_tp):
                        candidates = [*candidates, t]
                if len(candidates) == 0:
                    result_tp = tops.construct_tp_intersection(
                        subject_tp.tp, e.VarTp(intersect_tp))
                else:
                    result_tp = reduce(
                        tops.construct_tp_union,  # type: ignore[arg-type]
                        candidates)
            else:
                result_tp = tops.construct_tp_intersection(
                    subject_tp.tp, e.VarTp(intersect_tp))
            result_card = e.CMMode(
                e.Fin(0),
                subject_tp.mode.upper, 
                subject_tp.mode.multiplicity
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
                ctx, next_expr, bound_tp)
            (next_tp, next_ck) = synthesize_type(new_ctx, body)
            result_expr = e.WithExpr(
                bound_ck, eops.abstract_over_expr(next_ck, bound_var))
            result_tp, result_card = next_tp
        case e.FilterOrderExpr(subject=subject, filter=filter, order=order):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            filter_ctx, filter_body, filter_bound_var = eops.tcctx_add_binding(
                ctx, filter, e.ResultTp(subject_tp.tp, e.CardOne))
            order_ctx, order_body, order_bound_var = eops.tcctx_add_binding(
                ctx, order, e.ResultTp(subject_tp.tp, e.CardOne))
            
            assert eops.is_effect_free(filter), "Expecting effect-free filter"
            assert eops.is_effect_free(order), "Expecting effect-free filter"

            (_, filter_ck) = check_type_no_card(
                filter_ctx, filter_body, e.BoolTp())
            (order_tp, order_ck) = synthesize_type(order_ctx, order_body)

            assert tops.is_order_spec(order_tp), "Expecting order spec"

            result_expr = e.FilterOrderExpr(
                subject_ck,
                eops.abstract_over_expr(filter_ck, filter_bound_var),
                eops.abstract_over_expr(order_ck, order_bound_var))
            result_tp = subject_tp.tp
            result_card = e.CMMode(
                e.Fin(0),
                subject_tp.mode.upper,
                subject_tp.mode.multiplicity
            )
        case e.OffsetLimitExpr(subject=subject, offset=offset, limit=limit):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            offset_ck = check_type(ctx, offset,
                                   e.ResultTp(e.IntTp(), e.CardAtMostOne))
            limit_ck = check_type(ctx, limit,
                                  e.ResultTp(e.IntInfTp(), e.CardAtMostOne))
            result_expr = e.OffsetLimitExpr(subject_ck, offset_ck, limit_ck)
            result_tp = subject_tp.tp
            result_card = e.CMMode(
                e.Fin(0),
                subject_tp.mode.upper,
                subject_tp.mode.multiplicity
            )
            if isinstance(limit_ck, e.IntVal):
                lim_num = limit_ck.val
                result_card = e.CMMode(
                    e.Fin(0),
                    e.min_cardinal(result_card.upper, e.Fin(lim_num)),
                    e.min_cardinal(result_card.multiplicity, e.Fin(lim_num)))
        case e.InsertExpr(name=tname, new=arg):
            tname_tp = tops.get_runtime_tp(ctx.schema.val[tname])
            arg_shape_tp, arg_ck = check_shape_transform(
                ctx, arg, tname_tp, is_insert_shape=True)
            # assert arg_tp.mode == e.CardOne, (
            #         "Expecting single object in inserts"
            #     )
            # assert isinstance(arg, e.ObjectExpr), (
            #     "Expecting object expr in inserts")
            assert all(isinstance(k, e.StrLabel) for k in arg.shape.keys()), (
                        "Expecting object expr in inserts")
            # assert isinstance(arg_shape_tp, e.ObjectTp), (
            #     "Expecting inserts to be of object tp")
            tops.assert_insert_subtype(ctx, arg_shape_tp, tname_tp)
            result_expr = e.InsertExpr(tname, arg_ck)
            result_tp = tname_tp
            result_card = e.CardOne
        case e.UpdateExpr(subject=subject, shape=shape_expr):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            assert eops.is_effect_free(shape_expr), (
                "Expecting shape expr to be effect-free")
            assert eops.is_effect_free(subject), (
                "Expecting subject expr to be effect-free")
            (after_tp, shape_ck) = check_shape_transform(
                ctx, shape_expr, subject_tp.tp)
            tops.assert_insert_subtype(ctx, after_tp, subject_tp.tp)
            result_expr = e.UpdateExpr(subject_ck, shape_ck)
            result_tp, result_card = subject_tp
        case e.DeleteExpr(subject=subject):
            (subject_tp, subject_ck) = synthesize_type(ctx, subject)
            assert eops.is_effect_free(subject), (
                "Expecting subject expr to be effect-free")
            result_expr = e.DeleteExpr(subject_ck)
            result_tp, result_card = subject_tp
        case e.IfElseExpr(then_branch=then_branch,
                          condition=condition,
                          else_branch=else_branch):
            (_, condition_ck) = check_type_no_card(
                ctx, condition, e.BoolTp())
            then_tp, then_ck = synthesize_type(ctx, then_branch)
            else_tp, else_ck = synthesize_type(ctx, else_branch)
            # TODO: should we check if they are the same?
            result_tp = tops.construct_tp_union(then_tp.tp, else_tp.tp)
            result_card = e.CMMode(
                e.min_cardinal(then_tp.mode.lower, else_tp.mode.lower),
                e.max_cardinal(then_tp.mode.upper, else_tp.mode.upper),
                e.max_cardinal(then_tp.mode.multiplicity,
                               else_tp.mode.multiplicity)
            )
            result_expr = e.IfElseExpr(
                    then_branch=then_ck,
                    condition=condition_ck,
                    else_branch=else_ck)
        case e.ForExpr(bound=bound, next=next):
            (bound_tp, bound_ck) = synthesize_type(ctx, bound)
            new_ctx, next_body, bound_var = eops.tcctx_add_binding(
                ctx, next, e.ResultTp(bound_tp.tp, e.CardOne))
            (next_tp, next_ck) = synthesize_type(new_ctx, next_body)
            result_expr = e.ForExpr(
                bound=bound_ck,
                next=eops.abstract_over_expr(next_ck, bound_var))
            result_tp = next_tp.tp
            result_card = next_tp.mode * bound_tp.mode
        case e.OptionalForExpr(bound=bound, next=next):
            (bound_tp, bound_ck) = synthesize_type(ctx, bound)
            bound_card = e.CMMode(
                e.max_cardinal(e.Fin(0), e.min_cardinal(e.Fin(1),
                                                        bound_tp.mode.lower)),
                e.Fin(1),
                e.Fin(1)
            )
            new_ctx, next_body, bound_var = eops.tcctx_add_binding(
                ctx, next, e.ResultTp(bound_tp.tp, bound_card))
            (next_tp, next_ck) = synthesize_type(new_ctx, next_body)
            result_expr = e.OptionalForExpr(
                bound=bound_ck,
                next=eops.abstract_over_expr(next_ck, bound_var))
            result_tp = next_tp.tp
            result_card = next_tp.mode * e.CMMode(
                e.max_cardinal(e.min_cardinal(
                    e.Fin(1), bound_tp.mode.lower), bound_tp.mode.lower),
                e.max_cardinal(e.min_cardinal(
                    e.Fin(1), bound_tp.mode.upper), bound_tp.mode.upper),
                e.max_cardinal(e.min_cardinal(
                    e.Fin(1), bound_tp.mode.multiplicity),
                    bound_tp.mode.multiplicity))
        case e.UnnamedTupleExpr(val=arr):
            [res_tps, cks] = zip(*[synthesize_type(ctx, v) for v in arr])
            result_expr = e.UnnamedTupleExpr(list(cks))
            [tps, cards] = zip(*res_tps)
            result_tp = e.UnnamedTupleTp(list(tps))
            result_card = reduce(operator.mul, cards, e.CardOne)
        case e.NamedTupleExpr(val=arr):
            [res_tps, cks] = zip(*[synthesize_type(ctx, v)
                                   for _, v in arr.items()])
            result_expr = e.NamedTupleExpr({k: c
                                            for k, c in zip(arr.keys(), cks)})
            [tps, cards] = zip(*res_tps)
            result_tp = e.NamedTupleTp({k: t for k, t in zip(arr.keys(), tps)})
            result_card = reduce(operator.mul, cards, e.CardOne)
        case e.ArrExpr(elems=arr):
            assert len(arr) > 0, "Empty array does not support type synthesis"
            (first_tp, first_ck) = synthesize_type(ctx, arr[0])
            rest_card: Sequence[e.CMMode]
            (rest_card, rest_cks) = zip(
                *[check_type_no_card(ctx, arr_elem, first_tp.tp)
                  for arr_elem in arr[1:]])
            # TODO: change to use unions
            result_expr = e.ArrExpr([first_ck] + list(rest_cks))
            result_tp = e.ArrTp(first_tp.tp)
            result_card = reduce(operator.mul, rest_card,
                                 first_tp.mode)  # type: ignore[arg-type]
        case e.MultiSetExpr(expr=arr):
            if len(arr) == 0:
                return (e.ResultTp(e.UnifiableTp(e.next_id()),
                                   e.CardZero), expr)  # this is a hack
            # assert len(arr) > 0, ("Empty multiset does not"
            #                       " support type synthesis")
            (first_tp, first_ck) = synthesize_type(ctx, arr[0])
            if len(arr[1:]) == 0:
                result_expr = e.MultiSetExpr([first_ck])
                result_tp = first_tp.tp
                result_card = first_tp.mode
            else:
                (rest_res_tps, rest_cks) = zip(
                    *[synthesize_type(ctx, arr_elem)
                        for arr_elem in arr[1:]])
                rest_tps, rest_cards = zip(*rest_res_tps)
                result_expr = e.MultiSetExpr([first_ck] + list(rest_cks))
                result_tp = reduce(tops.construct_tp_union, rest_tps, first_tp.tp)
                result_card = reduce(operator.add, rest_cards,
                                     first_tp.mode)  # type: ignore[arg-type]
        case _:
            raise ValueError("Not Implemented", expr)

    # enforce singular
    # result_expr = enforce_singular(result_expr)

    return (e.ResultTp(result_tp, result_card), result_expr)


def check_type_no_card(ctx: e.TcCtx, expr: e.Expr,
                       tp: e.Tp) -> Tuple[e.CMMode, e.Expr]:
    match expr:
        case _:
            expr_tp, expr_ck = synthesize_type(ctx, expr)
            tops.assert_real_subtype(ctx, expr_tp.tp, tp)
            return (expr_tp.mode, expr_ck)
            # if tops.is_real_subtype(expr_tp.tp, tp):
            #     return (expr_tp.mode, expr_ck)
            # else:
            # raise ValueError("Type mismatch, ", expr_tp, "is not a sub"
            #                  "type of ", tp, "when checking", expr)


def check_type(ctx: e.TcCtx, expr: e.Expr, tp: e.ResultTp) -> e.Expr:
    synth_mode, expr_ck = check_type_no_card(ctx, expr, tp.tp)
    tops.assert_cardinal_subtype(synth_mode, tp.mode)
    # ( "Expecting cardinality %s, got %s" % (tp.mode, synth_mode))
    return expr_ck
    

def check_object_tp_comp_validity(
        dbschema: e.DBSchema,
        subject_tp: e.Tp,
        tp_comp: e.Tp,
        tp_comp_card: e.CMMode) -> e.Tp:
    match tp_comp:
        case e.LinkPropTp(subject=l_sub, linkprop=l_prop):
            return e.LinkPropTp(
                    subject=l_sub,
                    linkprop=check_object_tp_validity(
                        dbschema=dbschema,
                        subject_tp=tops.get_runtime_tp(tp_comp),
                        obj_tp=l_prop))
        case e.UncheckedComputableTp(expr=c_expr):
            if not isinstance(c_expr, e.BindingExpr):  # type: ignore
                raise ValueError(
                    "Computable type must be a binding expression")
            new_ctx, c_body, bnd_var = eops.tcctx_add_binding(
                eops.emtpy_tcctx_from_dbschema(dbschema),
                c_expr,  # type: ignore
                e.ResultTp(subject_tp, e.CardOne)
            )
            c_body = path_factor.select_hoist(c_body, dbschema)
            synth_tp, c_body_ck = synthesize_type(new_ctx, c_body)
            tops.assert_cardinal_subtype(synth_tp.mode, tp_comp_card)
            return e.ComputableTp(
                expr=eops.abstract_over_expr(c_body_ck, bnd_var),
                tp=synth_tp.tp)
        case e.ComputableTp(expr=c_expr, tp=c_tp):
            if not isinstance(c_expr, e.BindingExpr):  # type: ignore
                raise ValueError(
                    "Computable type must be a binding expression")
            new_ctx, c_body, bnd_var = eops.tcctx_add_binding(
                eops.emtpy_tcctx_from_dbschema(dbschema),
                c_expr,  # type: ignore
                e.ResultTp(subject_tp, e.CardOne)
            )
            c_body = path_factor.select_hoist(c_body, dbschema)
            synth_tp, c_body_ck = synthesize_type(new_ctx, c_body)
            tops.assert_cardinal_subtype(synth_tp.mode, tp_comp_card)
            tops.assert_real_subtype(new_ctx, synth_tp.tp, c_tp)
            return e.ComputableTp(
                expr=eops.abstract_over_expr(c_body_ck, bnd_var),
                tp=c_tp)
        # This code is mostly copied from the above
        # TODO: Can we not copy?
        case e.DefaultTp(expr=c_expr, tp=c_tp):
            if not isinstance(c_expr, e.BindingExpr):  # type: ignore
                raise ValueError(
                    "Computable type must be a binding expression")
            new_ctx, c_body, bnd_var = eops.tcctx_add_binding(
                eops.emtpy_tcctx_from_dbschema(dbschema),
                c_expr,  # type: ignore
                e.ResultTp(subject_tp, e.CardOne)
            )
            c_body = path_factor.select_hoist(c_body, dbschema)
            synth_tp, c_body_ck = synthesize_type(new_ctx, c_body)
            tops.assert_cardinal_subtype(synth_tp.mode, tp_comp_card)
            match c_tp:
                case e.LinkPropTp(subject=c_tp_subject, linkprop=_):
                    tops.assert_real_subtype(new_ctx, synth_tp.tp,
                                             c_tp_subject)
                case _:
                    tops.assert_real_subtype(new_ctx, synth_tp.tp, c_tp)
            return e.DefaultTp(
                expr=eops.abstract_over_expr(c_body_ck, bnd_var),
                tp=c_tp)
        case _:
            return tp_comp


def check_object_tp_validity(dbschema: e.DBSchema,
                             subject_tp: e.Tp,
                             obj_tp: e.ObjectTp) -> e.ObjectTp:
    result_vals: Dict[str, e.ResultTp] = {}
    for lbl, (t_comp_tp, t_comp_card) in obj_tp.val.items():
        result_vals[lbl] = e.ResultTp(
            check_object_tp_comp_validity(
                dbschema=dbschema,
                subject_tp=subject_tp,
                tp_comp=t_comp_tp,
                tp_comp_card=t_comp_card), t_comp_card)
    return e.ObjectTp(result_vals)


def check_schmea_validity(dbschema: e.DBSchema) -> e.DBSchema:
    result_vals: Dict[str, e.ObjectTp] = {}
    for t_name, obj_tp in dbschema.val.items():
        result_vals = {**result_vals, t_name:
                       check_object_tp_validity(
                            dbschema, e.VarTp(t_name),
                            obj_tp)}
    return e.DBSchema(val=result_vals, fun_defs=dbschema.fun_defs)


