
from ..data import data_ops as e
from ..data import expr_ops as eops
from ..data import type_ops as tops
from ..data import path_factor as pops
from typing import List, Tuple, Dict, Optional
# from itertools import *
from functools import reduce
import operator


def check_args_ret_type_match(ctx : e.TcCtx, tps_syn: List[e.Tp], tps_ck: e.FunArgRetType) -> Optional[e.Tp]: # Returns the result Tp if matches
    """
    If matches, return the result type. Need to return result type because we have parametric ploymorphism.
    """

    some_tp_mapping: Dict[int, e.Tp] = {}

    args_ck_tps = tps_ck.args_tp
    ret_tp = tps_ck.ret_tp.tp

    if len(args_ck_tps) != len(tps_syn):
        return None
    
    for i, (syn_tp, ck_tp) in enumerate(zip(tps_syn, args_ck_tps)):
        real_ck_tp : e.Tp 
        if isinstance(ck_tp, e.SomeTp):
            if ck_tp.index in some_tp_mapping:
                real_ck_tp = some_tp_mapping[ck_tp.index]
            else:
                some_tp_mapping[ck_tp.index] = syn_tp
                real_ck_tp = syn_tp
        else:
            real_ck_tp = ck_tp
        assert real_ck_tp is not None
        if not tops.check_is_subtype(ctx, syn_tp, real_ck_tp):
            return None

    if isinstance(ret_tp, e.SomeTp):
        if ret_tp.index in some_tp_mapping:
            return some_tp_mapping[ret_tp.index]
        else:
            raise ValueError("Function's return type has to be grounded by input types.")
    else:
        return ret_tp


def func_call_checking(ctx: e.TcCtx, fun_call: e.FunAppExpr) -> Tuple[e.ResultTp, e.FunAppExpr]:
    # for breaking circular dependency
    from . import typechecking as tc

    match fun_call:
        case e.FunAppExpr(fun=fname, args=args, overloading_index=idx):
            assert idx is None, ("Overloading should be empty "
                                 "before type checking")
            fun_tp = ctx.schema.fun_defs[fname].tp
            assert len(args) == len(fun_tp.args_mod), "argument count mismatch"
            [res_tps, args_cks] = zip(*[tc.synthesize_type(ctx, v) for v in args])
            [tps, arg_cards] = zip(*res_tps)


            # take the product of argument cardinalities
            arg_card_product = reduce(
                operator.mul,
                (tops.match_param_modifier(param_mod, arg_card)
                    for param_mod, arg_card
                    in zip(fun_tp.args_mod, arg_cards, strict=True)))

            if idx is not None:
                args_ret_type = fun_tp.args_ret_types[idx]
                result_tp = check_args_ret_type_match(ctx, tps, args_ret_type)
                if result_tp is None:
                    raise ValueError("Overloading for function incorrectly calculated", fun_call)
            else:
                ok_candidates : List[Tuple[int, e.Tp]] = []
                for i, args_ret_type in enumerate(fun_tp.args_ret_types):
                    result_tp = check_args_ret_type_match(ctx, tps, args_ret_type)
                    if result_tp is not None:
                        ok_candidates.append((i, result_tp))
                if len(ok_candidates) == 0:
                    raise ValueError("No overloading matches", fun_call)
                elif len(ok_candidates) == 1:
                    idx, result_tp = ok_candidates[0]
                else:
                    raise ValueError("Ambiguous overloading", fun_call)
            result_card = (arg_card_product
                            * fun_tp.args_ret_types[idx].ret_tp.mode)
            result_expr = e.FunAppExpr(fun=fname, args=args_cks, overloading_index=idx)
            return (e.ResultTp(result_tp, result_card), result_expr)
        case _:
            raise ValueError("impossible", fun_call)
  # special processing of cardinality inference for certain functions
    # match fname:
    #     case "??":
    #         assert len(arg_cards) == 2
    #         result_card = e.CMMode(
    #             e.min_cardinal(arg_cards[0].lower,
    #                             arg_cards[1].lower),
    #             e.max_cardinal(arg_cards[0].upper,
    #                             arg_cards[1].upper),
    #             # e.max_cardinal(arg_cards[0].multiplicity,
    #             #                arg_cards[1].multiplicity)
    #         )
    #     case _:
            
    
    # result_tp = args_ret_type.ret_tp.tp
    #         result_expr = e.FunAppExpr(fun=fname, args=arg_cks,
    #                                     overloading_index=idx)
    #         return result_tp, result_card, result_expr

