
from ..data import data_ops as e
from ..data import expr_ops as eops
from ..data import type_ops as tops
from ..data import path_factor as pops
from ..data import expr_to_str as pp
from ..data import module_ops as mops
from typing import List, Tuple, Dict, Optional
# from itertools import *
from functools import reduce
import operator
from edb.common import debug
from ..data import expr_to_str as pp


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
        if not tops.check_is_subtype_with_instantiation(ctx, syn_tp, ck_tp, some_tp_mapping):
            return None

    final_ret_tp = tops.recursive_instantiate_tp(ret_tp, some_tp_mapping)
    return final_ret_tp
    # if isinstance(ret_tp, e.SomeTp):
    #     if ret_tp.index in some_tp_mapping:
    #         return some_tp_mapping[ret_tp.index]
    #     else:
    #         raise ValueError("Function's return type has to be grounded by input types.")
    # else:
    #     return ret_tp


def func_call_checking(ctx: e.TcCtx, fun_call: e.FunAppExpr) -> Tuple[e.ResultTp, e.FunAppExpr]:
    # for breaking circular dependency
    from . import typechecking as tc

    match fun_call:
        case e.FunAppExpr(fun=fname, args=args, overloading_index=idx):
            qualified_fname, fun_def = mops.resolve_raw_name_and_func_def(ctx, fname)
            fun_tp = fun_def.tp
            # assert len(args) == len(fun_tp.args_mod), "argument count mismatch"
            [res_tps, args_cks] = zip(*[tc.synthesize_type(ctx, v) for v in args])
            [tps, arg_cards] = zip(*res_tps)



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
                
                if len(ok_candidates) > 1 :
                    print("WARNING: Ambiguous overloading", pp.show_qname(qualified_fname),
                          "picking the first one")
                    ok_candidates = ok_candidates[0:1]

                if len(ok_candidates) == 0:
                    raise ValueError("No overloading matches", pp.show_qname(qualified_fname), 
                                     "args type", [pp.show_tp(tp) for tp in tps],
                                     "candidates", [pp.show_func_tps(args_ret_type) for args_ret_type in fun_tp.args_ret_types],
                                     pp.show_expr(fun_call))
                elif len(ok_candidates) == 1:
                    idx, result_tp = ok_candidates[0]
                else:
                    raise ValueError("Ambiguous overloading", pp.show_qname(qualified_fname), 
                                     "args type", [pp.show_tp(tp) for tp in tps],
                                     "candidates", [pp.show_func_tps(fun_tp.args_ret_types[i]) for i,args_ret_type in ok_candidates],
                                     pp.show_expr(fun_call))

            # take the product of argument cardinalities
            arg_card_product = reduce(
                operator.mul,
                (tops.match_param_modifier(param_mod, arg_card)
                    for param_mod, arg_card
                    in zip(fun_tp.args_ret_types[idx].args_mod, arg_cards, strict=True)))
            # special processing of cardinality inference for certain functions
            match qualified_fname:
                case e.QualifiedName(["std", "??"]):
                    assert len(arg_cards) == 2
                    result_card = e.CMMode(
                        e.min_cardinal(arg_cards[0].lower,
                                        arg_cards[1].lower),
                        e.max_cardinal(arg_cards[0].upper,
                                        arg_cards[1].upper),
                    )
                case _:
                    result_card = (arg_card_product
                                    * fun_tp.args_ret_types[idx].ret_tp.mode)
            result_expr = e.FunAppExpr(fun=qualified_fname, args=args_cks, overloading_index=idx)
            return (e.ResultTp(result_tp, result_card), result_expr)
        case _:
            raise ValueError("impossible", fun_call)
            
    
    # result_tp = args_ret_type.ret_tp.tp
    #         result_expr = e.FunAppExpr(fun=fname, args=arg_cks,
    #                                     overloading_index=idx)
    #         return result_tp, result_card, result_expr

