
from typing import *

from ..data import data_ops as e
# from ..data.data_ops import (ArrTp, ArrVal, BoolTp, BoolVal, BuiltinFuncDef,
#                              CardAny, CardOne, FunArgRetType, FunType,
#                               IndirectionIndexOp, IndirectionSliceOp,
#                              IntInfTp, IntInfVal, IntTp, IntVal, Val,
#                              ParamSetOf, ParamSingleton, SomeTp, StrTp, StrVal)
from .errors import FunCallErr

# indirection_index_tp = FunType(
#     args_ret_types=[
#         FunArgRetType(
#             args_mod=[ParamSingleton(),
#                     ParamSingleton()],
#             args_tp=[StrTp(),
#                      IntTp()],
#             ret_tp=e.ResultTp(StrTp(), CardOne)),
#         FunArgRetType(
#             args_mod=[ParamSingleton(),
#                     ParamSingleton()],
#             args_tp=[
#                 ArrTp(
#                     SomeTp(0)),
#                 IntTp()],
#             ret_tp=e.ResultTp(
#                 ArrTp(
#                     SomeTp(0)),
#                 CardOne))])




# indirection_slice_tp = FunType(
#     args_ret_types=[
#         FunArgRetType(args_mod=[ParamSingleton(), ParamSingleton(), ParamSingleton()],
#                       args_tp=[StrTp(), IntTp(), IntInfTp()],
#                       ret_tp=e.ResultTp(StrTp(), CardOne)),
#         FunArgRetType(args_mod=[ParamSingleton(), ParamSingleton(), ParamSingleton()],
#                       args_tp=[ArrTp(SomeTp(0)), IntTp(), IntInfTp()],
#                       ret_tp=e.ResultTp(ArrTp(SomeTp(0)), CardOne))])


def indirection_slice_start_stop_impl(arg: Sequence[Sequence[e.Val]]) -> Sequence[e.Val]:
    match arg:
        case [[e.ScalarVal(t1,s)], [e.ScalarVal(_,start)], [e.ScalarVal(_,end)]]:
            return [e.ScalarVal(t1, s[start:end])]
        case [[e.ArrVal(val=arr)], [e.ScalarVal(_,start)], [e.ScalarVal(_,end)]]:
            return [e.ArrVal(val=arr[start:end])]
    raise FunCallErr()

def indirection_slice_start_impl(arg: Sequence[Sequence[e.Val]]) -> Sequence[e.Val]:
    match arg:
        case [[e.ScalarVal(t1,s)], [e.ScalarVal(_,start)]]:
            return [e.ScalarVal(t1, s[start:])]
        case [[e.ArrVal(val=arr)], [e.ScalarVal(_,start)]]:
            return [e.ArrVal(val=arr[start:])]
    raise FunCallErr()

def indirection_slice_stop_impl(arg: Sequence[Sequence[e.Val]]) -> Sequence[e.Val]:
    match arg:
        case [[e.ScalarVal(t1,s)], [e.ScalarVal(_,end)]]:
            return [e.ScalarVal(t1, s[:end])]
        case [[e.ArrVal(val=arr)], [e.ScalarVal(_,end)]]:
            return [e.ArrVal(val=arr[:end])]
    raise FunCallErr()

def indirection_index_impl(arg: Sequence[Sequence[e.Val]]) -> Sequence[e.Val]:
    match arg:
        case [[e.ScalarVal(t1,s)], [e.ScalarVal(_,idx)]]:
            return [e.ScalarVal(t1, s[idx])]
        case [[e.ArrVal(val=arr)], [e.ScalarVal(_,idx)]]:
            return [e.ArrVal(val=arr[idx])]
    raise FunCallErr()

# if_else_tp = FunType(
#     args_mod=[ParamSetOf(), ParamSingleton(), ParamSetOf()],
#     args_ret_types=[
#         FunArgRetType(
#             args_tp=[SomeTp(0), BoolTp(), SomeTp(0)],
#             ret_tp=e.ResultTp(SomeTp(0), CardAny))])


# def if_else_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
#     match arg:
#         case [l1, [BoolVal(val=True)], l2]:
#             return l1
#         case [l1, [BoolVal(val=False)], l2]:
#             return l2
#     raise FunCallErr()


# all_reserved_ops: Dict[str, BuiltinFuncDef] = {
#     IndirectionIndexOp: BuiltinFuncDef(tp=indirection_index_tp,
#                                        impl=indirection_index_impl),
#     IndirectionSliceOp: BuiltinFuncDef(tp=indirection_slice_tp,
#                                        impl=indirection_slice_impl),
#     # IfElseOp: BuiltinFuncDef(tp=if_else_tp, impl=if_else_impl)
# }
