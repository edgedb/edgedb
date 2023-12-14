
from typing import *

from ..data import data_ops as e
from ..data.data_ops import (
    AnyTp, ArrTp, ArrVal, BoolVal, BuiltinFuncDef, CardAny,
    CardOne, FunArgRetType,  IntVal, Val,
    ParamSetOf, ParamSingleton, SomeTp, 
    StrVal, UnnamedTupleTp, Val, UnnamedTupleVal)
from .errors import FunCallErr


def val_is_true(v: Val) -> bool:
    match v:
        case e.ScalarVal(_, v):
            assert isinstance(v, bool)
            return v
        case _:
            raise ValueError("not a boolean")


# std_all_tp = FunType(
#     args_ret_types=[FunArgRetType(
#         args_mod=[ParamSetOf()], 
#         args_tp=[BoolTp()],
#         ret_tp=e.ResultTp(BoolTp(), CardOne))])


def std_all_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l1]:
            return [BoolVal(all(val_is_true(v) for v in l1))]
    raise FunCallErr()


# std_any_tp = FunType(
#     args_ret_types=[FunArgRetType(
#         args_mod=[ParamSetOf()], 
#         args_tp=[BoolTp()],
#         ret_tp=e.ResultTp(BoolTp(), CardOne))])


def std_any_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:

    match arg:
        case [l1]:
            return [BoolVal(any(val_is_true(v) for v in l1))]
    raise FunCallErr()


# std_array_agg_tp = FunType(
#     args_ret_types=[FunArgRetType(
#         args_mod=[ParamSetOf()],
#         args_tp=[SomeTp(0)],
#         ret_tp=e.ResultTp(ArrTp(SomeTp(0)), CardOne))])


def std_array_agg_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l1]:
            return [ArrVal(val=l1)]
    raise FunCallErr()


# std_array_unpack_tp = FunType(
#     args_ret_types=[FunArgRetType(
#         args_mod=[ParamSingleton()],
#         args_tp=[ArrTp(SomeTp(0))],
#         ret_tp=e.ResultTp(SomeTp(0), CardAny))])


def std_array_unpack_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[ArrVal(val=arr)]]:
            return arr
    raise FunCallErr()


# std_count_tp = FunType(
#     args_ret_types=[FunArgRetType(
#         args_mod=[ParamSetOf()], 
#         args_tp=[AnyTp()],
#         ret_tp=e.ResultTp(IntTp(), CardOne))])


def std_count_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l1]:
            return [IntVal(val=len(l1))]
    raise FunCallErr()


# std_enumerate_tp = FunType(
#     args_ret_types=[FunArgRetType(
#         args_mod=[ParamSetOf()],
#         args_tp=[SomeTp(0)],
#         ret_tp=e.ResultTp(UnnamedTupleTp(
#                 val=[IntTp(),
#                      SomeTp(0)]),
#                     CardAny))])


def std_enumerate_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l1]:
            return [UnnamedTupleVal(val=[IntVal(i), v])
                    for (i, v) in enumerate(l1)]
    raise FunCallErr()


# std_len_tp = FunType(
#                      args_ret_types=[
#     FunArgRetType(
#         args_mod=[ParamSingleton()],
#         args_tp=[StrTp()], 
#         ret_tp=e.ResultTp(IntTp(), CardOne)),
#     FunArgRetType(
#         args_mod=[ParamSingleton()],
#         args_tp=[ArrTp(AnyTp())], 
#         ret_tp=e.ResultTp(IntTp(), CardOne)),
# ])


def std_len_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(t, v)]]:
            return [IntVal(len(v))]
        case [[ArrVal(arr)]]:
            return [IntVal(len(arr))]
    raise FunCallErr()


# std_sum_tp = FunType(args_ret_types=[
#     FunArgRetType(
#         args_mod=[ParamSetOf()],
#         args_tp=[IntTp()], 
#         ret_tp=e.ResultTp(IntTp(), CardOne)),
# ])


def std_sum_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l]:
            if all(isinstance(elem, e.ScalarVal) and isinstance(elem.val, int) for elem in l):
                return [IntVal(sum(elem.val
                                   for elem in l
                                   ))]
            else:
                raise ValueError("not implemented: std::sum")
    raise FunCallErr()



def std_assert_single(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l, [msg]]:
            if len(l) > 1:
                raise ValueError(msg)
            else:
                return l
    raise FunCallErr()

def std_assert_exists(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l, [msg]]:
            if len(l) == 0:
                raise ValueError(msg)
            else:
                return l
    raise FunCallErr()