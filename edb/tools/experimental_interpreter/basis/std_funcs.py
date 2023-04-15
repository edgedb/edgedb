
from typing import *

from ..data import data_ops as e
from ..data.data_ops import (
    AnyTp, ArrTp, ArrVal, BoolTp, BoolVal, BuiltinFuncDef, CardAny,
    CardOne, FunArgRetType, FunType, IntTp, IntVal, Val,
    ParamSetOf, ParamSingleton, SomeTp, StrTp,
    StrVal, UnnamedTupleTp, Val, UnnamedTupleVal)
from .errors import FunCallErr


def val_is_true(v: Val) -> bool:
    match v:
        case BoolVal(val=True):
            return True
        case BoolVal(val=False):
            return False
        case _:
            raise ValueError("not a boolean")


std_all_tp = FunType(args_mod=[ParamSetOf()], args_ret_types=[
                     FunArgRetType(args_tp=[BoolTp()],
                                   ret_tp=e.ResultTp(BoolTp(), CardOne))])


def std_all_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l1]:
            return [BoolVal(all(val_is_true(v) for v in l1))]
    raise FunCallErr()


std_any_tp = FunType(args_mod=[ParamSetOf()], args_ret_types=[
                     FunArgRetType(args_tp=[BoolTp()],
                                   ret_tp=e.ResultTp(BoolTp(), CardOne))])


def std_any_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:

    match arg:
        case [l1]:
            return [BoolVal(any(val_is_true(v) for v in l1))]
    raise FunCallErr()


std_array_agg_tp = FunType(
    args_mod=[ParamSetOf()],
    args_ret_types=[
        FunArgRetType(
            args_tp=[SomeTp(0)],
            ret_tp=e.ResultTp(ArrTp(SomeTp(0)),
                              CardOne))])


def std_array_agg_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l1]:
            return [ArrVal(val=l1)]
    raise FunCallErr()


std_array_unpack_tp = FunType(
    args_mod=[ParamSingleton()],
    args_ret_types=[
        FunArgRetType(
            args_tp=[ArrTp(SomeTp(0))],
            ret_tp=e.ResultTp(SomeTp(0),
                              CardAny))])


def std_array_unpack_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[ArrVal(val=arr)]]:
            return arr
    raise FunCallErr()


std_count_tp = FunType(args_mod=[ParamSetOf()], args_ret_types=[
                       FunArgRetType(args_tp=[AnyTp()],
                                     ret_tp=e.ResultTp(IntTp(), CardOne))])


def std_count_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l1]:
            return [IntVal(val=len(l1))]
    raise FunCallErr()


std_enumerate_tp = FunType(
    args_mod=[ParamSetOf()],
    args_ret_types=[
        FunArgRetType(
            args_tp=[SomeTp(0)],
            ret_tp=e.ResultTp(UnnamedTupleTp(
                val=[IntTp(),
                     SomeTp(0)]),
                    CardAny))])


def std_enumerate_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l1]:
            return [UnnamedTupleVal(val=[IntVal(i), v])
                    for (i, v) in enumerate(l1)]
    raise FunCallErr()


std_len_tp = FunType(args_mod=[ParamSingleton()],
                     args_ret_types=[
    FunArgRetType(args_tp=[StrTp()], ret_tp=e.ResultTp(IntTp(), CardOne)),
    FunArgRetType(args_tp=[ArrTp(AnyTp())], ret_tp=e.ResultTp(IntTp(), CardOne)),
])


def std_len_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[StrVal(s)]]:
            return [IntVal(len(s))]
        case [[ArrVal(arr)]]:
            return [IntVal(len(arr))]
    raise FunCallErr()


std_sum_tp = FunType(args_mod=[ParamSetOf()],
                     args_ret_types=[
    FunArgRetType(args_tp=[IntTp()], ret_tp=e.ResultTp(IntTp(), CardOne)),
])


def std_sum_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l]:
            if all(isinstance(elem, e.IntTp) for elem in l):
                return [IntVal(sum(elem.val
                                   for elem in l
                                   if isinstance(elem, e.IntVal)))]
            else:
                raise ValueError("not implemented: std::sum")
    raise FunCallErr()


all_std_funcs: Dict[str, BuiltinFuncDef] = {
    "std::all": BuiltinFuncDef(tp=std_all_tp, impl=std_all_impl),
    "std::any": BuiltinFuncDef(tp=std_any_tp, impl=std_any_impl),
    "std::array_agg": BuiltinFuncDef(tp=std_array_agg_tp,
                                     impl=std_array_agg_impl),
    "std::array_unpack": BuiltinFuncDef(tp=std_array_unpack_tp,
                                        impl=std_array_unpack_impl),
    "std::count": BuiltinFuncDef(tp=std_count_tp, impl=std_count_impl),
    "std::enumerate": BuiltinFuncDef(tp=std_enumerate_tp,
                                     impl=std_enumerate_impl),
    "std::len": BuiltinFuncDef(tp=std_len_tp, impl=std_len_impl),
    "std::sum": BuiltinFuncDef(tp=std_sum_tp, impl=std_sum_impl),
}
