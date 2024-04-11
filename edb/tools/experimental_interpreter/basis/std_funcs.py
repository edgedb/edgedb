
from typing import *
import json

from ..data import data_ops as e
from ..data.data_ops import (
    AnyTp, ArrTp, ArrVal, BoolVal, BuiltinFuncDef, CardAny,
    CardOne, FunArgRetType,  IntVal, Val,
    ParamSetOf, ParamSingleton, SomeTp, 
    StrVal, UnnamedTupleTp, Val, UnnamedTupleVal)
from .errors import FunCallErr
import random
from .. import interpreter_logging

from datetime import datetime, timezone

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
        case [l, []]:
            if len(l) > 1:
                raise ValueError("Expected a single value, got more than one.")
            else:
                return l
    raise FunCallErr()

def std_assert_distinct(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [vset, pmsg]:
            msg = None

            match pmsg:
                case [msg]:
                    msg = msg
                case []:
                    msg = "Expected distinct values, got duplicates."
                
            if all(isinstance(v, e.RefVal) for v in vset):
                ids = {v.refid : v for v in vset}.values() # type: ignore
                if len(ids) != len(vset):
                    raise ValueError(msg)
                else:
                    return vset
            elif all(isinstance(v, e.ArrVal | e.UnnamedTupleVal | e.NamedTupleVal) for v in vset):
                if len(set(vset)) != len(vset):
                    raise ValueError(msg)
                else:
                    return vset
            else:
                raise ValueError("Not implemented: assert_distinct")
    raise FunCallErr()
def std_assert_exists(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l, [msg]]:
            if len(l) == 0:
                raise ValueError(msg)
            else:
                return l
        case [l, []]:
            if len(l) == 0:
                raise ValueError("Expected a value, got none.")
            else:
                return l
    raise FunCallErr()


def std_datetime_current(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case []:
            current_datetime = datetime.now()
            val = current_datetime.strftime("%Y-%m-%dT%H:%M:%S%z")
            return [e.ScalarVal(e.ScalarTp(e.QualifiedName(["std", "datetime"])), 
                                val)]
    raise FunCallErr()


def str_split(s, delimiter):
    return [part for part in s.split(delimiter)]

def str_split_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(_, s)], [e.ScalarVal(_, delimiter)]]:
            return [ArrVal(val=[e.ScalarVal(e.ScalarTp(e.QualifiedName(["std", "str"])), part) for part in str_split(s, delimiter)])]
    raise FunCallErr()

def str_upper_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(_, s)]]:
            return [e.ScalarVal(e.ScalarTp(e.QualifiedName(["std", "str"])), s.upper())]
    raise FunCallErr()

def str_lower_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(_, s)]]:
            return [e.ScalarVal(e.ScalarTp(e.QualifiedName(["std", "str"])), s.lower())]
    raise FunCallErr()

def to_json_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(_, s)]]:
            return [e.ScalarVal(e.ScalarTp(e.QualifiedName(["std", "json"])), json.loads(s))]
    raise FunCallErr()

def random_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case []:
            return [e.ScalarVal(e.ScalarTp(e.QualifiedName(["std", "float64"])), random.random())]
    raise FunCallErr()

def cal_to_local_datetime_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(_, s)], [e.ScalarVal(_, tz)]]:
            interpreter_logging.print_warning("Warning: cal::to_local_datetime is implemented properly. It is a no-op.")
            return [e.ScalarVal(e.ScalarTp(e.QualifiedName(["cal", "local_datetime"])), s)]
    raise FunCallErr()

def math_mean_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [l]:
            return [e.ScalarVal(e.ScalarTp(e.QualifiedName(["std", "float64"])), sum(elem.val for elem in l) / len(l))]
    raise FunCallErr()