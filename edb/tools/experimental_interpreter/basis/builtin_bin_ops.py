
from typing import *

from ..data import data_ops as e

from ..data.data_ops import (
    AnyTp, BoolTp, BoolVal, BuiltinFuncDef, CardOne, FunArgRetType,
    IntTp, IntVal, Val, ParamOptional, ParamSetOf,
    ParamSingleton, RefVal, SomeTp, StrVal, CardAny, ArrVal, StrTp, ArrTp)
from .errors import FunCallErr
from .built_ins import *
import fnmatch
import operator


def add_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(t1, v1)], [e.ScalarVal(t2, v2)]]:
            assert t1 == t2
            return [e.ScalarVal(t1, v1 + v2)]
    raise FunCallErr()

def subtract_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(t1, v1)], [e.ScalarVal(t2, v2)]]:
            assert t1 == t2
            return [e.ScalarVal(t1, v1 - v2)]
    raise FunCallErr()


multiply_impl = lift_binary_scalar_op(operator.mul)



mod_impl = lift_binary_scalar_op(operator.mod)


def eq_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[
            e.ScalarVal(t1, v1)], [
            e.ScalarVal(t2, v2)]]:
            assert t1 == t2
            return [BoolVal(v1 == v2)]
        case [[e.RefVal(id1, v1)], [e.RefVal(id2, v2)]]:    
            return [BoolVal(id1 == id2)]
    raise FunCallErr(arg)


def not_eq_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(t1, v1)], [e.ScalarVal(t2, v2)]]:
            assert t1 == t2
            return [BoolVal(v1 != v2)]
        case [[RefVal(_) as r1], [RefVal(_) as r2]]:
            return [BoolVal(r1 != r2)]
    raise FunCallErr(arg)


def opt_eq_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[], []]:
            return [BoolVal(True)]
        case [l1, l2]:
            if len(l1) == 0 or len(l2) == 0:
                return [BoolVal(False)]
            else:
                return eq_impl(arg)
    raise FunCallErr()



def opt_not_eq_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[], []]:
            return [BoolVal(False)]
        case [l1, l2]:
            if len(l1) == 0 or len(l2) == 0:
                return [BoolVal(True)]
            else:
                return not_eq_impl(arg)
    raise FunCallErr()





gt_impl = lift_binary_scalar_op(operator.gt, override_ret_tp=e.BoolTp())




def concatenate_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(_, s1)], [e.ScalarVal(_, s2)]]:
            return [StrVal(s1 + s2)]
        case [[ArrVal(arr1)], [ArrVal(arr2)]]:
            return [ArrVal([*arr1, *arr2])]
    raise FunCallErr()




def coalescing_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[], default]:
            return default
        case [[non_empty], _]:
            return [non_empty]
    raise FunCallErr()




def in_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[singleton], l]:
            if isinstance(singleton, RefVal):
                assert all(isinstance(v, RefVal) for v in l)
                return [BoolVal(singleton.refid in [v.refid for v in l])]
            else:
                return [BoolVal(singleton in l)]
    raise FunCallErr()



def exists_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[]]:
            return [BoolVal(False)]
        case [_]:
            return [BoolVal(True)]
    raise FunCallErr()



def or_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(_, b1)], [e.ScalarVal(_, b2)]]:
            return [e.BoolVal(b1 or b2)]
        case [_]:
            return [BoolVal(True)]
    raise FunCallErr()

and_impl = lift_binary_scalar_op(operator.and_)

less_than_impl = lift_binary_scalar_op(operator.lt, override_ret_tp=e.BoolTp())

def like(value, pattern) -> bool:
    fnmatch_pattern = pattern.replace('%', '*').replace('_', '?').replace(r'\%', '%').replace(r'\_', '_')
    return fnmatch.fnmatch(value, fnmatch_pattern)

def ilike(value, pattern) -> bool:
    fnmatch_pattern = pattern.replace('%', '*').replace('_', '?').replace(r'\%', '%').replace(r'\_', '_')
    value = value.lower()
    pattern = pattern.lower()
    return fnmatch.fnmatch(value, fnmatch_pattern)

def like_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(_, value)], [e.ScalarVal(_, pattern)]]:
            return [e.BoolVal(like(value, pattern))]
    raise FunCallErr()


def not_like_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(_, value)], [e.ScalarVal(_, pattern)]]:
            return [e.BoolVal(not like(value, pattern))]
    raise FunCallErr()

def ilike_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(_, value)], [e.ScalarVal(_, pattern)]]:
            return [e.BoolVal(ilike(value, pattern))]
    raise FunCallErr()

def not_ilike_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [[e.ScalarVal(_, value)], [e.ScalarVal(_, pattern)]]:
            return [e.BoolVal(not ilike(value, pattern))]
    raise FunCallErr()


def distinct_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [vset]:
            if all(isinstance(v, e.RefVal) for v in vset):
                return {v.refid : v for v in vset}.values() # type: ignore
            else:
                return list(set(vset))
    raise FunCallErr()


not_impl = lift_unary_scalar_op(operator.not_)


def intersect_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [arg1, arg2]:
            if all(isinstance(v, e.RefVal) for v in arg1) and all(isinstance(v, e.RefVal) for v in arg2):
                id1 = {v.refid : v for v in arg1} # type: ignore
                return {v.refid : v for v in arg2 if v.refid in id1}.values() # type: ignore
            else:
                return [v for v in arg1 if v in arg2]
    raise FunCallErr()

def except_impl(arg: Sequence[Sequence[Val]]) -> Sequence[Val]:
    match arg:
        case [arg1, arg2]:
            if all(isinstance(v, e.RefVal) for v in arg1) and all(isinstance(v, e.RefVal) for v in arg2):
                id2 = {v.refid : v for v in arg2} # type: ignore
                return [v for v in arg1 if v.refid not in id2] # type: ignore
            else:
                return [v for v in arg1 if v not in arg2]
    raise FunCallErr()

