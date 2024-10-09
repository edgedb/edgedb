from __future__ import annotations
from typing import Dict, Callable, Sequence

from ..data import data_ops as e
from . import std_funcs as stdf
from . import builtin_bin_ops as bbo
from .reserved_ops import (
    indirection_index_impl,
    indirection_slice_start_stop_impl,
    indirection_slice_start_impl,
    indirection_slice_stop_impl,
)
from ..data.casts import type_cast


all_server_std_funcs: Dict[
    str, Callable[[Sequence[Sequence[e.Val]]], Sequence[e.Val]]
] = {
    "+": bbo.add_impl,
    "-": bbo.subtract_impl,
    "*": bbo.multiply_impl,
    "%": bbo.mod_impl,
    "//": bbo.floor_divide_impl,
    "=": bbo.eq_impl,
    "!=": bbo.not_eq_impl,
    "?=": bbo.opt_eq_impl,
    "?!=": bbo.opt_not_eq_impl,
    ">": bbo.gt_impl,
    "^": bbo.pow_impl,
    "<=": bbo.less_than_or_equal_to_impl,
    "++": bbo.concatenate_impl,
    "??": bbo.coalescing_impl,
    "IN": bbo.in_impl,
    "EXISTS": bbo.exists_impl,
    "OR": bbo.or_impl,
    "AND": bbo.and_impl,
    "NOT": bbo.not_impl,
    "LIKE": bbo.like_impl,
    "NOT LIKE": bbo.not_like_impl,
    "ILIKE": bbo.ilike_impl,
    "NOT ILIKE": bbo.not_ilike_impl,
    "DISTINCT": bbo.distinct_impl,
    "EXCEPT": bbo.except_impl,
    "INTERSECT": bbo.intersect_impl,
    "<": bbo.less_than_impl,
    "all": stdf.std_all_impl,
    "any": stdf.std_any_impl,
    "array_agg": stdf.std_array_agg_impl,
    "array_unpack": stdf.std_array_unpack_impl,
    "count": stdf.std_count_impl,
    "enumerate": stdf.std_enumerate_impl,
    "len": stdf.std_len_impl,
    "sum": stdf.std_sum_impl,
    "assert_exists": stdf.std_assert_exists,
    "assert_single": stdf.std_assert_single,
    "assert_distinct": stdf.std_assert_distinct,
    "str_split": stdf.str_split_impl,
    "str_upper": stdf.str_upper_impl,
    "str_lower": stdf.str_lower_impl,
    "to_json": stdf.to_json_impl,
    "datetime_current": stdf.std_datetime_current,
    "contains": stdf.std_contains_impl,
    "random": stdf.random_impl,
    "re_test": stdf.std_re_test_impl,
    e.IndirectionIndexOp: indirection_index_impl,
    e.IndirectionSliceStartStopOp: indirection_slice_start_stop_impl,
    e.IndirectionSliceStartOp: indirection_slice_start_impl,
    e.IndirectionSliceStopOp: indirection_slice_stop_impl,
}
all_server_cal_funcs: Dict[
    str, Callable[[Sequence[Sequence[e.Val]]], Sequence[e.Val]]
] = {
    "to_local_datetime": stdf.cal_to_local_datetime_impl,
}
all_server_math_funcs: Dict[
    str, Callable[[Sequence[Sequence[e.Val]]], Sequence[e.Val]]
] = {
    "mean": stdf.math_mean_impl,
}


def get_default_func_impl_for_function(
    name: e.QualifiedName,
) -> Callable[[Sequence[Sequence[e.Val]]], Sequence[e.Val]]:
    if len(name.names) == 2:

        def default_impl(arg: Sequence[Sequence[e.Val]]) -> Sequence[e.Val]:
            raise ValueError("Not implemented: ", name)

        match name.names[0]:
            case 'std':
                if name.names[1] in all_server_std_funcs:
                    return all_server_std_funcs[name.names[1]]
                else:
                    return default_impl
            case "std::cal":
                if name.names[1] in all_server_cal_funcs:
                    return all_server_cal_funcs[name.names[1]]
                else:
                    return default_impl
            case "std::math":
                if name.names[1] in all_server_math_funcs:
                    return all_server_math_funcs[name.names[1]]
                else:
                    return default_impl
            case _:
                raise ValueError(
                    "Cannot get a default implementaiton"
                    " for a non-std function",
                    name,
                )
    else:
        raise ValueError(
            "Cannot get a default implementaiton for a non-std function", name
        )


def get_default_func_impl_for_cast(
    from_tp: e.Tp, to_tp: e.Tp
) -> Callable[[e.Val], e.Val]:
    def default_impl(arg: e.Val) -> e.Val:
        return type_cast(to_tp, arg)

    return default_impl
