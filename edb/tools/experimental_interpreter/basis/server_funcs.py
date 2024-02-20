
from typing import *

from ..data import data_ops as e
from .errors import FunCallErr
from .std_funcs import *
from .builtin_bin_ops import *
from .reserved_ops import *
from ..data.casts import type_cast


all_server_std_funcs: Dict[str, Callable[[Sequence[Sequence[e.Val]]], Sequence[e.Val]]] = {
    "+": add_impl,
    "-": subtract_impl,
    "*": multiply_impl,
    "%": mod_impl,
    "//": floor_divide_impl,
    "=": eq_impl,
    "!=": not_eq_impl,
    "?=": opt_eq_impl,
    "?!=": opt_not_eq_impl,
    ">": gt_impl,
    "^": pow_impl,
    "<=": less_than_or_equal_to_impl,
    "++": concatenate_impl,
    "??": coalescing_impl,
    "IN": in_impl,
    "EXISTS": exists_impl,
    "OR": or_impl,
    "AND": and_impl,
    "NOT": not_impl,
    "LIKE": like_impl,
    "NOT LIKE": not_like_impl,
    "ILIKE": ilike_impl,
    "NOT ILIKE": not_ilike_impl,
    "DISTINCT": distinct_impl,
    "EXCEPT": except_impl,
    "INTERSECT": intersect_impl,
    "<": less_than_impl,
    "all": std_all_impl,
    "any": std_any_impl,
    "array_agg": std_array_agg_impl,
    "array_unpack": std_array_unpack_impl,
    "count": std_count_impl,
    "enumerate": std_enumerate_impl,
    "len": std_len_impl,
    "sum": std_sum_impl,
    "assert_exists": std_assert_exists,
    "assert_single": std_assert_single,
    "assert_distinct": std_assert_distinct,
    "str_split": str_split_impl,
    "str_upper": str_upper_impl,
    "str_lower": str_lower_impl,
    "to_json": to_json_impl,
    "datetime_current": std_datetime_current,
    "random": random_impl,
    e.IndirectionIndexOp: indirection_index_impl,
    e.IndirectionSliceStartStopOp: indirection_slice_start_stop_impl,
    e.IndirectionSliceStartOp: indirection_slice_start_impl,
    e.IndirectionSliceStopOp: indirection_slice_stop_impl,
}


def get_default_func_impl_for_function(
        name: e.QualifiedName) -> Callable[[Sequence[Sequence[e.Val]]], Sequence[e.Val]]:
    if len(name.names) == 2 and name.names[0] == "std":
        if name.names[1] in all_server_std_funcs:
            return all_server_std_funcs[name.names[1]]
        else:
            def default_impl(arg: Sequence[Sequence[e.Val]]) -> Sequence[e.Val]:
                raise ValueError("Not implemented: ", name)
            return default_impl
    else:
        raise ValueError("Cannot get a default implementaiton for a non-std function", name)
    
    
def get_default_func_impl_for_cast(
        from_tp: e.Tp, to_tp: e.Tp) -> Callable[[e.Val], e.Val]:
    def default_impl(arg: e.Val) -> e.Val:
        return type_cast(to_tp, arg)
        # raise ValueError("Not implemented: cast ", from_tp, to_tp)
    return default_impl
    
    