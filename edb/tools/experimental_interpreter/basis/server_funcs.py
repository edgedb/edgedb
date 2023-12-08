
from typing import *

from ..data import data_ops as e
from .errors import FunCallErr
from .std_funcs import *
from .builtin_bin_ops import *


all_server_std_funcs: Dict[str, Callable[[Sequence[Sequence[e.Val]]], Sequence[e.Val]]] = {
    "+": add_impl,
    "-": subtract_impl,
    "*": multiply_impl,
    "%": mod_impl,
    "=": eq_impl,
    "!=": not_eq_impl,
    "?=": opt_eq_impl,
    "?!=": opt_not_eq_impl,
    ">": gt_impl,
    "++": concatenate_impl,
    "??": coalescing_impl,
    "IN": in_impl,
    "EXISTS": exists_impl,
    "OR": or_impl,
    "all": std_all_impl,
    "any": std_any_impl,
    "array_agg": std_array_agg_impl,
    "array_unpack": std_array_unpack_impl,
    "count": std_count_impl,
    "enumerate": std_enumerate_impl,
    "len": std_len_impl,
    "sum": std_sum_impl,
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
    
    