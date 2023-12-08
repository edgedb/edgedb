from typing import *
# from .std_funcs import all_std_funcs
# from .builtin_bin_ops import all_builtin_ops
# from .reserved_ops import all_reserved_ops
from ..data import data_ops as e


# all_builtin_funcs = {**all_builtin_ops, **all_reserved_ops, **all_std_funcs}

def lift_binary_scalar_op(f: Callable[[Any, Any], Any]) -> Callable[[Sequence[Sequence[e.Val]]],  Sequence[e.Val]]:
    
    def multiply_impl(arg: Sequence[Sequence[e.Val]]) -> Sequence[e.Val]:
            match arg:
                case [[e.ScalarVal(t1, v1)], [e.ScalarVal(t2, v2)]]:
                    assert t1 == t2
                    return [e.ScalarVal(t1, f(v1, v2))]
            raise ValueError("Expecing two scalar values")
    return multiply_impl



