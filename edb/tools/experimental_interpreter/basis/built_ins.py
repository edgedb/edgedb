from typing import *
# from .std_funcs import all_std_funcs
# from .builtin_bin_ops import all_builtin_ops
# from .reserved_ops import all_reserved_ops
from ..data import data_ops as e


# all_builtin_funcs = {**all_builtin_ops, **all_reserved_ops, **all_std_funcs}

def lift_binary_scalar_op(f: Callable[[Any, Any], Any],
                          override_ret_tp: Optional[e.ScalarTp] = None
                          ) -> Callable[[Sequence[Sequence[e.Val]]],  Sequence[e.Val]]:
    
    def op_impl(arg: Sequence[Sequence[e.Val]]) -> Sequence[e.Val]:
            match arg:
                case [[e.ScalarVal(t1, v1)], [e.ScalarVal(t2, v2)]]:
                    if  t1 != t2 and override_ret_tp is None:
                         raise ValueError("Scalar types do not match", t1, t2)
                    ret_tp = t1
                    if override_ret_tp is not None:
                        ret_tp = override_ret_tp
                    return [e.ScalarVal(ret_tp, f(v1, v2))]
            raise ValueError("Expecing two scalar values")
    return op_impl




def lift_unary_scalar_op(f: Callable[[Any], Any]) -> Callable[[Sequence[Sequence[e.Val]]],  Sequence[e.Val]]:
    
    def impl(arg: Sequence[Sequence[e.Val]]) -> Sequence[e.Val]:
            match arg:
                case [[e.ScalarVal(t1, v1)]]:
                    return [e.ScalarVal(t1, f(v1))]
            raise ValueError("Expecing two scalar values")
    return impl

