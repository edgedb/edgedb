
from typing import *
from ..data.data_ops import *
from .errors import FunCallErr

std_enumerate_tp = FunType(args_mod=[ParamSetOf()], 
                args_ret_types=[FunArgRetType(args_tp=[SomeTp(0)], ret_tp=(UnnamedTupleTp(val=[IntTp(), SomeTp(0)]), CardAny))])

def std_enumerate_impl(arg : List[MultiSetVal]) -> MultiSetVal:
    match arg:
        case [l1]:
            return [UnnamedTupleVal(val=[IntVal(i), v]) for (i,v) in enumerate(l1)]
    raise FunCallErr()


all_std_funcs : Dict[str, BuiltinFuncDef] = {
        "std::enumerate" : BuiltinFuncDef(tp =std_enumerate_tp, impl=std_enumerate_impl),
    }
