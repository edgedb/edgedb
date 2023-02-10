from typing import *
from .data_ops import *

class FunCallErr(Exception):
    pass


add_tp_int_int = FunType([IntTp(), IntTp()], 
                    [ParamSingleton(), ParamSingleton()], 
                    (IntTp(), CardOne))
def add_impl_int_int (arg : List[MultiSetVal]) -> MultiSetVal:
    match arg:
        case [[IntVal(v1)], [IntVal(v2)]]:
            return [IntVal(v1 + v2)]
    raise FunCallErr()

eq_tp_str_str = FunType([StrTp(), StrTp()], 
                    [ParamSingleton(), ParamSingleton()], 
                    (BoolTp(), CardOne))
def eq_impl_str_str (arg : List[MultiSetVal]) -> MultiSetVal:
    match arg:
        case [[StrVal(s1)], [IntVal(s2)]]:
            return [BoolVal(s1 == s2)]
    raise FunCallErr()



all_builtin_funcs : Dict[str, List[BuiltinFuncDef]] = {
        "+" : 
            [BuiltinFuncDef (tp =add_tp_int_int,impl=add_impl_int_int)],

        "=" : 
            [BuiltinFuncDef (tp =eq_tp_str_str,impl=eq_impl_str_str)]
    }


