from typing import *
from .data_ops import *

class FunCallErr(Exception):
    pass


add_tp_int_int = FunType(args_mod=[ParamSingleton(), ParamSingleton()], 
                        args_ret_types=[FunArgRetType(args_tp=[IntTp(), IntTp()], 
                                                        ret_tp=(IntTp(), CardOne))]
                    )
def add_impl_int_int (arg : List[MultiSetVal]) -> MultiSetVal:
    match arg:
        case [[IntVal(v1)], [IntVal(v2)]]:
            return [IntVal(v1 + v2)]
    raise FunCallErr()

eq_tp_str_str = FunType(
                    args_mod=[ParamSingleton(), ParamSingleton()], 
                    args_ret_types=[FunArgRetType(args_tp=[StrTp(), StrTp()], ret_tp=(BoolTp(), CardOne))]
                    )

def eq_impl_str_str (arg : List[MultiSetVal]) -> MultiSetVal:
    match arg:
        case [[StrVal(s1)], [StrVal(s2)]]:
            return [BoolVal(s1 == s2)]
    raise FunCallErr(arg)


in_tp = FunType(args_mod=[ParamSingleton(), ParamSetOf()], 
                args_ret_types=[FunArgRetType(args_tp=[StrTp(), StrTp()], ret_tp=(BoolTp(), CardOne))])

def in_impl (arg : List[MultiSetVal]) -> MultiSetVal:
    match arg:
        case [[singleton], l]:
            return [BoolVal(singleton in l)]
    raise FunCallErr()


all_builtin_funcs : Dict[str, BuiltinFuncDef] = {
        "+" : BuiltinFuncDef (tp =add_tp_int_int,impl=add_impl_int_int),
        "=" : BuiltinFuncDef (tp =eq_tp_str_str,impl=eq_impl_str_str), 
        "IN": BuiltinFuncDef(tp=in_tp, impl=in_impl)
    }


