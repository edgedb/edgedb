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

eq_tp = FunType(
                    args_mod=[ParamSingleton(), ParamSingleton()], 
                    args_ret_types=[FunArgRetType(args_tp=[SomeTp(0), SomeTp(0)], ret_tp=(BoolTp(), CardOne))]
                    )

def eq_impl(arg : List[MultiSetVal]) -> MultiSetVal:
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

opt_eq_tp = FunType(args_mod=[ParamOptional(), ParamOptional()], 
                args_ret_types=[FunArgRetType(args_tp=[SomeTp(0), SomeTp(0)], ret_tp=(BoolTp(), CardOne))]
                )

def opt_eq_impl (arg : List[MultiSetVal]) -> MultiSetVal:
    match arg:
        case [[], []]:
            return [BoolVal(True)]
        case [l1, l2]:
            if len(l1) == 0 or len(l2) == 0:
                return [BoolVal(False)]
            else:
                return eq_impl(arg)
    raise FunCallErr()

all_builtin_ops : Dict[str, BuiltinFuncDef] = {
        "+" : BuiltinFuncDef(tp =add_tp_int_int,impl=add_impl_int_int),
        "=" : BuiltinFuncDef(tp =eq_tp,impl=eq_impl), 
        "?=" : BuiltinFuncDef(tp =opt_eq_tp,impl=opt_eq_impl), 
        "IN": BuiltinFuncDef(tp=in_tp, impl=in_impl)
    }

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

all_builtin_funcs = {**all_builtin_ops, **all_std_funcs}


