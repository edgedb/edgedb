from typing import *
from ..data.data_ops import *
from .errors import FunCallErr
from .std_funcs import all_std_funcs

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

indirection_index_tp = FunType(args_mod=[ParamSingleton(), ParamSingleton()], 
                args_ret_types=[FunArgRetType(args_tp=[StrTp(), IntTp()], ret_tp=(StrTp(), CardOne)),
                                FunArgRetType(args_tp=[ArrayTp(SomeTp(0)), IntTp()], ret_tp=(ArrayTp(SomeTp(0)), CardOne))
                            ]) 

def indirection_index_impl (arg : List[MultiSetVal]) -> MultiSetVal:
    match arg:
        case [[StrVal(val=s)], [IntVal(val=i)]]:
            return [StrVal(val=s[i])]
        case [[ArrayVal(val=arr)], [IntVal(val=i)]]:
            return [arr[i]]
    raise FunCallErr()

indirection_slice_tp = FunType(args_mod=[ParamSingleton(), ParamSingleton(), ParamSingleton()], 
                args_ret_types=[FunArgRetType(args_tp=[StrTp(), IntTp(), IntInfTp()], ret_tp=(StrTp(), CardOne)),
                                FunArgRetType(args_tp=[ArrayTp(SomeTp(0)), IntTp(), IntInfTp()], ret_tp=(ArrayTp(SomeTp(0)), CardOne))
                            ]) 

def indirection_slice_impl (arg : List[MultiSetVal]) -> MultiSetVal:
    match arg:
        case [[StrVal(val=s)], [IntVal(val=start)], [IntVal(val=end)]]:
            return [StrVal(val=s[start:end])]
        case [[StrVal(val=s)], [IntVal(val=start)], [IntInfVal()]]:
            return [StrVal(val=s[start:])]
        case [[ArrayVal(val=arr)], [IntVal(val=start)], [IntVal(val=end)]]:
            return [ArrayVal(val=arr[start:end])]
        case [[ArrayVal(val=arr)], [IntVal(val=start)], [IntInfVal()]]:
            return [ArrayVal(val=arr[start:])]
    raise FunCallErr()

all_builtin_ops : Dict[str, BuiltinFuncDef] = {
        "+" : BuiltinFuncDef(tp =add_tp_int_int,impl=add_impl_int_int),
        "=" : BuiltinFuncDef(tp =eq_tp,impl=eq_impl), 
        "?=" : BuiltinFuncDef(tp =opt_eq_tp,impl=opt_eq_impl), 
        "IN": BuiltinFuncDef(tp=in_tp, impl=in_impl),
    }

all_reserved_ops : Dict[str, BuiltinFuncDef] = {
        IndirectionIndexOp : BuiltinFuncDef(tp = indirection_index_tp, impl=indirection_index_impl),
        IndirectionSliceOp : BuiltinFuncDef(tp=indirection_slice_tp, impl=indirection_slice_impl),
    }


all_builtin_funcs = {**all_builtin_ops, **all_reserved_ops, **all_std_funcs}


