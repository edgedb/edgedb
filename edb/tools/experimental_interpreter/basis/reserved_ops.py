
from typing import *
from ..data.data_ops import *
from .errors import FunCallErr

indirection_index_tp = FunType(args_mod=[ParamSingleton(), ParamSingleton()], 
                args_ret_types=[FunArgRetType(args_tp=[StrTp(), IntTp()], ret_tp=(StrTp(), CardOne)),
                                FunArgRetType(args_tp=[ArrTp(SomeTp(0)), IntTp()], ret_tp=(ArrTp(SomeTp(0)), CardOne))
                            ]) 

def indirection_index_impl (arg : List[MultiSetVal]) -> MultiSetVal:
    match arg:
        case [[StrVal(val=s)], [IntVal(val=i)]]:
            return [StrVal(val=s[i])]
        case [[ArrVal(val=arr)], [IntVal(val=i)]]:
            return [arr[i]]
    raise FunCallErr()

indirection_slice_tp = FunType(args_mod=[ParamSingleton(), ParamSingleton(), ParamSingleton()], 
                args_ret_types=[FunArgRetType(args_tp=[StrTp(), IntTp(), IntInfTp()], ret_tp=(StrTp(), CardOne)),
                                FunArgRetType(args_tp=[ArrTp(SomeTp(0)), IntTp(), IntInfTp()], ret_tp=(ArrTp(SomeTp(0)), CardOne))
                            ]) 

def indirection_slice_impl (arg : List[MultiSetVal]) -> MultiSetVal:
    match arg:
        case [[StrVal(val=s)], [IntVal(val=start)], [IntVal(val=end)]]:
            return [StrVal(val=s[start:end])]
        case [[StrVal(val=s)], [IntVal(val=start)], [IntInfVal()]]:
            return [StrVal(val=s[start:])]
        case [[ArrVal(val=arr)], [IntVal(val=start)], [IntVal(val=end)]]:
            return [ArrVal(val=arr[start:end])]
        case [[ArrVal(val=arr)], [IntVal(val=start)], [IntInfVal()]]:
            return [ArrVal(val=arr[start:])]
    raise FunCallErr()


all_reserved_ops : Dict[str, BuiltinFuncDef] = {
        IndirectionIndexOp : BuiltinFuncDef(tp = indirection_index_tp, impl=indirection_index_impl),
        IndirectionSliceOp : BuiltinFuncDef(tp=indirection_slice_tp, impl=indirection_slice_impl),
    }

