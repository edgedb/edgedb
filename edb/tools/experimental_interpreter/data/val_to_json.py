from typing import *
from .data_ops import *

json_like = str | int | bool | Dict[str, Any] | List[Any]

def label_to_str(l : Label) -> str:
    match l:
        case StrLabel(s):
            return s
        case LinkPropLabel(s):
            return "@"+s
    raise ValueError("MATCH")

def objectval_to_json_like(objv : ObjectVal) -> json_like:
    return {label_to_str(k) : multi_set_val_to_json_like(v[1]) 
                for (k,v) in objv.val.items() if 
                v[0] == Visible}

def val_to_json_like(v : Val) -> json_like :
    match v:
        case StrVal(s):
            return s
        case IntVal(i):
            return i
        case BoolVal(b):
            return b
        case RefVal(refid, object):
            return objectval_to_json_like(object)
        case FreeVal(object):
            return objectval_to_json_like(object)
        case ArrayVal(val=array):
            return [val_to_json_like(v) for v in array]
        case UnnamedTupleVal(val=array):
            return [val_to_json_like(v) for v in array]
        case NamedTupleVal(val=dic):
            return {k : val_to_json_like(v) for (k,v) in dic.items()}
    raise ValueError("MATCH")
        

def multi_set_val_to_json_like(m : MultiSetVal) -> json_like :
    return [val_to_json_like(v) for v in m]