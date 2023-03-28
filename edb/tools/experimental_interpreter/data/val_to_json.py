from typing import Any, Dict, Sequence

from .data_ops import (ArrVal, BoolVal, FreeVal, IntVal, Label, LinkPropLabel,
                       MultiSetVal, NamedTupleVal, ObjectVal, RefVal, StrLabel,
                       StrVal, UnnamedTupleVal, Val, Visible, LinkPropVal)
from . import expr_ops as eops

json_like = str | int | bool | Dict[str, Any] | Sequence[Any]


def label_to_str(lbl: Label) -> str:
    match lbl:
        case StrLabel(s):
            return s
        case LinkPropLabel(s):
            return "@" + s
    raise ValueError("MATCH")


def objectval_to_json_like(objv: ObjectVal) -> json_like:
    return {label_to_str(k): multi_set_val_to_json_like(v[1])
            for (k, v) in objv.val.items() if
            isinstance(v[0], Visible)}


def val_to_json_like(v: Val) -> json_like:
    match v:
        case StrVal(s):
            return s
        case IntVal(i):
            return i
        case BoolVal(b):
            return b
        case RefVal(_, object):
            return objectval_to_json_like(object)
        case FreeVal(object):
            return objectval_to_json_like(object)
        case ArrVal(val=array):
            return [val_to_json_like(v) for v in array]
        case UnnamedTupleVal(val=array):
            return [val_to_json_like(v) for v in array]
        case NamedTupleVal(val=dic):
            return {k: val_to_json_like(v) for (k, v) in dic.items()}
        case LinkPropVal(refid=_, linkprop=linkprop):
            # obj_json = val_to_json_like(obj)
            # assert isinstance(obj_json, dict)
            linkprop_json = objectval_to_json_like(
                eops.link_prop_obj_to_obj(linkprop))
            assert isinstance(linkprop_json, dict)
            return linkprop_json
    raise ValueError("MATCH", v)


def multi_set_val_to_json_like(m: MultiSetVal) -> json_like:
    result = [val_to_json_like(v) for v in m]
    return result
