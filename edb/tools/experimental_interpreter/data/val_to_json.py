from typing import Any, Dict, Sequence

from . import data_ops as e
from .data_ops import (ArrVal, BoolVal, FreeVal, IntVal, Label, LinkPropLabel,
                       MultiSetVal, NamedTupleVal, ObjectVal, RefVal, StrLabel,
                       StrVal, UnnamedTupleVal, Val, Visible, LinkPropVal)
from . import expr_ops as eops
from . import type_ops as tops

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
            linkprop_json = objectval_to_json_like(linkprop)
            assert isinstance(linkprop_json, dict)
            return linkprop_json
    raise ValueError("MATCH", v)


def multi_set_val_to_json_like(m: MultiSetVal) -> json_like:
    if m.singleton:
        assert len(m.vals) <= 1, (
            "Single Multiset must have cardinality at most 1")
        if len(m.vals) == 1:
            result = val_to_json_like(m.vals[0])
        else:
            result = []
    else:
        result = [val_to_json_like(v) for v in m.vals]
    return result


def typed_objectval_to_json_like(objv: ObjectVal,
                                 obj_tp: e.ObjectTp | e.LinkPropTp,
                                 dbschema: e.DBSchema) -> json_like:
    result: Dict[str, json_like] = {}
    for (k, v) in objv.val.items():
        if isinstance(v[0], Visible):
            match k:
                case StrLabel(s):
                    match obj_tp:
                        case e.ObjectTp(val=tp_vals):
                            if s not in tp_vals.keys():
                                raise ValueError("label not found", s)
                            result[label_to_str(k)] = \
                                typed_multi_set_val_to_json_like(
                                    tp_vals[s], v[1], dbschema)
                        case e.LinkPropTp(subject=subject, linkprop=_):
                            if isinstance(subject, e.VarTp):
                                subject = tops.dereference_var_tp(
                                    dbschema, subject)
                            if not isinstance(subject, e.ObjectTp):
                                raise ValueError("Expecting objecttp", subject)
                            if s not in subject.val.keys():
                                raise ValueError("label not found", s)
                            result[label_to_str(k)] = \
                                typed_multi_set_val_to_json_like(
                                    subject.val[s], v[1], dbschema)
                        case _:
                            raise ValueError("Expecting objecttp", obj_tp)
                case LinkPropLabel(s):
                    if not isinstance(obj_tp, e.LinkPropTp):
                        raise ValueError("Expecting linkproptp", obj_tp)
                    if s not in obj_tp.linkprop.val.keys():
                        raise ValueError("label not found", s)
                    result[label_to_str(k)] = \
                        typed_multi_set_val_to_json_like(
                            obj_tp.linkprop.val[s], v[1], dbschema)
    return result


def typed_val_to_json_like(v: Val, tp: e.Tp,
                           dbschema: e.DBSchema) -> json_like:
    while isinstance(tp, e.UnifiableTp):
        assert tp.resolution is not None
        tp = tp.resolution
    match v:
        case StrVal(s):
            return s
        case IntVal(i):
            return i
        case BoolVal(b):
            return b
        case RefVal(_, object):
            if (isinstance(tp, e.LinkPropTp)
                    and tp.linkprop == e.ObjectTp({})):
                tp = tp.subject
            if not isinstance(tp, e.ObjectTp | e.LinkPropTp):
                raise ValueError("Expecing objecttp")
            return typed_objectval_to_json_like(object, tp, dbschema)
        case FreeVal(object):
            assert isinstance(tp, e.ObjectTp)
            return typed_objectval_to_json_like(object, tp, dbschema)
        case ArrVal(val=array):
            assert isinstance(tp, e.ArrTp)
            return [typed_val_to_json_like(v, tp.tp, dbschema) for v in array]
        case UnnamedTupleVal(val=array):
            if not isinstance(tp, e.UnnamedTupleTp):
                raise ValueError("Expecing unnamed tuple tp", tp)
            return [typed_val_to_json_like(v, t, dbschema)
                    for (v, t) in zip(array, tp.val, strict=True)]
        case NamedTupleVal(val=dic):
            assert isinstance(tp, e.NamedTupleTp)
            return {k: typed_val_to_json_like(v, tp.val[k], dbschema)
                    for (k, v) in dic.items()}
        case LinkPropVal(refid=_, linkprop=linkprop):
            assert isinstance(tp, e.LinkPropTp)
            linkprop_json = typed_objectval_to_json_like(
                eops.link_prop_obj_to_obj(linkprop),
                tp.linkprop, dbschema)
            assert isinstance(linkprop_json, dict)
            return linkprop_json
    raise ValueError("MATCH", v)


def typed_multi_set_val_to_json_like(
        tp: e.ResultTp,
        m: MultiSetVal,
        dbschema: e.DBSchema,
        top_level=False) -> json_like:
    """
    Convert a MultiSetVal to a JSON-like value.

    param top_level: If True, the result is a list of values, even if
                     the result's type is a singleton.
    """
    if tp.mode.upper == e.Fin(1):
        assert len(m.vals) <= 1, (
            "Single Multiset must have cardinality at most 1")
        if len(m.vals) == 1:
            result = typed_val_to_json_like(m.vals[0], tp.tp, dbschema)
            if top_level:
                result = [result]
        else:
            result = []
    else:
        result = [typed_val_to_json_like(v, tp.tp, dbschema) for v in m.vals]
    return result
