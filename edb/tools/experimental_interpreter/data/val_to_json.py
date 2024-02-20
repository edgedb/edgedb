from typing import Any, Dict, Sequence

from . import data_ops as e
from .data_ops import (ArrVal, BoolVal, IntVal, Label, LinkPropLabel,
                       MultiSetVal, NamedTupleVal, ObjectVal, RefVal, StrLabel,
                       StrVal, UnnamedTupleVal, Val, Visible)
from . import expr_ops as eops
from . import type_ops as tops
from . import module_ops as mops
from . import expr_to_str as pp

json_like = str | int | bool | Dict[str, Any] | Sequence[Any]


def label_to_str(lbl: Label) -> str:
    match lbl:
        case StrLabel(s):
            return s
        case LinkPropLabel(s):
            return "@" + s
    raise ValueError("MATCH")


def objectval_to_json_like(objv: ObjectVal) -> Dict[str, json_like]:
    return {label_to_str(k): multi_set_val_to_json_like(v[1])
            for (k, v) in objv.val.items() if
            isinstance(v[0], Visible)}


def val_to_json_like(v: Val) -> json_like:
    match v:
        case e.ScalarVal(_, v):
            if isinstance(v, int) or isinstance(v, str) or isinstance(v, bool):
                return v
            else:
                raise ValueError("not implemented")
        case RefVal(refid, object):
            object_val_result =  objectval_to_json_like(object)
            if len(object_val_result) == 0:
                object_val_result['id'] = refid
            return object_val_result
        # case FreeVal(object):
        #     return objectval_to_json_like(object)
        case ArrVal(val=array):
            return [val_to_json_like(v) for v in array]
        case UnnamedTupleVal(val=array):
            return [val_to_json_like(v) for v in array]
        case NamedTupleVal(val=dic):
            return {k: val_to_json_like(v) for (k, v) in dic.items()}
        # case LinkPropVal(refid=_, linkprop=linkprop):
        #     linkprop_json = objectval_to_json_like(linkprop)
        #     assert isinstance(linkprop_json, dict)
        #     return linkprop_json
    raise ValueError("MATCH", v)


def multi_set_val_to_json_like(m: MultiSetVal) -> json_like:
    # do not dedup when converting to json (see test_edgeql_shape_for_01)
    result = [val_to_json_like(v) for v in m.getRawVals()]
    return result


def typed_objectval_to_json_like(objv: ObjectVal,
                                 obj_tp: e.ObjectTp | e.NominalLinkTp | e.NamedNominalLinkTp | e.UnionTp,
                                 dbschema: e.DBSchema) -> Dict[str, json_like]:
    result: Dict[str, json_like] = {}
    for (k, v) in objv.val.items():
        if isinstance(v[0], Visible):
            sub_tp = tops.tp_project(dbschema, e.ResultTp(tp=obj_tp, mode=e.CardOne), k)
            result[label_to_str(k)] = typed_multi_set_val_to_json_like(
                sub_tp, v[1], dbschema)

            # match k:
            #     case StrLabel(s):
            #         match obj_tp:
            #             case e.ObjectTp(val=tp_vals):
            #                 if s not in tp_vals.keys():
            #                     raise ValueError("label not found", s)
            #                 result[label_to_str(k)] = \
            #                     typed_multi_set_val_to_json_like(
            #                         tp_vals[s], v[1], dbschema)
            #             case e.NamedNominalLinkTp(name=name, linkprop=_):
            #                 assert isinstance(name, e.QualifiedName)
            #                 subject = tops.dereference_var_tp(dbschema, name)
            #                 result[label_to_str(k)] = \
            #                     typed_multi_set_val_to_json_like(
            #                         subject.val[s], v[1], dbschema)
            #             case e.NominalLinkTp(name=_, subject=subject, linkprop=_):
            #                 if not isinstance(subject, e.ObjectTp):
            #                     raise ValueError("Expecting objecttp", subject)
            #                 if s not in subject.val.keys():
            #                     raise ValueError("label not found", s)
            #                 result[label_to_str(k)] = \
            #                     typed_multi_set_val_to_json_like(
            #                         subject.val[s], v[1], dbschema)
            #             case _:
            #                 raise ValueError("Expecting objecttp", obj_tp)
            #     case LinkPropLabel(s):
            #         if (not isinstance(obj_tp, e.NominalLinkTp)
            #             and not isinstance(obj_tp, e.NamedNominalLinkTp)):
            #             raise ValueError("Expecting linkproptp", obj_tp)
            #         if s not in obj_tp.linkprop.val.keys():
            #             raise ValueError("label not found", s)
            #         result[label_to_str(k)] = \
            #             typed_multi_set_val_to_json_like(
            #                 obj_tp.linkprop.val[s], v[1], dbschema)
    return result


def typed_val_to_json_like(v: Val, tp: e.Tp,
                           dbschema: e.DBSchema) -> json_like:
    # while isinstance(tp, e.UnifiableTp):
    #     assert tp.resolution is not None
    #     tp = tp.resolution
    match v:
        case e.ScalarVal(s_tp, v):
            if isinstance(v, int) or isinstance(v, str) or isinstance(v, bool):
                return v
            elif s_tp == e.ScalarTp(e.QualifiedName(["std", "json"])):
                return v
            else:
                raise ValueError("not implemented")
        case RefVal(refid, object):
            if not isinstance(tp, e.ObjectTp | e.NominalLinkTp | e.NamedNominalLinkTp | e.UnionTp | e.IntersectTp):
                raise ValueError("Expecing objecttp", tp)
            object_val_result =  typed_objectval_to_json_like(object, tp, dbschema)
            if len(object_val_result) == 0:
                object_val_result['id'] = refid
            return object_val_result
        case ArrVal(val=array):
            match tp:
                case e.CompositeTp(kind=e.CompositeTpKind.Array, tps=tps):
                    return [typed_val_to_json_like(v, tps[0], dbschema) for v in array]
                case e.UnionTp(l, r):
                    tps = tops.collect_tp_union(tp)
                    if all(isinstance(tp, e.CompositeTp) and tp.kind == e.CompositeTpKind.Array for tp in tps):
                        return [typed_val_to_json_like(v, tops.construct_tps_union([tp.tps[0] for tp in tps]), dbschema) for v in array] # type: ignore
                    else:
                        raise ValueError("Expecing array tp", pp.show(tp))
                case _:
                    raise ValueError("Expecing array tp", pp.show(tp))
        case UnnamedTupleVal(val=array):
            if not isinstance(tp, e.CompositeTp) or tp.kind != e.CompositeTpKind.Tuple:
                match tp:
                    case e.IntersectTp(l, r):
                        all_i_tps = tops.collect_tp_intersection(tp)
                        if all(isinstance(tp, e.CompositeTp) and tp.kind == e.CompositeTpKind.Tuple for tp in all_i_tps):
                            tps = all_i_tps[0].tps # type: ignore
                    case e.UnionTp(l, r):
                        all_u_tps = tops.collect_tp_union(tp)
                        if all(isinstance(tp, e.CompositeTp) and tp.kind == e.CompositeTpKind.Tuple for tp in all_u_tps):
                            if all(len(all_u_tps[0].tps) == len(tp.tps) for tp in all_u_tps):# type: ignore
                                tps = [ tops.construct_tps_union([tp.tps[i] for tp in all_u_tps]) # type: ignore
                                    for i in range(len(all_u_tps[0].tps))# type: ignore
                                ]
                    case _:
                        raise ValueError("Expecing unnamed tuple tp", pp.show(tp))
            else:
                tps = tp.tps
            return [typed_val_to_json_like(v, t, dbschema)
                    for (v, t) in zip(array, tps, strict=True)]
        case NamedTupleVal(val=dic):
            assert isinstance(tp, e.CompositeTp) and tp.kind == e.CompositeTpKind.Tuple
            return {k: typed_val_to_json_like(v, tp.tps[tp.labels.index(k)], dbschema)
                    for (k, v) in dic.items()}
        # case LinkPropVal(refid=_, linkprop=linkprop):
        #     assert isinstance(tp, e.LinkPropTp)
        #     linkprop_json = typed_objectval_to_json_like(
        #         eops.link_prop_obj_to_obj(linkprop),
        #         tp.linkprop, dbschema)
        #     assert isinstance(linkprop_json, dict)
        #     return linkprop_json
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
    if tp.mode.upper == e.CardNumOne:
        if len(m.getVals()) > 1:
            raise ValueError("Single Multiset must have cardinality at most 1")
        if len(m.getVals()) == 1:
            result = typed_val_to_json_like(m.getVals()[0], tp.tp, dbschema)
            if top_level:
                result = [result]
        else:
            if top_level:
                result = []
            else:
                result = None
    else:
        # do not dedup when converting to json (see test_edgeql_shape_for_01)
        result = [typed_val_to_json_like(v, tp.tp, dbschema) for v in m.getRawVals()]
    return result
