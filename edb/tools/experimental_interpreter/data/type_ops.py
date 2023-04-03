
from .data_ops import DBSchema
from . import data_ops as e


def is_nominal_subtype_in_schema(
        subtype: str, supertype: str, dbschema: DBSchema):
    # TODO: properly implement
    return subtype == supertype


def is_real_subtype(tp1: e.Tp, tp2: e.Tp) -> bool:
    # TODO: implement
    return tp1 == tp2

def is_cardinal_subtype(cm: e.CMMode, cm2: e.CMMode) -> bool:
    return (
        cm2.lower <= cm.lower and
        cm.upper <= cm2.upper and
        cm.multiplicity <= cm2.multiplicity
    )
    



def get_runtime_tp(tp : e.Tp) -> e.Tp:
    """Drops defaults and computed"""
    return tp

def tp_is_primitive(tp : e.Tp) -> bool:
    match tp:
        case e.IntTp():
            return True
        case e.ObjectTp(_):
            return False
        case _:
            raise ValueError("Not implemented")


def match_param_modifier(p : e.ParamModifier, m : e.CMMode) -> e.CMMode:
    match p:
        case e.ParamSingleton():
            return m
        case e.ParamOptional():
            return e.CMMode(e.max_cardinal(e.Fin(1), m.lower), 
                            e.max_cardinal(e.Fin(1), m.upper),
                            e.max_cardinal(e.Fin(1), m.multiplicity))
        case e.ParamSetOf():
            return e.CardOne
        case _:
            raise ValueError("Not possible")


def is_order_spec(tp: e.ResultTp) -> bool:
    print("WARNING: is_order_spec not implemented")
    return True
    

def tp_project(tp: e.ResultTp, label: e.Label) -> e.ResultTp:

    def post_process_result_base_tp(result_base_tp: e.Tp, 
                                    result_mode: e.CMMode) -> e.ResultTp:
        if not tp_is_primitive(result_base_tp):
            result_mode = e.CMMode(
                e.min_cardinal(e.Fin(1), result_mode.lower),
                result_mode.upper,
                e.min_cardinal(e.Fin(1),
                               result_mode.multiplicity))
        return e.ResultTp(result_base_tp, result_mode)
    match label:
        case e.LinkPropLabel(label=lbl):
            match tp.tp:
                case e.LinkPropTp(subject=_, linkprop=tp_linkprop):
                    return tp_project(e.ResultTp(tp_linkprop, tp.mode),
                                      e.StrLabel(lbl))
                case _:
                    raise ValueError("Cannot tp_project a linkprop "
                                     "from a non linkprop type")
        case e.StrLabel(label=lbl):
            match tp.tp:
                case e.LinkPropTp(subject=tp_subject, linkprop=_):
                    return tp_project(e.ResultTp(tp_subject, tp.mode),
                                      e.StrLabel(lbl))
                case e.ObjectTp(val=tp_obj):
                    if lbl in tp_obj:
                        result_base_tp = tp_obj[lbl].tp
                        result_mode = tp.mode * tp_obj[lbl].mode
                        return post_process_result_base_tp(
                            result_base_tp, result_mode)
                    else:
                        raise ValueError("Label not found")
                case e.NamedTupleTp(val=tp_tuple):
                    if lbl in tp_tuple.keys():
                        result_base_tp = tp_tuple[lbl]
                        result_mode = tp.mode 
                        return post_process_result_base_tp(
                            result_base_tp, result_mode)
                    elif lbl.isdigit():
                        result_base_tp = list(tp_tuple.values())[int(lbl)]
                        result_mode = tp.mode
                        return post_process_result_base_tp(
                            result_base_tp, result_mode)
                    else:
                        raise ValueError("Label not found")
                case e.UnnamedTupleTp(val=tp_tuple):
                    if lbl.isdigit():
                        result_base_tp = tp_tuple[int(lbl)]
                        result_mode = tp.mode
                        return post_process_result_base_tp(
                            result_base_tp, result_mode)
                    else:
                        raise ValueError("Label not found")
                case _:
                    raise ValueError("Cannot tp_project a label "
                                     "from a non object type")
