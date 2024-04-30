from typing import Dict
from ..data import data_ops as e
from ..data.data_ops import (
    Val,
    Tp,
    ObjectTp,
    RefVal,
    ObjectVal,
    LinkPropLabel,
    Visible,
    MultiSetVal,
    StrLabel,
)
from ..data import expr_ops as eops
from ..data import type_ops as tops
from ..data import expr_to_str as pp


def make_storage_atomic(val: Val, tp: Tp) -> Val:
    def do_coerce_value_to_linkprop_tp(tp_linkprop: ObjectTp) -> Val:
        match val:
            case RefVal(refid=id, tpname=tpname, val=obj):
                obj_link_prop = eops.remove_unless_link_props(obj)
                temp_obj = eops.link_prop_obj_to_obj(obj_link_prop)
                after_obj = coerce_to_storage(temp_obj, tp_linkprop)
                return RefVal(
                    id,
                    tpname,
                    ObjectVal(
                        {
                            LinkPropLabel(k): (Visible(), v)
                            for (k, v) in after_obj.items()
                        }
                    ),
                )
            case _:
                raise ValueError("Cannot Coerce to LinkPropType", val)

    match tp:
        case e.NamedNominalLinkTp(name=_, linkprop=tp_linkprop):
            return do_coerce_value_to_linkprop_tp(tp_linkprop=tp_linkprop)
        case e.NominalLinkTp(name=_, subject=_, linkprop=tp_linkprop):
            return do_coerce_value_to_linkprop_tp(tp_linkprop=tp_linkprop)
        case e.ScalarTp(_):
            return val
        case e.DefaultTp(expr=_, tp=d_tp):
            return make_storage_atomic(val, d_tp)
        case e.UnionTp(_, _):
            all_tps = tops.collect_tp_union(tp)
            assert len(all_tps) > 0
            if all(
                isinstance(tp, e.NamedNominalLinkTp | e.NominalLinkTp)
                for tp in all_tps
            ):
                lp: ObjectTp = all_tps[0].linkprop  # type: ignore
                if all(tp.linkprop == lp for tp in all_tps):  # type: ignore
                    return do_coerce_value_to_linkprop_tp(tp_linkprop=lp)
                else:
                    raise ValueError("TODO")
            else:
                raise ValueError("TODO")
        case e.CompositeTp(_, tps, _):
            if all(tops.tp_is_primitive(tp) for tp in tps):
                return val
            else:
                raise ValueError("TODO")
        case _:
            raise ValueError("Coercion Not Implemented for", tp)


# we require fmt to be a storage tp -- No Computable Types should be present
def coerce_to_storage(val: ObjectVal, fmt: ObjectTp) -> Dict[str, MultiSetVal]:
    # ensure no redundant keys
    extra_keys = [
        k
        for k in val.val.keys()
        if k not in [StrLabel(k) for k in fmt.val.keys()]
    ]
    if extra_keys:
        raise ValueError(
            "Coercion failed, object contains redundant keys:",
            extra_keys,
            "val_keys are",
            val.val.keys(),
            "fmt_keys are",
            fmt.val.keys(),
            "when coercing ",
            pp.show_val(val),
            " to ",
            pp.show_tp(fmt),
        )
    left_out_keys = [
        k for k in fmt.val.keys() if StrLabel(k) not in val.val.keys()
    ]
    if left_out_keys:
        raise ValueError(
            "Coercion failed, object missing keys:",
            left_out_keys,
            "when coercing ",
            pp.show(val),
            " to ",
            pp.show(fmt),
        )
    return {
        k: (
            e.ResultMultiSetVal(
                [
                    make_storage_atomic(v, tp[0])
                    for v in val.val[StrLabel(k)][1].getVals()
                ]
            )
            if StrLabel(k) in val.val.keys()
            else e.ResultMultiSetVal([])
        )
        for (k, tp) in fmt.val.items()
    }
