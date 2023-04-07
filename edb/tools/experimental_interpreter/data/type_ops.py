
from .data_ops import DBSchema
from .expr_to_str import show_tp
from . import data_ops as e
from typing import List


def construct_tp_intersection(tp1: e.Tp, tp2: e.Tp) -> e.Tp:
    # TODO: optimize so that if tp1 is a subtype of tp2, we return tp2
    if tp1 == e.AnyTp():
        return tp2
    elif tp2 == e.AnyTp():
        return tp1
    else:
        return e.IntersectTp(tp1, tp2)


def construct_tp_union(tp1: e.Tp, tp2: e.Tp) -> e.Tp:
    # TODO: optimize so that if tp1 is a subtype of tp2, we return tp2
    if tp1 == tp2:
        return tp1
    else:
        return e.UnionTp(tp1, tp2)


def collect_tp_union(tp1: e.Tp) -> List[e.Tp]:
    # TODO: optimize so that if tp1 is a subtype of tp2, we return tp2
    match tp1:
        case e.UnionTp(left=tp1, right=tp2):
            return collect_tp_union(tp1) + collect_tp_union(tp2)
        case _:
            return [tp1]


def is_nominal_subtype_in_schema(
        subtype: str, supertype: str, dbschema: DBSchema):
    # TODO: properly implement
    return subtype == supertype


def mode_is_optional(m: e.CMMode) -> bool:
    return isinstance(m.lower, e.FiniteCardinal) and m.lower.value == 0


def object_tp_is_essentially_optional(tp: e.ObjectTp) -> bool:
    return all(mode_is_optional(md_tp.mode) for md_tp in tp.val.values())


def dereference_var_tp(ctx: e.TcCtx, tp: e.VarTp) -> e.Tp:
    if tp.name in ctx.statics.schema.val:
        return ctx.statics.schema.val[tp.name]
    else:
        raise ValueError("Type not found")


def assert_insert_subtype(ctx: e.TcCtx, tp1: e.Tp, tp2: e.Tp) -> None:
    assert_real_subtype(ctx, tp1, tp2, is_insert_subtp=True)


def assert_real_subtype(ctx: e.TcCtx, tp1: e.Tp, tp2: e.Tp,
                        is_insert_subtp: bool = False) -> None:
    if tp_is_primitive(tp1) and tp_is_primitive(tp2):
        match tp1, tp2:
            case e.IntTp(), e.IntInfTp():
                pass
            case _:
                if tp1 != tp2:
                    raise ValueError("not subtype")
                else:
                    pass

    # Unifications
    elif isinstance(tp1, e.UnifiableTp):
        if tp1.resolution is None:
            tp1.resolution = tp2
        else:
            assert_real_subtype(ctx, tp1.resolution, tp2, is_insert_subtp)
    elif isinstance(tp2, e.UnifiableTp):
        if tp2.resolution is None:
            tp2.resolution = tp1
        else:
            assert_real_subtype(ctx, tp1, tp2.resolution, is_insert_subtp)

    # Variable expansion
    elif isinstance(tp1, e.VarTp) and isinstance(tp2, e.VarTp):
        if tp1.name != tp2.name:
            raise ValueError("TODO: nominal subtype")
        else:
            pass
    elif isinstance(tp1, e.VarTp):
        tp1_tp = ctx.statics.schema.val[tp1.name]
        assert_real_subtype(ctx, tp1_tp, tp2, is_insert_subtp)
    elif isinstance(tp2, e.VarTp):
        tp2_tp = ctx.statics.schema.val[tp2.name]
        assert_real_subtype(ctx, tp1, tp2_tp, is_insert_subtp)

    else:
        match tp1, tp2:
            case _, e.AnyTp():
                pass
            case e.ObjectTp(val=tp1_val), e.ObjectTp(val=tp2_val):
                for lbl, md_tp in tp1_val.items():
                    if lbl not in tp2_val.keys():
                        # This is fine because of computed and inheritance
                        continue
                    md_tp_2 = tp2_val[lbl]
                    assert_cardinal_subtype(md_tp.mode, md_tp_2.mode)
                    assert_real_subtype(ctx, md_tp.tp, md_tp_2.tp,
                                        is_insert_subtp)
                if is_insert_subtp:
                    pass
                else:
                    if any(tp2_key not in tp1_val.keys()
                           for tp2_key in tp2_val.keys()):
                        raise ValueError("not subtype, tp_2 has more keys")
                    else:
                        pass
            case (e.LinkPropTp(subject=s_1, linkprop=lp_1),
                    e.LinkPropTp(subject=s_2, linkprop=lp_2)):
                assert_real_subtype(ctx, s_1, s_2, is_insert_subtp)
                assert_real_subtype(ctx, lp_1, lp_2, is_insert_subtp)
            case (e.LinkPropTp(subject=s_1, linkprop=e.ObjectTp({})), _):
                assert_real_subtype(ctx, s_1, tp2, is_insert_subtp)
            case (e.ObjectTp(_), e.LinkPropTp(subject=s_2, linkprop=lp_2)):
                # The semantics here is a pending discussion
                # (see item 3 of the google doc)
                # Ideally, I advocate for approach (B), this is not allowed
                # Temporarily approach (A) is implemented, which
                # will incur a runtime ISE if linkprop is projected
                if object_tp_is_essentially_optional(lp_2):
                    assert_real_subtype(ctx, tp1, s_2, is_insert_subtp)
                else:
                    raise ValueError("not subtype, non optional linkprop",
                                     tp1, tp2)

            # Union and intersections
            case (e.UnionTp(left=tp1_left, right=tp1_right), _):
                assert_real_subtype(ctx, tp1_left, tp2, is_insert_subtp)
                assert_real_subtype(ctx, tp1_right, tp2, is_insert_subtp)

            # Other structural typing
            case (e.ArrTp(tp=tp1_val), e.ArrTp(tp=tp2_val)):
                assert_real_subtype(ctx, tp1_val, tp2_val, is_insert_subtp)



            # For debugging Purposes
            case ((e.ArrTp(_), e.StrTp())
                  | (e.ArrTp(_), e.IntTp())
                  ):
                raise ValueError("not subtype", tp1, tp2)
            case _:
                print("Not Implemented Subtyping Check:",
                      show_tp(tp1), show_tp(tp2))
                raise ValueError("Not implemented", tp1, tp2,
                                 is_insert_subtp)


def assert_cardinal_subtype(cm: e.CMMode, cm2: e.CMMode) -> None:
    if not (
        cm2.lower <= cm.lower and
        cm.upper <= cm2.upper and
        cm.multiplicity <= cm2.multiplicity
    ):
        raise ValueError("not subtype", cm, cm2)


def get_runtime_tp(tp: e.Tp) -> e.Tp:
    """Drops defaults and computed"""
    return tp


def tp_is_primitive(tp: e.Tp) -> bool:
    match tp:
        case (e.IntTp()
              | e.StrTp()
              | e.BoolTp()
              | e.IntInfTp()
              | e.UuidTp()
              ):
            return True
        case (e.ObjectTp(_)
              | e.SomeTp(_)
              | e.UnifiableTp(_)
              | e.LinkPropTp(_)
              | e.VarTp(_)
              | e.UnnamedTupleTp(_)
              | e.ArrTp(_)
              | e.AnyTp()
              ):
            return False
        case (e.UnionTp(left=left_tp, right=right_tp)
              | e.IntersectTp(left=left_tp, right=right_tp)
              ):
            # return tp_is_primitive(left_tp) and tp_is_primitive(right_tp)
            return False  # this case is actually ambiguous
        case _:
            raise ValueError("Not implemented", tp)


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
    

def tp_project(ctx: e.TcCtx, tp: e.ResultTp, label: e.Label) -> e.ResultTp:

    def post_process_result_base_tp(result_base_tp: e.Tp, 
                                    result_mode: e.CMMode) -> e.ResultTp:
        if not tp_is_primitive(result_base_tp):
            result_mode = e.CMMode(
                e.min_cardinal(e.Fin(1), result_mode.lower),
                result_mode.upper,
                e.min_cardinal(e.Fin(1),
                               result_mode.multiplicity))
        return e.ResultTp(result_base_tp, result_mode)
    if isinstance(tp.tp, e.VarTp):
        target_tp = dereference_var_tp(ctx, tp.tp)
        return tp_project(ctx, e.ResultTp(target_tp, tp.mode),
                          label)

    match label:
        case e.LinkPropLabel(label=lbl):
            match tp.tp:
                case e.LinkPropTp(subject=_, linkprop=tp_linkprop):
                    return tp_project(ctx, e.ResultTp(tp_linkprop, tp.mode),
                                      e.StrLabel(lbl))
                case _:
                    raise ValueError("Cannot tp_project a linkprop "
                                     "from a non linkprop type")
        case e.StrLabel(label=lbl):
            match tp.tp:
                case e.LinkPropTp(subject=tp_subject, linkprop=_):
                    return tp_project(ctx, e.ResultTp(tp_subject, tp.mode),
                                      e.StrLabel(lbl))
                case e.ObjectTp(val=tp_obj):
                    if lbl in tp_obj.keys():
                        result_base_tp = tp_obj[lbl].tp
                        result_mode = tp.mode * tp_obj[lbl].mode
                        return post_process_result_base_tp(
                            result_base_tp, result_mode)
                    elif lbl == "id":
                        return post_process_result_base_tp(
                            e.UuidTp(), tp.mode
                        )
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
                                     "from a non object type", tp.tp)
