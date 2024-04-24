from . import data_ops as e
from . import expr_ops as eops
from typing import List, Dict, Optional, Callable
from . import module_ops as mops
from ..data import expr_to_str as pp
from functools import reduce
from ..schema import subtyping_resolution

import edgedb


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
    assert not isinstance(tp1, e.UncheckedTypeName) and not isinstance(
        tp2, e.UncheckedTypeName
    )
    if tp1 == tp2:
        return tp1
    else:
        return e.UnionTp(tp1, tp2)


def construct_tps_union(tps: List[e.Tp]) -> e.Tp:
    assert len(tps) > 0
    return reduce(construct_tp_union, tps)


def collect_tp_intersection(tp1: e.Tp) -> List[e.Tp]:
    match tp1:
        case e.IntersectTp(left=tp1, right=tp2):
            return collect_tp_intersection(tp1) + collect_tp_intersection(tp2)
        case _:
            return [tp1]


def collect_tp_union(tp1: e.Tp) -> List[e.Tp]:
    match tp1:
        case e.UnionTp(left=tp1, right=tp2):
            return collect_tp_union(tp1) + collect_tp_union(tp2)
        case _:
            return [tp1]


def is_nominal_subtype_in_schema(
    ctx: e.TcCtx | e.DBSchema,
    subtype: e.QualifiedName,
    supertype: e.QualifiedName,
):
    if subtype == supertype:
        return True
    else:
        schema = ctx.schema if isinstance(ctx, e.TcCtx) else ctx
        if subtype in schema.subtyping_relations:
            if supertype == e.QualifiedName(["std", "anytype"]):
                return True
            return (
                supertype
                in subtyping_resolution.find_all_supertypes_of_tp_in_schema(
                    schema, subtype
                )
            )
        else:
            return False


def mode_is_optional(m: e.CMMode) -> bool:
    return isinstance(m.lower, e.ZeroCardinal)


def object_tp_is_essentially_optional(tp: e.ObjectTp) -> bool:
    return all(mode_is_optional(md_tp.mode) for md_tp in tp.val.values())


def dereference_var_tp(
    dbschema: e.DBSchema, qn: e.QualifiedName
) -> e.ObjectTp:
    resolved = mops.resolve_type_name(dbschema, qn)
    assert isinstance(resolved, e.ObjectTp)
    return resolved


def assert_real_subtype(
    ctx: e.TcCtx,
    tp1: e.Tp,
    tp2: e.Tp,
) -> None:
    if not check_is_subtype(ctx, tp1, tp2):
        raise ValueError("not subtype", tp1, tp2)
    else:
        pass


def resolve_named_nominal_link_tp(
    ctx: e.TcCtx, tp: e.NamedNominalLinkTp
) -> e.NominalLinkTp:
    match tp:
        case e.NamedNominalLinkTp(name=e.QualifiedName(n_1), linkprop=lp_1):
            resolved_type_def = mops.resolve_type_name(
                ctx, e.QualifiedName(n_1)
            )
            assert isinstance(resolved_type_def, e.ObjectTp)
            return e.NominalLinkTp(
                subject=resolved_type_def,
                name=e.QualifiedName(n_1),
                linkprop=lp_1,
            )
        case _:
            raise ValueError("Type Error")


def type_subtyping_walk(
    recurse: Callable[[e.TcCtx, e.Tp, e.Tp], bool],
    ctx: e.TcCtx,
    tp1: e.Tp,
    tp2: e.Tp,
) -> bool:
    """
    Walks the type equality tree.
    Subtrees are checked for equality using the recurse function,
    to account for possible unifications.
    """
    if tp_is_primitive(tp1) and tp_is_primitive(tp2):
        if isinstance(tp1, e.ScalarTp) and isinstance(tp2, e.ScalarTp):
            return is_nominal_subtype_in_schema(ctx, tp1.name, tp2.name)
        else:
            match tp1, tp2:
                case _, e.DefaultTp(tp=tp2_inner, expr=_):
                    return recurse(ctx, tp1, tp2_inner)
                case e.DefaultTp(tp=tp1_inner, expr=_), _:
                    return recurse(ctx, tp1_inner, tp2)
                case _, e.ComputableTp(tp=tp2_inner, expr=_):
                    return recurse(ctx, tp1, tp2_inner)
                case e.ComputableTp(tp=tp1_inner, expr=_), _:
                    return recurse(ctx, tp1_inner, tp2)
                case _:
                    raise ValueError("TODO")
    else:
        match tp1, tp2:
            case _, e.AnyTp():
                return True

            # Union and intersections before expansion of names
            case (e.UnionTp(left=tp1_left, right=tp1_right), _):
                return recurse(ctx, tp1_left, tp2) and recurse(
                    ctx, tp1_right, tp2
                )

            case (_, e.UnionTp(left=tp2_left, right=tp2_right)):
                return recurse(ctx, tp1, tp2_left) or recurse(
                    ctx, tp1, tp2_right
                )

            case (_, e.IntersectTp(left=tp2_left, right=tp2_right)):
                return recurse(ctx, tp1, tp2_left) and recurse(
                    ctx, tp1, tp2_right
                )

            case (e.IntersectTp(left=tp1_left, right=tp1_right), _):
                return recurse(ctx, tp1_left, tp2) or recurse(
                    ctx, tp1_right, tp2
                )

            case e.ObjectTp(val=tp1_val), e.ObjectTp(val=tp2_val):
                if set(tp1_val.keys()) != set(tp2_val.keys()):
                    return False
                for k in tp1_val.keys():
                    if not recurse(ctx, tp1_val[k].tp, tp2_val[k].tp):
                        return False
                    if not is_cardinal_subtype(
                        tp1_val[k].mode, tp2_val[k].mode
                    ):
                        return False
                return True
            case (
                e.NominalLinkTp(name=n_1, subject=s_1, linkprop=lp_1),
                e.NominalLinkTp(name=n_2, subject=s_2, linkprop=lp_2),
            ):

                if is_nominal_subtype_in_schema(ctx, n_1, n_2):
                    return recurse(ctx, s_1, s_2) and recurse(ctx, lp_1, lp_2)
                else:
                    return False
            case (
                e.NamedNominalLinkTp(name=n_1, linkprop=lp_1),
                e.NamedNominalLinkTp(name=n_2, linkprop=lp_2),
            ):
                assert isinstance(n_1, e.QualifiedName)
                assert isinstance(n_2, e.QualifiedName)
                if is_nominal_subtype_in_schema(ctx, n_1, n_2):
                    return recurse(ctx, lp_1, lp_2)
                else:
                    return False
            case (
                _,
                e.NamedNominalLinkTp(name=e.QualifiedName(n_2), linkprop=lp_2),
            ):
                assert isinstance(tp2, e.NamedNominalLinkTp)
                return recurse(
                    ctx, tp1, resolve_named_nominal_link_tp(ctx, tp2)
                )
            case (
                e.NamedNominalLinkTp(name=e.QualifiedName(n_1), linkprop=lp_1),
                _,
            ):
                assert isinstance(tp1, e.NamedNominalLinkTp)
                return recurse(
                    ctx, resolve_named_nominal_link_tp(ctx, tp1), tp2
                )

            # Other structural typing
            case (
                e.CompositeTp(kind=kind1, tps=tps1),
                e.CompositeTp(kind=kind2, tps=tps2),
            ):
                if kind1 != kind2 or len(tps1) != len(tps2):
                    return False
                else:
                    return all(
                        recurse(ctx, tp1, tp2) for tp1, tp2 in zip(tps1, tps2)
                    )

            case _:
                return False
        raise ValueError(
            "should not be reachable, check if returns are missing?", tp1, tp2
        )


def check_is_subtype(
    ctx: e.TcCtx,
    tp1: e.Tp,
    tp2: e.Tp,
) -> bool:
    return type_subtyping_walk(check_is_subtype, ctx, tp1, tp2)


def collect_is_subtype_with_instantiation(
    ctx: e.TcCtx,
    syn_tp: e.Tp,
    ck_tp: e.Tp,
    some_tp_mapping: Dict[int, List[e.Tp]],
) -> bool:
    """
    Here, tp2 may be a some type
    (parametric morphism, and need to be instantiated)
    """
    if isinstance(ck_tp, e.SomeTp):
        if ck_tp.index in some_tp_mapping:
            some_tp_mapping[ck_tp.index] = [
                syn_tp,
                *some_tp_mapping[ck_tp.index],
            ]
        else:
            some_tp_mapping[ck_tp.index] = [syn_tp]
        return True
    else:

        def recurse(ctx: e.TcCtx, tp1: e.Tp, tp2: e.Tp) -> bool:
            return collect_is_subtype_with_instantiation(
                ctx, tp1, tp2, some_tp_mapping
            )

        return type_subtyping_walk(recurse, ctx, syn_tp, ck_tp)


def check_is_subtype_with_instantiation(
    ctx: e.TcCtx, syn_tp: e.Tp, ck_tp: e.Tp, some_tp_mapping: Dict[int, e.Tp]
) -> bool:
    if isinstance(ck_tp, e.SomeTp):
        if ck_tp.index in some_tp_mapping:
            real_ck_tp = some_tp_mapping[ck_tp.index]
            return check_is_subtype_with_instantiation(
                ctx, syn_tp, real_ck_tp, some_tp_mapping
            )
        else:
            some_tp_mapping[ck_tp.index] = syn_tp
            return True
    else:

        def recurse(ctx: e.TcCtx, tp1: e.Tp, tp2: e.Tp) -> bool:
            return check_is_subtype_with_instantiation(
                ctx, tp1, tp2, some_tp_mapping
            )

        return type_subtyping_walk(recurse, ctx, syn_tp, ck_tp)


def recursive_instantiate_tp(
    tp: e.Tp, some_tp_mapping: Dict[int, e.Tp]
) -> e.Tp:
    """
    Instantiate all Some(i) sub term in a type.
    Used to compute parametric function's return type.
    """

    def inst_func(tp: e.Tp) -> Optional[e.Tp]:
        if isinstance(tp, e.SomeTp):
            if tp.index in some_tp_mapping:
                return some_tp_mapping[tp.index]
            else:
                raise ValueError("some tp not found")
        else:
            return None

    return eops.map_tp(inst_func, tp)


def is_cardinal_subtype(cm: e.CMMode, cm2: e.CMMode) -> bool:
    return cm2.lower <= cm.lower and cm.upper <= cm2.upper


def assert_cardinal_subtype(cm: e.CMMode, cm2: e.CMMode) -> None:
    if not (is_cardinal_subtype(cm, cm2)):
        raise ValueError("not subtype", cm, cm2)


def get_runtime_tp(tp: e.Tp) -> e.Tp:
    """Drops defaults and computed"""

    def map_func(candidate: e.Tp) -> Optional[e.Tp]:
        match candidate:
            case e.ComputableTp(expr=_, tp=c_tp):
                return get_runtime_tp(c_tp)
            case e.DefaultTp(expr=_, tp=c_tp):
                return get_runtime_tp(c_tp)
            case _:
                return None

    return eops.map_tp(map_func, tp)


def get_storage_tp(fmt: e.ObjectTp) -> e.ObjectTp:
    """
    Returns the storage type of a given type.
    In particular, computable attributes should be removed.

    """

    def drop_computable(t: e.ObjectTp):
        return e.ObjectTp(
            {
                k: tp
                for (k, tp) in t.val.items()
                if not isinstance(tp.tp, e.ComputableTp)
            }
        )

    def get_lp_storage(t: e.ResultTp) -> e.ResultTp:
        match t.tp:
            case e.NamedNominalLinkTp(name=n, linkprop=lp):
                return e.ResultTp(
                    e.NamedNominalLinkTp(name=n, linkprop=drop_computable(lp)),
                    t.mode,
                )
            case e.DefaultTp(tp=tp, expr=_):
                return get_lp_storage(e.ResultTp(tp, t.mode))
            case e.UnionTp(left=left_tp, right=right_tp):
                return e.ResultTp(
                    e.UnionTp(
                        left=get_lp_storage(e.ResultTp(left_tp, t.mode)).tp,
                        right=get_lp_storage(e.ResultTp(right_tp, t.mode)).tp,
                    ),
                    t.mode,
                )
            case e.CompositeTp(kind=kind, tps=tps, labels=labels):
                return e.ResultTp(
                    e.CompositeTp(
                        kind=kind,
                        tps=[
                            get_lp_storage(e.ResultTp(tp, t.mode)).tp
                            for tp in tps
                        ],
                        labels=labels,
                    ),
                    t.mode,
                )
            case _:
                if tp_is_primitive(t.tp):
                    return t
                else:
                    raise ValueError("Cannot get lp storage", t.tp)

    return e.ObjectTp(
        {
            k: get_lp_storage(tp)
            for (k, tp) in fmt.val.items()
            if not isinstance(tp.tp, e.ComputableTp)
        }
    )


def tp_is_primitive(tp: e.Tp) -> bool:
    match tp:
        case e.ScalarTp(_):
            return True
        case (
            e.ObjectTp(_)
            | e.SomeTp(_)
            | e.NamedNominalLinkTp(_)
            | e.NominalLinkTp(_)
            | e.AnyTp()
        ):
            return False
        case e.UnionTp(left=_, right=_) | e.IntersectTp(left=_, right=_):
            return False  # this case is actually ambiguous
        case e.ComputableTp(tp=under_tp, expr=_):
            return tp_is_primitive(under_tp)
        case e.DefaultTp(tp=under_tp, expr=_):
            return tp_is_primitive(under_tp)
        case e.CompositeTp(_):
            return False
        case _:
            raise ValueError("Not implemented", tp)


def match_param_modifier(p: e.ParamModifier, m: e.CMMode) -> e.CMMode:
    match p:
        case e.ParamSingleton():
            return m
        case e.ParamOptional():
            return e.CMMode(
                e.max_cardinal(e.CardNumOne, m.lower),
                e.max_cardinal(e.CardNumOne, m.upper),
            )
        case e.ParamSetOf():
            return e.CardOne
        case _:
            raise ValueError("Not possible")


def is_order_spec(tp: e.ResultTp) -> bool:
    print("WARNING: is_order_spec not implemented")
    return True


def is_tp_projection_tuple_proj(tp: e.Tp) -> bool:
    match tp:
        case e.CompositeTp(kind=e.CompositeTpKind.Tuple, tps=_):
            return True
        case _:
            return False


def can_project_label_from_tp(
    ctx: e.TcCtx | e.DBSchema, tp: e.Tp, label: e.Label
) -> bool:
    match tp:
        case e.UnionTp(l, r):
            return can_project_label_from_tp(
                ctx, l, label
            ) and can_project_label_from_tp(ctx, r, label)
        case e.IntersectTp(l, r):
            return can_project_label_from_tp(
                ctx, l, label
            ) or can_project_label_from_tp(ctx, r, label)
        case _:
            pass
    match label:
        case e.LinkPropLabel(label=lbl):
            match tp:
                case e.NominalLinkTp(name=_, subject=_, linkprop=tp_linkprop):
                    return can_project_label_from_tp(
                        ctx, tp_linkprop, e.StrLabel(lbl)
                    )
                case e.NamedNominalLinkTp(name=_, linkprop=tp_linkprop):
                    return can_project_label_from_tp(
                        ctx, tp_linkprop, e.StrLabel(lbl)
                    )
                case _:
                    raise ValueError("LP Label Unimplemented")
        case e.StrLabel(label=lbl):
            match tp:
                case e.NominalLinkTp(name=_, subject=tp_subject, linkprop=_):
                    return can_project_label_from_tp(ctx, tp_subject, label)
                case e.NamedNominalLinkTp(name=name, linkprop=_):
                    _, tp_def = mops.resolve_raw_name_and_type_def(ctx, name)
                    assert isinstance(tp_def, e.ObjectTp)
                    return can_project_label_from_tp(ctx, tp_def, label)
                case e.ObjectTp(val=tp_obj):
                    if lbl in tp_obj.keys():
                        return True
                    elif lbl == "id":
                        return True
                    else:
                        return False
                case _:
                    raise ValueError("Unimplemented")


def tp_project(
    ctx: e.TcCtx | e.DBSchema, tp: e.ResultTp, label: e.Label
) -> e.ResultTp:

    def post_process_result_base_tp(
        result_base_tp: e.Tp, result_mode: e.CMMode
    ) -> e.ResultTp:

        if isinstance(result_base_tp, e.UncheckedTypeName):
            raise ValueError("Must not return UncheckedTypeName")
        return e.ResultTp(result_base_tp, result_mode)

    # if isinstance(tp.tp, e.VarTp):
    #     target_tp = dereference_var_tp(ctx.schema, tp.tp)
    #     return tp_project(ctx, e.ResultTp(target_tp, tp.mode),
    #                       label)
    match tp.tp:
        case e.UnionTp(_, _):
            tps = collect_tp_union(tp.tp)
            result_tps = [
                tp_project(ctx, e.ResultTp(u_tp, tp.mode), label)
                for u_tp in tps
            ]
            result = result_tps[0]
            if all(r_tp.mode == result.mode for r_tp in result_tps):
                return e.ResultTp(
                    construct_tps_union([r_tp.tp for r_tp in result_tps]),
                    result.mode,
                )
            else:
                raise ValueError("Ambiguous union projection", result_tps)
        case e.IntersectTp(_, _):
            tps = collect_tp_intersection(tp.tp)
            projectable_tps = [
                itp
                for itp in tps
                if can_project_label_from_tp(ctx, itp, label)
            ]
            if len(projectable_tps) == 0:
                raise edgedb.InvalidReferenceError(
                    f"property '{(label.label)}' does not exist."
                )
            else:
                projected_tps = [
                    tp_project(ctx, e.ResultTp(itp, tp.mode), label)
                    for itp in projectable_tps
                ]
                if all(
                    r_tp.mode == projected_tps[0].mode
                    for r_tp in projected_tps
                ):
                    return e.ResultTp(
                        construct_tps_union(
                            [r_tp.tp for r_tp in projected_tps]
                        ),
                        projected_tps[0].mode,
                    )
                else:
                    raise ValueError(
                        "Ambiguous intersection projection", projected_tps
                    )
        case _:
            pass

    match label:
        case e.LinkPropLabel(label=lbl):
            match tp.tp:
                case e.NominalLinkTp(name=_, subject=_, linkprop=tp_linkprop):
                    return tp_project(
                        ctx, e.ResultTp(tp_linkprop, tp.mode), e.StrLabel(lbl)
                    )
                case e.NamedNominalLinkTp(name=_, linkprop=tp_linkprop):
                    return tp_project(
                        ctx, e.ResultTp(tp_linkprop, tp.mode), e.StrLabel(lbl)
                    )
                case _:
                    raise ValueError(
                        "Cannot tp_project a linkprop "
                        "from a non linkprop type",
                        pp.show(tp.tp),
                    )
        case e.StrLabel(label=lbl):
            match tp.tp:
                case e.NominalLinkTp(name=_, subject=tp_subject, linkprop=_):
                    return tp_project(
                        ctx, e.ResultTp(tp_subject, tp.mode), e.StrLabel(lbl)
                    )
                case e.NamedNominalLinkTp(name=name, linkprop=_):
                    _, tp_def = mops.resolve_raw_name_and_type_def(ctx, name)
                    assert isinstance(tp_def, e.ObjectTp)
                    return tp_project(
                        ctx, e.ResultTp(tp_def, tp.mode), e.StrLabel(lbl)
                    )
                case e.ObjectTp(val=tp_obj):
                    if lbl in tp_obj.keys():
                        result_base_tp = tp_obj[lbl].tp
                        result_mode = tp.mode * tp_obj[lbl].mode
                        return post_process_result_base_tp(
                            result_base_tp, result_mode
                        )
                    elif lbl == "id":
                        return post_process_result_base_tp(e.UuidTp(), tp.mode)
                    elif lbl == "__type__":
                        return post_process_result_base_tp(
                            e.NamedNominalLinkTp(
                                name=e.QualifiedName(["schema", "ObjectType"]),
                                linkprop=e.ObjectTp({}),
                            ),
                            tp.mode,
                        )
                    else:
                        raise ValueError("Label not found", lbl, tp_obj.keys())
                case e.CompositeTp(
                    kind=e.CompositeTpKind.Tuple, tps=tp_tuple, labels=lbls
                ):
                    if lbl.isdigit():
                        result_base_tp = tp_tuple[int(lbl)]
                        result_mode = tp.mode
                        return post_process_result_base_tp(
                            result_base_tp, result_mode
                        )
                    if lbl in lbls:
                        result_base_tp = tp_tuple[lbls.index(lbl)]
                        result_mode = tp.mode
                        return post_process_result_base_tp(
                            result_base_tp, result_mode
                        )
                    else:
                        raise ValueError("Label not found")
                case _:
                    raise ValueError(
                        "Cannot tp_project a label ",
                        label,
                        "from a non object type",
                        pp.show(tp.tp),
                    )


def combine_object_tp(o1: e.ObjectTp, o2: e.ObjectTp) -> e.ObjectTp:
    if isinstance(o1, e.ObjectTp) and isinstance(o2, e.ObjectTp):
        return e.ObjectTp({**o1.val, **o2.val})
    else:
        raise ValueError(
            "not implemented combine object tp", pp.show(o1), pp.show(o2)
        )


def combine_tp_with_subject_tp(ctx: e.TcCtx, o1: e.Tp, o2: e.ObjectTp) -> e.Tp:
    match o1:
        case e.NominalLinkTp(
            name=name, subject=subject_tp, linkprop=linkprop_tp
        ):
            return e.NominalLinkTp(
                name=name,
                subject=combine_object_tp(subject_tp, o2),
                linkprop=linkprop_tp,
            )
        case e.NamedNominalLinkTp(name=name, linkprop=linkprop_tp):
            return combine_tp_with_subject_tp(
                ctx, resolve_named_nominal_link_tp(ctx, o1), o2
            )
        case e.UnionTp(l, r):
            return create_union_tp(
                combine_tp_with_subject_tp(ctx, l, o2),
                combine_tp_with_subject_tp(ctx, r, o2),
            )
        case e.IntersectTp(l, r):
            return create_intersect_tp(
                combine_tp_with_subject_tp(ctx, l, o2),
                combine_tp_with_subject_tp(ctx, r, o2),
            )
        case _:
            raise ValueError(
                "not implemented combine tp", pp.show(o1), pp.show(o2)
            )


def combine_tp_with_linkprop_tp(
    ctx: e.TcCtx, o1: e.Tp, o2: e.ObjectTp
) -> e.Tp:
    match o1:
        case e.NominalLinkTp(
            name=name, subject=subject_tp, linkprop=linkprop_tp
        ):
            return e.NominalLinkTp(
                name=name,
                subject=subject_tp,
                linkprop=combine_object_tp(linkprop_tp, o2),
            )
        case e.NamedNominalLinkTp(name=name, linkprop=linkprop_tp):
            return combine_tp_with_linkprop_tp(
                ctx, resolve_named_nominal_link_tp(ctx, o1), o2
            )
        case e.UnionTp(l, r):
            return e.UnionTp(
                combine_tp_with_linkprop_tp(ctx, l, o2),
                combine_tp_with_linkprop_tp(ctx, r, o2),
            )
        case e.IntersectTp(l, r):
            return e.IntersectTp(
                combine_tp_with_linkprop_tp(ctx, l, o2),
                combine_tp_with_linkprop_tp(ctx, r, o2),
            )
        case _:
            raise ValueError(
                "not implemented combine tp", pp.show(o1), pp.show(o2)
            )


def create_union_tp(tp1: e.Tp, tp2: e.Tp) -> e.Tp:
    if tp1 == tp2:
        return tp1
    else:
        return e.UnionTp(tp1, tp2)


def create_intersect_tp(tp1: e.Tp, tp2: e.Tp) -> e.Tp:
    if tp1 == tp2:
        return tp1
    else:
        return e.IntersectTp(tp1, tp2)
