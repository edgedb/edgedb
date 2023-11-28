

from typing import Callable, Dict, Optional, Sequence, Tuple, cast

from .data_ops import (ArrExpr, ArrVal, BackLinkExpr, BindingExpr, BoolVal,
                       BoundVarExpr, DetachedExpr, Expr, FilterOrderExpr,
                       ForExpr, FreeVarExpr, FunAppExpr, InsertExpr,
                       IntVal, LinkPropLabel, LinkPropProjExpr,
                       MultiSetExpr, MultiSetVal, NamedTupleExpr,
                       ObjectProjExpr, ObjectTp, ObjectVal,
                       OffsetLimitExpr, OptionalForExpr, RefVal,
                       ShapedExprExpr, ShapeExpr, StrLabel, StrVal,
                       SubqueryExpr, Tp, TpIntersectExpr, TypeCastExpr,
                       UnionExpr, UnnamedTupleExpr, UnnamedTupleVal,
                       UpdateExpr, Val, VarExpr, Visible, WithExpr, next_id,
                       next_name)
from . import data_ops as e
from  . import expr_to_str as pp

from typing import List


def map_tp(
        f: Callable[[Tp],
                    Optional[Tp]],
        tp: Tp) -> Tp:
    """ maps a function over free variables and bound variables,
    and does not modify other nodes

    f : called with current expression and the current level, which refers to
        the first binder outside of the entire expression expr
    level : this value refers to the first binder OUTSIDE of the expression
            being mapped, it should be called with initially = 1.
            Increases as we encounter abstractions
    """

    tentative = f(tp)
    if tentative is not None:
        return tentative
    else:
        def recur(expr):
            return map_tp(f, expr)

        match tp:
            case (e.IntTp() | e.BoolTp() | e.StrTp() | e.IntInfTp()
                  | e.AnyTp() ):
                return tp
            case e.ObjectTp(val=val):
                return e.ObjectTp(val={k: e.ResultTp(recur(v), card)
                                       for k, (v, card) in val.items()})
            case e.NamedTupleTp(val=val):
                return e.NamedTupleTp(val={k: recur(v)
                                           for k, v in val.items()})
            case e.UnnamedTupleTp(val=val):
                return e.UnnamedTupleTp(val=[recur(v) for v in val])
            case e.ArrTp(tp=arr_tp):
                return e.ArrTp(tp=recur(arr_tp))
            case e.NamedNominalLinkTp(name=name, linkprop=linkprop):
                return e.NamedNominalLinkTp(name=name, 
                                    linkprop=recur(linkprop))
            case e.UncheckedNamedNominalLinkTp(name=name, linkprop=linkprop):
                return e.UncheckedNamedNominalLinkTp(name=name, 
                                    linkprop=recur(linkprop))
            case e.NominalLinkTp(name=name, subject=subject, linkprop=linkprop):
                return e.NominalLinkTp(name=name, 
                                    subject=recur(subject),
                                    linkprop=recur(linkprop))
            case e.UncheckedComputableTp(_):
                return tp
            case e.ComputableTp(expr=expr, tp=tp):
                return e.ComputableTp(expr=expr, tp=recur(tp))
            case e.DefaultTp(expr=expr, tp=tp):
                return e.DefaultTp(expr=expr, tp=recur(tp))
            case _:
                raise ValueError("Not Implemented", tp)


def map_expr(
        f: Callable[[Expr],
                    Optional[Expr]],
        expr: Expr) -> Expr:
    """ maps a function over free variables and bound variables,
    and does not modify other nodes

    f : called with current expression and the current level, which refers to
        the first binder outside of the entire expression expr
    level : this value refers to the first binder OUTSIDE of the expression
            being mapped, it should be called with initially = 1.
            Increases as we encounter abstractions
    """
    tentative = f(expr)
    if tentative is not None:
        return tentative
    else:
        def recur_tp(expr):
            def f_tp(tp: Tp) -> Optional[Tp]:
                result = f(tp)
                # if result is not None:
                #     assert isinstance(result, Tp)
                return cast(Optional[Tp], result)
            return map_tp(f_tp, expr)

        def recur(expr):
            return map_expr(f, expr)

        match expr:
            case (FreeVarExpr(_) | BoundVarExpr(_) | StrVal(_) | BoolVal(_) |
                    IntVal(_) | RefVal(_) 
                    | ArrVal(_) | UnnamedTupleVal(_)):
                return expr
            case BindingExpr(var=var, body=body):
                return BindingExpr(var=var, body=map_expr(f, body))
            case UnnamedTupleExpr(val=val):
                return UnnamedTupleExpr(val=[recur(e) for e in val])
            case NamedTupleExpr(val=val):
                return NamedTupleExpr(
                    val={k: recur(e) for (k, e) in val.items()})
            case ObjectProjExpr(subject=subject, label=label):
                return ObjectProjExpr(subject=recur(subject), label=label)
            case BackLinkExpr(subject=subject, label=label):
                return BackLinkExpr(subject=recur(subject), label=label)
            case TpIntersectExpr(subject=subject, tp=tp_name):
                return TpIntersectExpr(subject=recur(subject),
                                       tp=tp_name)
            case LinkPropProjExpr(subject=subject, linkprop=label):
                return LinkPropProjExpr(
                    subject=recur(subject),
                    linkprop=label)
            case FunAppExpr(fun=fname, args=args, overloading_index=idx):
                return FunAppExpr(
                    fun=fname, args=[recur(arg) for arg in args],
                    overloading_index=idx)
            case FilterOrderExpr(subject=subject, filter=filter, order=order):
                return FilterOrderExpr(
                    subject=recur(subject),
                    filter=recur(filter),
                    order={l : recur(o) for (l,o) in order.items()})
            case ShapedExprExpr(expr=expr, shape=shape):
                return ShapedExprExpr(expr=recur(expr), shape=recur(shape))
            case ShapeExpr(shape=shape):
                return ShapeExpr(
                    shape={k: recur(e_1) for (k, e_1) in shape.items()})
            case TypeCastExpr(tp=tp, arg=arg):
                return TypeCastExpr(tp=recur_tp(tp), arg=recur(arg))
            case UnionExpr(left=left, right=right):
                return UnionExpr(left=recur(left), right=recur(right))
            case ArrExpr(elems=arr):
                return ArrExpr(elems=[recur(e) for e in arr])
            case MultiSetExpr(expr=arr):
                return MultiSetExpr(expr=[recur(e) for e in arr])
            case OffsetLimitExpr(subject=subject, offset=offset, limit=limit):
                return OffsetLimitExpr(
                    subject=recur(subject),
                    offset=recur(offset),
                    limit=recur(limit))
            case WithExpr(bound=bound, next=next):
                return WithExpr(bound=recur(bound), next=recur(next))
            case InsertExpr(name=name, new=new):
                return InsertExpr(name=name, new={k: recur(v) for (k,v) in new.items()})
            case e.FreeObjectExpr():
                return e.FreeObjectExpr()
            case e.ConditionalDedupExpr(expr=sub):
                return e.ConditionalDedupExpr(recur(sub))
            # case ObjectExpr(val=val):
            #     return ObjectExpr(
            #         val={label: recur(item)
            #              for (label, item) in val.items()})
            case DetachedExpr(expr=expr):
                return DetachedExpr(expr=recur(expr))
            case SubqueryExpr(expr=expr):
                return SubqueryExpr(expr=recur(expr))
            case UpdateExpr(subject=subject, shape=shape):
                return UpdateExpr(
                    subject=recur(subject),
                    shape=recur(shape))
            case e.DeleteExpr(subject=subject):
                return e.DeleteExpr(subject=recur(subject))
            case ForExpr(bound=bound, next=next):
                return ForExpr(bound=recur(bound), next=recur(next))
            case OptionalForExpr(bound=bound, next=next):
                return OptionalForExpr(
                    bound=recur(bound),
                    next=recur(next))
            case e.IfElseExpr(
                    then_branch=then_branch,
                    condition=condition,
                    else_branch=else_branch):
                return e.IfElseExpr(
                    then_branch=recur(then_branch),
                    condition=recur(condition),
                    else_branch=recur(else_branch))
    raise ValueError("Not Implemented: map_expr ", expr)


def map_var(f: Callable[[VarExpr], Optional[Expr]], expr: Expr) -> Expr:
    """ maps a function over free variables and bound variables,
    and does not modify other nodes

    f : if not None, replace with the result
    """
    def map_func(e: Expr) -> Optional[Expr]:
        match e:
            case FreeVarExpr(var=_):
                return f(e)
            case BoundVarExpr(var=_):
                return f(e)
        return None
    return map_expr(map_func, expr)


def get_free_vars(e: Expr) -> Sequence[str]:
    res: Sequence[str] = []

    def map_var_func(ve: VarExpr) -> None:
        nonlocal res
        if isinstance(ve, FreeVarExpr):
            if ve.var not in res:
                res = [*res, ve.var]
        return None

    map_var(map_var_func, e)
    return res


def ensure_no_capture(avoid_list: Sequence[str],
                      e: BindingExpr) -> BindingExpr:
    assert isinstance(e, BindingExpr)
    candidate_name = e.var
    while candidate_name in avoid_list:
        candidate_name = next_name(candidate_name)
    if candidate_name != e.var:
        return abstract_over_expr(
            instantiate_expr(FreeVarExpr(candidate_name), e),
            candidate_name)
    else:
        return e


def instantiate_expr(e2: Expr, bnd_expr: BindingExpr) -> Expr:
    if not isinstance(bnd_expr, BindingExpr):
        raise ValueError(bnd_expr)

    result = subst_expr_for_expr(e2, BoundVarExpr(bnd_expr.var), bnd_expr.body)

    return result


def subst_expr_for_expr(expr2: Expr, replace: Expr, subject: Expr):
    assert not isinstance(replace, BindingExpr)

    e2_free_vars = get_free_vars(expr2)

    def map_func(candidate: Expr) -> Optional[Expr]:
        if candidate == replace:
            return expr2
        elif isinstance(candidate, BindingExpr):
            # shortcut : if we are substituting a free var and a
            # binder binds that var, then we can early stop.
            # The reason is that the var will not occur after alpha renaming
            match replace:
                case (BoundVarExpr(v) | FreeVarExpr(v)):
                    if v == candidate.var:
                        return candidate

            # otherwise we need to ensure that no variable is captured
            no_capture_cand = ensure_no_capture(e2_free_vars, candidate)

            return BindingExpr(
                var=no_capture_cand.var,
                body=subst_expr_for_expr(
                    expr2,
                    replace,
                    no_capture_cand.body))
        else:
            return None

    return map_expr(map_func, subject)


def abstract_over_expr(
        expr: Expr, var: Optional[str] = None) -> BindingExpr:
    """Construct a BindingExpr that binds var"""

    if var is None:
        var = next_name()

    new_body = subst_expr_for_expr(BoundVarExpr(var), FreeVarExpr(var), expr)

    return BindingExpr(var=var, body=new_body)


def iterative_subst_expr_for_expr(
        expr2: Sequence[Expr],
        replace: Sequence[Expr],
        subject: Expr):
    """ Iteratively perform substitution from right to left,
        comptues: [expr2[0]/replace[0]]...[expr[n-1]/replace[n-1]]subject """

    assert len(expr2) == len(replace)
    result = subject
    for i in reversed(list(range(len(replace)))):
        result = subst_expr_for_expr(expr2[i], replace[i], result)
    return result


def appears_in_expr_pred(search_pred: Callable[[Expr], bool],
                         subject: Expr) -> bool:

    class ReturnTrue(Exception):
        pass

    def map_func(candidate: Expr) -> Optional[Expr]:
        if search_pred(candidate):
            raise ReturnTrue()
        else:
            return None

    try:
        map_expr(map_func, subject)
    except ReturnTrue:
        return True
    return False


def appears_in_expr(search: Expr, subject: Expr):
    class ReturnTrue(Exception):
        pass

    expr_is_var: Optional[str]
    match search:
        case FreeVarExpr(vname):
            expr_is_var = vname
        case BoundVarExpr(vname):
            expr_is_var = vname
        case _:
            expr_is_var = None

    def map_func(candidate: Expr) -> Optional[Expr]:
        if (expr_is_var is not None
                and isinstance(candidate, BindingExpr)
                and candidate.var == expr_is_var):

            # terminate search here
            return candidate
        if candidate == search:
            raise ReturnTrue()
        else:
            return None
    try:
        map_expr(map_func, subject)
    except ReturnTrue:
        return True
    return False


def binding_is_unnamed(expr: BindingExpr) -> bool:
    return not appears_in_expr(BoundVarExpr(expr.var), expr.body)


def operate_under_binding(e: BindingExpr, op: Callable[[Expr], Expr]):
    name = next_name()
    return abstract_over_expr(
        op(instantiate_expr(FreeVarExpr(name),
                            e)),
        name)


def get_object_val(val: Val) -> ObjectVal:
    match val:
        # case FreeVal(dictval):
        #     return dictval
        case RefVal(_, dictval):
            return dictval
    raise ValueError("Cannot get object val", val)


def val_is_primitive(rt: Val) -> bool:
    match rt:
        case (StrVal(_) | IntVal(_) | ArrVal(_)
                | UnnamedTupleVal(_) | BoolVal(_)):
            return True
        case RefVal(_):
            return False
    raise ValueError("not implemented")


def val_is_object(rt: Val) -> bool:
    match rt:
        case RefVal(_):
            return True
        case (StrVal(_) | IntVal(_) | ArrVal(_)
                | UnnamedTupleVal(_) | BoolVal(_)):
            return False
    raise ValueError("not implemented", rt)


def val_is_ref_val(rt: Val) -> bool:
    match rt:
        case RefVal(_):
            return True
    return False


def remove_link_props(rt: Val) -> Val:
    match rt:
        case RefVal(refid=id, val=ObjectVal(val=dic)):
            return RefVal(
                refid=id,
                val=ObjectVal(
                    val={k: v for (k, v) in dic.items()
                         if isinstance(k, StrLabel)}))
    raise ValueError("Expected RefVal")


def remove_unless_link_props(dic: ObjectVal) -> ObjectVal:
    return ObjectVal(
        val={k: v for (k, v) in dic.val.items()
             if isinstance(k, LinkPropLabel)})


def conversion_error():
    class ConversionError(Exception):
        pass
    raise ConversionError


def obj_to_link_prop_obj(dic: ObjectVal) -> ObjectVal:
    return ObjectVal(
        val={(LinkPropLabel(k.label)
              if isinstance(k, StrLabel) else conversion_error()): v
             for (k, v) in dic.val.items()})


def link_prop_obj_to_obj(dic: ObjectVal) -> ObjectVal:
    return ObjectVal(
        val={(StrLabel(k.label)
              if
              isinstance(k, LinkPropLabel) else
              conversion_error()): v
             for (k, v) in dic.val.items()})


def combine_object_val(o1: ObjectVal, o2: ObjectVal) -> ObjectVal:
    return ObjectVal({**o1.val, **o2.val})


def combine_object_tp(o1: ObjectTp, o2: ObjectTp) -> ObjectTp:
    return ObjectTp({**o1.val, **o2.val})


# def object_to_shape(expr: ObjectExpr) -> ShapeExpr:
#     return ShapeExpr(
#         shape={lbl: abstract_over_expr(e) for (lbl, e) in expr.val.items()})


def make_storage_atomic(val: Val, tp: Tp) -> Val:
    def do_coerce_value_to_linkprop_tp(tp_linkprop: ObjectTp) -> Val:
        match val:
            case RefVal(id, obj):
                obj_link_prop = remove_unless_link_props(obj)
                temp_obj = link_prop_obj_to_obj(obj_link_prop)
                after_obj = coerce_to_storage(temp_obj, tp_linkprop)
                return RefVal(id, ObjectVal({LinkPropLabel(k):(Visible(), v) for (k,v) in after_obj.items()}))
            # case LinkPropVal(refid=id,
            #                  linkprop=linkprop):
            #     after_obj = coerce_to_storage(linkprop, tp_linkprop)
            #     return LinkPropVal(id, ObjectVal({StrLabel(k):(Visible(), v) for (k,v) in after_obj.items()}))
            case _:
                raise ValueError("Cannot Coerce to LinkPropType", val)
    match tp:
        case e.NamedNominalLinkTp(name=_, linkprop=tp_linkprop):
            return do_coerce_value_to_linkprop_tp(tp_linkprop=tp_linkprop)
        case e.NominalLinkTp(name=_, subject=_, linkprop=tp_linkprop):
            return do_coerce_value_to_linkprop_tp(tp_linkprop=tp_linkprop)
        # case e.VarTp():
        #     return do_coerce_value_to_linkprop_tp(tp_linkprop=ObjectTp({}))
        case (e.IntTp() | e.StrTp()):
            return val
        case e.DefaultTp(expr=_, tp=d_tp):
            return make_storage_atomic(val, d_tp)
        case _:
            raise ValueError("Coercion Not Implemented for", tp)


# we require fmt to be a storage tp -- No Computable Types should be present
def coerce_to_storage(val: ObjectVal, fmt: ObjectTp) -> Dict[str, MultiSetVal]:
    # ensure no redundant keys
    extra_keys = [k for k in val.val.keys()
                  if k not in [StrLabel(k) for k in fmt.val.keys()]]
    if extra_keys:
        raise ValueError(
            "Coercion failed, object contains redundant keys:", extra_keys,
            "val_keys are", val.val.keys(),
            "fmt_keys are", fmt.val.keys(),
            "when coercing ", pp.show_val(val), " to ", pp.show_tp(fmt))
    left_out_keys = [k for k in fmt.val.keys()
                  if StrLabel(k) not in val.val.keys()]
    if left_out_keys:
        raise ValueError(
            "Coercion failed, object missing keys:", left_out_keys,
            "when coercing ", val, " to ", fmt)
    return {
        k: (MultiSetVal(
                        [make_storage_atomic(v, tp[0])
                         for v in val.val[StrLabel(k)][1].vals])
                       if StrLabel(k) in val.val.keys()
                       else MultiSetVal([]))
        for (k, tp) in fmt.val.items()
        # if not isinstance(tp.tp, e.ComputableTp)
    }



def object_dedup(val: Sequence[Val]) -> Sequence[Val]:
    temp: Dict[int, RefVal] = {}
    for v in val:
        match v:
            case RefVal(refid=id, val=_):
                if id in temp:
                    temp[id] = RefVal(refid=id, val=combine_object_val(temp[id].val, v.val))
                else:
                    temp[id] = v
            # case FreeVal(_):
            #     # Should link dedup apply to free objects?
            #     temp[next_id()] = v
            case _:
                raise ValueError("must pass in objects")
    return list(temp.values())


# def get_link_target(val: Val) -> Val:
#     match val:
#         case LinkPropVal(refid=id, linkprop=_):
#             return RefVal(refid=id, val=ObjectVal({}))
#         case _:
#             raise ValueError("Not LinkPropVal")


# def assume_link_target(val: MultiSetVal) -> MultiSetVal:
#     targets = [get_link_target(v) if isinstance(
#         v, LinkPropVal) else v for v in val.vals]
#     if all(val_is_object(t) for t in targets):
#         return MultiSetVal(object_dedup(targets))
#     elif all(val_is_primitive(t) for t in targets):
#         return MultiSetVal(targets)
#     else:
#         raise ValueError("link targets not uniform", val)


# def map_assume_link_target(
#         sv: Sequence[MultiSetVal]) -> Sequence[MultiSetVal]:
#     return [assume_link_target(v) for v in sv]


# def map_assume_link_target(
#         sv: Sequence[Sequence[Val]]) -> Sequence[Sequence[Val]]:
#     return [assume_link_target(v) for v in sv]


def map_expand_multiset_val(
        sv: Sequence[MultiSetVal]) -> Sequence[Sequence[Val]]:
    return [v.vals for v in sv]


def val_is_link_convertible(val: Val) -> bool:
    match val:
        case RefVal(refid=_, val=obj):
            return all(
                [isinstance(label, LinkPropLabel)
                 for label in obj.val.keys()])
        case _:
            return False


# def convert_to_link(val: Val) -> LinkPropVal:
#     assert val_is_link_convertible(val)
#     match val:
#         case RefVal(refid=id, val=obj):
#             return LinkPropVal(refid=id,
#                 linkprop=obj)
#         case _:
#             raise ValueError("Val is not link convertible, check va")


def is_path(e: Expr) -> bool:
    match e:
        case FreeVarExpr(_):
            return True
        case LinkPropProjExpr(subject=subject, linkprop=_):
            return is_path(subject)
        case ObjectProjExpr(subject=subject, label=_):
            return is_path(subject)
        case BackLinkExpr(subject=subject, label=_):
            return is_path(subject)
        case TpIntersectExpr(subject=BackLinkExpr(subject=subject, label=_), tp=_):
            return is_path(subject)
        case _:
            return False

def get_path_head(e: Expr) -> e.FreeVarExpr:
    match e:
        case FreeVarExpr(_):
            return e
        case LinkPropProjExpr(subject=subject, linkprop=_):
            return get_path_head(subject)
        case ObjectProjExpr(subject=subject, label=_):
            return get_path_head(subject)
        case BackLinkExpr(subject=subject, label=_):
            return get_path_head(subject)
        case TpIntersectExpr(subject=BackLinkExpr(subject=subject, label=_), tp=_):
            return get_path_head(subject)
        case _:
            raise ValueError("not a path")


def get_first_path_component(e: Expr) -> e.Optional[e.Expr]:
    match e:
        case FreeVarExpr(_):
            return None
        case LinkPropProjExpr(subject=FreeVarExpr(_), linkprop=_):
            return e
        case ObjectProjExpr(subject=FreeVarExpr(_), label=_):
            return e
        case BackLinkExpr(subject=FreeVarExpr(_), label=_):
            return e
        case TpIntersectExpr(subject=BackLinkExpr(subject=FreeVarExpr(_), label=_), tp=_):
            return e
        case _:
            raise ValueError("not a path")



def tcctx_add_binding(ctx: e.TcCtx,
                      bnd_e: BindingExpr,
                      binder_tp: e.ResultTp) -> Tuple[e.TcCtx, Expr, str]:
    bnd_e = ensure_no_capture(list(get_free_vars(bnd_e))
                              + list(ctx.varctx.keys()), bnd_e)
    new_ctx = e.TcCtx(ctx.schema, ctx.current_module, {**ctx.varctx, bnd_e.var: binder_tp})
    after_e = instantiate_expr(e.FreeVarExpr(bnd_e.var), bnd_e)
    return new_ctx, after_e, bnd_e.var


def emtpy_tcctx_from_dbschema(dbschema: e.DBSchema, current_module_name: Tuple[str, ...]) -> e.TcCtx:
    return e.TcCtx(
        schema=dbschema,
        current_module=current_module_name,
        varctx={})


def is_effect_free(expr: Expr) -> bool:
    def pred(expr: Expr) -> bool:
        if (isinstance(expr, e.InsertExpr) or
                isinstance(expr, e.UpdateExpr) or
                isinstance(expr, e.DeleteExpr)):
            return True
        else:
            return False
    return not appears_in_expr_pred(pred, expr)
