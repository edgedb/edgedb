
from ..data import data_ops as e
from ..data import expr_ops as eops
from ..data import type_ops as tops
from ..data import path_factor as pops
from typing import List, Tuple, Dict


def get_key_dependency(tp : e.DefaultTp) -> List[str]:
    new_head_name = eops.next_name("head_name_test")
    instantiaed = eops.instantiate_expr(e.FreeVarExpr(new_head_name), tp.expr)
    paths = pops.get_all_paths(instantiaed)
    deps = []
    for p in paths:
        if eops.get_path_head(p) == new_head_name:
            first_path_comp = eops.get_first_path_component(p)
            match first_path_comp:
                case e.FreeVarExpr(_):
                    raise ValueError("Path head is FreeVar, cannot do insert")
                case e.ObjectProjExpr(subject=_, label=lbl):
                    deps.append(lbl)
                case e.LinkPropProjExpr(_, _):
                    raise ValueError("default expr should not do link prop proj")
                case e.BackLinkExpr(_) | e.TpIntersectExpr(_):
                    continue # this is fine
    return deps
                

def insert_checking(ctx: e.TcCtx, expr: e.InsertExpr) -> e.Expr:
    # for breaking circular dependency
    from .typechecking import synthesize_type, check_type

    schema_tp = ctx.schema.val[expr.name]
    new_v: Dict[str, e.Expr] = {}
    for (k, v) in expr.new.items():
        if k not in schema_tp.val:
            raise ValueError(f"Key {k} not in schema for {expr.name}")
        target_tp = schema_tp.val[k]
        if isinstance(target_tp, e.ComputableTp):
            raise ValueError(f"Key {k} is computable in {expr.name}, modification of computable types prohibited")
        vv = check_type(ctx, v, e.ResultTp(tops.get_runtime_tp(schema_tp.val[k].tp),
                                           schema_tp.val[k].mode))
        new_v = {**new_v, k: vv}

    # add optional fields that do not have a default
    for (k, target_tp) in schema_tp.val.items():
        if (tops.mode_is_optional(target_tp.mode)
            and not isinstance(target_tp, e.DefaultTp)):
            new_v = {**new_v, k: e.MultiSetExpr(expr=[])}

    # check non-optional fields
    missing_keys = []
    for (k, target_tp) in schema_tp.val.items():
        if isinstance(target_tp, e.ComputableTp):
            continue
        if isinstance(target_tp, e.DefaultTp):
            continue
        if k not in new_v:
            missing_keys.append(k)
    if len(missing_keys) > 0:
        raise ValueError(f"Missing keys {missing_keys} in insert for {expr.name}")

    
    dependent_keys : Dict[str, str] = {} # a list of keys that are dependent, need to extract them in this order, second element provides the binder name
    def add_deps_from_new_v(deps: List[str]) -> None:
        for k in deps:
            if k in new_v and k not in dependent_keys:
                dependent_keys[k] = eops.next_name(f"insert_{expr.name}_{k}")
    def get_shaped_from_deps(deps: List[str]) -> e.ShapedExprExpr:
        return e.ShapedExprExpr(
                expr=e.FreeObjectExpr(),
                shape=e.ShapeExpr(
                    shape={e.StrLabel(k): eops.abstract_over_expr(e.FreeVarExpr(dependent_keys[k])) for k in deps}
                ))

    pending_default: Dict[str, List[str]] = {} # key and its dependent keys
    # topologically sort the default insertions.
    for (k, target_tp) in schema_tp.val.items():
        if isinstance(target_tp, e.DefaultTp):
            deps = get_key_dependency(target_tp)
            if len(deps) == 0:
                actual_v = eops.instantiate_expr(target_tp.expr, e.FreeVarExpr("INSERT_SHOULD_NOT_OCCUR"))
                new_v = {**new_v, k: actual_v}
            else:
                # add to dependent_keys those in new_v
                add_deps_from_new_v(deps)
                # if all dependencies are currently in add new_v
                if all(k in dependent_keys.keys() for k in deps):
                    actual_v = e.WithExpr(
                        get_shaped_from_deps(deps),
                        target_tp.expr)
                    new_v = {**new_v, k: actual_v}
                else:
                    assert k not in pending_default, "only iterating over schema once, no duplicate keys"
                    pending_default[k] = deps

    # process pending keys until all are resolved
    while len(pending_default) > 0:
        # find a key that has all dependencies resolved
        for (k, deps) in pending_default.items():
            add_deps_from_new_v(deps)
            if all(k in dependent_keys.keys() for k in deps):
                target_tp = schema_tp.val[k]
                assert isinstance(target_tp, e.DefaultTp)
                actual_v = e.WithExpr(
                    get_shaped_from_deps(deps),
                    target_tp.expr)
                new_v = {**new_v, k: actual_v}
                del pending_default[k]
                break
        else:
            raise ValueError(f"Cannot resolve default value for because of circular dependencies: {pending_default}")


    # now abstract over the dependent keys in order
    result_expr : e.Expr = e.InsertExpr(
        name=expr.name,
        new={k: v  
             for (k, v) in new_v.items()
             if k not in dependent_keys})
    for (k, binder_name) in reversed(dependent_keys.items()):
        result_expr = e.WithExpr(
            new_v[k], 
            eops.abstract_over_expr(result_expr, binder_name)
        )
    return result_expr