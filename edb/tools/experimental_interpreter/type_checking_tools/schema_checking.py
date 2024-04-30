from typing import Tuple, Dict

from ..data import data_ops as e
from ..data import expr_ops as eops
from ..data import type_ops as tops
from ..data import module_ops as mops
from ..data import path_factor as path_factor
from ..data import expr_to_str as pp
from . import name_resolution as name_res
from .typechecking import check_type_valid, synthesize_type, check_type
from . import module_check_tools as mck
from . import inheritance_populate as inheritance_populate


def check_object_tp_comp_validity(
    root_ctx: e.TcCtx, subject_tp: e.Tp, tp_comp: e.Tp, tp_comp_card: e.CMMode
) -> e.Tp:
    match tp_comp:
        case e.UncheckedTypeName(name):
            return check_type_valid(root_ctx, tp_comp)
        case e.NamedNominalLinkTp(name=name, linkprop=l_prop):
            if isinstance(name, e.UnqualifiedName):
                name_ck = mops.resolve_simple_name(root_ctx, name)
            else:
                name_ck = name
            resolved_tp = mops.try_resolve_type_name(root_ctx, name_ck)
            if not isinstance(resolved_tp, e.ObjectTp):
                raise ValueError(
                    "Scalar type cannot carry link props", tp_comp
                )
            return e.NamedNominalLinkTp(
                name=name_ck,
                linkprop=check_object_tp_validity(
                    root_ctx=root_ctx,
                    subject_tp=tops.get_runtime_tp(tp_comp),
                    obj_tp=l_prop,
                ),
            )
        case e.NominalLinkTp(subject=l_sub, name=name, linkprop=l_prop):
            return e.NominalLinkTp(
                subject=l_sub,
                name=name,
                linkprop=check_object_tp_validity(
                    root_ctx=root_ctx,
                    subject_tp=tops.get_runtime_tp(tp_comp),
                    obj_tp=l_prop,
                ),
            )
        case e.UncheckedComputableTp(expr=c_expr):
            if not isinstance(c_expr, e.BindingExpr):  # type: ignore
                raise ValueError(
                    "Computable type must be a binding expression"
                )
            new_ctx, c_body, bnd_var = eops.tcctx_add_binding(
                root_ctx,
                c_expr,  # type: ignore
                e.ResultTp(subject_tp, e.CardOne),
            )
            c_body = path_factor.select_hoist(c_body, new_ctx)
            synth_tp, c_body_ck = synthesize_type(new_ctx, c_body)
            tops.assert_cardinal_subtype(synth_tp.mode, tp_comp_card)
            return e.ComputableTp(
                expr=eops.abstract_over_expr(c_body_ck, bnd_var),
                tp=synth_tp.tp,
            )
        case e.ComputableTp(expr=c_expr, tp=c_tp):
            if not isinstance(c_expr, e.BindingExpr):  # type: ignore
                raise ValueError(
                    "Computable type must be a binding expression"
                )
            new_ctx, c_body, bnd_var = eops.tcctx_add_binding(
                root_ctx,
                c_expr,  # type: ignore
                e.ResultTp(subject_tp, e.CardOne),
            )
            c_body = path_factor.select_hoist(c_body, new_ctx)
            synth_tp, c_body_ck = synthesize_type(new_ctx, c_body)
            tops.assert_cardinal_subtype(synth_tp.mode, tp_comp_card)
            tops.assert_real_subtype(new_ctx, synth_tp.tp, c_tp)
            return e.ComputableTp(
                expr=eops.abstract_over_expr(c_body_ck, bnd_var), tp=c_tp
            )
        # This code is mostly copied from the above
        # TODO: Can we not copy?
        case e.DefaultTp(expr=c_expr, tp=c_tp):
            if not isinstance(c_expr, e.BindingExpr):  # type: ignore
                raise ValueError(
                    "Computable type must be a binding expression"
                )
            c_tp_ck = c_tp
            new_ctx, c_body, bnd_var = eops.tcctx_add_binding(
                root_ctx,
                c_expr,  # type: ignore
                e.ResultTp(subject_tp, e.CardOne),
            )
            c_body = path_factor.select_hoist(c_body, new_ctx)
            synth_tp, c_body_ck = synthesize_type(new_ctx, c_body)
            tops.assert_cardinal_subtype(synth_tp.mode, tp_comp_card)
            tops.assert_real_subtype(new_ctx, synth_tp.tp, c_tp_ck)
            return e.DefaultTp(
                expr=eops.abstract_over_expr(c_body_ck, bnd_var), tp=c_tp_ck
            )
        case e.ScalarTp(_):
            return tp_comp
        case e.UnionTp(l, r):
            return e.UnionTp(
                check_object_tp_comp_validity(
                    root_ctx, subject_tp, l, tp_comp_card
                ),
                check_object_tp_comp_validity(
                    root_ctx, subject_tp, r, tp_comp_card
                ),
            )
        case e.CompositeTp(kind=kind, tps=tps, labels=labels):
            return e.CompositeTp(
                kind=kind,
                tps=[
                    check_object_tp_comp_validity(
                        root_ctx, subject_tp, t_comp_tp, tp_comp_card
                    )
                    for t_comp_tp in tps
                ],
                labels=labels,
            )
        case e.OverloadedTargetTp(_):
            raise ValueError(
                "Overloaded target tp should not appear in type checking, "
                "check whether the inheritance processing is intact",
                tp_comp,
            )
        case _:
            raise ValueError("Not Implemented", pp.show(tp_comp))


def check_object_tp_validity(
    root_ctx: e.TcCtx, subject_tp: e.Tp, obj_tp: e.ObjectTp
) -> e.ObjectTp:
    result_vals: Dict[str, e.ResultTp] = {}
    for lbl, (t_comp_tp, t_comp_card) in obj_tp.val.items():
        result_vals[lbl] = e.ResultTp(
            check_object_tp_comp_validity(
                root_ctx=root_ctx,
                subject_tp=subject_tp,
                tp_comp=t_comp_tp,
                tp_comp_card=t_comp_card,
            ),
            t_comp_card,
        )
    return e.ObjectTp(result_vals)


def param_modifier_to_paramter_cardinality(mod: e.ParamModifier) -> e.CMMode:
    match mod:
        case e.ParamSingleton():
            return e.CardOne
        case e.ParamSetOf():
            return e.CardAny
        case e.ParamOptional():
            return e.CardAtMostOne
        case _:
            raise ValueError("Not Implemented", mod)


def check_fun_def_validity(ctx: e.TcCtx, fun_def: e.FuncDef) -> e.FuncDef:
    match fun_def:
        case e.DefinedFuncDef(tp=tp, impl=impl, defaults=defaults):
            binders = []
            for i, arg_tp in enumerate(tp.args_tp):
                assert isinstance(impl, e.BindingExpr)
                arg_mod = param_modifier_to_paramter_cardinality(
                    tp.args_mod[i]
                )
                ctx, impl, binder_name = eops.tcctx_add_binding(
                    ctx, impl, e.ResultTp(arg_tp, arg_mod)
                )
                binders.append(binder_name)
            impl_ck = check_type(ctx, impl, tp.ret_tp)
            for binder in binders[::-1]:
                impl_ck = eops.abstract_over_expr(impl_ck, binder)
            return e.DefinedFuncDef(
                tp=tp,
                impl=impl_ck,
                defaults={
                    k: synthesize_type(ctx, v)[1] for k, v in defaults.items()
                },
            )
        case e.BuiltinFuncDef(tp=tp, impl=impl, defaults=defaults):
            # do not check validity for builtin funcs
            return e.BuiltinFuncDef(tp=tp, impl=impl, defaults=defaults)
        case _:
            raise ValueError("Not Implemented", fun_def)


def check_module_validity(
    dbschema: e.DBSchema, module_name: Tuple[str, ...]
) -> e.DBSchema:
    """
    Checks the validity of an unchecked module in dbschema.
    Modifies the db schema after checking
    """
    name_res.module_name_resolve(dbschema, module_name)
    inheritance_populate.module_subtyping_resolve(dbschema)
    inheritance_populate.module_inheritance_populate(dbschema, module_name)
    mck.unchecked_module_map(
        dbschema,
        module_name,
        check_object_tp_comp_validity,
        check_fun_def_validity,
    )
    dbschema.modules[module_name] = dbschema.unchecked_modules[module_name]
    del dbschema.unchecked_modules[module_name]
    return dbschema


def re_populate_module_inheritance(
    dbschema: e.DBSchema, module_name: Tuple[str, ...]
) -> None:
    """
    Checks the validity of an unchecked module in dbschema.
    Modifies the db schema after checking
    """
    dbschema.unchecked_modules[module_name] = dbschema.modules[module_name]
    del dbschema.modules[module_name]
    inheritance_populate.module_subtyping_resolve(dbschema)
    inheritance_populate.module_inheritance_populate(dbschema, module_name)
    dbschema.modules[module_name] = dbschema.unchecked_modules[module_name]
    del dbschema.unchecked_modules[module_name]
