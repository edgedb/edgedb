
from functools import reduce
import operator
from typing import Tuple, Dict, Sequence, Optional, List

from ..data import data_ops as e
from ..data import expr_ops as eops
from ..data import type_ops as tops
from edb.common import debug
from ..data import path_factor as path_factor
from .dml_checking import *
from ..data import expr_to_str as pp
from .function_checking import *
from . import name_resolution as name_res
from .typechecking import *
from . import module_check_tools as mck
from . import inheritance_populate as inheritance_populate

def check_object_tp_comp_validity(
        root_ctx: e.TcCtx,
        subject_tp: e.Tp,
        tp_comp: e.Tp,
        tp_comp_card: e.CMMode) -> e.Tp:
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
                raise ValueError("Scalar type cannot carry link props", tp_comp)
            # name_ck = check_type_valid(root_ctx, name)
            return e.NamedNominalLinkTp(
                    name=name_ck,
                    linkprop=check_object_tp_validity(
                        root_ctx=root_ctx,
                        subject_tp=tops.get_runtime_tp(tp_comp),
                        obj_tp=l_prop))
        case e.NominalLinkTp(subject=l_sub, name=name, linkprop=l_prop):
            return e.NominalLinkTp(
                    subject=l_sub,
                    name=name,
                    linkprop=check_object_tp_validity(
                        root_ctx=root_ctx,
                        subject_tp=tops.get_runtime_tp(tp_comp),
                        obj_tp=l_prop))
        case e.UncheckedComputableTp(expr=c_expr):
            if not isinstance(c_expr, e.BindingExpr):  # type: ignore
                raise ValueError(
                    "Computable type must be a binding expression")
            new_ctx, c_body, bnd_var = eops.tcctx_add_binding(
                root_ctx,
                c_expr,  # type: ignore
                e.ResultTp(subject_tp, e.CardOne)
            )
            c_body = path_factor.select_hoist(c_body, new_ctx)
            synth_tp, c_body_ck = synthesize_type(new_ctx, c_body)
            tops.assert_cardinal_subtype(synth_tp.mode, tp_comp_card)
            return e.ComputableTp(
                expr=eops.abstract_over_expr(c_body_ck, bnd_var),
                tp=synth_tp.tp)
        case e.ComputableTp(expr=c_expr, tp=c_tp):
            if not isinstance(c_expr, e.BindingExpr):  # type: ignore
                raise ValueError(
                    "Computable type must be a binding expression")
            new_ctx, c_body, bnd_var = eops.tcctx_add_binding(
                root_ctx,
                c_expr,  # type: ignore
                e.ResultTp(subject_tp, e.CardOne)
            )
            c_body = path_factor.select_hoist(c_body, new_ctx)
            synth_tp, c_body_ck = synthesize_type(new_ctx, c_body)
            tops.assert_cardinal_subtype(synth_tp.mode, tp_comp_card)
            tops.assert_real_subtype(new_ctx, synth_tp.tp, c_tp)
            return e.ComputableTp(
                expr=eops.abstract_over_expr(c_body_ck, bnd_var),
                tp=c_tp)
        # This code is mostly copied from the above
        # TODO: Can we not copy?
        case e.DefaultTp(expr=c_expr, tp=c_tp):
            if not isinstance(c_expr, e.BindingExpr):  # type: ignore
                raise ValueError(
                    "Computable type must be a binding expression")
            c_tp_ck = c_tp
            new_ctx, c_body, bnd_var = eops.tcctx_add_binding(
                root_ctx,
                c_expr,  # type: ignore
                e.ResultTp(subject_tp, e.CardOne)
            )
            c_body = path_factor.select_hoist(c_body, new_ctx)
            synth_tp, c_body_ck = synthesize_type(new_ctx, c_body)
            tops.assert_cardinal_subtype(synth_tp.mode, tp_comp_card)
            # match c_tp:
                # case e.LinkPropTp(subject=c_tp_subject, linkprop=_):
                #     tops.assert_real_subtype(new_ctx, synth_tp.tp,
                #                              c_tp_subject)
                # case _:
            tops.assert_real_subtype(new_ctx, synth_tp.tp, c_tp_ck)
            return e.DefaultTp(
                expr=eops.abstract_over_expr(c_body_ck, bnd_var),
                tp=c_tp_ck)
        case e.ScalarTp(_):
            return tp_comp
        case e.UnionTp(l, r):
            return e.UnionTp(
                check_object_tp_comp_validity(root_ctx, subject_tp, l, tp_comp_card),
                check_object_tp_comp_validity(root_ctx, subject_tp, r, tp_comp_card))
        case e.CompositeTp(kind=kind, tps=tps, labels=labels):
            return e.CompositeTp(
                kind=kind,
                tps=[check_object_tp_comp_validity(root_ctx, subject_tp, t_comp_tp, tp_comp_card)
                        for  t_comp_tp in tps],
                labels=labels)
        case e.OverloadedTargetTp(_):
            raise ValueError("Overloaded target tp should not appear in type checking, check whether the inheritance processing is intact", tp_comp)
        case _:
            raise ValueError("Not Implemented", pp.show(tp_comp))


def check_object_tp_validity(root_ctx: e.TcCtx,
                             subject_tp: e.Tp,
                             obj_tp: e.ObjectTp) -> e.ObjectTp:
    result_vals: Dict[str, e.ResultTp] = {}
    for lbl, (t_comp_tp, t_comp_card) in obj_tp.val.items():
        result_vals[lbl] = e.ResultTp(
            check_object_tp_comp_validity(
                root_ctx=root_ctx,
                subject_tp=subject_tp,
                tp_comp=t_comp_tp,
                tp_comp_card=t_comp_card), t_comp_card)
    return e.ObjectTp(result_vals)


def check_module_validity(dbschema: e.DBSchema, module_name : Tuple[str, ...]) -> e.DBSchema:
    """
    Checks the validity of an unchecked module in dbschema. 
    Modifies the db schema after checking
    """
    name_res.module_name_resolve(dbschema, module_name)
    inheritance_populate.module_subtyping_resolve(dbschema)
    inheritance_populate.module_inheritance_populate(dbschema, module_name)
    # dbmodule = dbschema.unchecked_modules[module_name]
    mck.unchecked_module_map(dbschema, module_name, check_object_tp_comp_validity)
    # for t_name, t_me in dbmodule.defs.items():
    #     match t_me:
    #         case e.ModuleEntityTypeDef(typedef=typedef, is_abstract=is_abstract):
    #             if isinstance(typedef, e.ObjectTp):
    #                 result_vals = {
    #                     **result_vals, 
    #                     t_name: e.ModuleEntityTypeDef(typedef=
    #                         check_object_tp_validity(
    #                             dbschema, 
    #                             module_name,
    #                             e.NamedNominalLinkTp(name=e.QualifiedName([*module_name,t_name]), linkprop=e.ObjectTp({})),
    #                             typedef), is_abstract=is_abstract)}
    #             else:
    #                 assert isinstance(typedef, e.ScalarTp)
    #                 result_vals = {**result_vals, t_name: t_me}
    #         case _:
    #             raise ValueError("Unimplemented", t_me)
    dbschema.modules[module_name] = dbschema.unchecked_modules[module_name]
    del dbschema.unchecked_modules[module_name]
    return dbschema



