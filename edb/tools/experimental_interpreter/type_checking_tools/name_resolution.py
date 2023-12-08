
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
from . import typechecking as tck

def object_tp_comp_name_resolve(
        root_ctx: e.TcCtx,
        tp_comp: e.Tp,
        ) -> e.Tp:
    match tp_comp:
        case e.UncheckedTypeName(name):
            return tck.check_type_valid(root_ctx, tp_comp)
        case e.NamedNominalLinkTp(name=name, linkprop=l_prop):
            if isinstance(name, e.UnqualifiedName):
                name_ck = mops.resolve_simple_name(root_ctx, name)
            else:
                name_ck = name
            resolved_tp = mops.try_resolve_type_name(root_ctx, name_ck)
            if not isinstance(resolved_tp, e.ObjectTp):
                raise ValueError("Scalar type cannot carry link props", tp_comp)

            linkprop_ck: Dict[str, e.ResultTp] = {}
            for lbl, (t_comp_tp, t_comp_card) in l_prop.val.items():
                linkprop_ck[lbl] = e.ResultTp(
                    object_tp_comp_name_resolve(
                        root_ctx=root_ctx,
                        tp_comp=t_comp_tp,
                        ), t_comp_card)
            
            return e.NamedNominalLinkTp(
                    name=name_ck,
                    linkprop=e.ObjectTp(linkprop_ck))
        case e.NominalLinkTp(subject=l_sub, name=name, linkprop=l_prop):
            raise ValueError("No nominal link tp should appear in name resolution", tp_comp)
        case e.UncheckedComputableTp(expr=c_expr):
            return tp_comp
        case e.ComputableTp(expr=c_expr, tp=c_tp):
            return e.CompositeTp(expr=c_expr, tp=object_tp_comp_name_resolve(root_ctx,c_tp))
        case e.DefaultTp(expr=c_expr, tp=c_tp):
            return e.DefaultTp(expr=c_expr, tp=object_tp_comp_name_resolve(root_ctx,c_tp))
        case _:
            raise ValueError("Not Implemented", tp_comp)


def object_tp_name_resolve(dbschema: e.DBSchema,
                             current_module_name: Tuple[str, ...],
                             subject_tp: e.Tp,
                             obj_tp: e.ObjectTp) -> e.ObjectTp:
    result_vals: Dict[str, e.ResultTp] = {}
    root_ctx = eops.emtpy_tcctx_from_dbschema(dbschema, current_module_name)
    for lbl, (t_comp_tp, t_comp_card) in obj_tp.val.items():
        result_vals[lbl] = e.ResultTp(
            object_tp_comp_name_resolve(
                root_ctx=root_ctx,
                tp_comp=t_comp_tp,
                ), t_comp_card)
    return e.ObjectTp(result_vals)


def module_name_resolve(dbschema: e.DBSchema, module_name : Tuple[str, ...]) -> None:
    """
    Modifies the db schema after checking
    """
    result_vals: Dict[str, e.ModuleEntity] = {}
    dbmodule = dbschema.unchecked_modules[module_name]
    for t_name, t_me in dbmodule.defs.items():
        match t_me:
            case e.ModuleEntityTypeDef(typedef=typedef, is_abstract=is_abstract):
                assert isinstance(typedef, e.ObjectTp), "Scalar type definitions not supported"
                result_vals = {
                    **result_vals, 
                    t_name: e.ModuleEntityTypeDef(typedef=
                        object_tp_name_resolve(
                            dbschema, 
                            module_name,
                            e.NamedNominalLinkTp(name=e.QualifiedName([*module_name,t_name]), linkprop=e.ObjectTp({})),
                            typedef), is_abstract=is_abstract)}
            case _:
                raise ValueError("Unimplemented", t_me)
    dbschema.unchecked_modules[module_name] = e.DBModule(result_vals)


