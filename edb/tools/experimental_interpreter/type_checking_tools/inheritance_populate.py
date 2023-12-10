
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


def merge_result_tp(ctx: e.TcCtx,
                    l: e.ResultTp,
                    r: e.ResultTp) -> e.ResultTp:
    if l.card != r.card:
        raise ValueError("Cardinality mismatch", l, r)
    match l.tp, r.tp:
        case e.NamedNominalLinkTp(name=l_name, linkprop=l_linkprop), e.NamedNominalLinkTp(name=r_name, linkprop=r_linkprop):
            if l_name != r_name:
                raise ValueError("Named nominal link tp name mismatch", l, r)
            new_link_prop : Dict[str, e.ResultTp] = {}
            for lbl, (l_comp_tp, l_comp_card) in l_linkprop.val.items():
                new_link_prop[lbl] = e.ResultTp(l_comp_tp, l_comp_card)
            for lbl, (r_comp_tp, r_comp_card) in r_linkprop.val.items():
                if lbl not in new_link_prop:
                    new_link_prop[lbl] = e.ResultTp(r_comp_tp, r_comp_card)
                else:
                    new_link_prop[lbl] = merge_result_tp(ctx, new_link_prop[lbl], e.ResultTp(r_comp_tp, r_comp_card))
            return e.ResultTp(e.NamedNominalLinkTp(name=l_name, linkprop=e.ObjectTp(new_link_prop)), l.card)
        case _:
            if l.tp != r.tp:
                raise ValueError("Type mismatch", l, r)
            return l




def copy_construct_inheritance(ctx: e.TcCtx, 
                               typedef: e.ObjectTp, 
                               super_types: List[e.QualifiedName]) -> e.ObjectTp:
    
    definitions = [mops.resolve_type_name(ctx, super_type) for super_type in super_types]
    final_tp_dict : Dict[str, e.ResultTp] = {}
    for i, definition in enumerate(definitions):
        assert isinstance(definition, e.ObjectTp)
        def_dep = ctx.schema.subtyping_relations[super_types[i]]
        definition_ck = copy_construct_inheritance(ctx, definition, def_dep)

        for lbl, (t_comp_tp, t_comp_card) in definition_ck.val.items():
            if lbl not in final_tp_dict:
                final_tp_dict[lbl] = e.ResultTp(
                    t_comp_tp,
                    t_comp_card)
            else:
                final_tp_dict[lbl] = merge_result_tp(ctx, final_tp_dict[lbl], e.ResultTp(t_comp_tp, t_comp_card))
        
    for lbl, (t_comp_tp, t_comp_card) in typedef.val.items():
        if lbl not in final_tp_dict:
            final_tp_dict[lbl] = e.ResultTp(
                    t_comp_tp,
                    t_comp_card)
        else:
            final_tp_dict[lbl] = merge_result_tp(ctx, final_tp_dict[lbl], e.ResultTp(t_comp_tp, t_comp_card))
    return e.ObjectTp(final_tp_dict)

    


def module_inheritance_populate(dbschema: e.DBSchema, module_name : Tuple[str, ...]) -> None:
    """
    Modifies the db schema after checking
    """
    result_vals: Dict[str, e.ModuleEntity] = {}
    dbmodule = dbschema.unchecked_modules[module_name]
    for t_name, t_me in dbmodule.defs.items():
        root_ctx = eops.emtpy_tcctx_from_dbschema(dbschema, module_name)
        match t_me:
            case e.ModuleEntityTypeDef(typedef=typedef, is_abstract=is_abstract):
                if isinstance(typedef, e.ObjectTp):
                    if e.QualifiedName([*module_name, t_name]) in dbschema.subtyping_relations:
                        result_vals = {**result_vals, t_name: e.ModuleEntityTypeDef(typedef=
                            copy_construct_inheritance(
                                root_ctx,
                                typedef,
                                dbschema.subtyping_relations[e.QualifiedName([*module_name, t_name])]), is_abstract=is_abstract)}
                    else:
                        result_vals = {**result_vals, t_name: t_me}
                else:
                    assert isinstance(typedef, e.ScalarTp)
                    result_vals = {**result_vals, t_name: t_me}
            case _:
                raise ValueError("Unimplemented", t_me)
    dbschema.unchecked_modules[module_name] = e.DBModule(result_vals)


