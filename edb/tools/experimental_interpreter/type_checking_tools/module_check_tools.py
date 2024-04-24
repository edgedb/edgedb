from typing import Tuple, Dict, Callable

from ..data import data_ops as e
from ..data import expr_ops as eops
from ..data import path_factor as path_factor


def unchecked_module_map(
    dbschema: e.DBSchema,
    module_name: Tuple[str, ...],
    f: Callable[[e.TcCtx, e.Tp, e.Tp, e.CMMode], e.Tp],
    g: Callable[[e.TcCtx, e.FuncDef], e.FuncDef],
) -> None:
    """
    Modifies the db schema after checking
    """
    root_ctx = eops.emtpy_tcctx_from_dbschema(dbschema, module_name)

    def unchecked_object_tp_map(
        subject_tp: e.Tp, obj_tp: e.ObjectTp
    ) -> e.ObjectTp:
        result_vals: Dict[str, e.ResultTp] = {}
        for lbl, (t_comp_tp, t_comp_card) in obj_tp.val.items():
            result_vals[lbl] = e.ResultTp(
                f(root_ctx, subject_tp, t_comp_tp, t_comp_card), t_comp_card
            )
        return e.ObjectTp(result_vals)

    result_vals: Dict[str, e.ModuleEntity] = {}
    dbmodule = dbschema.unchecked_modules[module_name]
    for t_name, t_me in dbmodule.defs.items():
        match t_me:
            case e.ModuleEntityTypeDef(
                typedef=typedef,
                is_abstract=is_abstract,
                constraints=constraints,
                indexes=indexes,
            ):
                if isinstance(typedef, e.ObjectTp):
                    result_vals = {
                        **result_vals,
                        t_name: e.ModuleEntityTypeDef(
                            typedef=unchecked_object_tp_map(
                                e.NamedNominalLinkTp(
                                    name=e.QualifiedName(
                                        [*module_name, t_name]
                                    ),
                                    linkprop=e.ObjectTp({}),
                                ),
                                typedef,
                            ),
                            is_abstract=is_abstract,
                            constraints=constraints,
                            indexes=indexes,
                        ),
                    }
                else:
                    assert isinstance(typedef, e.ScalarTp)
                    result_vals = {**result_vals, t_name: t_me}
            case e.ModuleEntityFuncDef(funcdefs=funcdefs):
                result_vals = {
                    **result_vals,
                    t_name: e.ModuleEntityFuncDef(
                        funcdefs=[g(root_ctx, funcdef) for funcdef in funcdefs]
                    ),
                }
            case _:
                raise ValueError("Unimplemented", t_me)
    dbschema.unchecked_modules[module_name] = e.DBModule(result_vals)
