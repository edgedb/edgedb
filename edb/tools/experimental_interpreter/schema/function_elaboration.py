from ..data import data_ops as e
from ..data import expr_ops as eops
from typing import List

from edb.edgeql import ast as qlast
from .. import elaboration as elab
from ..basis.server_funcs import get_default_func_impl_for_function
from edb.edgeql import qltypes as qltypes
from .. import elab_schema as elab_schema
from ..type_checking_tools import name_resolution as name_res
from typing import Optional


def fun_arg_type_polymorphism_post_processing(tp: e.Tp) -> e.Tp:
    """
    replace any type with Some(0)
    TODO: handling anyelem
    This is how the current polymorphism status quo.
    """

    def replace_any(tp: e.Tp) -> Optional[e.Tp]:
        match tp:
            case e.AnyTp(spec):
                if spec == "type":
                    return e.SomeTp(0)
                else:
                    return None
            case _:
                return None

    return eops.map_tp(replace_any, tp)


def elaboarate_ret_typemod(ret_typemod: qltypes.TypeModifier) -> e.CMMode:
    match ret_typemod:
        case qltypes.TypeModifier.OptionalType:
            return e.CardAtMostOne
        case qltypes.TypeModifier.SingletonType:
            return e.CardOne
        case qltypes.TypeModifier.SetOfType:
            return e.CardAny
        case _:
            raise ValueError("TODO", ret_typemod)


def elaborate_fun_def_arg_type(
    params: List[qlast.FuncParam],
    ret_tp: qlast.TypeExpr,
    ret_typemod: qltypes.TypeModifier,
) -> e.FunArgRetType:
    return_cad = elaboarate_ret_typemod(ret_typemod)
    return_tp = elab.elab_single_type_expr(ret_tp)
    return_tp = fun_arg_type_polymorphism_post_processing(return_tp)
    params_elab = []
    params_mod_elab = []
    params_label_elab = []
    for param in params:
        match param:
            case qlast.FuncParam(
                name=param_name, type=param_type, typemod=modifier
            ):
                params_label_elab.append(param_name)
                param_type_raw = elab.elab_single_type_expr(param_type)
                param_type_ck = fun_arg_type_polymorphism_post_processing(
                    param_type_raw
                )
                params_elab.append(param_type_ck)
                params_mod_elab.append(elab.elab_param_modifier(modifier))
            case _:
                raise ValueError("TODO", param)
    return e.FunArgRetType(
        args_tp=params_elab,
        args_mod=params_mod_elab,
        args_label=params_label_elab,
        ret_tp=e.ResultTp(return_tp, return_cad),
    )


def process_builtin_fun_def(
    schema: e.DBSchema,
    name: qlast.ObjectRef,
    params: List[qlast.FuncParam],
    ret_tp: qlast.TypeExpr,
    ret_typemod: qltypes.TypeModifier,
) -> None:
    match name:
        case qlast.ObjectRef(name=fun_name, module=module_name):
            assert (
                module_name is not None
            ), "Functions cannot be created in top level"
            func_type = elaborate_fun_def_arg_type(params, ret_tp, ret_typemod)
            func_type = name_res.fun_arg_ret_type_name_resolve(
                eops.emtpy_tcctx_from_dbschema(schema), func_type
            )
            defaults = {
                p.name: elab.elab(p.default) for p in params if p.default
            }
            this_def = e.BuiltinFuncDef(
                tp=func_type,
                impl=get_default_func_impl_for_function(
                    e.QualifiedName([module_name, fun_name])
                ),
                defaults=defaults,
            )
            if fun_name in schema.modules[(module_name,)].defs:
                current_def = schema.modules[(module_name,)].defs[fun_name]
                assert isinstance(current_def, e.ModuleEntityFuncDef)
                current_def.funcdefs.append(this_def)
            else:
                schema.modules[(module_name,)].defs[fun_name] = (
                    e.ModuleEntityFuncDef([this_def])
                )
        case _:
            raise ValueError("TODO", name)
