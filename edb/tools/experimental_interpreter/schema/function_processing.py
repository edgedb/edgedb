
from ..data import data_ops as e
from ..data import expr_ops as eops
from typing import List

from edb.edgeql import ast as qlast
from edb.common import debug
from .. import elaboration as elab
from ..basis.server_funcs import get_default_func_impl_for_function
from edb.edgeql import qltypes as qltypes
from .. import elab_schema as elab_schema
from ..type_checking_tools import typechecking as tck
from ..interpreter_logging import print_warning

from edb.common import debug


def fun_arg_type_polymorphism_post_processing(tp: e.Tp) -> e.Tp:
    """
    replace any type with Some(0)
    TODO: handling anyelem
    This is how the current polymorphism status quo.
    """

    def replace_any(tp: e.Tp) -> e.Tp:
        match tp:
            case e.AnyTp(spec):
                if spec == "type":
                    return e.SomeTp(0)
                else:
                    return None
            case _:
                return None
    return eops.map_tp(replace_any, tp)

def elaboarate_ret_typemod(ret_typemod: qltypes.TypeModifier) -> e.Cardinality:
    match ret_typemod:
        case qltypes.TypeModifier.OptionalType:
            return e.CardAtMostOne
        case qltypes.TypeModifier.SingletonType:
            return e.CardOne
        case qltypes.TypeModifier.SetOfType:
            return e.CardAny
        case _:
            raise ValueError("TODO", ret_typemod)


def process_ddl_fun_def(schema: e.DBSchema,
            name : qlast.ObjectRef, 
                    params: List[qlast.FuncParam],
                    ret_tp: qlast.TypeName, 
                    ret_typemod: qltypes.TypeModifier) -> None:
    return_cad = elaboarate_ret_typemod(ret_typemod)
    match name:
        case qlast.ObjectRef(name=fun_name, module=module_name):
            params_ck = []
            params_mod_ck = []
            for param in params:
                match param:
                    case qlast.FuncParam(
                        name = param_name,
                        type = param_type,
                        typemod = modifier):
                        param_type_raw = elab.elab_TypeName(param_type)
                        param_type_ck = tck.check_type_valid(schema, param_type_raw)
                        param_type_ck = fun_arg_type_polymorphism_post_processing(param_type_ck)
                        params_ck.append(param_type_ck)
                        params_mod_ck.append(elab.elab_param_modifier(modifier))
                    case _:
                        raise ValueError("TODO", param)
            result_tp_raw = elab.elab_TypeName(ret_tp)
            result_tp_ck = tck.check_type_valid(schema, result_tp_raw)
            result_tp_ck = fun_arg_type_polymorphism_post_processing(result_tp_ck)
            func_type = e.FunArgRetType(
                args_tp= params_ck,
                args_mod= params_mod_ck,
                ret_tp= e.ResultTp(result_tp_ck, return_cad))
            assert module_name is not None, "Functions cannot be created in top level"
            assert "::" not in module_name, "TODO"
            if fun_name in schema.modules[(module_name, )].defs:
                current_def = schema.modules[(module_name, )].defs[fun_name]
                assert isinstance(current_def, e.ModuleEntityFuncDef)
                current_def.funcdef.tp.args_ret_types.append(func_type)
            else:
                schema.modules[(module_name, )].defs[fun_name] = \
                    e.ModuleEntityFuncDef(
                        e.FuncDef(
                            tp=e.FunType([func_type]),
                            impl=get_default_func_impl_for_function(e.QualifiedName([module_name, fun_name])))
                        )
        case _:
            raise ValueError("TODO", name)
