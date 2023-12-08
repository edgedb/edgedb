
from ..data import data_ops as e
from typing import List

from edb.edgeql import ast as qlast
from edb.common import debug
from .. import elaboration as elab
from ..basis.server_funcs import get_default_func_impl_for_function
from edb.edgeql import qltypes as qltypes
from .. import elab_schema as elab_schema
from ..type_checking_tools import typechecking as tck

from edb.common import debug

def process_fun_def(schema: e.DBSchema,
            name : qlast.ObjectRef, 
                    params: List[qlast.FuncParam],
                    ret_tp: qlast.TypeName, 
                    ret_typemod: qltypes.TypeModifier) -> None:
    match ret_typemod:
        case qltypes.TypeModifier.OptionalType:
            return_cad = e.CardAtMostOne
        case qltypes.TypeModifier.SingletonType:
            return_cad = e.CardOne
        case qltypes.TypeModifier.SetOfType:
            return_cad = e.CardAny
        case _:
            raise ValueError("TODO", ret_typemod)
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
                        params_ck.append(param_type_ck)
                        params_mod_ck.append(elab.elab_param_modifier(modifier))
                    case _:
                        raise ValueError("TODO", param)
            result_tp_raw = elab.elab_TypeName(ret_tp)
            result_tp_ck = tck.check_type_valid(schema, result_tp_raw)
            func_type = e.FunArgRetType(
                args_tp= params_ck,
                args_mod= params_mod_ck,
                ret_tp= e.ResultTp(result_tp_ck, return_cad))
            assert "::" not in module_name, "TODO"
            if name in schema.modules[(module_name, )].defs:
                current_def = schema.modules[(module_name, )].defs[fun_name]
                assert isinstance(current_def, e.ModuleEntityFuncDef)
                current_def.funcdef.tp.args_ret_types.append(func_type)
            else:
                schema.modules[(module_name, )].defs[fun_name] = \
                    e.ModuleEntityFuncDef(
                        e.FuncDef(
                            tp=e.FunType([func_type]),
                            impl=get_default_func_impl_for_function(e.QualifiedName([module_name, name])))
                        )
        case _:
            raise ValueError("TODO", name)


def process_ddl(
    schema: e.DBSchema,
    ddl: qlast.DDLOperation) -> None:
    """
    Process a single DDL operation.
    """
    # debug.dump_edgeql(ddl)
    match ddl:
        case qlast.CreateModule(
            name=qlast.ObjectRef(name=module_name), 
            commands=[]):
            assert "::" not in module_name, "TODO"
            schema.modules[(module_name, )] = e.DBModule({})
        case (
            qlast.CreatePseudoType()
            | qlast.CreateAnnotation()
            | qlast.AlterAnnotation()
        ):
            print("WARNING: not supported yet", ddl)
            print("WARNING: not supported yet", ddl)
        case qlast.CreateScalarType(
            name=qlast.ObjectRef(
                module=module_name, name=type_name), 
            commands=[], 
            bases=bases, 
            abstract=is_abstract):
            assert module_name is not None, "Scalar types cannot be created in top level"
            assert "::" not in module_name, "TODO"
            schema.modules[(module_name, )].defs[type_name] = e.ModuleEntityTypeDef(e.ScalarTp(e.QualifiedName([module_name, type_name])), is_abstract=is_abstract)
            # We require DDL to contain fully qualified names
            schema.subtyping_relations[e.QualifiedName([module_name, type_name])] = []
            for base_tp in bases:
                base_elabed = elab.elab_TypeName(base_tp)
                match base_elabed:
                    # for bare ddl, we assume qualified type name is actually checked
                    case e.UncheckedTypeName(name=e.QualifiedName(_)):
                        schema.subtyping_relations[e.QualifiedName([module_name, type_name])].append(base_elabed.name)
                    case e.AnyTp(spec):
                        # choice: make anytype live in std
                        schema.subtyping_relations[e.QualifiedName([module_name, type_name])].append(e.QualifiedName(["std", "any"+spec]))
                    case e.CompositeTp(kind=e.CompositeTpKind.Enum, tps=_):
                        print("WARNING: behavior of extending enum types undefined", base_elabed)
                    case _:
                        raise ValueError("Must inherit from single name", base_elabed)
        case qlast.CreateOperator(
            kind = _,
            params = params,
            name = name,
            returning = ret_tp, 
            returning_typemod = ret_typemod,
            ):
            process_fun_def(schema, name, params, ret_tp, ret_typemod)
        case qlast.CreateFunction(
            commands = commands,
            params = params,
            name = name,
            returning = ret_tp,
            returning_typemod = ret_typemod,
        ):
            process_fun_def(schema, name, params, ret_tp, ret_typemod)
            
        case qlast.CreateCast():
            print("WARNING: not supported yet", ddl)
        case qlast.CreateConstraint():
            print("WARNING: not supported yet", ddl)
        case qlast.CreateProperty():
            print("WARNING: not supported yet", ddl)
        case qlast.CreateLink():
            print("WARNING: not supported yet", ddl)
        
        case qlast.CreateObjectType(bases=_,
                    commands=commands,
                    name=qlast.ObjectRef(name=name, module=module_name),
                    abstract=abstract):
            assert module_name is not None, "Object types cannot be created in top level"
            assert "::" not in module_name, "TODO"
            obj_tp = elab_schema.elab_create_object_tp(commands)
            schema.modules[(module_name, )].defs[name] = e.ModuleEntityTypeDef(obj_tp, is_abstract=abstract)
        case _:
            debug.dump(ddl)
            raise ValueError("DDL not yet supported", ddl)

def process_ddls(
    schema: e.DBSchema, 
    ddls: List[qlast.DDLOperation]) -> None:
    for ddl in ddls:
        process_ddl(schema, ddl)