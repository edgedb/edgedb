
from typing import Any, Dict, Optional, Sequence, Tuple, Union, cast, List

from edb.edgeql import ast as qlast

# from .basis.built_ins import all_builtin_funcs
from .data.data_ops import (CMMode, DBSchema,  ObjectTp,
                            ResultTp, Tp)
from .elaboration import elab_single_type_expr, elab_expr_with_default_head, elab
from .helper_funcs import parse_sdl
from .data import data_ops as e
from .data import expr_ops as eops
from .type_checking_tools import schema_checking  as tck
from edb.common import debug
from .interpreter_logging import print_warning
from .schema import function_elaboration as fun_elab
from .type_checking_tools import name_resolution as name_res

def elab_schema_error(obj: Any) -> Any:
    raise ValueError(obj)


def elab_schema_cardinality(
        is_required: Optional[bool],
        cardinality: Optional[qlast.qltypes.SchemaCardinality]) -> CMMode:
    return CMMode(e.CardNumOne if is_required else e.CardNumZero,
                  e.CardNumInf
                  if cardinality == qlast.qltypes.SchemaCardinality.Many
                  else e.CardNumOne)


def elab_schema_target_tp(
        target: Optional[Union[qlast.Expr, qlast.TypeExpr]]) -> Tp:
    return (elab_single_type_expr(target)
            if isinstance(target, qlast.TypeExpr)
            else elab_schema_error(target))

def construct_final_schema_target_tp(base : Tp, linkprops: Dict[str, ResultTp]) -> Tp:
    if linkprops:
        match base:
            case e.UnionTp(tp1, tp2):
                return e.UnionTp(construct_final_schema_target_tp(tp1, linkprops), 
                                construct_final_schema_target_tp(tp2, linkprops))
            case e.UncheckedTypeName(e.QualifiedName(name)):
                return e.NamedNominalLinkTp(name=e.QualifiedName(name), linkprop=ObjectTp(linkprops))
            case e.UncheckedTypeName(e.UnqualifiedName(name)):
                return e.NamedNominalLinkTp(name=e.UnqualifiedName(name), linkprop=ObjectTp(linkprops))
            case e.OverloadedTargetTp(linkprop=None):
                return e.OverloadedTargetTp(linkprop=ObjectTp(linkprops))
            case _:
                if linkprops:
                    raise ValueError("cannot construct schema target type", base, linkprops)
                else:
                    return base
    else:
        return base
            
def elab_create_object_tp(commands: List[qlast.DDLOperation]) -> ObjectTp:
    object_tp_content: Dict[str, ResultTp] = {}
    for cmd in commands:
        match cmd:
            case qlast.CreateConcretePointer(
                    bases=_,
                    name=qlast.ObjectRef(name=pname),
                    target=ptarget,
                    is_required=p_is_required,
                    cardinality=p_cardinality,
                    declared_overloaded=declared_overloaded,
                    commands=pcommands):
                if ptarget is None:
                    if declared_overloaded:
                        base_target_type = e.OverloadedTargetTp(linkprop=None)
                    else:
                        raise ValueError("expecting target")
                if isinstance(ptarget, qlast.TypeExpr):
                    base_target_type = elab_schema_target_tp(
                        ptarget)
                elif isinstance(ptarget, qlast.Expr):
                    base_target_type = e.UncheckedComputableTp(
                        elab_expr_with_default_head(ptarget)
                    )
                else:
                    print_warning(
                        "WARNING: not implemented ptarget",
                        ptarget)
                link_property_tps: Dict[str, ResultTp] = {}
                p_has_set_default: Optional[e.BindingExpr] = None
                for pcmd in pcommands:
                    match pcmd:
                        case qlast.CreateConcretePointer(
                                bases=_,
                                name=qlast.ObjectRef(name=plname),
                                target=pltarget,
                                is_required=pl_is_required,
                                cardinality=pl_cardinality,
                                commands=plcommands):
                            pl_has_set_default: Optional[
                                e.BindingExpr] = None
                            if plcommands:
                                for plcommand in plcommands:
                                    match plcommand:
                                        case qlast.SetField(
                                                name=set_field_name,  # noqa: E501
                                                value=set_field_value):  # noqa: E501
                                            match set_field_name:
                                                case "default":
                                                    assert isinstance(set_field_value, qlast.Expr)  # noqa: E501
                                                    pl_has_set_default = (  # noqa: E501
                                                        elab_expr_with_default_head(  # noqa: E501
                                                                set_field_value))  # noqa: E501
                                                case _:
                                                    print(
                                                        "WARNING: "
                                                        "not "
                                                        "implemented "  # noqa: E501
                                                        "set_field_name",  # noqa: E501
                                                        set_field_name)  # noqa: E501
                                        case _:
                                            print(
                                                "WARNING: not "
                                                "implemented plcmd",  # noqa: E501
                                                plcommand)
                            if isinstance(
                                    pltarget, qlast.TypeExpr):
                                lp_base_tp = elab_schema_target_tp(
                                        pltarget)
                            elif isinstance(pltarget, qlast.Expr):
                                lp_base_tp = (
                                    e.UncheckedComputableTp(
                                    elab_expr_with_default_head(
                                        pltarget))
                                )
                            else:
                                print(
                                    "WARNING: "
                                    "not implemented pltarget",
                                    pltarget)
                            if pl_has_set_default is not None:
                                assert not isinstance(
                                    lp_base_tp,
                                    e.UncheckedComputableTp)
                                assert not isinstance(
                                    lp_base_tp,
                                    e.ComputableTp)
                                lp_base_tp = e.DefaultTp(
                                    pl_has_set_default,
                                    lp_base_tp)
                            link_property_tps = {
                                **link_property_tps,
                                plname:
                                ResultTp(
                                    lp_base_tp,
                                    elab_schema_cardinality(
                                            pl_is_required,
                                            pl_cardinality
                                            ))}
                        case qlast.CreateConcreteConstraint():
                            print_warning("WARNING: not implemented pcmd"
                                    " (constraint)", pcmd)
                        case qlast.SetField(
                                name=set_field_name,
                                value=set_field_value):
                            match set_field_name:
                                case "default":
                                    assert isinstance(set_field_value, qlast.Expr)  # noqa: E501
                                    p_has_set_default = (
                                        elab_expr_with_default_head(
                                            set_field_value))
                                case _:
                                    print(
                                        "WARNING: "
                                        "not implemented "
                                        "set_field_name",
                                        set_field_name)
                        case _:
                            print(
                                "WARNING: not "
                                "implemented pcmd",
                                pcmd)
                final_target_type = construct_final_schema_target_tp(base_target_type, link_property_tps)
                # (
                #     LinkPropTp(base_target_type,
                #                ObjectTp(link_property_tps))
                #     if link_property_tps
                #     else base_target_type)
                if p_has_set_default is not None:
                    assert not isinstance(final_target_type,
                                            e.UncheckedComputableTp)
                    assert not isinstance(final_target_type,
                                            e.ComputableTp)
                    final_target_type = (
                        e.DefaultTp(expr=p_has_set_default,
                                    tp=final_target_type))
                object_tp_content = {
                    **object_tp_content,
                    pname:
                    ResultTp(final_target_type,
                                elab_schema_cardinality(
                                is_required=p_is_required,
                                cardinality=p_cardinality))}
            case _:
                print_warning("WARNING: not implemented cmd", cmd)
                # debug.dump(cmd)
    return ObjectTp(val=object_tp_content)


def add_bases_for_name(schema: e.DBSchema, 
                       current_module_name: Tuple[str, ...] ,
                       current_type_name: str, bases: List[qlast.TypeName]) -> None:
    base_tps = [elab_single_type_expr(base) for base in bases]
    base_tps_ck : List[Tuple[Tuple[str, ...], e.RawName]] = []
    this_type_name = e.QualifiedName([*current_module_name, current_type_name])
    for base_tp in base_tps:
        match base_tp:
            case e.UncheckedTypeName(raw_name):
                base_tps_ck.append((current_module_name,raw_name))
            case _:
                raise ValueError("TODO", base_tp)
    assert this_type_name not in schema.unchecked_subtyping_relations
    schema.unchecked_subtyping_relations[this_type_name] = base_tps_ck

def elab_schema(existing: e.DBSchema, sdef: qlast.Schema) -> Tuple[str, ...]:
    if (len(sdef.declarations) != 1
            or sdef.declarations[0].name.name != "default"):
        raise ValueError(
            "Expect single module declaration named default in schema")
    types_decls = cast(
        Sequence[qlast.ModuleDeclaration],
        sdef.declarations)[0].declarations

    current_module_name = ("default", )
    

    type_defs: Dict[str, e.ModuleEntityTypeDef | e.ModuleEntityFuncDef] = {}
    existing.unchecked_modules[current_module_name] = e.DBModule(type_defs)
    for t_decl in types_decls:
        match t_decl:
            case qlast.CreateObjectType(bases=bases,
                                        commands=commands,
                                        name=qlast.ObjectRef(name=name),
                                        abstract=abstract):
                obj_tp = elab_create_object_tp(commands)
                add_bases_for_name(existing, ("default",), name, bases)
                assert name not in type_defs
                type_defs[name] = e.ModuleEntityTypeDef(obj_tp, is_abstract=abstract)
            case qlast.CreateScalarType(
                name=qlast.ObjectRef(name=name, module=None),
                bases=bases,
                abstract=abstract,
            ):
                this_name = e.QualifiedName(["default", name])
                add_bases_for_name(existing, current_module_name, name, bases)
                assert name not in type_defs
                type_defs[name] = e.ModuleEntityTypeDef(e.ScalarTp(this_name), is_abstract=abstract)
            case qlast.CreateConstraint():
                print_warning("WARNING: not supported yet", t_decl)
            case qlast.CreateFunction(
                name=qlast.ObjectRef(name=name, module=None),
                params=params,
                returning=returning,
                returning_typemod=returning_typemod,
                nativecode=nativecode,
            ):
                this_name = e.QualifiedName(["default", name])
                assert isinstance(returning, qlast.TypeName), "TODO"
                func_type = fun_elab.elaborate_fun_def_arg_type(params, returning, returning_typemod)
                assert nativecode is not None, "TODO"
                func_body = elab(nativecode)
                for label in reversed(func_type.args_label):
                    func_body = eops.abstract_over_expr(func_body, label)
                this_def = e.DefinedFuncDef(
                                tp=func_type,
                                impl= func_body)
                if name in type_defs:
                    current_def = type_defs[name]
                    assert isinstance(current_def, e.ModuleEntityFuncDef)
                    current_def.funcdefs.append(this_def)
                else:
                    type_defs[name] = e.ModuleEntityFuncDef([this_def])
            case _:
                raise ValueError("TODO", t_decl)

    # module_defs : Dict[str, e.ModuleEntity]= {k: v for k, v in type_defs.items() if k in type_defs}
    # return (("default",), e.DBModule(module_defs))
    return ("default",)


def add_module_from_sdl_defs(
        schema: e.DBSchema,
        module_defs: str,
                         ) -> e.DBSchema:
    name = elab_schema(schema, parse_sdl(module_defs))
    checked_schema = tck.check_module_validity(schema, name)
    return checked_schema



def add_module_from_sdl_file(
        schema: e.DBSchema,
        init_sdl_file_path: str,
                         ) -> e.DBSchema:
    with open(init_sdl_file_path) as f:
        return add_module_from_sdl_defs(schema, f.read())
