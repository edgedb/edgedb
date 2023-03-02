
from .data.data_ops import *
from .basis.built_ins import all_builtin_funcs
from edb.edgeql import ast as qlast
from .helper_funcs import parse_sdl
from edb.common import debug
from .elaboration import elab_single_type_expr


def elab_schema_error(obj: Any) -> Any:
    raise ValueError(obj)


def elab_schema_cardinality(is_required: Optional[bool]) -> CMMode:
    return CMMode(Fin(1) if is_required else Fin(0), Inf())


def elab_schema_target_tp(target: Optional[Union[qlast.Expr, qlast.TypeExpr]]) -> Tp:
    return (elab_single_type_expr(target) if isinstance(target, qlast.TypeExpr) else elab_schema_error(target))


def elab_schema(sdef: qlast.Schema) -> DBSchema:
    if (len(sdef.declarations) != 1
            or sdef.declarations[0].name.name != "default"):
        raise ValueError(
            "Expect single module declaration named default in schema")
    types_decls = cast(Sequence[qlast.ModuleDeclaration], sdef.declarations)[
        0].declarations

    type_defs: Dict[str, ObjectTp] = {}
    for t_decl in types_decls:
        match t_decl:
            case qlast.CreateObjectType(bases=bases, commands=commands,
                                        name=qlast.ObjectRef(name=name), abstract=is_abstract):
                object_tp_content: Dict[str, Tuple[Tp, CMMode]] = {}
                for cmd in commands:
                    match cmd:
                        case qlast.CreateConcretePointer(bases=pbases,
                                                         name=qlast.ObjectRef(name=pname), target=ptarget, is_required=p_is_required,
                                                         commands=pcommands):
                            if isinstance(ptarget, qlast.TypeExpr):
                                base_target_type = elab_schema_target_tp(
                                    ptarget)
                            else:
                                print("WARNING: not implemented ptarget", ptarget)
                            link_property_tps: Dict[str, ResultTp] = {}
                            for pcmd in pcommands:
                                match pcmd:
                                    case qlast.CreateConcretePointer(bases=plbases,
                                                                     name=qlast.ObjectRef(name=plname), target=pltarget, is_required=pl_is_required,
                                                                     commands=plcommands):
                                        if plcommands:
                                            print(
                                                "WARNING: not implemented plcmd", plcommands)
                                        if isinstance(pltarget, qlast.TypeExpr):
                                            link_property_tps = {**link_property_tps, plname: (
                                                elab_schema_target_tp(pltarget), elab_schema_cardinality(pl_is_required))}
                                        else:
                                            print(
                                                "WARNING: not implemented pltarget", pltarget)
                                    case _:
                                        print(
                                            "WARNING: not implemented pcmd", pcmd)
                            final_target_type = LinkPropTp(base_target_type, ObjectTp(
                                link_property_tps)) if link_property_tps else base_target_type
                            object_tp_content = {**object_tp_content, pname:
                                                 (final_target_type, elab_schema_cardinality(
                                                     is_required=p_is_required))
                                                 }
                        case _:
                            print("WARNING: not implemented cmd", cmd)
                            # debug.dump(cmd)

                type_defs = {**type_defs,
                             name: ObjectTp(val=object_tp_content)}
            case _:
                print("WARNING: not implemented t_decl", t_decl)

    return DBSchema(type_defs, all_builtin_funcs)


def schema_from_sdl_defs(schema_defs: str) -> DBSchema:
    return elab_schema(parse_sdl(schema_defs))


def schema_from_sdl_file(init_sdl_file_path: str) -> DBSchema:
    with open(init_sdl_file_path) as f:
        return schema_from_sdl_defs(f.read())
