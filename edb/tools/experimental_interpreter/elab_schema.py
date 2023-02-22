
from .data.data_ops import *
from .basis.built_ins import all_builtin_funcs
from edb.edgeql import ast as qlast
from .helper_funcs import parse_sdl
from edb.common import debug
from .elaboration import elab_single_type_expr


def elab_schema(sdef : qlast.Schema) -> DBSchema:
    if (len(sdef.declarations) != 1 
            or sdef.declarations[0].name.name != "default"):
        raise ValueError("Expect single module declaration named default in schema")
    types_decls = cast(List[qlast.ModuleDeclaration], sdef.declarations)[0].declarations

    type_defs : Dict[str, ResultTp] = {}
    for t_decl in types_decls:
        match t_decl:
            case qlast.CreateObjectType(bases=bases, commands=commands, 
                    name=qlast.ObjectRef(name=name), abstract=is_abstract):
                object_tp_content : Dict[str, Tp] = {}
                for cmd in commands:
                    match cmd:
                        case qlast.CreateConcreteUnknownPointer(bases=pbases, 
                                name=qlast.ObjectRef(name=pname), target=ptarget, is_required=p_is_required,
                                commands=pcommands):
                                for pcmd in pcommands:
                                    print("WARNING: not implemented pcmd", pcmd)
                                object_tp_content = {**object_tp_content, pname : 
                                    (elab_single_type_expr(ptarget), CMMode(Fin(1) if p_is_required else Fin(0), Inf()))
                                    }
                        case _:
                            print("WARNING: not implemented cmd", cmd)
                            # debug.dump(cmd)

                type_defs = {**type_defs, **object_tp_content}
            case _:
                print("WARNING: not implemented t_decl", t_decl)
        



    return DBSchema(type_defs, all_builtin_funcs)


def schema_from_sdl_defs(schema_defs : str) -> DBSchema:
    return elab_schema(parse_sdl(schema_defs))


def schema_from_sdl_file(init_sdl_file_path : str) -> DBSchema:
    with open(init_sdl_file_path) as f:
        return schema_from_sdl_defs(f.read())