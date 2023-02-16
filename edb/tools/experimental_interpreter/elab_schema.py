
from .data.data_ops import *
from .data.built_in_ops import all_builtin_funcs
from edb.edgeql import ast as qlast
from .helper_funcs import parse_sdl
from edb.common import debug

def elab_schema(sdef : qlast.Schema) -> DBSchema:
    raise ValueError("unimplemented", sdef)


def schema_from_sdl_defs(schema_defs : str) -> DBSchema:
    return elab_schema(parse_sdl(schema_defs))


def schema_from_sdl_file(init_sdl_file_path : str) -> DBSchema:
    with open(init_sdl_file_path) as f:
        return schema_from_sdl_defs(f.read())