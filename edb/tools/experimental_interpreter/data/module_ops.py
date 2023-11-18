
from . import data_ops as e
from typing import Optional

default_open_scopes = ["default", "std"]

def resolve_module_in_schema(schema: e.DBSchema, name: str) -> e.DBModule:
    if name in schema.unchecked_modules:
        assert name not in schema.modules
        return schema.unchecked_modules[name]
    else:
        return schema.modules[name]


def try_resolve_type_name(ctx: e.TcCtx, name: str) -> Optional[e.ObjectTp]:

    """
    Resolve the name in this order:
    1. Current module
    2. Open scopes in Schema (scopes are also module names)
    3. The default `std` module
    """
    current_module = resolve_module_in_schema(ctx.schema, ctx.current_module)
    if name in current_module.type_defs:
        return current_module.type_defs[name]
    
    # TODO: open scopes
    for default_scope in default_open_scopes:
        std_module = resolve_module_in_schema(ctx.schema, default_scope)
        if name in std_module.type_defs:
            return std_module.type_defs[name]
        
    return None

def resolve_type_name(ctx: e.TcCtx, name: str) -> e.ObjectTp:
    resolved = try_resolve_type_name(ctx, name)
    if resolved is None:
        raise ValueError(f"Type {name} not found")
    else:
        return resolved
    

def resolve_func_name(ctx: e.TcCtx, name: str) -> e.FuncDef:
    """
    Resolve the name in this order:
    1. Current module
    2. Open scopes in Schema (scopes are also module names)
    3. The default `std` module
    """
    current_module = resolve_module_in_schema(ctx.schema, ctx.current_module)
    if name in current_module.fun_defs:
        return current_module.fun_defs[name]
    
    # TODO: open scopes

    for default_scope in default_open_scopes:
        std_module = resolve_module_in_schema(ctx.schema, default_scope)
        if name in std_module.fun_defs:
            return std_module.fun_defs[name]
    raise ValueError(f"Function {name} not found")