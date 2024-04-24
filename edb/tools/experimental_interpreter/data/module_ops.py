from . import data_ops as e
from typing import Optional, List, Tuple

default_open_scopes = [("std",)]


def resolve_module_in_schema(
    schema: e.DBSchema, name: Tuple[str, ...]
) -> e.DBModule:
    if name in schema.unchecked_modules:
        assert name not in schema.modules
        return schema.unchecked_modules[name]
    elif name in schema.modules:
        return schema.modules[name]
    else:
        raise ValueError(f"Module {name} not found")


def try_resolve_module_entity(
    ctx: e.TcCtx | e.DBSchema, name: e.QualifiedName
) -> Optional[e.ModuleEntity]:
    """
    Resolve a module entity using the ABS method.
    https://github.com/edgedb/edgedb/discussions/4883
    """
    assert len(name.names) >= 2
    if name.names[0] == "module":
        assert isinstance(ctx, e.TcCtx), (
            "qualified names beginning with"
            " module cannot be resolved in a schema"
        )
        name = e.QualifiedName([*ctx.current_module, *name.names[1:]])
    module: e.DBModule
    if isinstance(ctx, e.TcCtx):
        module = resolve_module_in_schema(ctx.schema, tuple(name.names[:-1]))
    else:
        module = resolve_module_in_schema(ctx, tuple(name.names[:-1]))
    if name.names[-1] in module.defs:
        return module.defs[name.names[-1]]
    else:
        return None


def try_resolve_type_name(
    ctx: e.TcCtx | e.DBSchema, name: e.QualifiedName
) -> Optional[e.ObjectTp | e.ScalarTp]:
    me = try_resolve_module_entity(ctx, name)
    if me is not None:
        if isinstance(me, e.ModuleEntityTypeDef):
            return me.typedef
        else:
            raise ValueError(f"{name} is not a type")
    else:
        return None


def resolve_type_def(
    ctx: e.TcCtx | e.DBSchema, name: e.QualifiedName
) -> e.ModuleEntityTypeDef:
    me = try_resolve_module_entity(ctx, name)
    if me is not None:
        if isinstance(me, e.ModuleEntityTypeDef):
            return me
        else:
            raise ValueError(f"{name} is not a type")
    else:
        raise ValueError(f"Type {name} not found")


def resolve_type_name(
    ctx: e.TcCtx | e.DBSchema, name: e.QualifiedName
) -> e.ObjectTp | e.ScalarTp:
    resolved = try_resolve_type_name(ctx, name)
    if resolved is None:
        raise ValueError(f"Type {name} not found")
    else:
        return resolved


def resolve_func_name(
    ctx: e.TcCtx | e.DBSchema, name: e.QualifiedName
) -> List[e.FuncDef]:
    me = try_resolve_module_entity(ctx, name)
    if me is not None:
        if isinstance(me, e.ModuleEntityFuncDef):
            return me.funcdefs
        else:
            raise ValueError(f"{name} is not a function")
    else:
        raise ValueError(f"Function {name} not found")


def try_resolve_simple_name(
    ctx: e.TcCtx | e.DBSchema, unq_name: e.UnqualifiedName
) -> Optional[e.QualifiedName]:
    """
    Resolve the name (may refer to a type or a function) in this order:
    1. Current module
    2. The default `std` module
    """
    name = unq_name.name

    if isinstance(ctx, e.TcCtx):
        current_module = resolve_module_in_schema(
            ctx.schema, ctx.current_module
        )
        if name in current_module.defs:
            return e.QualifiedName([*ctx.current_module, name])

    if isinstance(ctx, e.TcCtx):
        schema = ctx.schema
    else:
        schema = ctx

    for default_scope in default_open_scopes:
        std_module = resolve_module_in_schema(schema, default_scope)
        if name in std_module.defs:
            return e.QualifiedName([*default_scope, name])
    return None


def resolve_simple_name(
    ctx: e.TcCtx | e.DBSchema, unq_name: e.UnqualifiedName
) -> e.QualifiedName:
    name = try_resolve_simple_name(ctx, unq_name)
    if name is not None:
        return name
    else:
        raise ValueError(f"Name {name} not found")


def resolve_raw_name_and_type_def(
    ctx: e.TcCtx | e.DBSchema, name: e.QualifiedName | e.UnqualifiedName
) -> Tuple[e.QualifiedName, e.ObjectTp | e.ScalarTp]:
    if isinstance(name, e.UnqualifiedName):
        name = resolve_simple_name(ctx, name)
    return (name, resolve_type_name(ctx, name))


def resolve_raw_name_and_func_def(
    ctx: e.TcCtx | e.DBSchema, name: e.QualifiedName | e.UnqualifiedName
) -> Tuple[e.QualifiedName, List[e.FuncDef]]:
    if isinstance(name, e.UnqualifiedName):
        name = resolve_simple_name(ctx, name)
    return (name, resolve_func_name(ctx, name))


def enumerate_all_object_type_defs(
    ctx: e.TcCtx,
) -> List[Tuple[e.QualifiedName, e.ObjectTp]]:
    """
    Enumerate all type definitions in the current module
    and the default `std` module.
    """
    result: List[Tuple[e.QualifiedName, e.ObjectTp]] = []
    for module_name, module_def in [
        *ctx.schema.modules.items(),
        *ctx.schema.unchecked_modules.items(),
    ]:
        for tp_name, me in module_def.defs.items():
            if isinstance(me, e.ModuleEntityTypeDef) and isinstance(
                me.typedef, e.ObjectTp
            ):
                result.append(
                    (e.QualifiedName([*module_name, tp_name]), me.typedef)
                )

    return result


def tp_name_is_abstract(name: e.QualifiedName, schema: e.DBSchema) -> bool:
    me = try_resolve_module_entity(schema, name)
    assert isinstance(me, e.ModuleEntityTypeDef)
    return me.is_abstract
