##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL compiler schema helpers."""


import typing

from edgedb.lang.common import parsing

from edgedb.lang.schema import error as s_err
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as s_obj

from edgedb.lang.edgeql import ast as qlast

from . import context


def get_schema_object(
        name: typing.Union[str, qlast.ClassRef],
        module: typing.Optional[str]=None, *,
        ctx: context.ContextLevel,
        srcctx: typing.Optional[parsing.ParserContext] = None) -> s_obj.Class:

    if isinstance(name, qlast.ClassRef):
        if srcctx is None:
            srcctx = name.context
        module = name.module
        name = name.name

    if module:
        name = sn.Name(name=name, module=module)

    try:
        return ctx.schema.get(name=name, module_aliases=ctx.namespaces)
    except s_err.SchemaError as e:
        e.context = srcctx
        raise


def resolve_schema_name(
        name: str, module: str, *,
        ctx: context.ContextLevel) -> sn.Name:
    schema_module = ctx.namespaces.get(module)
    if schema_module is None:
        return None
    else:
        return sn.Name(name=name, module=schema_module)
