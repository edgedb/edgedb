##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL compiler path scope helpers."""


from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import inference as irinference

from edgedb.lang.schema import objects as s_obj

from edgedb.lang.edgeql import errors

from . import context


def get_path_id(scls: s_obj.Class, *,
                ctx: context.CompilerContext) -> irast.PathId:
    return irast.PathId(scls, namespace=ctx.path_id_namespace)


def register_set_in_scope(
        ir_set: irast.Set, *,
        ctx: context.CompilerContext) -> None:
    if ctx.path_as_type:
        return
    try:
        ctx.path_scope.add_path(ir_set.path_id)
    except irast.InvalidScopeConfiguration as e:
        raise errors.EdgeQLError(e.args[0], context=ir_set.context) from e


def mark_path_as_optional(
        path_id: irast.PathId, *,
        ctx: context.CompilerContext) -> None:
    ctx.path_scope.mark_as_optional(path_id)


def set_path_alias(
        path_id: irast.PathId, alias: irast.PathId, *,
        ctx: context.CompilerContext) -> None:
    ctx.path_scope.set_alias(path_id, alias)


def enforce_singleton(expr: irast.Base, *, ctx: context.ContextLevel) -> None:
    scope_fence = ctx.path_scope.fence.parent
    if scope_fence is not None:
        singletons = scope_fence.get_all_visible()
    else:
        singletons = set()
    cardinality = irinference.infer_cardinality(expr, singletons, ctx.schema)
    if cardinality != irast.Cardinality.ONE:
        raise errors.EdgeQLError(
            'possibly more than one element returned by an expression '
            'where only singletons are allowed',
            context=expr.context)
