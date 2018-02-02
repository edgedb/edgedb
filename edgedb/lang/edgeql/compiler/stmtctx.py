##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL compiler statement-level context management."""


import typing

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import name as s_name
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import schema as s_schema
from edgedb.lang.schema import types as s_types

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import parser as qlparser

from . import astutils
from . import context
from . import dispatch
from . import pathctx
from . import schemactx
from . import setgen


def init_context(
        *,
        schema: s_schema.Schema,
        arg_types: typing.Optional[typing.Iterable[s_obj.Class]]=None,
        modaliases: typing.Optional[typing.Iterable[str]]=None,
        anchors: typing.Optional[typing.Dict[str, s_obj.Class]]=None,
        security_context: typing.Optional[str]=None,
        derived_target_module: typing.Optional[str]=None,
        result_view_name: typing.Optional[str]=None) -> \
        context.ContextLevel:
    stack = context.CompilerContext()
    ctx = stack.current
    ctx.schema = schema.get_overlay(extra=ctx.view_nodes)

    if modaliases:
        ctx.namespaces.update(modaliases)

    if arg_types:
        ctx.arguments.update(arg_types)

    if anchors:
        populate_anchors(anchors, ctx=ctx)

    ctx.derived_target_module = derived_target_module
    ctx.toplevel_result_view_name = result_view_name

    return ctx


def fini_expression(
        ir: irast.Base, *,
        ctx: context.ContextLevel) -> irast.Statement:
    result = irast.Statement(
        expr=ir,
        params=ctx.arguments,
        views=ctx.view_nodes,
        source_map=ctx.source_map,
    )
    irutils.infer_type(result, schema=ctx.schema)
    return result


def populate_anchors(
        anchors: typing.Dict[str, s_obj.Class], *,
        ctx: context.ContextLevel) -> None:

    for anchor, scls in anchors.items():
        if isinstance(scls, s_types.Type):
            step = setgen.class_set(scls, ctx=ctx)
            step.anchor = anchor
            step.show_as_anchor = anchor

        elif isinstance(scls, s_links.Link):
            if scls.source:
                path = setgen.extend_path(
                    setgen.class_set(scls.source, ctx=ctx), scls,
                    s_pointers.PointerDirection.Outbound,
                    scls.target, ctx=ctx)
            else:
                Object = ctx.schema.get('std::Object')

                ptrcls = scls.get_derived(
                    ctx.schema, Object, Object,
                    mark_derived=True, add_to_schema=False)

                path = setgen.extend_path(
                    setgen.class_set(Object, ctx=ctx), ptrcls,
                    s_pointers.PointerDirection.Outbound,
                    ptrcls.target, ctx=ctx)

            step = path
            step.anchor = anchor
            step.show_as_anchor = anchor

        elif isinstance(scls, s_lprops.LinkProperty):
            if scls.source.source:
                path = setgen.extend_path(
                    setgen.class_set(scls.source.source, ctx=ctx),
                    scls.source,
                    s_pointers.PointerDirection.Outbound,
                    scls.source.target, ctx=ctx)
            else:
                Object = ctx.schema.get('std::Object')
                ptrcls = scls.source.get_derived(
                    ctx.schema, Object, Object,
                    mark_derived=True, add_to_schema=False)
                path = setgen.extend_path(
                    setgen.class_set(Object, ctx=ctx), ptrcls,
                    s_pointers.PointerDirection.Outbound,
                    ptrcls.target, ctx=ctx)

            step = setgen.extend_path(
                path, scls,
                s_pointers.PointerDirection.Outbound,
                scls.target, ctx=ctx)

            step.anchor = anchor
            step.show_as_anchor = anchor

        else:
            step = scls

        ctx.anchors[anchor] = step


def declare_inline_view(
        ir_set: irast.Set, alias: str, *,
        ctx: context.ContextLevel) -> irast.Set:

    if not ir_set.scls.is_view():
        hint = alias or 'view'
        c = schemactx.derive_view(
            ir_set.scls, derived_name_quals=[ctx.aliases.get(hint)], ctx=ctx)
        path_scope = ir_set.path_scope
        ir_set.path_scope = None
        ir_set = setgen.generated_set(irast.SelectStmt(result=ir_set),
                                      path_id=pathctx.get_path_id(c, ctx=ctx),
                                      ctx=ctx)
        ir_set.scls = c
        ir_set.path_scope = path_scope
    else:
        c = ir_set.scls

    ctx.aliased_views[alias] = c
    ctx.view_sets[c] = ir_set

    return ir_set


def declare_view(
        expr: qlast.Base, alias: str, *,
        temp_scope: bool=True,
        ctx: context.ContextLevel) -> irast.Set:
    expr = astutils.ensure_qlstmt(expr)

    if temp_scope:
        scopemgr = ctx.newscope(temporary=True, fenced=True)
    else:
        scopemgr = ctx.new()

    with scopemgr as subctx:
        if not temp_scope and expr.implicit:
            subctx.pending_path_scope = ctx.path_scope

        if isinstance(alias, s_name.SchemaName):
            basename = alias
        else:
            basename = s_name.SchemaName(module='__view__', name=alias)

        subctx.stmt = ctx.stmt.parent_stmt
        view_name = s_name.SchemaName(
            module='_',
            name=s_obj.NamedClass.get_specialized_name(
                basename,
                ctx.aliases.get('w')
            )
        )
        subctx.toplevel_result_view_name = view_name

        substmt = setgen.ensure_set(
            dispatch.compile(expr, ctx=subctx), ctx=subctx)

        if temp_scope:
            substmt.path_scope = subctx.path_scope

        ctx.aliased_views[alias] = substmt.scls

    return substmt


def declare_view_from_schema(
        viewcls: s_obj.Class, *,
        ctx: context.ContextLevel) -> irast.Set:
    vc = ctx.view_class_map.get(viewcls)
    if vc is not None:
        return vc

    subctx = init_context(schema=ctx.schema)
    subctx.stmt = ctx.stmt
    subctx.path_id_namespace = ctx.aliases.get('ns')
    subctx.path_scope = ctx.path_scope
    subctx.toplevel_stmt = ctx.toplevel_stmt

    view_expr = qlparser.parse(viewcls.expr)
    declare_view(view_expr, alias=viewcls.name, ctx=subctx)

    vc = subctx.aliased_views[viewcls.name]
    ctx.view_class_map[viewcls] = vc
    ctx.source_map.update(subctx.source_map)
    ctx.aliased_views[viewcls.name] = subctx.aliased_views[viewcls.name]
    ctx.view_nodes[vc.name] = vc
    ctx.view_sets[vc] = subctx.view_sets[vc]

    return vc
